"""
Main class for building streaming pipelines
"""
from __future__ import annotations

import json
from collections import defaultdict, deque
from inspect import signature, Parameter
from logging import getLogger
from multiprocessing import Pool
from operator import eq
from os import PathLike
from pickle import PicklingError
from random import shuffle, seed

from more_itertools.more import windowed

from pypey import px

try:  # _tuplegetter is only available from 3.8
    from collections import _tuplegetter
except ImportError:
    _tuplegetter = property

from pathos.multiprocessing import ProcessPool  # type: ignore
from collections.abc import Sized
from functools import reduce
from heapq import nlargest
from itertools import chain, tee, accumulate, filterfalse, islice, dropwhile, takewhile, cycle, zip_longest, product
from more_itertools import tail, random_permutation, ichunked, unique_everseen, partition, unzip, \
    always_reversible, interleave, interleave_longest, split_into
from sys import stdout
from typing import Iterator, Iterable, Tuple, Generic, Union, Any, Optional, List, AnyStr, IO, Sequence, NamedTuple, \
    Deque, Dict

from pypey.func import Fn, ident, H, I, T, X, Y, require, require_val

__all__ = ['Pype', 'SPLIT_MODES', 'Total', 'TOTAL']

logger = getLogger(__name__)

flatten = chain.from_iterable

SPLIT_MODES = frozenset({'at', 'after', 'before'})


class Total(str):
    def __str__(self):
        return '_TOTAL_'

    def __repr__(self):
        return str(self)


#: Constant indicating the aggregated counts in :func:`Pype.freqs`
TOTAL = Total()

UNARY_WITHOUT_SIGNATURE = {__import__,
                           bool, bytearray, bytes,
                           classmethod,
                           dict, dir,
                           frozenset,
                           getattr,
                           int, iter,
                           map, max, min,
                           next,
                           print,
                           set, staticmethod, str, super,
                           type,
                           vars}

N_ARY_WITHOUT_SIGNATURE = {filter, slice, range, zip, breakpoint}

_sent = object()


class Pype(Generic[T]):
    __slots__ = '_it',

    def __getstate__(self: Pype[T]) -> Iterable[T]:
        """
        Returns this pipe's backing ``Iterable`` as its state.

        :return: this pipe's backing ``Iterable``
        """

        return self._it

    def __init__(self: Pype[T], it: Iterable[T]):
        """
        Creates pipe from ``Iterable``. No argument validation is carried out.

        :param it: an ``Iterable``
        """
        self._it = it

    def __iter__(self: Pype[T]) -> Iterator[T]:
        """
        Returns iterator either by a call on this pipe or by calling built-in ``iter`` on it.

        :return: an ``Iterator`` for this pipe's data
        """
        return iter(self._data())

    def __setstate__(self: Pype[T], state: Iterable[T]):
        """
        Set this Pype's state to be the given iterable. This method is a counterpart :func:`Pype.__get_state__`.

        :param state: an iterable to be the state of this Pype
        :return: nothing
        """
        self._it = state

    def accum(self: Pype[T], fn: Fn[[X, T], X], init: Optional[X] = None) -> Pype[X]:
        """
        Returns a pipe where each item is the result of combining a running total with the corresponding item in the
        original pipe:
        ::

            >>> from pypey import pype
            >>> list(pype([1, 2, 3]).accum(lambda total, n: total+n))
            [1, 3, 6]

        When an initial value is given, the resulting pipe will have one more item than the original one:
        ::

            >>> from pypey import pype
            >>> list(pype([1, 2, 3]).accum(lambda total, n: total+n, init=0))
            [0, 1, 3, 6]

        Similar to ``itertools.accumulate``.

        :param init: optional initial value to start the accumulation with
        :param fn: function where the first argument is the running total and the second the current item
        :return: a pipe with accumulated items
        :raises: ``TypeError`` if ``fn`` is not a ``Callable``
        """
        require(fn is not None, 'fn cannot be None')

        # Try block necessary to support <3.8 as init argument has not been implemented in those versions
        try:

            return Pype(accumulate(self._data(), fn, initial=init))

        except TypeError:

            return Pype(_accumulate(self._data(), fn, init))

    def broadcast(self: Pype[T], fn: Fn[[T], Iterable[X]]) -> Pype[Tuple[T, X]]:
        """
        Returns the flattened Cartesian product of this pipe's items and the items returned by ``fn``.
        Conceptually similar to ``numpy``-'s broadcasting.
        ::

            >>> from pypey import pype
            >>> list(pype(['a', 'fun', 'day']).broadcast(tuple).map(lambda word, char: f'{word} -> {char}'))
            ['a -> a', 'fun -> f', 'fun -> u', 'fun -> n', 'day -> d', 'day -> a', 'day -> y']

        :param fn: function to create ``Iterable`` from each of this pipe's items
        :return: a pipe where each item is a pair with the first element being the nth instance of this pipe's items
            and the second an element of ``fn``-s returned ``Iterable``
        :raises: ``TypeError`` if ``fn`` is not a ``Callable``
        """
        ufn, data = self._ufn(fn), self._data()

        return Pype(flatten(product([item], ufn(item)) for item in data))

    def cat(self: Pype[T], other: Iterable[X]) -> Pype[Union[T, X]]:
        """
        Concatenates this pipe with the given ``Iterable``.

        >>> list(pype([1, 2, 3]).cat([4, 5, 6]))
        [1, 2, 3, 4, 5, 6]

        :param other: ``Iterable`` to append to this one
        :return: a concatenated pipe
        :raises: ``TypeError`` if ``other`` is not an ``Iterable``
        """
        require(isinstance(other, Iterable), 'other needs to be an Iterable')

        return Pype(chain(self._data(), other))

    def chunk(self: Pype[T], size: Union[int, Iterable[Optional[int]]]) -> Pype[Pype[T]]:
        """
        Breaks pipe into sub-pipes with up to ``size`` items each:
        ::

            >>> from pypey import pype
            >>> [list(chunk) for chunk in pype([1, 2, 3, 4]).chunk(2)]
            [[1, 2], [3, 4]]

        If this pipe's size is not a multiple of ``size``, the last chunk will have fewer items than ``size``:
        ::

            >>> from pypey import pype
            >>> [list(chunk) for chunk in pype([1, 2, 3]).chunk(2)]
            [[1, 2], [3]]

        If ``size`` is larger than this pipe's size, only one chunk will be returned:
        ::

            >>> from pypey import pype
            >>> [list(chunk) for chunk in pype([1, 2, 3, 4]).chunk(5)]
            [[1, 2, 3, 4]]

        If ``size`` is an iterable of ints, chunks will have corresponding sizes:
        ::

            >>> [list(chunk) for chunk in pype([1, 2, 3, 4]).chunk([1, 3])]
            [[1], [2, 3, 4]]

        If the sum of sizes is smaller than this pipe's length, the remaining items will not be returned:
        ::

            >>> from pypey import pype
            >>> list(chunk) for chunk in pype([1, 2, 3, 4]).chunk([1, 2])]
            [[1], [2, 3]]

        If the sum of sizes is larger than this pipe's length, fewer items will be returned in the chunk that overruns
        the pipe and further chunks will be empty:
        ::

            >>> from pypey import pype
            >>> [list(chunk) for chunk in pype([1, 2, 3, 4]).chunk([1, 2, 3, 4])]
            [[1], [2, 3], [4], []]

        The last size can be ``None``, in wich case, the last chunk's will be remaining items after the one-but-last
        size.
        ::

            >>> from pypey import pype
            >>> [list(chunk) for chunk in pype([1, 2, 3, 4]).chunk([1, None])]
            [[1], [2, 3, 4]]

        This method tees the backing ``Iterable``.

        Similar to ``more_itertools.ichunked`` and ``more_itertools.split_into``.

        :param size: chunk size or sizes
        :return: a pipe of pipes with up to `size` items each or with sizes specified by iterable of sizes
        :raises: ``TypeError`` if ``size`` is not an ``int`` or an ``Iterable`` of ``int``-s
        :raises: ``ValueError`` if ``size`` is not positive or if any of the iterable of sizes is not positive
        """
        sizes = [s for s in size if isinstance(s, int)] if isinstance(size, Iterable) else []
        require(isinstance(size, int) or bool(sizes), f'n must be an int or an iterable of ints but was [{type(size)}]')
        require_val(size > 0 if isinstance(size, int) else all(s > 0 for s in sizes), f'n must be > 0 but was [{size}]')

        fn = ichunked if isinstance(size, int) else split_into

        return Pype(map(Pype, fn(self._data(), size)))

    def clone(self: Pype[T]) -> Pype[T]:
        """
        Lazily clones this pipe. This method tees the backing ``Iterable`` and replaces it with a new copy.

        >>> from pypey import pype
        >>> list(pype([1, 2, 3]).clone())
        [1, 2, 3]

        Similar to ``itertools.tee``.

        :return: a copy of this pipe
        """

        return Pype(self._data(teed=True))

    def cycle(self: Pype[T], n: Optional[int] = None) -> Pype[T]:
        """
        Returns items in pipe ``n`` times if ``n`` is not ``None``:
        ::

            >>> from pypey import pype
            >>> list(pype([1, 2, 3]).cycle(2))
            [1, 2, 3, 1, 2, 3]

        else it returns infinite copies:
        ::

            >>> from pypey import pype
            >>> list(pype([1, 2, 3]).cycle().take(6))
            [1, 2, 3, 1, 2, 3]

        Similar to ``itertools.cycle`` with ``n`` = ``None`` and to ``more_itertools.ncycles`` with integer ``n``.

        :param n: number of concatenated copies or ``None`` for infinite copies
        :return: a pipe that cycles  either ``n`` or infinite times over the items of this one
        :raises: ``TypeError`` if ``n`` is neither an ``int`` nor ``None``
        :raises: ``ValueError`` if ``n`` is not negative
        """

        require(n is None or isinstance(n, int), f'n needs to be an int or None but was [{n}]')
        require_val(n is None or n >= 0, f'n needs to be non-negative [{n}]')

        if n == 0:
            return Pype(())

        data = self._data()

        return Pype(cycle(data) if n is None else _ncycles(data, n))

    def dist(self: Pype[T], n: int) -> Pype[Pype[T]]:
        """
        Returns a pipe with ``n`` items, each being smaller pipes containing this pipe's elements distributed equally
        amongst them:
        ::

            >>> from pypey import pype
            >>> [list(segment) for segment in pype([1, 2, 3, 4, 5, 6]).dist(2)]
            [[1, 3, 5], [2, 4, 6]]

        If this pipe's size is not evenly divisible by ``n``, then the size of the returned ``Iterable``
        items will not be identical:
        ::

            >>> from pypey import pype
            >>> [list(segment) for segment in pype([1, 2, 3, 4, 5]).dist(2)]
            [[1, 3, 5], [2, 4]]

        If this pipe's size is smaller than ``n``, the last pipes in the returned pipe will be empty:
        ::

            >>> from pypey import pype
           >>> [list(segment) for segment in pype([1, 2, 3, 4, 5]).dist(7)]
            [[1], [2], [3], [4], [5], [], []]

        This method tees the backing ``Iterable``.

        Similar to ``more_itertools.distribute``.

        :param n: the number of pipes with distributed elements
        :return: a pipe with this pipe's items distributed amongst the contained pipes
        :raises: ``TypeError`` if ``n`` is not an ``int``
        :raises: ``ValueError`` if ``n`` is not positive
        """

        require(isinstance(n, int), f'n needs to be an int but was [{type(n)}]')
        require_val(n > 0, f'n needs to be greater than 0 but was [{n}]')

        # implementation based on ``more_itertools.distribute``
        return Pype(Pype(islice(child, idx, None, n)) for idx, child in enumerate(tee(self._data(), n)))

    def divide(self: Pype[T], n: int) -> Pype[Pype[T]]:
        """
        Breaks pipe into ``n`` sub-pipes:
        ::

            >>> from pypey import pype
            >>> [list(div) for div in pype([1, 2, 3, 4, 5, 6]).divide(2)]
            [[1, 2, 3], [4, 5, 6]]

        If this pipe's size is not a multiple of ``n``, the sub-pipes' sizes will be equal  except the last one, which
        will contain all excess items:
        ::

            >>> from pypey import pype
            >>> [list(div) for div in pype([1, 2, 3, 4, 5, 6, 7]).divide(3)]
            [[1, 2], [3, 4], [5, 6, 7]]

        If this pipe's size is smaller than ``n``, the resulting pipe will contain as many single-item pipes as there
        are in it, followed by ``n`` minus this pipe's size empty pipes.
        ::

            >>> from pypey import pype
            >>> [list(div) for div in pype([1, 2, 3]).divide(4)]
            [[1], [2], [3], []]

        This method requires calculating the size of this pipe, and thus will eagerly consume the backing ``Iterable``
        if it's lazy.

        Similar to ``more_itertools.divide``.

        :param n: number of segments
        :return: a pipe of ``n`` pipes
        :raises: ``TypeError`` if ``n`` is not an ``int``
        :raises: ``ValueError`` if ``n`` is not positive
        """
        require(isinstance(n, int), f'n needs to be an int but was [{type(n)}]')
        require_val(n > 0, f'n needs to be greater than 0 but was [{n}]')

        return Pype(map(Pype, _deferred_divide(self._data(), n)))

    def do(self: Pype[T], fn: Fn[[T], Any], *, now: bool = False, workers: int = 0, chunk_size: int = 100) -> Pype[T]:
        """
        Produces a side effect for each item, with the given function's return value ignored. It is typically used to
        execute an operation that is not functionally pure such as printing to console, updating a GUI, writing to disk
        or sending data over a network.

        ::

            >>> from pypey import pype
            >>> p = pype(iter([1, 2, 3])).do(lambda n: print(f'{n}'))
            >>> list(p)
            1
            2
            3
            [1, 2, 3]

        If ``now`` is set to ``True`` the side effect will take place immediately and the backing ``Iterable`` will be
        consumed if lazy.
        ::

            >>> from pypey import pype
            >>> p = pype(iter([1, 2, 3])).do(lambda n: print(f'{n}'), now=True)
            1
            2
            3
            >>> list(p)
            []

        If ``workers`` is greater  than ``0`` the side effect will be parallelised using ``multiprocessing`` if
        possible, or ``pathos`` if not. ``pathos`` multiprocessing implementation is slower and limited vs the built-in
        multiprocessing but it does allow using lambdas and local functions. When using workers, the backing
        ``Iterable`` is teed to avoid consumption. Using a large ``chunk_size`` can greately speed up parallelisation;
        it is ignored if ``workers`` is ``0``.

        Also known as ``for_each``, ``tap`` and ``sink``.

        Similar to ``more_itertools.side_effect``.

        :param fn: a function taking a possibly unpacked item
        :param now: ``False`` to defer the side effect until iteration, ``True`` to write immediately
        :param workers: number of extra processes to parallelise this method's side effect function
        :param chunk_size: size of subsequence of ``Iterable`` to be processed by workers
        :return: this pipe
        :raises: ``TypeError`` if ``fn`` is not a ``Callable`` or ``workers`` or ``chunk_size`` are not ``int``
        :raises: ``ValueError`` if ``workers`` is negative or ``chunk_size`` is non-positive
        """
        require(isinstance(workers, int), f'workers should be non-negative ints but were [{workers}]')
        require(isinstance(chunk_size, int), f'chunk size should be a positive int but was [{chunk_size}]')
        require_val(chunk_size > 0, f'chunk size should be a positive int but was [{chunk_size}]')

        ufn = self._ufn(fn)

        if workers:

            mapping = _parallel_map(self._data(), ufn, workers, chunk_size)

            if now:
                for _ in mapping:
                    pass

                return self

            return Pype(_side_effect(ident, mapping))

        if now:

            for item in self._data():
                ufn(item)

            return self

        return Pype(_side_effect(ufn, self._data()))

    def drop(self: Pype[T], n: int) -> Pype[T]:
        """
        Returns this pipe but with the first or last ``n`` items missing:
        ::

            >>> from pypey import pype
            >>> list(pype([1, 2, 3, 4]).drop(2))
            [3, 4]

            >>> from pypey import pype
            >>> list(pype([1, 2, 3, 4]).drop(-2))
            [1, 2]

        :param n: number of items to skip, positive if at the beginning of the pipe, negative at the end
        :return: pipe with ``n`` dropped items
        :raises: ``TypeError`` if ``n`` is not an ``int``

        """
        require(isinstance(n, int), f'n needs to be an int but was [{type(n)}]')

        if n == 0:
            return self

        return Pype(islice(self._data(), n, None) if n > 0 else _clip(self.it(), -n))

    def drop_while(self: Pype[T], pred: Fn[..., bool]) -> Pype[T]:
        """
        Drops items as long as the given predicate is ``True``; afterwards, it returns every item:

        >>> from pypey import pype
        >>> list(pype([1, 2, 3, 4]).drop_while(lambda n: n < 3))
        [3, 4]

        Similar to ``itertools.dropwhile``.

        :param pred: A function taking a possibly unpacked item and returning a boolean
        :return: a pipe that is a subset of this one
        :raises: ``TypeError`` if ``pred`` is not a ``Callable``
        """

        return Pype(dropwhile(self._ufn(pred), self._data()))

    def eager(self: Pype[T]) -> Pype[T]:
        """
        Returns a pype with the same contents as this one but with an eager backing collection. This will trigger
        reading the whole backing ``Iterable`` into memory.

        >>> from pypey import pype
        >>> p = pype(range(-5, 5)).map(abs)
        >>> p.size()
        10
        >>> p.size()
        0
        >>> p = pype(range(-5, 5)).map(abs).eager()
        >>> p.size()
        10
        >>> p.size()
        10

        :return: this pipe, but eager
        """
        if isinstance(self._data(), Sequence) and not isinstance(self._data(), range):
            return self

        return Pype(tuple(self._data()))

    def enum(self: Pype[T], start: int = 0, *, swap: bool = False) -> Pype[Union[Tuple[int, T], Tuple[T, int]]]:
        """
        Pairs each item with an increasing integer index:
        ::

            >>> from pypey import pype
            >>> list(pype(['a', 'fun', 'day']).enum())
            [(0, 'a'), (1, 'fun'), (2, 'day')]

        ``swap`` = ``True`` will swap the index and item around:
        ::

            >>> from pypey import pype
            >>> list(pype(['a', 'fun', 'day']).enum(swap=True))
            [('a', 0), ('fun', 1), ('day', 2)]

        Similar to  built-in ``enumerate``.

        :param start: start of the index sequence
        :param swap: if ``True`` index will be returned second, else it will be returned first
        :return: a pipe the same size as this one but with each item being ``tuple`` of index and original item
        :raises: ``TypeError`` if ``start`` is not an ``int``
        """
        enumerated = enumerate(self._data(), start=start)

        return Pype(((item, idx) for idx, item in enumerated) if swap else enumerated)

    def flat(self: Pype[I]) -> Pype[T]:
        """
        Flattens iterable items into a collection of their elements:
        ::

            >>> from pypey import pype
            >>> list(pype(['a', 'fun', 'day']).flat())
            ['a', 'f', 'u', 'n', 'd', 'a', 'y']

        Similar to ``itertools.chain.from_iterable``

        :return: a pipe with the elements of its ``Iterable`` items as items
        :raises: ``TypeError`` if items are not ``Iterable``
        """

        return Pype(flatten(self._data()))

    def flatmap(self: Pype[I], fn: Fn[..., I]) -> Pype[X]:
        """
        Maps ``Iterable`` items and then flattens the result into their elements.

        Equivalent to :func:`Pype.map` followed by :func:`Pype.flat`

        :param fn: function taking a possibly unpacked item and returning a value
        :return: a pipe with mapped flattened items
        :raises: ``TypeError`` if items are not ``Iterable`` or ``fn`` is not a ``Callable``
        """

        return Pype(flatten(map(self._ufn(fn), self._data())))

    def freqs(self: Pype[T], total: bool = True) -> Pype[Tuple[Union[T, object], int, float]]:
        """
        Computes this pipe's items' absolute and relative frequencies and optionally the total:
        ::

            >>> from pypey import pype
            >>> tuple(pype('AAB').freqs())
            (('A', 2, 0.6666666666666666), ('B', 1, 0.3333333333333333), (_TOTAL_, 3, 1.0))

        If `total` is `False`, the total is left out:
        ::

            >>> from pypey import pype
            >>> tuple(pype('AAB').freqs(total=False))
            (('A', 2, 0.6666666666666666), ('B', 1, 0.3333333333333333))

        :return: a pype containing tuples with this pipe's uniques items, plus the total as the ``pype.TOTAL`` item,
            with their absolute and relative frequencies
        """

        return Pype(_deferred_freqs(self._data(), total))

    def group_by(self: Pype[T], key: Fn[..., Y]) -> Pype[Tuple[Y, List[T]]]:
        """
        Groups items according to the key returned by the ``key`` function:

        >>> from pypey import pype
        >>> list(pype(['a', 'fun', 'day']).group_by(len))
        [(1, ['a']), (3, ['fun', 'day'])]

        This method is eager and will consume the backing ``Iterable`` if it's lazy.

        Similar to ``itertools.groupby`` except elements don't need to be sorted

        :param key: function taking a possibly unpacked item and returning a grouping key
        :return: a pipe made up of pairs of keys and lists of items
        :raises: ``TypeError`` if ``fn`` is not a ``Callable``
        """
        ufn, data = self._ufn(key), self._data()

        return Pype(_deferred_group_by(data, ufn))

    def interleave(self: Pype[T], other: Iterable[X], n: int = 1, trunc: bool = True) -> Pype[Union[T, X, Any]]:
        """
        Returns a pipe where items in this pipe are interleaved with items in the given ``Iterable``, in order.
        If either this pipe or the other ``Iterable`` are exhausted the interleaving stops:
        ::

            >>> from pypey import pype
            >>> list(pype(['a', 'fun', 'fun', 'day']).interleave([1, 2, 3]))
            ['a', 1, 'fun', 2, 'fun', 3]

        Setting ``trunc`` to ``True`` will keep adding the left over items in the ``Iterable`` that hasn't been
        exhausted after the other one is:
        ::

            >>> from pypey import pype
            >>> list(pype(['a', 'fun', 'fun', 'day']).interleave([1, 2, 3], trunc=False))
            ['a', 1, 'fun', 2, 'fun', 3, 'day']

        The number of items in this pipe's to leave between the items in the ``Iterable`` can be varied:
        ::

            >>> from pypey import pype
            >>> list(pype(['a', 'fun', 'fun', 'day']).interleave([1, 2, 3], n=2))
            ['a', 'fun', 1, 'fun', 'day', 2]

        This operation is lazy.

        A cross between ``more_itertools.interleave_longest``, ``more_itertools.interleave`` and
        ``more_itertools.intersperse``.

        :param other: the ``Iterable`` whose items will be interleaved with this pipe's
        :param n: the number of this pipe's items to leave between each of the ``Iterable``-'s ones
        :param trunc: ``True`` if the unexhausted ``Iterable`` should be truncated once the other one is
        :return: A pipe with this pipe's elements and the given ``Iterable``-'s in order
        :raises: ``TypeError`` if ``other`` is not an ``Iterable`` or ``n` is not an ``int``
        :raises: ``ValueError`` if ``n`` is less than one
        """
        require(isinstance(n, int), f'n needs to be a positive integer but was [{n}]')
        require_val(n >= 1, f'n needs to be an int but was [{type(n)}]')

        fn = interleave if trunc else interleave_longest

        if n == 1:
            return Pype(fn(self._data(), other))

        return (self
                .chunk(size=n)
                .zip(other, trunc=trunc)
                .map(lambda chunk, item: chunk.cat((item,) if item is not None else chunk) if chunk else (item,))
                .flat())

    def it(self: Pype[T]) -> Iterator[T]:
        """
        Returns an ``Iterator`` for this pipe's items. It's a more concise version of, and functionally identical
        to, :func:`Pype.__iter__`

        >>> from pypey import pype
        >>> list(iter(pype([1, 2, 3]))) == list(pype([1, 2, 3]).it())
        True

        :return: an ``Iterator`` for this pipe's items
        """
        return iter(self)

    def map(self: Pype[T], fn: Fn[..., Y], *other_fns: Fn[..., X], workers: int = 0, chunk_size: int = 100) \
            -> Pype[Union[X, Y]]:
        """
        Transforms this pipe's items according to the given function(s):
        ::

            >>> from math import sqrt
            >>> from pypey import pype
            >>> list(pype([1, 2, 3]).map(sqrt))
            [1.0, 1.4142135623730951, 1.7320508075688772]

        If more than one function is provided, they will be chained into a single one before being applied to each item:
        ::

            >>> from pypey import pype
            >>> list(pype(['a', 'fun', 'day']).map(str.upper, reversed, list))
            [['A'], ['N', 'U', 'F'], ['Y', 'A', 'D']]

        If ``workers`` is greater than ``0`` the mapping will be parallelised using ``multiprocessing`` if possible
        or ``pathos`` if not. ``pathos`` multiprocessing implementation is slower and has different limitations than the
        built-in multiprocessing but it does allow using lambdas. When using workers, the backing ``Iterable`` is teed
        to avoid consumption. Using a large ``chunk_size`` can greately speed up parallelisation; it is ignored if
        ``workers`` is ``0``.

        Similar to built-in ``map``.

        :param fn: a function taking a possibly unpacked item and returning a value
        :param other_fns: other functions to be chained with ``fn``, taking a possibly unpacked item and returning a
            value
        :param workers: number of extra processes to parallelise this method's mapping function(s)
        :param chunk_size: size of subsequence of ``Iterable`` to be processed by workers
        :return: a pipe with this pipe's items mapped to values
        :raises: ``TypeError`` if ``fn`` is not a ``Callable`` or ``other_fns`` is not a ``tuple`` of ``Callable`` or
            if ``workers`` or ``chunk_size`` are not ``int``
        :raises: ``ValueError`` if ``workers`` is negative or ``chunk_size`` is non-positive
        """
        require(isinstance(workers, int), f'workers should be non-negative ints but where [{workers}]')
        require(isinstance(chunk_size, int), f'chunk size should be a positive int but was [{chunk_size}]')
        require_val(chunk_size > 0, f'chunk size should be a positive int but was [{chunk_size}]')

        combo_fn = self._ufn(fn) if not other_fns else px(_pipe, functions=[self._ufn(fn) for fn in (fn,) + other_fns])

        if workers:
            return Pype(_parallel_map(self._data(), combo_fn, workers, chunk_size))

        return Pype(map(combo_fn, self._data()))

    def partition(self: Pype[T], pred: Fn[..., bool]) -> Tuple[Pype[T], Pype[T]]:
        """
        Splits this pipe's items into two pipes, according to whether the given
        predicate returns ``True`` or ``False``:

        >>> from pypey import pype
        >>> [list(p) for p in pype([1, 2, 3, 4]).partition(lambda n: n%2)]
        [[2, 4], [1, 3]]

        This method tees the backing ``Iterable``.

        :param pred: A function taking a possibly unpacked item and returning a boolean
        :return: a 2-``tuple`` with the first item being a pipe with items for which ``pred`` is ``False`` and the
            second a pipe with items for which it is ``True``
        :raises: ``TypeError`` if ``pred`` is not a ``Callable``
        """

        falses, trues = partition(self._ufn(pred), self._data())

        return Pype(falses), Pype(trues)

    def pick(self: Pype[Union[Sequence, NamedTuple]], key: Any) -> Pype[Any]:
        """
        Maps each item to the given ``key``. Allowed keys are any supported by ``object.__item__`` :
        ::

            >>> from pypey import pype
            >>> list(pype(['a', 'fun', 'day']).pick(0))
            ['a', 'f', 'd']

        as well as ``@property``-defined object properties and ``namedtuple`` attributes:
        ::

            >>> from collections import namedtuple
            >>> from pypey import pype
            >>> Person = namedtuple('Person', ['age'])
            >>> list(pype([Person(42), Person(24)]).pick(Person.age))
            [42, 24]

        Equivalent to :func:`Pype.map(lambda item: item.key)` and :func:`Pype.map(lambda item: item[key])`.

        :param key: key to pick from each item
        :return: a pipe where each item in this pipe has been replaced with the given key
        """
        is_prop = isinstance(key, property) or isinstance(key, _tuplegetter)

        return Pype(key.__get__(item) if is_prop else item[key] for item in self._data())

    def print(self: Pype[T],
              fn: Fn[..., str] = str,
              *,
              sep: str = ' ',
              end: str = '\n',
              file: IO = stdout,
              flush: bool = False,
              now: bool = True) -> Pype[T]:
        """
        Prints string returned by given function using built-in ``print``:
        ::

            >>> from pypey import pype
            >>> p = pype(iter([1, 2, 3])).print()
            >>> list(p)
            1
            2
            3
            [1, 2, 3]

        If ``now`` is set to ``True``, the printing  takes place immediately and the backing ``Iterable`` is consumed:
        ::

            >>> from pypey import pype
            >>> p = pype(iter([1, 2, 3])).print(now=True)
            1
            2
            3
            >>> list(p)
            []

        The keyword-only parameters are the same as the built-in ``print`` (minus the ``now`` flag).

        :param fn: A function taking a possibly unpacked item and returning a string
        :param sep: separator as per built-in ``print``
        :param end: terminator, as per built-in ``print``
        :param file: text stream, as per built-in ``print``
        :param flush: flush, as per built-in ``print``
        :param now: ``False`` if the printing should be deferred, ``True`` otherwise
        :return: this pipe, with a possibly consumed backing ``Iterable`` if ``now`` is set to ``True``
        :raises: ``TypeError`` if ``fn`` is not a ``Callable``
        """

        ufn, data = (fn if fn == str else self._ufn(fn), self._data())

        if now:

            for item in data:
                print(ufn(item), sep=sep, end=end, file=file, flush=flush)

            return self

        return self.do(px(_print_fn, ufn=ufn, sep=sep, end=end, file=file, flush=flush))

    def reduce(self: Pype[T], fn: Fn[[H, T], H], init: Optional[X] = None) -> H:
        """
        Reduces this pipe to a single value through the application of the given aggregating function
        to each item:
        ::

            >>> from operator import add
            >>> from pypey import pype
            >>> pype([1, 2, 3]).reduce(add)
            6

        If ``init`` is not ``None``, it will be placed as the start of the returned pipe and
        serve as a default value in case the pipe is empty:
        ::

            >>> from pypey import pype
            >>> pype([1, 2, 3]).reduce(add, init=-1)
            5

        This function is eager and immediate.

        Similar to ``functools.reduce``.

        :param fn: a function taking an aggregate of the previous items as its first argument
            and the current item as its second, and returning a new aggregate
        :param init: a value to be placed before all other items if it's not ``None``
        :return: a value of the same type as the return of the given function, or ``init`` if the pipe is empty
        :raises: ``TypeError`` if ``fn`` is not a ``Callable``
        :raises: ``ValueError`` if this pipe is empty and ``init`` is ``None``
        """

        data = self._data()

        return reduce(fn, data) if init is None else reduce(fn, data, init)

    def reject(self: Pype[T], pred: Fn[..., bool]) -> Pype[T]:
        """
        Returns a pipe with only the items for each the given predicate returns ``False``:
        ::

            >>> from pypey import pype
            >>> list(pype(['a', 'FUN', 'day']).reject(str.isupper))
            ['a', 'day']

        Opposite of :func:`Pype.select`.

        Similar to built-in ``filterfalse``.

        :param pred: a function taking a possibly unpacked item and returning a boolean
        :return: a pipe with the subset of this pipe's items for which ``pred`` returns ``False``
        :raises: ``TypeError`` if ``pred`` is not a ``Callable``
        """

        return Pype(filterfalse(self._ufn(pred), self._data()))

    def reverse(self: Pype[T]) -> Pype[T]:
        """
        Returns a pipe where this pipe's items appear in reversed order:
        ::

            >>> from pypey import pype
            >>> list(pype([1, 2, 3]).reverse())
            [3, 2, 1]

        This operation is eager but deferred.

        Similar to built-in ``reversed``.

        :return: a pipe with items in reversed order
        """

        return Pype(_deferred_reverse(self._data()))

    def roundrobin(self: Pype[I]) -> Pype[T]:
        """
        Returns a pipe where each item is taken from each of this pipe's elements' in turn:

        >>> from pypey import pype
        >>> list(pype(['a', 'fun', 'day']).roundrobin())
        ['a', 'f', 'd', 'u', 'a', 'n', 'y']

        This operation is eager but deferred.

        Similar to ``more_itertools.interleave_longest``.

        :return: A pipe with items taken from this pipe's ``Iterable`` items
        :raises: ``TypeError`` if any of this pipe's items is not an ``Iterable``
        """
        # implementation based on ``more_itertools.interleave_longest``
        return Pype(_deferred_roundrobin(self._data()))

    def sample(self: Pype[T], k: int, seed_: Optional[Any] = None) -> Pype[T]:
        """
        Returns a pipe with ``k`` items sampled without replacement from this pipe:

        >>> from pypey import pype
        >>> list(pype([1, 2, 3, 4, 5]).sample(2))
        [1, 3]

        This operation is eager but deferred.

        Similar to ``random.sample``.

        :param k: a non negative ``int`` specifying how many items to sample
        :param seed_: an value to seed the random number generator
        :return: a pipe with sampled items from this pipe
        :raises: ``TypeError`` if k is not an ``int``
        :raises: ``ValueError`` if ``k`` is negative
        """
        if seed_:
            seed(seed_)

        require(isinstance(k, int), f'k needs to be an int but was [{type(k)}]')

        return Pype(_deferred_sample(self._data(), k))

    def select(self: Pype[T], pred: Fn[..., bool]) -> Pype[T]:
        """
        Returns a pipe with only the items for each ``pred`` returns ``True``, opposite of :func:`Pype.reject`:
        ::

            >>> from pypey import pype
            >>> list(pype(['a', 'FUN', 'day']).select(str.isupper))
            ['FUN']

        Also known as ``filter``.

        Similar to built-in ``filter``.

        :param pred: a function taking a possibly unpacked item and returning a boolean
        :return: a pipe with the subset of this pipe's items for which the given ``pred`` returns ``True``
        :raises: ``TypeError`` if ``pred`` is not a ``Callable``
        """

        return Pype(filter(self._ufn(pred), self._data()))

    def shuffle(self: Pype[T], seed_: Optional[Any] = None) -> Pype[T]:
        """
        Returns a shuffled version of this pipe:
        ::

            >>> from pypey import pype
            >>> list(pype([1, 2, 3, 4, 5]).shuffle())
            [3, 2, 1, 5, 4]

        This method is eager but deferred.

        Similar to  ``random.shuffle``

        :param seed_: a value to seed the random generator
        :return: This pipe, but with its items shuffled
        """

        if seed_ is not None:
            seed(seed_)

        return Pype(_deferred_shuffle(self._data()))

    def size(self: Pype[T]) -> int:
        """
        Returns number of items in this pipe:
        ::

            >>> from pypey import pype
            >>> pype([1, 2, 3]).size()
            3

        This operation is eager and immediate.

        :return: an ``int`` correpsonding to the cardinality of this pipe
        """

        if isinstance(self._it, Sized):
            return len(self._it)

        return sum(1 for _ in self._data())

    def slice(self: Pype[T], start: int, end: int) -> Pype[T]:
        """
        Returns a slice of this pipe between items at positions ``start`` and ``end``, exclusive:
        ::

            >>> from pypey import pype
            >>> list(pype([1, 2, 3, 4]).slice(1, 3))
            [2, 3]

        Similar to ``itertools.islice``.

        :param start: index of first element to be returned
        :param end: index of element after the last element to be returned
        :return: pipe with a slice of the items of this pipe
        :raises: ``TypeError`` if ``start`` or ``end`` are not ``int``-s
        :raises: ``ValueError`` if ``start`` or ``end`` are negative or if ``end`` is smaller than ``start``
        """
        require(isinstance(start, int), f'start needs to be an int but was [{type(start)}]')
        require(isinstance(end, int), f'start needs to be an int but was [{type(end)}]')
        require_val(start <= end, f'start cannot be larger than end but was [{start}] > [{end}]')

        return Pype(islice(self._data(), start, end))

    def sort(self: Pype[T], key: Optional[Fn[..., Y]] = None, *, rev: bool = False) -> Pype[T]:
        """
        Sorts this pipe's items, using the return value of ``key`` if not ``None``:
        ::

            >>> from pypey import pype
            >>> list(pype(['a', 'funny', 'day']).sort(len))
            ['a', 'day', 'funny']

        This method is eager but deferred.

        Similar to builtin ``sorted``.

        :param key: a function possibly taking a unpacked item and returning a value to sort by, or ``None``
        :param rev: ``True`` if the sort order should be reversed, ``False`` otherwise.
        :return: a sorted pipe
        :raises: ``TypeError`` if ``key`` is not a ``Callable``
        """
        ufn, data = (None if key is None else self._ufn(key), self._data())

        return Pype(_deferred_sort(data, ufn, rev))

    def split(self: Pype[T], when: Fn[..., bool], mode: str = 'after') -> Pype[Pype[T]]:
        """
        Returns a pipe containing sub-pipes split off this pipe where the given ``when`` predicate is ``True``:
        ::

            >>> from pypey import pype
            >>> [list(split) for split in pype(list('afunday')).split(lambda char: char == 'a')
            [['a'], ['f', 'u', 'n', 'd', 'a'], ['y']]

        The default mode is to split after every item for which the predicate is ``True``. When ``mode`` is set to
        ``before``, the split is done before:
        ::

            >>> from pypey import pype
            >>> [list(split) for split in pype(list('afunday')).split(lambda char: char == 'a', 'before')]
            [['a', 'f', 'u', 'n', 'd'], ['a', 'y']]

        And when ``mode`` is set to ``at``, the pipe will be split both before and after, leaving the splitting item
        out:
        ::

            >>> from pypey import pype
            >>> [list(split) for split in pype(list('afunday')).split(lambda char: char == 'a', 'at')]
            [[], ['f', 'u', 'n', 'd'], ['y']]

        Similar to ``more_itertools.split_before``, ``more_itertools.split_after`` and ``more_itertools.split_at``.

        :param when: A function possibly taking a unpacked item and returning ``True`` if this pipe should be split
            before this item
        :param mode: which side of the splitting item the pipe is split, one of ``after``, ``at`` or ``before``
        :return: a pipe of pipes split off this pipe at items where ``when`` returns ``True``
        :raises: ``TypeError`` if ``when`` is not a ``Callable`` or ``mode` is not a ``str``
        :raises: ``ValueError`` if ``mode`` is a ``str`` but not one the supported ones
        """
        require(isinstance(mode, str), f'mode should be a str but was [{mode}] instead')
        require_val(mode in SPLIT_MODES, f'mode should be on of {SPLIT_MODES} but was [{mode}]')
        # implementation based on ``more_itertools``'s ``split_before``, ``split_after`` and ``split_at``
        splitting = _split_before if mode == 'before' else _split_after if mode == 'after' else _split_at

        ufn, data = self._ufn(when), self._data()

        return Pype(map(Pype, splitting(data, ufn)))

    def take(self: Pype[T], n: int) -> Pype[T]:
        """
        Returns a pipe containing the first or last ``n`` items of this pipe, depending on the sign of ``n``:
        ::

            >>> from pypey import pype
            >>> list(pype([1, 2, 3, 4]).take(-2))
            [3, 4]

            >>> from pypey import pype
            >>>list(pype([1, 2, 3, 4]).take(2))
            [1, 2]

        This operation is eager but deferred when ``n`` is negative else it's lazy.

        Also know as `head` and `tail`.

        :param n: a negative ``int`` specifying the number of items of this pipe's tail or a positive ``int`` for the
            first ``n``  elements
        :return: a pipe with this pipe's first or last ``n`` items
        :raises: ``TypeError`` if ``n`` is not an ``int``

        """
        require(isinstance(n, int), f'n needs to be an int but was [{type(n)}]')

        slicing = islice if n >= 0 else _deferred_tail

        return Pype(slicing(self._data(), abs(n)))

    def take_while(self: Pype[T], pred: Fn[..., bool]) -> Pype[T]:
        """
        Returns a pipe containing this pipe's items until ``pred`` returns ``False`` :
        ::

            >>> from pypey import pype
            >>> list(pype([1, 2, 3, 4]).take_while(lambda n: n < 4))
            [1, 2, 3]

        Similar to ``itertools.takewhile``.

        :param pred: a function taking a possibly unpacked item and returning a boolean
        :return: a pipe that is a subset of this one minus the items after ``pred`` returns ``True``
        :raises: ``TypeError`` if ``pred`` is not a ``Callable``
        """

        return Pype(takewhile(self._ufn(pred), self._data()))

    def tee(self: Pype[T], n: int) -> Pype[Pype[T]]:
        """
        Returns ``n`` lazy copies of this pipe:

        >>> from pypey import pype
        >>> [list(copy) for copy in pype([1, 2, 3]).tee(2)]
        [[1, 2, 3], [1, 2, 3]]

        This method tees the backing ``Iterable`` but does not replace it (unlike :func:`Pype.clone`).

        Similar to ``itertools.tee``.

        :return: a pipe containing ``n`` copies of this pipe
        :raises: ``TypeError`` if ``n`` is not an ``int``
        :raises: ``ValueError`` if ``n`` is non-positive
        """
        require_val(n > 0, f'n should be greater than 0 but was [{n}]')

        return Pype(map(Pype, tee(self._it, n)))

    def to(self: Pype[T], fn: Fn[[Iterable[T]], Y], *other_fns: Fn[..., X]) -> Union[Y, X]:
        """
        Applies given function to this pipe:
        ::

            >>> from pypey import pype
            >>> pype(['a', 'fun', 'day']).to(list)
            ['a', 'fun', 'day']

        This method is eager if the given function is eager and lazy if it's lazy:
        ::

            >>> from pypey import pype
            >>> p = pype(['a', 'fun', 'day']).to(enumerate)
            >>> p
            <enumerate object at 0x7fdb743003c0>
            >>> list(p)
            [(0, 'a'), (1, 'fun'), (2, 'day')]

        If provided with more than one function, it will pipe them together:
        ::

            >>> from pypey import pype
            >>> pype(['a', 'fun', 'day']).to(list, len)
            3

        Equivalent to ``fn_n(...fn2(fn1(pipe)))``.

        :param fn: function to apply to this pipe
        :param other_fns: other functions to be chained with ``fn``
        :return: the return value of the given function(s)
        :raises: ``TypeError`` if any of the provided functions is not a ``Callable``
        """

        return px(_pipe, functions=(fn,) + other_fns)(self)

    def to_file(self: Pype[T],
                target: Union[AnyStr, PathLike, int],
                *,
                mode: str = 'w',
                buffering: int = -1,
                encoding: Optional[str] = 'utf8',
                errors: Optional[str] = None,
                newline: Optional[str] = None,
                closefd: bool = True,
                opener: Optional[Fn[..., int]] = None,
                eol: bool = True,
                now: bool = True) -> Pype[T]:
        """
        Writes items to file:
        ::

            >>> from tempfile import gettempdir
            >>> from os.path import join
            >>> from pypey import pype
            >>> p = pype(['a', 'fun', 'day']).to_file(join(gettempdir(), 'afunday.txt'), eol=False)
            >>>list(p)
            ['a', 'fun', 'day']
            >>> list(pype.file(join(gettempdir(), 'afunday.txt')))
            ['afunday']

        The first eight parameters are identical to built-in ``open``. If ``eol`` is set to ``True``, each item will be
        converted to string and a line terminator will be appended to it:
        ::

            >>> from pypey import pype
            >>> p = pype([1, 2, 3]).to_file(join(gettempdir(), '123.txt', eol=True))
            >>> list(p)
            [1, 2, 3]
            >>> list(pype.file(join(gettempdir(), '123.txt')))
            ['1', '2', '3']

        This method is intrinsically lazy but it's set to immediate/eager by default. As such, if ``now`` is set to
        ``True`` and the backing ``Iterable`` is lazy, it will be consumed and this method will return an empty pipe:
        ::

            >>> from pypey import pype
            >>> p = pype(iter([1, 2, 3])).to_file(join(gettempdir(), '123.txt'), now=True)
            >>> list(p)
            []
            >>> list(pype.file(join(gettempdir(), '123.txt')))
            ['1', '2', '3']

        :param target: target to write this pipe's items to
        :param mode: mode as per built-in ``open``, except no read modes are allowed
        :param buffering: buffering as per built-in ``open``
        :param encoding: encoding as per built-in ``open`` except the default value is ``utf8`` instead of None
        :param errors: errors as per built-in ``open``
        :param newline: newline as per built-in ``open``
        :param closefd: closefd as per built-in ``open``
        :param opener: opener as per built-in ``open``
        :param eol: ``True`` if a line separator should be added to each item, ``False`` otherwise
        :param now: ``False`` to defer writing until pipe is iterated through, ``True`` to write immediately
        :return: this pipe, possibly after writing its items to file
        :raises: ``ValueError`` if ``mode`` has `r` or `+`
        """

        require_val('r' not in mode and '+' not in mode, f'mode cannot be read, was {mode}')

        _lines = _lines_to(target,
                           self._data(),
                           eol,
                           mode=mode,
                           buffering=buffering,
                           encoding=encoding,
                           errors=errors,
                           newline=newline,
                           closefd=closefd,
                           opener=opener)

        if now:
            for _ in _lines:
                pass

        return self if now else Pype(_lines)

    def to_json(self: Pype[T],
                target: Union[AnyStr, PathLike, int],
                *,
                mode: str = 'w',
                skipkeys=False,
                ensure_ascii=True,
                check_circular=True,
                allow_nan=True,
                cls=None,
                indent=None,
                separators=None,
                default=None,
                sort_keys=False,
                as_dict: bool = True):
        """
        Writes items to a file as a json value:
        ::

            >>> from tempfile import gettempdir
            >>> from os.path import join
            >>> from pypey import pype
            >>> p = pype(['a', 'fun', 'day']).to_json(join(gettempdir(), 'afunday.json'))
            <pypey.pype.Pype object at 0x7f7c1971a8d0>
            >>> list(pype.json(join(gettempdir(), 'afunday.json')))
            ['a', 'fun', 'day']

        The first parameter is the same to built-in ``open``, and the rest are identical to the ones in ``json.dump``
        excep the last one which specifies if pairs should be written as dict or as a list. This method will never
        write single primitives if the pipe contains a single value.

        This method is eager and immediate

        :param target: target to write this pipe's items to
        :param mode: mode as per built-in ``open``, except no read modes are allowed
        :param skipkeys: skipkeys as per built-in ``json.dump``
        :param ensure_ascii: ensure_ascii as per built-in ``json.dump``
        :param check_circular: check_circular as per built-in ``json.dump``
        :param allow_nan: allow_nan as per built-in ``json.dump``
        :param cls: cls as per built-in ``json.dump``
        :param indent: indent as per built-in ``json.dump``
        :param separators: separators as per built-in ``json.dump``
        :param default: default as per built-in ``json.dump``
        :param sort_keys: sort_keys as per built-in ``json.dump``
        :param as_dict: True if item pairs should be written as key-value pairs in an object, False if as a list
        :return: this pipe, after writing its items to a file as a json value
        :raises: ``ValueError`` if ``mode`` has `r` or `+`
        :raises: ``TypeError`` if ``as_dict`` is ``True`` and items are not pairs
        """

        require_val('r' not in mode and '+' not in mode, f'mode cannot be read, was {mode}')

        data = tuple(self._data())

        if as_dict:
            try:
                data = dict(data)
            except TypeError:
                raise TypeError(f'items cannot be written as dictionary because they are not pairs {data[0:3]}...')

        with open(target, mode=mode) as json_file:
            json.dump(data,
                      json_file,
                      skipkeys=skipkeys,
                      ensure_ascii=ensure_ascii,
                      check_circular=check_circular,
                      allow_nan=allow_nan,
                      cls=cls,
                      indent=indent,
                      separators=separators,
                      default=default,
                      sort_keys=sort_keys)

        return self

    def top(self: Pype[T], n: int, key: Fn[[T], Any] = ident) -> Pype[T]:
        """
        Returns a pipe with the ``n`` items having the highest value, as defined by the ``key`` function.

        >>> from pypey import pype
        >>> list(pype(['a', 'fun', 'day']).top(2, len))
        ['fun', 'day']

        This method is eager but deferred.

        :param n: the number of items to return
        :param key: the function defining the value to find the top elements for
        :return: a pipe with the top ``n`` elements
        :raises: ``TypeError` if ``n`` is not an ``int`` or ``key`` is not a ``Callable``
        :raises: ``ValueError`` if ``n`` is non-positive
        """
        require_val(n > 0, f'n needs to be non-negative but was [{n}]')

        ufn, data = self._ufn(key), self._data()

        return Pype(_deferred_top(data, n, ufn))

    def uniq(self: Pype[T]) -> Pype[T]:
        """
        Returns unique number of items:
        ::

            >>> from pypey import pype
            >>> list(pype(['a', 'b', 'b', 'a']).uniq())
            ['a', 'b']

        This method tees the backing ``Iterable``.

        Similar to ``more_itertools.unique_everseen``.

        :return: A pipe with the unique items in this pipe
        """

        return Pype(unique_everseen(self._data()))

    def unzip(self: Pype[I]) -> Pype[Pype[Any]]:
        """
        Returns a pipe of pipes each with the items of this pipe's ``Iterable`` items:

        >>> from pypey import pype
        >>> [list(p) for p in pype(['any', 'fun', 'day']).unzip()]
        [['a', 'f', 'd'], ['n', 'u', 'a'], ['y', 'n', 'y']]

        This method is eager but deferred.

        Similar to ``more_itertools.unzip``

        :return: a pipe of pipes with the unzipped items in this pipe's ``Iterable`` items
        :raises: ``TyperError`` if any of this pipe's items is not an ``Iterable``
        """

        return Pype(map(Pype, _deferred_unzip(self._data())))

    def window(self: Pype[T], size: int, *, shift: int = 1, pad: Optional[Any] = None) -> Pype[Tuple[Optional[T], ...]]:
        """
        Returns a pipe containing pipes, each being a sliding window over this pipe's items:
        ::

            >>> from pypey import pype
            >>> list(pype(iter([1, 2, 3])).window(size=2))
            [(1, 2), (2, 3)]

        If ``size`` is larger than this pipe, ``pad`` is used fill in the missing values:
        ::

            >>> from pypey import pype
            >>> list(pype(iter([1, 2, 3])).window(size=4, pad=-1))
            [(1, 2, 3, -1)]

        Similar to ``more_itertools.windowed``.

        :param size: the size of the window
        :param shift: the shift between successive windows
        :param pad: the value to use to fill missing values
        :return: a pipe of pipes, each being a sliding window over this pipe
        :raises: ``TypeError`` if either ```size`` or ``shift`` is not an ``int``
        :raises: ``ValueError`` if ``size`` is negative or ``shift`` is non-positive
        """

        return Pype(windowed(self._data(), n=size, fillvalue=pad, step=shift))

    def zip(self: Pype[I],
            *others: Iterable[Any],
            trunc: bool = True,
            pad: Optional[Any] = None) -> Pype[Tuple[T, ...]]:
        """
        Zips items in this pipe with each other or with items in each of the given ``Iterable``-s. If no ``Iterable``-s
        are provided, the items in this pipe will be zipped with each other:
        ::

            >>> from pypey import pype
            >>> list(pype(['a', 'fun', 'day']).zip(trunc=False, pad='?'))
            [('a', 'f', 'd'), ('?', 'u', 'a'), ('?', 'n', 'y')]


        Self-zipping will consume the backing ``Iterable`` if it's lazy. If other ``Iterable``-s are provided, the items
        in this pipe will be zipped with the items in those:
        ::

            >>> from pypey import pype
            >>> list(pype(['a', 'fun', 'day']).zip([1, 2, 3, 4]))
            [('a', 1), ('fun', 2), ('day', 3)]

        Similar to built-in ``zip`` and ``itertools.zip_longest``.

        :param others: ``Iterables`` to be zipped with this with this pipe
        :param trunc: ``True`` to truncate all ``Iterable``-s to the size of the shortest one, ``False``
            to  pad all to the size of the longest one
        :param pad: value to pad shorter ``Iterable``-s with if ``trunc`` is ``False``; if it's ``True``
            it's ignored
        :return: a pipe with the zipped items of this pipe with each other or with the given ``Iterable``-s' ones
        :raises: ``TypeError`` any of ``others`` is not an ``Iterable``
        """
        data = self._data()

        if others == ():
            return Pype(_deferred_zip(data, trunc, pad))

        require(all(isinstance(p, Iterable) for p in others), 'Inputs to be zipped should be all Iterables')

        return Pype(zip(data, *others) if trunc else zip_longest(data, *others, fillvalue=pad))

    def zip_with(self: Pype[T], fn: Fn[..., Y]) -> Pype[Tuple[T, Y]]:
        """
        Returns a pipe where each item is a 2-``tuple`` with this pipe's item as the first and the output of ``fn``
        as the second. This is useful for adding an extra piece of data to the current pipeline:
        ::

            >>> from pypey import pype
            >>> list(pype(['a','fun', 'day']).zip_with(len))
            [('a', 1), ('fun', 3), ('day', 3)]

        and it's a more concise version of:
        ::

            >>> from pypey import pype
            >>> list(pype(['a','fun', 'day']).map(lambda w: (w, len(w))))
            [('a', 1), ('fun', 3), ('day', 3)]

        :param fn: a function taking a possibly unpacked item and returning a value to be zipped with this pipe's item
        :return: a new pipe with zipped items
        """
        ufn = self._ufn(fn)

        return Pype(map(lambda item: (item, ufn(item)), self._data()))

    def _data(self: Pype[T], teed: bool = False) -> Iterable[T]:

        if teed and not isinstance(self._it, Sized):
            self._it, copy = tee(self._it)
            return copy

        return self._it

    def _ufn(self: Pype[T], fn: Fn[[T], Y]) -> Fn[[T], Y]:
        head, tail = _head_tail(self._it)

        if head is _sent:
            return fn

        self._it = head + tail if isinstance(tail, tuple) else chain([head], tail)

        return _unpack_fn(fn, head)


def _accumulate(data: Iterable[T], func: Fn[[H, T], H], initial: Optional[H] = None) -> Iterator[H]:
    it = iter(data)
    total = initial

    if initial is None:
        total = next(it, _sent)
        if total == _sent:
            return None

    yield total

    for element in it:
        total = func(total, element)
        yield total


def _clip(data: Iterator[T], n: int) -> Iterable[T]:
    n_last: Deque = deque()

    try:

        for _ in range(n):
            n_last.append(next(data))

    except StopIteration:

        return data

    while True:
        try:

            n_last.append(next(data))
            yield n_last.popleft()

        except StopIteration:
            break


def _deferred_divide(data: Iterable[T], n: int) -> Iterator[Iterable[T]]:
    _data = data if isinstance(data, Sequence) else tuple(data)

    size, rem = divmod(len(_data), n)

    if not rem:

        yield from chain((_data[s * size: (s + 1) * size] for s in range(n)))

    elif len(_data) > n:

        yield from chain((_data[s * size: (s + 1) * size] for s in range(n - 1)), (_data[-(size + rem):],))

    else:

        yield from chain(((item,) for item in _data), (() for _ in range(n - len(_data))))


def _deferred_freqs(data: Iterable[T], total: bool) -> Iterator[Tuple[Union[T, Total], int, float]]:
    item_to_freq: Dict[Union[T, Total], int] = defaultdict(int)
    n = 0

    for item in data:
        item_to_freq[item] += 1
        n += 1

    if total:
        item_to_freq[TOTAL] = n

    yield from ((item, freq, freq / (n or 1)) for item, freq in item_to_freq.items())


def _deferred_group_by(data: Iterable[T], key: Fn[..., Y]) -> Iterator[Tuple[Y, List[T]]]:
    key_to_group = defaultdict(list)

    for element in data:
        key_to_group[key(element)].append(element)

    yield from key_to_group.items()


def _deferred_reverse(data: Iterable[T]) -> Iterator[T]:
    yield from always_reversible(data)


def _deferred_roundrobin(data: Iterable[I]) -> Iterator[T]:
    yield from filterfalse(px(eq, _sent), flatten(zip_longest(*data, fillvalue=_sent)))


def _deferred_sample(data: Iterable[T], k: int) -> Iterator[T]:
    yield from random_permutation(data, k)


def _deferred_shuffle(data: Iterable[T]) -> Iterator[T]:
    data = list(data)
    shuffle(data)

    yield from data


def _deferred_sort(data: Iterable[T], key: Optional[Fn[..., Y]], rev: bool) -> Iterator[T]:
    yield from sorted(data, key=key, reverse=rev)


def _deferred_tail(data: Iterable[T], n: int) -> Iterator[T]:
    yield from tail(n, data)


def _deferred_top(data: Iterable[T], n: int, key: Fn[..., Any]) -> Iterator[T]:
    yield from nlargest(n, data, key) if n > 1 else [max(data, key=key)]


def _deferred_unzip(data: Iterable[I]) -> Iterator[Any]:
    yield from unzip(data)


def _deferred_zip(data: Iterable[I], trunc: bool, pad: Any) -> Iterator[Tuple[Any, ...]]:
    yield from zip(*data) if trunc else zip_longest(*data, fillvalue=pad)


def _lines_to(target: Union[AnyStr, PathLike, int], lines: Iterable[T], eol: bool, **kwargs) -> Iterator[T]:
    """"""
    with open(target, **kwargs) as out:
        for line in lines:
            out.write(f'{str(line)}\n' if eol else line)

            yield line


def _ncycles(data: Iterable[T], n: int) -> Iterator[T]:
    saved = []

    for item in data:
        yield item

        saved.append(item)

    n_done = 1
    while saved and n_done < n:

        for item in saved:
            yield item

        n_done += 1


def _parallel_map(data: Iterable[T], fn: Fn[..., Y], workers: int, chunk_size: int) -> Iterable[Y]:
    try:
        # This tries to prevent the most common cause of PicklingError as that would lead to the consumption of `data`
        # if it's lazy and then the imap in the except clause will get a `data` with missing items
        func = fn.func if hasattr(fn, 'func') else fn

        if '<lambda>' in func.__qualname__ or '<locals>' in func.__qualname__:
            raise PicklingError

        with Pool(workers) as pool:
            yield from pool.imap(fn, data, chunksize=chunk_size)

    except (PicklingError, AttributeError):

        logger.warning('multiprocessing with pickle failed, using pathos with dill instead.')

        with ProcessPool(workers) as pool:
            yield from pool.imap(fn, data, chunksize=chunk_size)


def _print_fn(item: T, ufn: Fn[..., Y], sep: str, end: str, file: IO, flush: bool):
    # created as global function to avoid issues with multiprocessing
    print(ufn(item), sep=sep, end=end, file=file, flush=flush)


def _side_effect(fn: Fn[[T], Y], data: Iterable[T]) -> Iterable[T]:
    for item in data:
        fn(item)
        yield item


def _split_after(data: Iterable[T], pred: Fn[..., bool]) -> Iterator[List[T]]:
    chunk = []

    for item in data:
        chunk.append(item)

        if pred(item) and chunk:
            yield chunk
            chunk = []

    if chunk:
        yield chunk


def _split_at(data: Iterable[T], pred: Fn[..., bool]) -> Iterator[List[T]]:
    chunk: List[T] = []

    for item in data:

        if pred(item):
            yield chunk
            chunk = []

        else:
            chunk.append(item)

    if chunk:
        yield chunk


def _split_before(data: Iterable[T], pred: Fn[..., bool]) -> Iterator[List[T]]:
    chunk: List[T] = []

    for item in data:

        if pred(item) and chunk:
            yield chunk
            chunk = []

        chunk.append(item)

    if chunk:
        yield chunk


def _head_tail(data: Iterable[T]) -> Tuple[Union[T, object, Tuple], Iterable[T]]:
    if isinstance(data, Sequence):

        if len(data) > 1:
            return tuple(data[:1]), tuple(data[1:])

        elif len(data) == 1:
            return tuple(data[:1]), tuple(data[0:0])

        else:
            return _sent, data

    try:
        data = iter(data)
        return next(data), data

    except StopIteration:

        return _sent, data


def _unpack_fn(fn: Fn[..., T], item: T) -> Fn[..., T]:
    require(callable(fn), f'this method takes a function but [{fn}] was found instead')

    if not hasattr(item, '__iter__') or fn == Pype:
        return fn

    # These two conditionals deal with built-in functions as they often don't have a __code__ attribute
    if fn in UNARY_WITHOUT_SIGNATURE or hasattr(fn, 'func') and fn.func in UNARY_WITHOUT_SIGNATURE:
        return fn

    if fn in N_ARY_WITHOUT_SIGNATURE or hasattr(fn, 'func') and fn.func in N_ARY_WITHOUT_SIGNATURE:
        return lambda iter_item: fn(*iter_item)

    try:
        num_args = 0

        for name, param in signature(fn).parameters.items():
            if param.default == Parameter.empty and \
                    param.kind != Parameter.KEYWORD_ONLY and \
                    param.kind != Parameter.VAR_KEYWORD and \
                    param.kind != Parameter.VAR_POSITIONAL and \
                    name != 'self' and \
                    type(fn) != type:
                num_args += 1
            elif param.kind == Parameter.VAR_POSITIONAL:
                num_args += 2
            elif name == 'self':
                num_args = 1
                break

            if num_args > 1:
                break

        if num_args > 1:
            return lambda iter_item: fn(*iter_item)

        return fn

    except Exception:

        return fn


def _pipe(*arg: Any, functions: Tuple[Fn[..., Any], ...]) -> Any:
    # created as global function to avoid issues with multiprocessing
    result = arg if len(arg) > 1 else arg[0]

    for fn in functions:
        result = fn(result)

    return result
