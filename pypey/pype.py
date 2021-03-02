from __future__ import annotations

from collections import defaultdict
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

from pathos.multiprocessing import ProcessPool
from collections.abc import Sized
from functools import reduce
from heapq import nlargest
from inspect import signature, Parameter
from itertools import chain, tee, accumulate, filterfalse, islice, dropwhile, takewhile, cycle, zip_longest
from more_itertools import tail, random_permutation, ichunked, unique_everseen, side_effect, partition, unzip, \
    always_reversible
from pathlib import Path
from sys import stdout
from typing import Iterator, Iterable, Tuple, Generic, Union, Any, Optional, List, AnyStr, IO, Sequence, NamedTuple

from pypey.func import Fn, ident, H, I, T, X, Z, require, require_val, pipe

__all__ = ['Pype']

logger = getLogger(__name__)

flatten = chain.from_iterable

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
N_ARY_WITHOUT_SIGNATURE = {breakpoint, filter, slice, range, zip}

_sent = object()


class Pype(Generic[T]):
    __slots__ = '_it',

    def __getstate__(self: Pype[T]) -> Iterable[T]:
        """
        Returns the state of this pipe as a tuple if it's not an eager collection already. This stops ``Dill`` from
        crashing due to long recursive calls, as it follows the object tree, when there are many instances  of
        a pipe in a pipe.

        :return: this pipe's backing ``Iterable`` if it's eager or a ``tuple`` based on it if it's not
        """

        return self._it if isinstance(self._it, Sized) else tuple(self._it)

    def __init__(self: Pype[T], it: Iterable[T]):
        """
        Creates pipe from ``Iterable``. No argument validation is carried out.

        :param it: an ``Iterable``
        """
        self._it = it

    def __iter__(self: Pype[T]) -> Iterator[T]:
        """
        Returns iterator either by a call on this pipe or by calling ``iter`` on it.

        :return: an ``Iterator`` for this pipe's data
        """
        return iter(self._data())

    def __setstate__(self: Pype[T], state: Iterable[T]):
        """
        Set this Pype's state to be the given iterable. This method is necessary because, and a counterpart of,
        ``Pype.__get_state__``.

        :param state: an iterable to be the state of this Pype
        :return: nothing
        """
        self._it = state

    def accum(self: Pype[T], fn: Fn[[Z, T], Z], init: Optional[Z] = None) -> Pype[Z]:
        """
        Returns a pipe where each item is the result of combining a running total with the corresponding item in the
        original pipe:
        ::

            >>> list(pype([1, 2, 3]).accum(lambda total, n: total+n))
            [1, 3, 6]

        When an initial value is given, the resulting pipe will have one more item than the original one:
        ::

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

            return Pype(accumulate(self._data(), fn, init))

        except TypeError:

            return Pype(_accumulate(self._data(), fn, init))

    def cat(self: Pype[T], other: Pype[Z]) -> Pype[Union[T, Z]]:
        """
        Concatenates this pipe with the given one.

        >>> list(pype([1, 2, 3]).cat(pype([4, 5, 6])))
        [1, 2, 3, 4, 5, 6]

        :param other: pipe to append to this one
        :return: a concatenated pipe
        :raises: ``TypeError`` if ``other`` is not a ``Pype``
        """
        require(isinstance(other, Pype), 'fn other needs to be a Pype')

        return Pype(chain(self._data(), other))

    def chunk(self: Pype[T], size: int) -> Pype[Pype[T]]:
        """
        Breaks pipe into sub-pipes with up to ``size`` items each:
        ::

            >>> [list(chunk) for chunk in pype([1, 2, 3, 4]).chunk(2)]
            [[1, 2], [3, 4]]

        If this pipe's size is not a multiple of ``size``, the last chunk will have fewer items than ``size``:
        ::

            >>> [list(chunk) for chunk in pype([1, 2, 3]).chunk(2)]
            [[1, 2], [3]]

        If ``size`` is larger than this pipe's size, only one chunk will be returned:
        ::

            >>> [list(chunk) for chunk in pype([1, 2, 3, 4]).chunk(5)]
            [[1, 2, 3, 4]]

        This method tees the backing ``Iterable``.

        Similar to ``more_itertools.ichunked``.

        :param size: chunk size
        :return: a pipe of pipes with up to size items each
        :raises: ``TypeError`` if ``size`` is not an ``int``
        :raises: ``ValueError`` if ``size`` is not positive
        """
        require(isinstance(size, int), f'n needs to be an int but was [{type(size)}]')
        require_val(size > 0, f'n needs to be greater than 0 but was [{size}]')

        return Pype(map(Pype, ichunked(self._data(), size)))

    def clone(self: Pype[T]) -> Pype[T]:
        """
        Lazily clones this pipe. This method tees the backing  ``Iterable`` and replaces it with a new copy.

        >>> list(pype([1, 2, 3]).clone())
        [1, 2, 3]

        Similar to ``itertools.tee``.

        :return: a copy of this pipe
        """

        self._it, copy = tee(self._it)

        return Pype(copy)

    def cycle(self: Pype[T], n: Optional[int] = None) -> Pype[T]:
        """
        Returns items in pipe ``n`` times if ``n`` is not ``None``:
        ::

            >>> list(pype([1, 2, 3]).cycle(2))
            [1, 2, 3, 1, 2, 3]

        else it returns infinite copies:
        ::

            >>> list(pype([1, 2, 3]).cycle().head(6))
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
        Returns a pipe with ``n`` items being smaller pipes containing this pipe's elements distributed equally
        amongst them:
        ::

            >>> [list(segment) for segment in pype([1, 2, 3, 4, 5, 6]).dist(2)]
            [[1, 3, 5], [2, 4, 6]]

        If this pipe's size is not evenly divisible by ``n``, then the size of the returned ``Iterable``
        items will not be identical:
        ::

            >>> [list(segment) for segment in pype([1, 2, 3, 4, 5]).dist(2)]
            [[1, 3, 5], [2, 4]]

        If this pipe's size is smaller than ``n``, the last pipes in the returned pipe will be empty:
        ::

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

            >>> [list(div) for div in pype([1, 2, 3, 4, 5, 6]).divide(2)]
            [[1, 2, 3], [4, 5, 6]]

        If this pipe's size is not a multiple of ``n``, the sub-pipes' sizes will be equal  except the last one, which
        will contain all excess items:
        ::

            >>> [list(div) for div in pype([1, 2, 3, 4, 5, 6, 7]).divide(3)]
            [[1, 2], [3, 4], [5, 6, 7]]

        If this pipe's size is smaller than ``n``, the resulting pipe will contain as many single-item pipes as there
        are in it, followed by ``n``  minus this pipe's size empty pipes.
        ::

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

    def do(self: Pype[T], fn: Fn[..., Any], *, now: bool = False, workers: int = 0) -> Pype[T]:
        """
        Produces a side effect for each item, with the given function's return value ignored. It is typically used to
        execute an operation that is not functionally pure such as printing to console, updating a GUI, writing to disk
        or sending data over a network.

        ::

            >>> p = pype(iter([1, 2, 3])).do(lambda n: print(f'{n}'))
            >>> list(p)
            1
            2
            3
            [1, 2, 3]

        If ``now`` is set to ``True`` the side effect will take place immediately and the backing ``Iterable`` will be
        consumed if lazy.
        ::

            >>> p = pype(iter([1, 2, 3])).do(lambda n: print(f'{n}'), now=True)
            1
            2
            3
            >>> list(p)
            []

        If ``workers`` is greater  than ``0`` the side effect will be parallelised using ``multiprocessing`` if
        possible, or ``pathos`` if not. ``pathos`` multiprocessing implementation is slower and limited vs the built-in
        multiprocessing but it does allow using lambdas. When using workers, the backing ``Iterable`` is teed to avoid
        consumption.

        Also known as ``for_each`` ``tap``, and ``sink``.

        Similar to ``more_itertools.side_effect``.

        :param workers: number of extra processes to parallelise this method's side effect function
        :param now: ``False`` to defer the side effect until iteration, ``True`` to write immediately
        :param fn: a function taking a possibly unpacked item
        :return: this pipe
        :raises: ``TypeError`` if ``fn`` is not a ``Callable`` or ``workers`` is not an ``int``
        :raises: ``ValueError`` if ``workers`` is negative
        """
        require(isinstance(workers, int), f'workers should be non-negative ints but where [{workers}]')

        ufn = _unpack_fn(fn)
        data = self._data()

        if workers:

            mapping = _parallel_map(px(self._data, True), ufn, workers)

            if now:
                for _ in mapping:
                    pass

                return self

            return Pype(side_effect(ident, mapping))

        if now:

            for item in data:
                ufn(item)

            return self

        return Pype(side_effect(ufn, data))

    def drop_while(self: Pype[T], pred: Fn[..., bool]) -> Pype[T]:
        """
        Drops items as long as the given predicate is ``True``; afterwards, it returns every item:

        >>> list(pype([1, 2, 3, 4]).drop_while(lambda n: n < 3))
        [3, 4]

        Similar to ``itertools.dropwhile``.

        :param pred: A function taking a possibly unpacked item and returning a boolean
        :return: a pipe that is a subset of this one
        :raises: ``TypeError`` if ``pred`` is not a ``Callable``
        """

        return Pype(dropwhile(_unpack_fn(pred), self._data()))

    def enum(self: Pype[T], start: int = 0, *, swap: bool = False) -> Pype[Union[Tuple[int, T], Tuple[T, int]]]:
        """
        Pairs each item with an increasing integer index:
        ::

            >>> list(pype(['a', 'fun', 'day']).enum())
            [(0, 'a'), (1, 'fun'), (2, 'day')]

        ``swap`` = ``True`` will swap the index and item around:
        ::

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

            >>> list(pype(['a', 'fun', 'day']).flat())
            ['a', 'f', 'u', 'n', 'd', 'a', 'y']

        Similar to ``itertools.chain.from_iterable``

        :return: a pipe with the elements of its ``Iterable`` items as items
        :raises: ``TypeError`` if items are not ``Iterable``
        """

        return Pype(flatten(self._data()))

    def flatmap(self: Pype[I], fn: Fn[..., I]) -> Pype[Z]:
        """
        Maps ``Iterable`` items and then flattens the result into their elements.

        Equivalent to :func:`Pype.map` followed by :func:`Pype.flat`

        :param fn: function taking a possibly unpacked item and returning a value
        :return: a pipe with mapped flattened items
        :raises: ``TypeError`` if items are not ``Iterable`` or ``fn`` is not a ``Callable``
        """

        return Pype(flatten(map(_unpack_fn(fn), self._data())))

    def group_by(self: Pype[T], key: Fn[..., Z]) -> Pype[Tuple[Z, List[T]]]:
        """
        Groups items according to the key returned by the ``key`` function:

        >>> list(pype(['a', 'fun', 'day']).group_by(len))
        [(1, ['a']), (3, ['fun', 'day'])]

        This method is eager and will consume the backing ``Iterable`` if it's lazy.

        Similar to ``itertools.groupby`` except elements don't need to be sorted

        :param key:  function taking a possibly unpacked item and returning a grouping key
        :return: a pipe made up of pairs of keys and lists of items
        :raises: ``TypeError`` if ``fn`` is not a ``Callable``
        """

        return Pype(_deferred_group_by(self._data(), _unpack_fn(key)))

    def head(self: Pype[T], n: int) -> Pype[T]:
        """
        Selects the first n items of this pipe.

        >>> list(pype([1, 2, 3]).head(2))
        [1, 2]

        Also known as ``take``.

        :param n: the number of elements to take
        :return: a pipe with the first ``n``  elements of this pipe
        :raises: ``TypeError`` if ``n`` is not an ``int``
        :raises: ``ValueError`` if ``n`` is negative
        """
        require(isinstance(n, int), f'n needs to be an int but was [{type(n)}]')

        return Pype(islice(self._data(), n))

    def it(self: Pype[T]) -> Iterator[T]:
        """
        Returns an ``Iterator`` for this pipe's items. It's a more concise version of, and functionally identical
        to, :func:`Pype.__iter__`

        >>> list(iter(pype([1, 2, 3]))) == list(pype([1, 2, 3]).it())
        True

        :return: an ``Iterator`` for this pipe's items
        """
        return iter(self)

    def map(self: Pype[T], fn: Fn[..., Z], *other_fns: Fn[..., X], workers: int = 0) -> Pype[Union[X, Z]]:
        """
        Transforms this pipe's items according to the given function(s):
        ::

            >>> from math import sqrt
            >>> list(pype([1, 2, 3]).map(sqrt))
            [1.0, 1.4142135623730951, 1.7320508075688772]

        If more than one function is provided, they will be chained into a single one before being applied to each item:
        ::

            >>> list(pype(['a', 'fun', 'day']).map(str.upper, reversed, list))
            [['A'], ['N', 'U', 'F'], ['Y', 'A', 'D']]

        If ``workers`` is greater than ``0`` the mapping will be parallelised using ``multiprocessing`` if possible
        or ``pathos`` if not. ``pathos`` multiprocessing implementation is slower and has different limitations than the
        built-in multiprocessing but it does allow using lambdas. When using workers, the backing ``Iterable`` is teed
        to avoid consumption.

        Similar to built-in ``map``.

        :param workers: number of extra processes to parallelise this method's mapping function(s)
        :param fn: a function taking a possibly unpacked item and returning a value
        :param other_fns: other functions to be chained with ``fn``, taking a possibly unpacked item and returning a
            value
        :return: a pipe with this pipe's items mapped to values
        :raises: ``TypeError`` if ``fn`` is not a ``Callable`` or ``other_fns`` is not a ``tuple`` of ``Callable`` or
            if ``workers`` is not an ``int``
        :raises: ``ValueError`` if ``workers`` is negative
        """
        require(isinstance(workers, int), f'workers should be non-negative ints but where [{workers}]')

        combo_fn = pipe(*map(_unpack_fn, (fn,) + other_fns))

        if workers:
            return Pype(_parallel_map(px(self._data, True), combo_fn, workers))

        return Pype(map(combo_fn, self._data()))

    def partition(self: Pype[T], pred: Fn[..., bool]) -> Tuple[Pype[T], Pype[T]]:
        """
        Splits this pipe's items into two pipes, according to whether the given
        predicate returns ``True`` or ``False``:

        >>> [list(p) for p in pype([1, 2, 3, 4]).partition(lambda n: n%2)]
        [[2, 4], [1, 3]]

        This method tees the backing ``Iterable``.

        :param pred: A function taking a possibly unpacked item and returning a boolean
        :return: a 2-``tuple`` with the first item being a pipe with items for which ``pred`` is ``False`` and the second a
            pipe with items for which it is ``True``
        :raises: ``TypeError`` if ``pred`` is not a ``Callable``
        """
        falses, trues = partition(_unpack_fn(pred), self._data())

        return Pype(falses), Pype(trues)

    def pick(self: Pype[Union[Sequence, NamedTuple]], key: Any) -> Pype[Any]:
        """
        Maps each item to the given ``key``. Allowed keys are any supported by ``object.__item__`` :
        ::

            >>> list(pype(['a', 'fun', 'day']).pick(0))
            ['a', 'f', 'd']

        as well as ``@property``-defined object properties and ``namedtuple`` attributes:
        ::

            >>> from collections import namedtuple
            >>> Person = namedtuple('Person', ['age'])
            >>> list(pype([Person(42), Person(24)]).pick(Person.age))
            [42, 24]

        Similar to :func:`Pype.map(lambda item: item.key)` and :func:`Pype.map(lambda item: item[key])`.

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
              now: bool = False) -> Pype[T]:
        """
        Prints string returned by given function using ``print``:
        ::

            >>> p = pype(iter([1, 2, 3])).print()
            >>> list(p)
            1
            2
            3
            [1, 2, 3]

        If ``now`` is set to ``True``, the printing  takes place immediately and the backing ``Iterable`` is consumed:
        ::

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

        ufn = fn if fn == str else _unpack_fn(fn)

        if now:

            for item in self._data():
                print(ufn(item), sep=sep, end=end, file=file, flush=flush)

            return self

        return self.do(px(_print_fn, ufn=ufn, sep=sep, end=end, file=file, flush=flush))

    def reduce(self: Pype[T], fn: Fn[[H, T], H], init: Optional[Z] = None) -> H:
        """
        Reduces this pipe to a single value through the application of the given aggregating function
        to each item:
        ::

            >>> from operator import add
            >>> pype([1, 2, 3]).reduce(add)
            6

        If ``init`` is not ``None``, it will be placed as the start of the returned pipe and
        serve as a default value in case the pipe is empty:
        ::

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

            >>> list(pype(['a', 'FUN', 'day']).reject(str.isupper))
            ['a', 'day']

        Opposite of :func:`Pype.select`.

        Similar to built-in ``filterfalse``.

        :param pred: a function taking a possibly unpacked item and returning a boolean
        :return: a pipe with the subset of this pipe's items for which ``pred`` returns ``False``
        :raises: ``TypeError`` if ``pred`` is not a ``Callable``
        """

        return Pype(filterfalse(_unpack_fn(pred), self._data()))

    def reverse(self: Pype[T]) -> Pype[T]:
        """
        Returns a pipe where this pipe's items appear in reversed order:
        ::

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

        >>> list(pype(['a', 'fun', 'day']).roundrobin())
        ['a', 'f', 'd', 'u', 'a', 'n', 'y']

        This operation is eager but deferred.

        Similar to ``more_itertools.interleave_longest``.

        :return: A pipe with items taken from this pipe's ``Iterable`` items
        :raises: ``TypeError`` if any of this pipe's items is not an ``Iterable``
        """
        # implementation based on ``more_itertools.interleave_longest``
        return Pype(_deferred_roundrob(self._data()))

    def sample(self: Pype[T], k: int, seed_: Optional[Any] = None) -> Pype[T]:
        """
        Returns a pipe with ``k`` items sampled without replacement from this pipe:

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

        require(isinstance(k, int), f'n needs to be an int but was [{type(k)}]')

        return Pype(_deferred_sample(self._data(), k))

    def select(self: Pype[T], pred: Fn[..., bool]) -> Pype[T]:
        """
        Returns a pipe with only the items for each ``pred`` returns ``True``, opposite of :func:`Pype.reject`:
        ::

            >>> list(pype(['a', 'FUN', 'day']).select(str.isupper))
            ['FUN']

        Also known as ``filter``.

        Similar to built-in ``filter``.

        :param pred: a function taking a possibly unpacked item and returning a boolean
        :return: a pipe with the subset of this pipe's items for which the given ``pred`` returns ``True``
        :raises: ``TypeError`` if ``pred`` is not a ``Callable``
        """

        return Pype(filter(_unpack_fn(pred), self._data()))

    def shuffle(self: Pype[T], seed_: Optional[Any] = None) -> Pype[T]:
        """
        Returns a shuffled version of this pipe:
        ::

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

            >>> pype([1, 2, 3]).size()
            3

        This operation is eager and immediate.

        :return: an ``integer`` correpsonding to the cardinality of this pipe
        """

        if isinstance(self._it, Sized):
            return len(self._it)

        return sum(1 for _ in self._data())

    def skip(self: Pype[T], n: int) -> Pype[T]:
        """
        Returns a pipe with the first ``n`` items missing:
        ::

            >>> list(pype([1, 2, 3, 4]).skip(2))
            [3, 4]

        :param n: number of items to skip
        :return: pipe with ``n`` skipped items
        :raises: ``TypeError`` if ``n`` is not an ``int``
        :raises: ``ValueError`` if ``n`` is negative
        """
        require(isinstance(n, int), f'start needs to be an int but was [{type(n)}]')
        require_val(n >= 0, f'start cannot be larger than end but was [{n}]')

        if n == 0:
            return self

        return Pype(islice(self._data(), n, None))

    def slice(self: Pype[T], start: int, end: int) -> Pype[T]:
        """
        Returns a slice of this pipe between items at positions ``start`` and ``end``, exclusive:
        ::

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

    def sort(self: Pype[T], key: Optional[Fn[..., Z]] = None, *, rev: bool = False) -> Pype[T]:
        """
        Sorts this pipe's items, using the return value of ``key`` if not ``None``:
        ::

            >>> list(pype(['a', 'funny', 'day']).sort(len))
            ['a', 'day', 'funny']

        This method is eager but deferred.

        Similar to builtin ``sorted``.

        :param key: a function possibly taking a unpacked item and returning a value to sort by, or ``None``
        :param rev: ``True`` if the sort order should be reversed, ``False`` otherwise.
        :return: a sorted pipe
        :raises: ``TypeError`` if ``key`` is not a ``Callable``
        """

        return Pype(_deferred_sort(self._data(), None if key is None else _unpack_fn(key), rev))

    def split(self: Pype[T], when: Fn[..., bool]) -> Pype[Pype[T]]:
        """
        Returns a pipe containing sub-pipes split off this pipe where the given ``when`` predicate is ``True``:
        ::

            >>> [list(split) for split in pype([1, 2, 3, 4, 5]).split(lambda n: n%3)]
            [[1], [2, 3], [4], [5]]

        Similar to ``more_itertools.split_before``.

        :param when: A function possibly taking a unpacked item and returning ``True`` if this pipe should be split
            before this item
        :return: a pipe of pipes split off this pipe at items where ``when`` returns ``True``
        :raises: ``TypeError`` if ``when`` is not a ``Callable``
        """
        # implementation based on ``more_itertools.split_before``
        return Pype(map(Pype, _split(self._data(), _unpack_fn(when))))

    def tail(self: Pype[T], n: int) -> Pype[T]:
        """
        Returns a pipe containing the last ``n`` items of this pipe:
        ::

            >>> list(pype([1, 2, 3, 4]).tail(2))
            [3, 4]

        This operation is eager but deferred.

        :param n: a positive ``int`` specifying the number of items of this pipe's tail
        :return: a pipe with this pipe's last ``n`` items
        :raises: ``TypeError`` if ``n`` is not an ``int``
        :raises: ``ValueError`` if ``n`` is negative
        """
        require(isinstance(n, int), f'n needs to be an int but was [{type(n)}]')

        return Pype(_deferred_tail(self._data(), n))

    def take_while(self: Pype[T], pred: Fn[..., bool]) -> Pype[T]:
        """
        Returns a pipe containing this pipe's items until ``pred`` returns ``False`` :
        ::

            >>> list(pype([1, 2, 3, 4]).take_while(lambda n: n < 4))
            [1, 2, 3]

        Similar to ``itertools.takewhile``.

        :param pred: a function taking a possibly unpacked item and returning a boolean
        :return: a pipe that is a subset of this one minus the items after ``pred`` returns ``True``
        :raises: ``TypeError`` if ``pred`` is not a ``Callable``
        """

        return Pype(takewhile(_unpack_fn(pred), self._data()))

    def tee(self: Pype[T], n: int) -> Pype[Pype[T]]:
        """
        Returns ``n`` lazy copies of this pipe:

        >>> [list(copy) for copy in pype([1, 2, 3]).tee(2)]
        [[1, 2, 3], [1, 2, 3]]

        This method tees the backing ``Iterable`` but does not replace it (unlike :func:`Pype.clone`).

        Similar to ``itertools.tee``.

        :return: a pipe containing ``n`` copies of this pipe
        :raises: ``TypeError`` if ``n`` is not an ``int``
        :raises: ``ValueError`` if ``n`` is non-positive
        """
        require_val(n > 0, f'number of copies should be greater than 0 but was [{n}]')

        return Pype(map(Pype, tee(self._it, n)))

    def to(self: Pype[T], fn: Fn[[Iterable[T]], Z,], *other_fns: Fn[..., X]) -> Union[Z, X]:
        """
        Applies given function to this pipe:
        ::

            >>> pype(['a', 'fun', 'day']).to(list)
            ['a', 'fun', 'day']

        This method is eager if the given function is eager and lazy if it's lazy:
        ::

            >>> p = pype(['a', 'fun', 'day']).to(enumerate)
            >>> p
            <enumerate object at 0x7fdb743003c0>
            >>> list(p)
            [(0, 'a'), (1, 'fun'), (2, 'day')]

        If provided with more than one function, it will pipe them together:
        ::

            >>> pype(['a', 'fun', 'day']).to(list, len)
            3

        Equivalent to ``fn_n(...fn2(fn1(pipe)))``.

        :param fn: function to apply to this pipe
        :param other_fns: other functions to be chained with ``fn``
        :return: the return value of the given function(s)
        :raises: ``TypeError`` if any of the provided functions is not a ``Callable``
        """

        return pipe(*(fn,) + other_fns)(self._data())

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
            >>> p = pype(['a', 'fun', 'day']).to_file(join(gettempdir(), 'afunday.txt'), eol=False)
            >>>list(p)
            ['a', 'fun', 'day']
            >>> list(pype.file(join(gettempdir(), 'afunday.txt')))
            ['afunday']

        The first eight parameters are identical to built-in ``open``. If ``eol`` is set to ``True``, each item will be
        converted to string and a line terminator will be appended to it:
        ::

            >>> p = pype([1, 2, 3]).to_file(join(gettempdir(), '123.txt', eol=True))
            >>> list(p)
            [1, 2, 3]
            >>> list(pype.file(join(gettempdir(), '123.txt')))
            ['1', '2', '3']


        This method is intrinsically lazy but it's set to immediate/eager by default. As such, if ``now`` is set to
        ``True`` and the backing ``Iterable`` is lazy, it will be consumed and this method will return an empty pipe:
        ::

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
        :raises: ``ValuError`` if ``mode`` has `r` or `+`
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

    def top(self: Pype[T], n: int, key: Fn[[T], Any] = ident) -> Pype[T]:
        """
        Returns a pipe with the ``n`` items having the highest value, as defined by the ``key`` function.

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

        return Pype(_deferred_top(self._data(), n, _unpack_fn(key)))

    def uniq(self: Pype[T]) -> Pype[T]:
        """
        Returns unique number of items:
        ::

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

            >>> list(pype(iter([1, 2, 3])).window(size=2))
            [(1, 2), (2, 3)]

        If ``size`` is larger than this pipe, ``pad`` is used fill in the missing values:
        ::

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
            *other_pipes: Pype[Any],
            trunc: bool = True,
            pad: Optional[Any] = None) -> Pype[Tuple[T, ...]]:
        """
        Zips items in this pipe with each other or with items in each of the given pipes. If no pipes are provided,
        the items in this pipe will be zipped with each other:
        ::

            >>> list(pype(['a', 'fun', 'day']).zip(trunc=False, pad='?'))
            [('a', 'f', 'd'), ('?', 'u', 'a'), ('?', 'n', 'y')]


        Self-zipping will consume the backing ``Iterable`` if it's lazy. If other pipes are provided, the items in this
        pipe will be zipped with the items in those:
        ::

            >>> list(pype(['a', 'fun', 'day']).zip(pype([1, 2, 3, 4])))
            [('a', 1), ('fun', 2), ('day', 3)]

        Similar to :func:`built-in zip` and :func:`itertools.zip_longest`.

        :param other_pipes: pipes to be zipped with this with this one
        :param trunc: ``True`` to truncate all ``Iterable``-s or ``Pype``-s to the size of the shortest one, ``False``
            to  pad all to the size of the longest one
        :param pad: value to pad shorter ``Iterable``-s or ``Pype``-s with if ``trunc`` is ``False``; if it's ``True``
            it's ignored
        :return: a pipe with the zipped items of this pipe with each other or with the given pipes' ones
        :raises: ``TypeError`` any of the other pipes is not a ``Pype``
        """
        data = self._data()

        if other_pipes == ():
            return Pype(_deferred_zip(data, trunc, pad))

        require(all(isinstance(p, Pype) for p in other_pipes), f'other pipes should be Pypes but were {other_pipes}')

        return Pype(zip(data, *other_pipes) if trunc else zip_longest(data, *other_pipes, fillvalue=pad))

    def zip_with(self: Pype[T], fn: Fn[..., Z]) -> Pype[Tuple[T, Z]]:
        """
        Returns a pipe where each item is a 2-``tuple`` with this pipe's item as the first and the output of ``fn``
        as the second. This is useful for adding an extra piece of data to the current pipeline:
        ::

            >>> list(pype(['a','fun', 'day']).zip_with(len))
            [('a', 1), ('fun', 3), ('day', 3)]

        and it's a more concise version of:
        ::

            >>> list(pype(['a','fun', 'day']).map(lambda w: (w, len(w))))
            [('a', 1), ('fun', 3), ('day', 3)]

        :param fn: a function taking a possibly unpacked item and returning a value to be zipped with this pipe's item
        :return: a new pipe with zipped items
        """

        return self.map(lambda item: (item, _unpack_fn(fn)(item)))

    def _data(self: Pype[T], should_tee: bool = False) -> Iterable[T]:

        if should_tee and not isinstance(self._it, Sized):
            self._it, copy = tee(self._it)
            return copy

        return self._it


def _accumulate(data: Iterable[T], func: Fn[[H, T], H], initial: Optional[H] = None) -> Iterator[H]:
    it = iter(data)
    total = initial

    if initial is None:
        try:
            total = next(it)
        except StopIteration:
            return None

    yield total

    for element in it:
        total = func(total, element)
        yield total


def _deferred_divide(data: Iterable[T], n: int) -> Iterator[Iterable[T]]:
    _data = data if isinstance(data, Sequence) else tuple(data)

    size, rem = divmod(len(_data), n)

    if not rem:

        yield from chain((_data[s * size: (s + 1) * size] for s in range(n)))

    elif len(_data) > n:

        yield from chain((_data[s * size: (s + 1) * size] for s in range(n - 1)), (_data[-(size + rem):],))

    else:

        yield from chain(((item,) for item in _data), (() for _ in range(n - len(_data))))


def _deferred_group_by(data: Iterable[T], key: Fn[..., Z]) -> Iterator[Tuple[Z, List[T]]]:
    key_to_group = defaultdict(list)

    for element in data:
        key_to_group[key(element)].append(element)

    yield from key_to_group.items()


def _deferred_reverse(data: Iterable[T]) -> Iterator[T]:
    yield from always_reversible(data)


def _deferred_roundrob(data: Iterable[I]) -> Iterator[T]:
    yield from filterfalse(px(eq, _sent), flatten(zip_longest(*data, fillvalue=_sent)))


def _deferred_sample(data: Iterable[T], k: int) -> Iterator[T]:
    yield from random_permutation(data, k)


def _deferred_shuffle(data: Iterable[T]) -> Iterator[T]:
    data = list(data)
    shuffle(data)

    yield from data


def _deferred_sort(data: Iterable[T], key: Optional[Fn[..., Z]], rev: bool) -> Iterator[T]:
    yield from sorted(data, key=key, reverse=rev)


def _deferred_tail(data: Iterable[T], n: int) -> Iterator[T]:
    yield from tail(n, data)


def _deferred_top(data: Iterable[T], n: int, key: Fn[..., Any]) -> Iterator[T]:
    yield from nlargest(n, data, key) if n > 1 else [max(data, key=key)]


def _deferred_unzip(data: Iterable[I]) -> Iterator[Any]:
    yield from unzip(data)


def _deferred_zip(data: Iterable[I], trunc: bool, pad: Any) -> Iterator[Tuple[Any, ...]]:
    yield from zip(*data) if trunc else zip_longest(*data, fillvalue=pad)


def _lines_to(target: Union[AnyStr, Path, int], lines: Iterable[T], eol: bool, **kwargs) -> Iterator[T]:
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


def _parallel_map(data_gen: Fn[[], Iterable[T]], fn: Fn[..., Z], workers: int) -> Iterable[Z]:
    try:

        with Pool(workers) as pool:
            return pool.map(fn, data_gen())

    except (PicklingError, AttributeError):

        logger.debug('multiprocessing with pickling failed, using pathos with dill instead.')

        with ProcessPool(workers) as pool:
            return pool.map(fn, data_gen())


def _print_fn(item: T, ufn: Fn[..., Z], sep: str, end: str, file: IO, flush: bool):
    # created as global function to avoid issues with multiprocessing
    print(ufn(item), sep=sep, end=end, file=file, flush=flush)


def _split(data: Iterable[T], pred: Fn[..., bool]) -> Iterator[List[T]]:
    buf: List[T] = []

    for item in data:

        if pred(item) and buf:
            yield buf
            buf = []

        buf.append(item)

    yield buf


def _unpack_fn(fn: Fn[..., T]) -> Fn[..., T]:
    # These conditionals are necessary because a number of built-in functions throw exceptions when calling
    # introspect.signature on them
    if fn in UNARY_WITHOUT_SIGNATURE or hasattr(fn, 'func') and fn.func in UNARY_WITHOUT_SIGNATURE:
        return fn

    if fn in N_ARY_WITHOUT_SIGNATURE or hasattr(fn, 'func') and fn.func in N_ARY_WITHOUT_SIGNATURE:
        return px(_unpacked_fn, fn=fn)

    try:

        params = signature(fn).parameters.values()

    except ValueError as e:

        if 'no signature found for builtin' in str(e):
            logger.debug(f'cannot introspect signature of function [{fn}], unable to unpack items.')
            return fn

        raise e

    num_args = len(tuple(filter(lambda par: par.default == par.empty and par.kind != Parameter.VAR_KEYWORD, params)))

    if num_args <= 1:
        return fn

    return px(_unpacked_fn, fn=fn)


def _unpacked_fn(item: Any, fn: Fn[..., Z]) -> Z:
    # created as global function to avoid issues with multiprocessing
    return fn(*item) if hasattr(item, '__iter__') else fn(item)
