"""
Pipeable versions of ``dict``, ``collections.defaultdict`` and ``collections.Counter``
"""
from __future__ import annotations

from collections import Counter, defaultdict, UserDict
from typing import TypeVar, Iterable, Tuple, Generic, Optional, Union, Set

from pypey.func import Y, Fn
from pypey.pype import Pype

K = TypeVar('K')
V = TypeVar('V')


class Dyct(Generic[K, V], UserDict):
    """
    A pipeable version of ``dict`` containing a superset of its methods
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def set(self, key: K, value: V) -> Dyct[K, V]:
        """
        sets given key to given value

        :param key: key to add/overwrite
        :param value: value to (re-)set
        :return: this ``Dyct``
        """
        self[key] = value

        return self

    def pype(self) -> Pype[Tuple[K, V]]:
        """
        Return this ``Dyct``-'s items as a ``Pype``

        :return: ``Pype`` containing pairs of key, values
        """
        return Pype(self.items())

    def reverse(self, overwrite: bool = True) -> Dyct[V, Union[K, Set[K]]]:
        """
        Reverses keys and values in this ``Dyct``. To prevent keys mapping to equal values from
        being lost, set ``overwrite`` to ``False`` and they will be kept in a ``Set``.

        :param overwrite: ``True`` if keys should be overwritten ``False`` if they should be preserved in a ``Set``.
        :return: a new ``Dyct`` with keys for values and values for keys
        """

        if not overwrite:

            value_to_keys = defaultdict(set)
            for key, value in self.items():
                value_to_keys[value].add(key)

            return Dyct(value_to_keys)

        return Dyct({value: key for key, value in self.items()})


class DefDyct(Generic[K, V], defaultdict):
    """
    A pipeable version of ``collections.defaultdict`` containing a superset of its methods
    """

    def __init__(self: DefDyct[K, V], default_factory: Optional[Fn[..., Y]], fn: Optional[Fn[[V, V], Y]] = None, *args,
                 **kwargs):
        super().__init__(default_factory, *args, **kwargs)
        self.fn = fn

    def add(self: DefDyct[K, V], key: K, value: V) -> DefDyct[K, V]:
        """
        Updates value associated with given key with given value, using ``fn`` passed in in constructor
        If ``fn`` is None, then inplace addition will be used.

        :param key: key to add
        :param value: value to add
        :return: this ``DefDyct``
        """
        if self.fn is None:
            self[key] += value
        else:
            self.fn(self[key], value)

        return self

    def pype(self: DefDyct[K, V]) -> Pype[Tuple[K, V]]:
        """
        Return this ``DefDyct``-'s items as a ``Pype``

        :return: ``Pype`` containing pairs of key, values
        """
        return Pype(self.items())


class CountDyct(Generic[K], Counter):
    """
       A pipeable version of ``collections.Counter`` containing a superset of its methods
       """

    def __init__(self: CountDyct[K], counts: Iterable[K] = ()):
        super().__init__(counts)

    def inc(self: CountDyct[K], item: K, count: int = 1) -> CountDyct[K]:
        """
        Increments counts of ``item`` by ``count``.

        :param item: item to be incremented
        :param count: increment
        :return: this ``CountDyct``
        """
        self[item] += count

        return self

    def pype(self: CountDyct[K]) -> Pype[Tuple[K, int]]:
        """
        Return this ``CountDyct``-'s items as a ``Pype``

        :return: ``Pype`` containing pairs of key, count
        """
        return Pype(item for item in self.items())
