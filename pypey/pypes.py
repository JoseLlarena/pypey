"""
Contains factory for creating pype from different sources.
"""
from collections import abc
from os import PathLike
from pathlib import Path
from typing import Iterable, Tuple, Union, Mapping, Iterator, AnyStr, Optional

from pypey import require
from pypey.func import Fn, K, V, T, require_val
from pypey.pype import Pype

__all__ = ['Pyper', 'pype']


class Pyper:
    """
    Factory for creating new pipes. Use :data:`~pypey.pypes.pype` instance.

    >>> from pypey import pype
    >>> list(pype([1,2,3]))
    [1, 2, 3]
    """

    def __call__(self, iterable: Iterable[T]) -> Pype[T]:
        """
        Creates a ``Pype`` with the given backing ``Iterable``.

        :param iterable: backing ``Iterable`` for this `Pype``
        :return: a new `Pype`` backed by the given ``Iterable``
        :raise ``TypeError`` if ``iterable`` is not an ``Iterable``
        """

        try:
            iter(iterable)

            return Pype(iterable)

        except TypeError:

            raise

    @staticmethod
    def file(src: Union[AnyStr, PathLike, int],
             *,
             mode: str = 'r',
             buffering: int = -1,
             encoding: Optional[str] = 'utf8',
             errors: Optional[str] = None,
             newline: Optional[str] = None,
             closefd: bool = True,
             opener: Optional[Fn[..., int]] = None,
             strip: bool = True) -> Pype[str]:
        """
        Reads lines from given file into a pipe.

        >>> from pypey import pype
        >>> from os.path import join, dirname
        >>> list(pype.file(join(dirname(__file__), 'unittests', 'test_file.txt')))
        ['line 1', 'line 2', 'line 3']

        :param src: path to the file or file descriptor, as per built-in ``open``  ``file`` argument
        :param mode: mode as per built-in ``open``, except no write modes are allowed
        :param buffering: buffering as per built-in ``open``
        :param encoding: encoding as per built-in ``open`` except the default value is ``utf8`` instead of ``None``
        :param errors: errors as per built-in ``open``
        :param newline: newline as per built-in ``open``
        :param closefd:  closefd as per built-in ``open``
        :param opener: opener as per built-in ``open``
        :param strip: ``True`` if end of line should be removed from each line, ``False`` otherwise
        :return: a pipe where each item is a line in the given file
        :raises: ``ValueError`` if ``mode`` has ``w``  (write) or ``+`` (append) in it
        """

        require_val('w' not in mode and '+' not in mode and 'a' not in mode,
                    f'mode cannot be write or append but was [{mode}]')

        return Pype(
            _lines_from(src,
                        strip,
                        mode=mode,
                        buffering=buffering,
                        encoding=encoding,
                        errors=errors,
                        newline=newline,
                        closefd=closefd,
                        opener=opener))

    @staticmethod
    def dict(dictionary: Mapping[K, V]) -> Pype[Tuple[K, V]]:
        """
        Returns a pipe where each item is a key-value pair in the given ``Mapping``.

        >>> from pypey import pype
        list(pype.dict({'fun':1, 'day':2}))
        [('fun', 1), ('day', 2)]

        :param dictionary: the dictionary to pipe
        :return: a pipe containing the dictionary's items
        :raises: ``TypeError`` if dictionary is not a ``Mapping``
        """
        require(isinstance(dictionary, abc.Mapping), f'argument should be dict-like but was [{dictionary}]')

        return Pype(dictionary.items())


def _lines_from(src: Union[AnyStr, Path, int],
                strip: bool,
                mode: str,
                buffering: int,
                encoding: Optional[str],
                errors: Optional[str],
                newline: Optional[str],
                closefd: bool,
                opener: Optional[Fn[..., int]] = None) -> Iterator[str]:
    with open(src,
              mode=mode,
              buffering=buffering,
              encoding=encoding,
              errors=errors,
              newline=newline,
              closefd=closefd,
              opener=opener) as in_file:
        yield from map(str.rstrip, in_file) if strip else in_file


pype: Pyper = Pyper()
"""Pype factory"""
