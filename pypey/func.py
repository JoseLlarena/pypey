"""
Functions and constants for a concise use of higher-level functions
"""
from functools import partial
from typing import Any, Callable as Fn, TypeVar, Iterable, Type, Tuple

__all__ = ['Fn', 'H', 'I', 'K', 'T', 'V', 'X', 'Y', 'px', 'ident', 'pipe', 'require', 'require_val', 'throw']

H = TypeVar('H', Any, Any)
I = TypeVar('I', bound=Iterable)
K = TypeVar('K')
T = TypeVar('T')
V = TypeVar('V')
X = TypeVar('X')
Y = TypeVar('Y')

#: Concise alias of ``functools.partial``
px: Fn[..., Fn] = partial


def ident(item: T) -> T:
    """
    Identity function, returns the argument passed to it.

    :param item: any argument
    :return: the argument passed in
    """
    return item


def pipe(*functions: Fn) -> Fn:
    """
    Chains given functions.
    ::

        >>> from pypey import pipe
        >>> from math import sqrt
        >>> [pipe(len, sqrt)(w) for w in ('a', 'fun','day')]
        [1.0, 1.7320508075688772, 1.7320508075688772]

    For functions taking multiple arguments, the return of the previous function in the chain
    will be unpacked only if it's a ``tuple``:
    ::

        >>> from pypey import pipe
        >>> pipe(divmod, lambda quotient, remainder: quotient + remainder)(10, 3)
        4

    If a function returns an ``Iterable`` that it's not a tuple but unpacking in the next function is still needed,
    built-in ``tuple`` can be inserted in between to achieve the desired effect:
    ::

        >>> from pypey import pipe
        >>> pipe(range, tuple,  lambda _1, _2_, _3: sum([_1, _3]))(3)
        2

    Conversely, if a function returns a ``tuple`` but unpacking is not required in the next function, built-in ``list``
    can be used to achieve the desired effect:
    ::

        >>> from pypey import pipe
        >>> pipe(divmod, list, sum)(10, 3)
        4

    Note that ``list`` is the only exception to the rule that ``tuple`` returns will be unpacked.

    :param functions: a variable number of functions
    :return: a combined function
    """

    if len(functions) == 1:
        return functions[0]

    return px(_pipe_functions, functions=functions)


def require(cond: bool, message: str, exception: Type[Exception] = TypeError):
    """
    Guard clause, useful for implementing exception-raising checks concisely, especially useful in lambdas.

    >>> from pypey import require, pype
    >>> pype([1,2,'3']).do(lambda n: require(isinstance(n, int), 'not an int'), now=True)
    Traceback (most recent call last):
       ...
    TypeError: not an int

    :param cond: if ``False`` the given exception will be thrown, otherwise this function is a no-op
    :param message: exception message
    :param exception: exception to throw if ``cond`` is ``False``, defaults to ``TypeError``
    :return: nothing
    """
    if not cond:
        raise exception(message)


def require_val(cond: bool, message: str):
    """
    Throws ``ValueError`` exception if ``cond`` is ``False``, equivalent to :func:`require` with
    ``exception=ValueError``.

    >>> from pypey import require_val, pype
    >>> pype([1,2,-3]).do(lambda n: require_val(n>0, 'not a positive number'), now=True)
    Traceback (most recent call last):
        ...
    ValueError: not a positive number

    :param cond: if ``False`` the a ValueError will be thrown, otherwise this function is a no-op
    :param message: the exception message
    :return: nothing
    """
    require(cond, message, ValueError)


def throw(exception: Type[Exception], message: str):
    """
    Throws given exception with given message, equivalent to built-in ``raise``. This function is useful for raising
    exceptions inside lambdas as ``raise`` is syntactically invalid in them.

    >>> from pypey import throw, pype
    >>> pype([1,2,3]).do(lambda n: throw(ValueError, 'test'), now=True)
    Traceback (most recent call last):
      ...
    ValueError: test

    :param exception: the exception to throw
    :param message: the exception message
    :return: nothing
    """
    raise exception(message)


def _pipe_functions(*arg: Any, functions: Tuple[Fn[..., Any], Any]) -> Any:
    # created as global function to avoid issues with multiprocessing
    result = arg

    for idx, fn in enumerate(functions):
        result = fn(*result) if idx == 0 or (idx > 0 and fn != list and isinstance(result, tuple)) else fn(result)

    return result
