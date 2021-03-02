from functools import partial
from typing import Any, Callable, TypeVar, Iterable, Type, Tuple, _VariadicGenericAlias

__all__ = ['Fn', 'H', 'I', 'K', 'T', 'V', 'X', 'Z', 'px', 'ident', 'pipe', 'require', 'require_val', 'throw']

H = TypeVar('H', Any, Any)
I = TypeVar('I', bound=Iterable)
K = TypeVar('K')
T = TypeVar('T')
V = TypeVar('V')
X = TypeVar('X')
Z = TypeVar('Z')

#:
Fn: _VariadicGenericAlias = Callable # Concise alias of ``typing.Callable``
#:
px: Callable[..., Callable] = partial # Concise alias of ``functools.partial``


def ident(item: T) -> T:
    """
    Identity function, returns the argument passed to it.

    :param item: any argument
    :return: the argument passed in
    """
    return item


def pipe(*functions: Fn[..., Any]) -> Fn[[Any], Any]:
    """
    Chains given functions. The first function can take any number of arguments, but the following ones must take
    a single argument.

    >>> from pypey import pipe
    >>> from math import sqrt
    >>> [pipe(len, sqrt)(w) for w in ('a', 'fun','day')]
    [1.0, 1.7320508075688772, 1.7320508075688772]

    :param functions: a variable number of function arguments
    :return: a combined function
    """
    return px(_pipe_functions, functions=functions)


def require(cond: bool, message: str, exception: Type[Exception] = TypeError):
    """
    Guard clause, useful for implementing exception-raising checks concisely, especially useful in lambdas.

    >>> from pypey import require
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

    >>> from pypey import require_val
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

    >>> from pypey import throw
    >>> pype([1,2,3]).do(lambda n: throw(ValueError, 'test'), now=True)
    Traceback (most recent call last):
      ...
    ValueError: test

    :param exception: the exception to throw
    :param message: the exception message
    :return: nothing
    """
    raise exception(message)


def _pipe_functions(arg: Any, functions: Tuple[Fn[..., Any], ...]) -> Any:
    # created as global function to avoid issues with multiprocessing
    result = arg

    for fn in functions:
        result = fn(result)

    return result
