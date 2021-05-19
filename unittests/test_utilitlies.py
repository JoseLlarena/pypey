from pypey import pype, throw
from pytest import raises
from math import sqrt


def test_throw_raises_exception_in_pipeline():
    with raises(ValueError):
        tuple(pype([-1, 0, 1]).map(lambda n: throw(ValueError, f' non-negative input {n}' if n < 0 else sqrt(n))))
