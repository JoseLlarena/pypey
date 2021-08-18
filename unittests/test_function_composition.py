""""
These tests test function composition utilities
"""
from pytest import raises

from pypey import pipe


def test_chains_unary_functions_with_non_iterable_returns():
    assert pipe(sum, abs)([-1, -2, -3]) == 6


def test_chains_n_nary_functions_with_tuple_returns():
    assert pipe(lambda n, m: (n * 2, m * 4), lambda nx2, mx4: nx2 % mx4 == 0)(2, 3) == False


def test_does_not_chain_n_nary_functions_with_non_tuple_iterable_returns():
    with raises(TypeError):
        pipe(lambda n, m: [n * 2, m * 4], lambda nx2, mx4: nx2 % mx4 == 0)(2, 3)


def test_chains_a_single_arg_function_after_a_tuple_returning_one_using_list():
    assert pipe(divmod, list, sum)(10, 3) == 4
