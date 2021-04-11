""""
These tests test function composition utilities
"""
from pypey import pipe


def test_chains_unary_functions():
    assert pipe(sum, abs)([-1, -2, -3]) == 6


def test_chains_n_nary_functions():
    assert pipe(lambda n, m: (n * 2, m * 2), lambda nx2, mx2: nx2 % mx2 == 0)([2, 3]) == False
