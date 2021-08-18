"""
Specifies behaviour of various pipeable dictionary data structures
"""
from collections import Counter

from pypey import CountDyct, DefDyct, Dyct


def test_computes_counts_from_scratch():
    assert CountDyct().inc('a').inc('b', 2).pype().to(dict) == {'a': 1, 'b': 2}


def test_computes_counts_from_counter():
    assert set(CountDyct(Counter('ccd')).inc('a').inc('c', -1).items()) == {('a', 1), ('c', 1), ('d', 1)}


def test_chain_builds_dictionary():
    assert Dyct().set('a', 1).set('b', 2) == {'a': 1, 'b': 2}


def test_reverses_dictionary():
    assert Dyct(a=1, b=2).reverse() == {1: 'a', 2: 'b'}


def test_reverses_dictionary_keeping_all_keys():
    assert Dyct(a=1, b=1).reverse(overwrite=False) == {1: {'a', 'b'}}


def test_chain_builds_dictionary_with_default_factory():
    assert DefDyct(list, list.append).add('a', 1).add('b', 2).add('a', 1).pype().to(dict) == {'a': [1, 1], 'b': [2]}


def test_chain_builds_dictionary_without_default_factory():
    assert dict(DefDyct(int).add('a', 1).add('a', 2)) == {'a': 3}
