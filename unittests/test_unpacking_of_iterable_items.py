""""
These tests check that operations taking a unary function as argument, also
support unpacking the input item to an equivalent variadic argument function.
"""

from unittest.mock import Mock, call, create_autospec

import sys

from unittests import _112233_pype, _112233, _aAfunFUNdayDAY_pype


def test_broadcasts_unpacked_items():
    assert list(_112233_pype().broadcast(lambda left, right: [left + 1, right + 2])) == \
           [((1, 1), 2), ((1, 1), 3), ((2, 2), 3), ((2, 2), 4), ((3, 3), 4), ((3, 3), 5)]


def test_produces_a_side_effect_per_unpacked_item():
    side_effect = create_autospec(lambda left, right: None)

    assert tuple(_112233_pype().do(side_effect)) == _112233

    side_effect.assert_has_calls([call(1, 1), call(2, 2), call(3, 3)])


def test_rejects_unpacked_items_until_condition_is_true():
    assert tuple(_112233_pype().drop_while(lambda left, right: left != 2)) == _112233[1:]


def test_transforms_unpacked_iterable_items_and_flattens_them_into_a_pipe_of_elements():
    def fn(lower: str, _: str) -> str:
        return lower.title()

    assert tuple(_aAfunFUNdayDAY_pype().flatmap(fn)) == ('A', 'F', 'u', 'n', 'D', 'a', 'y')


def test_groups_unpacked_items_by_given_key():
    assert tuple(_aAfunFUNdayDAY_pype().group_by(lambda lower, upper: len(lower))) \
           == ((1, [('a', 'A')]), (3, [('fun', 'FUN'), ('day', 'DAY')]))


def test_maps_unpacked_items():
    assert tuple(_112233_pype().map(lambda left, right: left * 2)) == (2, 4, 6)


def test_partitions_unpacked_items_according_to_predicate_into_a_tuple__of_pipes():
    assert tuple(map(tuple, _112233_pype().partition(lambda left, right: left < 2))) == (_112233[1:], (_112233[0:1]))


def test_prints_each_iterable_item_using_str():
    mock_stdout = Mock(spec_set=sys.stdout)

    _112233_pype().print(lambda left, right: str(left), file=mock_stdout, now=True)

    mock_stdout.write.assert_has_calls([call('1'), call('\n'), call('2'), call('\n'), call('3'), call('\n')])


def test_rejects_unpacked_items_that_fulfil_predicate():
    assert tuple(_112233_pype().reject(lambda left, right: left < 2)) == _112233[1:]


def test_selects_unpacked_items_that_fulfil_predicate():
    assert tuple(_112233_pype().select(lambda left, right: left < 2)) == _112233[0:1]


def test_sorts_with_key_unpacking_items():
    assert tuple(_aAfunFUNdayDAY_pype().sort(lambda left, right: left)) == (('a', 'A'), ('day', 'DAY'), ('fun', 'FUN'))


def test_splits_after_unpacked_items_fulfil_predicate():
    assert tuple(map(tuple, _112233_pype().split(lambda left, right: left == 2))) == (_112233[0:2], _112233[2:])


def test_splits_before_unpacked_items_fulfil_predicate():
    assert tuple(map(tuple, _112233_pype().split(lambda left, right: left == 2, 'before'))) \
           == (_112233[0:1], _112233[1:])


def test_splits_before_and_after_unpacked_items_fulfil_predicate():
    assert tuple(map(tuple, _112233_pype().split(lambda left, right: left == 2, 'at'))) == (_112233[0:1], _112233[2:])


def test_takes_items_while_predicate_with_unpacked_is_true():
    assert tuple(_112233_pype().take_while(lambda left, right: left < 3)) == _112233[0:2]


def test_zips_with_output_of_function_taking_unpacked_items():
    assert tuple(_112233_pype().zip_with(lambda left, right: left + right)) == (((1, 1), 2), ((2, 2), 4), ((3, 3), 6))
