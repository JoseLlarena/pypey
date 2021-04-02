""""
These tests specify an operation's effect on an empty pipe.
"""
from collections import namedtuple
from operator import add
from os.path import join
from unittest.mock import Mock, create_autospec

import sys
from pytest import raises

from unittests import _empty_pype, _123_pype, _123


def test_iteration_is_not_possible():
    with raises(StopIteration):
        next(iter(_empty_pype()))


def test_accumulation_returns_empty_pipe():
    assert tuple(_empty_pype().accum(add)) == ()


def test_accumulation_with_initial_value_returns_pipe_with_initial_value():
    assert tuple(_empty_pype().accum(add, 0)) == (0,)


def test_concatenation_with_an_empty_pipe_returns_an_empty_pipe():
    assert tuple(_empty_pype().cat(_empty_pype())) == ()


def test_concatenation_with_a_non_empty_pipe_returns_that_pipe():
    assert tuple(_empty_pype().cat(_123_pype())) == _123


def test_chunking_returns_empty_pipe():
    assert tuple(_empty_pype().chunk(2)) == ()


def test_cloning_returns_empty_pipe():
    assert tuple(_empty_pype().clone()) == ()


def test_finite_cycling_returns_empty_pipe():
    assert tuple(_empty_pype().cycle(3)) == ()


def test_infinite_cycling_returns_empty_pipe():
    assert tuple(_empty_pype().cycle()) == ()


def test_distributing_items_returns_pipe_with_empty_pipes():
    assert tuple(map(tuple, tuple(_empty_pype().dist(3)))) == ((), (), ())


def test_dividing_pipe_returns_a_pipe_with_empty_pipes():
    assert tuple(map(tuple, tuple(_empty_pype().divide(3)))) == ((), (), ())


def test_produces_no_side_effect():
    side_effect = create_autospec(lambda n: n)

    assert tuple(_empty_pype().do(side_effect)) == ()

    side_effect.assert_not_called()


def test_produces_no_side_effect_when_immediate():
    side_effect = create_autospec(lambda n: n)

    assert tuple(_empty_pype().do(side_effect, now=True)) == ()

    side_effect.assert_not_called()


def test_produces_no_side_effect_when_run_in_parallel():
    side_effect = create_autospec(lambda n: n)

    assert tuple(_empty_pype().do(side_effect, now=True, workers=2)) == ()

    side_effect.assert_not_called()


def test_rejecting_items_until_condition_is_true_returns_empty_pipe():
    assert tuple(_empty_pype().drop_while(lambda n: n != 2)) == ()


def test_enumerating_items_returns_an_empty_pipe():
    assert tuple(_empty_pype().enum(start=1)) == ()


def test_enumerating_items_with_swapped_index_returns_an_empty_pipe():
    assert tuple(_empty_pype().enum(start=1, swap=True)) == ()


def test_flattening_an_empty_pipe_returns_an_empty_pipe():
    assert tuple(_empty_pype().flat()) == ()


def test_transforming_and_flattening_iterable_items_returns_empty_pipe():
    assert tuple(_empty_pype().flatmap(str.upper)) == ()


def test_grouping_items_by_key_in_empty_pipe_returns_empty_pipe():
    assert tuple(_empty_pype().group_by(len)) == ()


def test_asking_for_the_head_of_an_empty_pipe_returns_an_empty_pipe():
    assert tuple(_empty_pype().head(1)) == ()


def test_concise_iteration_is_not_possible():
    with raises(StopIteration):
        next(_empty_pype().__iter__())


def test_mapping_an_empty_pype_returns_an_empty_pipe():
    assert tuple(_empty_pype().map(lambda n: n * 2)) == ()


def test_mapping_an_empty_pype_with_workers_returns_an_empty_pipe():
    assert tuple(_empty_pype().map(lambda n: n * 2, workers=2)) == ()


def test_partitioning_an_empty_pipe_returns_a_pair_of_empty_pipes():
    assert tuple(map(tuple, _empty_pype().partition(lambda n: n < 2))) == ((), ())


def test_picking_item_property_in_an_empty_pipe_returns_an_empty_pipe():
    Person = namedtuple('Person', ['age'])

    assert tuple(_empty_pype().pick(Person.age)) == ()


def test_picking_iterable_items_key_in_an_empty_pipe_returns_an_empty_pipe():
    assert tuple(_empty_pype().pick(0)) == ()


def test_does_not_print_anything():
    mock_stdout = Mock(spec_set=sys.stdout)

    _empty_pype().print(file=mock_stdout, now=True)

    mock_stdout.write.assert_not_called()


def test_reduces_to_init_value():
    assert _empty_pype().reduce(lambda summation, n: summation + n, init=1) == 1


def test_reducing_without_init_value_throws_exception():
    """"""
    with raises(TypeError):
        _empty_pype().reduce(lambda summation, n: summation + n)


def test_rejecting_items_that_fulfill_predicate_for_an_empty_pipe_returns_an_empty_pipe():
    assert tuple(_empty_pype().reject(lambda n: n < 2)) == ()


def test_reversing_an_empty_pipe_returns_an_empty_pipe():
    assert tuple(_empty_pype().reverse()) == ()


def test_roundrobbining_items_from_an_empty_pipe_returns_an_empty_pipe():
    assert tuple(_empty_pype().roundrobin()) == ()


def test_sampling_0_items_returns_empty_pipe():
    assert tuple(_empty_pype().sample(0)) == ()


def test_sampling_more_than_0_items_throws_exception():
    """"""
    with raises(ValueError):
        tuple(_empty_pype().sample(1))


def test_selecting_any_items_returns_empty_pipe():
    assert tuple(_empty_pype().select(lambda n: n < 2)) == ()


def test_shuffling_items_returns_empty_pipe():
    assert tuple(_empty_pype().shuffle()) == ()


def test_size_is_zero():
    assert _empty_pype().size() == 0


def test_skipping_first_n_items_an_empty_pipe():
    assert tuple(_empty_pype().skip(2)) == ()


def test_slicing_returns_an_empty_pipe():
    assert tuple(_empty_pype().slice(1, 2)) == ()


def test_sorting_returns_an_empty_pipe():
    assert tuple(_empty_pype().sort()) == ()


def test_splitting_pipe_returns_empty_pipe():
    assert tuple(_empty_pype().select(lambda n: n < 2)) == ()


def test_asking_for_tail_returns_empty_pipe():
    assert tuple(_empty_pype().tail(2)) == ()


def test_taking_items_until_condition_is_true_returns_empty_pipe():
    assert tuple(_empty_pype().take_while(lambda n: n < 3)) == ()


def test_teeing_returns_a_pipe_with_n_empty_pipes():
    assert tuple(map(tuple, _empty_pype().tee(3))) == ((), (), ())


def test_applies_function_to_itself():
    assert _empty_pype().to(list) == []


def test_applies_functions_to_itself():
    assert _empty_pype().to(list, sorted) == []


def test_creates_file_but_does_not_write_anything_to_it(tmpdir):
    target = join(tmpdir, 'strings.txt')

    _empty_pype().to_file(target, now=True)

    with open(target) as file:
        assert file.readlines() == []


def test_asking_for_top_items_returns_empty_pipe():
    assert tuple(_empty_pype().top(2)) == ()


def test_asking_for_the_unique_items_returns_an_empty_pipe():
    assert tuple(_empty_pype().uniq()) == ()


def test_unzipping_returns_an_empty_pipe():
    assert tuple(map(tuple, _empty_pype().unzip())) == ()


def test_sliding_a_window_of_size_0_over_items_returns_a_pipe_with_empty_window():
    assert tuple(map(tuple, _empty_pype().window(0))) == ((),)


def test_sliding_a_window_of_size_greater_than_0_over_items_returns_a_pipe_with_a_None_filled_window():
    assert tuple(map(tuple, _empty_pype().window(2))) == ((None, None),)


def test_zipping_with_an_empty_pipe_returns_an_empty_pipe():
    assert tuple(_empty_pype().zip(_empty_pype())) == ()


def test_zipping_with_a_non_empty_pipe_returns_an_empty_pipe():
    assert tuple(_empty_pype().zip(_123_pype())) == ()


def test_non_truncating_zipping_with_a_non_empty_pipe_returns_that_pipe_paired_with_the_pad_value():
    assert tuple(_empty_pype().zip(_123_pype(), trunc=False, pad=-1)) == ((-1, 1), (-1, 2), (-1, 3))


def test_zipping_with_itself_returns_an_empty_pipe():
    assert tuple(_empty_pype().zip()) == ()


def test_zipping_with_a_function_returns_an_empty_pipe():
    assert tuple(_empty_pype().zip_with(lambda n: n * 2)) == ()
