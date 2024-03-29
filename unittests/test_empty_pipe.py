""""
These tests specify an operation's effect on an empty pipe.
"""
from collections import namedtuple
from operator import add
from os.path import join
from unittest.mock import Mock, create_autospec

import json
import sys
from pytest import raises, mark

from pypey import Pype, TOTAL
from unittests import _empty_pype, _123_pype, _123, _a_fun_day_pype


def test_iteration_is_not_possible():
    with raises(StopIteration):
        next(iter(_empty_pype()))


def test_accumulation_returns_empty_pipe():
    assert tuple(_empty_pype().accum(add)) == ()


def test_accumulation_with_initial_value_returns_pipe_with_initial_value():
    assert tuple(_empty_pype().accum(add, 0)) == (0,)


def test_broadcasting_returns_empty_pype():
    assert tuple(_empty_pype().broadcast(range)) == ()


def test_concatenation_with_an_empty_iterable_returns_an_empty_pipe():
    assert tuple(_empty_pype().cat(_empty_pype())) == ()


def test_concatenation_with_a_non_empty_iterable_returns_that_iterable():
    assert tuple(_empty_pype().cat(_123_pype())) == _123


def test_chunking_returns_empty_pipe():
    assert tuple(_empty_pype().chunk(2)) == ()


def test_chunking_with_multiple_sizes_returns_as_many_empty_pipes_as_there_are_sizes():
    assert tuple(map(tuple, _empty_pype().chunk([2, 3]))) == ((), ())


def test_cloning_returns_empty_pipe():
    assert tuple(_empty_pype().clone()) == ()


def test_finite_cycling_returns_empty_pipe():
    assert tuple(_empty_pype().cycle(3)) == ()


def test_infinite_cycling_returns_empty_pipe():
    assert tuple(_empty_pype().cycle()) == ()


def test_distributing_items_returns_pipe_with_n_empty_pipes():
    assert tuple(map(tuple, tuple(_empty_pype().dist(3)))) == ((), (), ())


def test_dividing_pipe_returns_a_pipe_with_n_empty_pipes():
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


def test_dropping_first_n_items_returns_an_empty_pipe():
    assert tuple(_empty_pype().drop(2)) == ()


def test_dropping_last_n_items_returns_an_empty_pipe():
    assert tuple(_empty_pype().drop(-1)) == ()


def test_rejecting_items_until_condition_is_true_returns_empty_pipe():
    assert tuple(_empty_pype().drop_while(lambda n: n != 2)) == ()


def test_making_an_empty_pipe_eager_returns_an_empty_pipe():
    assert tuple(_empty_pype().eager()) == ()


def test_enumerating_items_returns_an_empty_pipe():
    assert tuple(_empty_pype().enum(start=1)) == ()


def test_enumerating_items_with_swapped_index_returns_an_empty_pipe():
    assert tuple(_empty_pype().enum(start=1, swap=True)) == ()


def test_flattening_an_empty_pipe_returns_an_empty_pipe():
    assert tuple(_empty_pype().flat()) == ()


def test_transforming_and_flattening_iterable_items_returns_empty_pipe():
    assert tuple(_empty_pype().flatmap(str.upper)) == ()


def test_computing_item_frequencies_returns_zero_frequency_total():
    assert tuple(_empty_pype().freqs()) == ((TOTAL, 0, 0.0),)


def test_computing_item_frequencies_returns_empty_pipe_if_total_is_left_out():
    assert tuple(_empty_pype().freqs(total=False)) == ()


def test_grouping_items_by_key_in_empty_pipe_returns_empty_pipe():
    assert tuple(_empty_pype().group_by(len)) == ()


def test_interleaving_with_an_iterable_with_truncation_returns_an_empty_pipe():
    pipe = _empty_pype()
    other_pipe = _a_fun_day_pype()

    assert tuple(pipe.interleave(other_pipe, trunc=False)) == ('a', 'fun', 'day')


def test_interleaving_with_an_iterable_with_skipping_returns_back_the_iterable_as_a_pipe():
    pipe = _empty_pype()
    other_pipe = _a_fun_day_pype()

    assert tuple(pipe.interleave(other_pipe, trunc=False)) == ('a', 'fun', 'day')


def test_interleaving_with_an_empty_iterable_returns_an_empty_pipe():
    pipe = _empty_pype()
    other_pipe = _empty_pype()

    assert tuple(pipe.interleave(other_pipe, trunc=False)) == ()


def test_concise_iteration_is_not_possible():
    with raises(StopIteration):
        next(_empty_pype().it())


def test_mapping_an_empty_pype_returns_an_empty_pipe():
    assert tuple(_empty_pype().map(lambda n: n * 2)) == ()


def test_mapping_an_empty_pype_in_parallel_returns_an_empty_pipe():
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

    _empty_pype().print(file=mock_stdout)

    mock_stdout.write.assert_not_called()


def test_reduces_to_init_value():
    assert _empty_pype().reduce(lambda summation, n: summation + n, init=1) == 1


def test_reducing_without_init_value_throws_exception():
    with raises(TypeError):
        _empty_pype().reduce(lambda summation, n: summation + n)


def test_rejecting_items_that_fulfill_predicate_returns_an_empty_pipe():
    assert tuple(_empty_pype().reject(lambda n: n < 2)) == ()


def test_reversing_returns_an_empty_pipe():
    assert tuple(_empty_pype().reverse()) == ()


def test_roundrobbining_items_returns_an_empty_pipe():
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


def test_slicing_returns_an_empty_pipe():
    assert tuple(_empty_pype().slice(1, 2)) == ()


def test_sorting_returns_an_empty_pipe():
    assert tuple(_empty_pype().sort()) == ()


@mark.parametrize('mode', ('after', 'at', 'before'))
def test_splitting_pipe_returns_empty_pipe(mode):
    assert tuple(_empty_pype().split(lambda n: n < 2, mode=mode)) == ()


def test_asking_for_the_first_n_items_returns_an_empty_pipe():
    assert tuple(_empty_pype().take(1)) == ()


def test_asking_for_the_last_n_items_returns_empty_pipe():
    assert tuple(_empty_pype().take(-2)) == ()


def test_taking_items_until_condition_is_true_returns_empty_pipe():
    assert tuple(_empty_pype().take_while(lambda n: n < 3)) == ()


def test_teeing_returns_a_pipe_with_n_empty_pipes():
    assert tuple(map(tuple, _empty_pype().tee(3))) == ((), (), ())


def test_applies_function_to_itself():
    assert _empty_pype().to(Pype.size) == 0


def test_applies_functions_to_itself():
    assert _empty_pype().to(list, sorted) == []


def test_creates_file_but_does_not_write_anything_to_it(tmpdir):
    target = join(tmpdir, 'strings.txt')

    _empty_pype().to_file(target)

    with open(target) as file:
        assert file.readlines() == []


def test_creates_json_with_empty_object(tmpdir):
    target = join(tmpdir, 'object.json')

    _empty_pype().to_json(target)

    with open(target) as file:
        assert json.load(file) == {}


def test_creates_json_with_empty_list(tmpdir):
    target = join(tmpdir, 'list.json')

    _empty_pype().to_json(target, as_dict=False)

    with open(target) as file:
        assert json.load(file) == []


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


def test_zipping_with_an_empty_iterable_returns_an_empty_iterable():
    assert tuple(_empty_pype().zip(_empty_pype())) == ()


def test_zipping_with_a_non_empty_iterable_returns_an_empty_iterable():
    assert tuple(_empty_pype().zip(_123_pype())) == ()


def test_non_truncating_zipping_with_a_non_empty_iterable_returns_that_iterable_paired_with_the_pad_value():
    assert tuple(_empty_pype().zip(_123_pype(), trunc=False, pad=-1)) == ((-1, 1), (-1, 2), (-1, 3))


def test_zipping_with_itself_returns_an_empty_pipe():
    assert tuple(_empty_pype().zip()) == ()


def test_zipping_with_a_function_returns_an_empty_pipe():
    assert tuple(_empty_pype().zip_with(lambda n: n * 2)) == ()
