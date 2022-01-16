""""
These tests specify an operation's effect on an non-empty pipe.
"""

import json
from collections import namedtuple
from multiprocessing import Value
from operator import add, neg
from os.path import join
from unittest.mock import Mock, call, create_autospec

import sys

from pypey import Pype, px, pype, TOTAL
from unittests import _123_pype, _123, _654_pype, _654, _empty_pype, _a_fun_day_pype, _23, _aba_pype, _ab, \
    _aAfunFUNdayDAY_pype, with_seed


def test_can_be_iterated_through():
    pipe = iter(_123_pype())

    assert next(pipe) == _123[0]
    assert next(pipe) == _123[1]
    assert next(pipe) == _123[2]


def test_accumulates_values_across_items():
    assert tuple(_123_pype().accum(add)) == (1, 3, 6)


def test_accumulates_values_across_items_with_initial_value():
    assert tuple(_123_pype().accum(add, -1)) == (-1, 0, 2, 5)


def test_broadcasts_items_to_iterables():
    assert tuple(_123_pype().broadcast(range)) == ((1, 0), (2, 0), (2, 1), (3, 0), (3, 1), (3, 2))


def test_concatenates_with_another_iterable():
    assert tuple(_123_pype().cat(_654_pype())) == _123 + _654


def test_concatenation_with_an_empty_iterable_returns_this_pipe():
    assert tuple(_123_pype().cat(_empty_pype())) == _123


def test_breaks_pipe_into_subpipes_of_at_most_the_given_number_of_items_if_pipe_is_no_smaller_than_number():
    chunks = tuple(_123_pype().chunk(2))

    assert tuple(map(tuple, chunks)) == ((1, 2), (3,))


def test_breaks_pipe_into_subpipe_of_the_same_size_as_the_pipe_if_given_number_is_larger_than_pipe():
    chunks = tuple(_123_pype().chunk(4))

    assert tuple(map(tuple, chunks)) == ((1, 2, 3),)


def test_breaks_pipe_into_subpipes_of_the_given_sizes():
    chunks = tuple(_123_pype().chunk([1, 2]))

    assert tuple(map(tuple, chunks)) == ((1,), (2, 3))


def test_breaks_pipe_into_subpipes_of_the_given_sizes_dropping_items_if_sum_of_sizes_is_smaller_than_length():
    chunks = tuple(_123_pype().chunk([1, 1]))

    assert tuple(map(tuple, chunks)) == ((1,), (2,))


def test_breaks_pipe_into_subpipes_of_the_given_sizes_or_smaller_if_sum_of_sizes_is_greater_than_length():
    chunks = tuple(_123_pype().chunk([1, 2, 3]))

    assert tuple(map(tuple, chunks)) == ((1,), (2, 3), ())


def test_clones_pipe():
    pipe = _123_pype()

    assert tuple(pipe.clone()) == tuple(pipe)


def test_cycles_through_pipe_for_the_given_number_of_times():
    assert tuple(_123_pype().cycle(n=3)) == _123 + _123 + _123


def test_cycles_through_pipe_forever_if_not_given_number_of_times():
    assert tuple(_123_pype().cycle().take(9)) == _123 + _123 + _123


def test_cycling_with_zero_number_of_times_returns_an_empty_pipe():  # FIXME SHOULD THIS BE ALLOWED?
    assert tuple(_123_pype().cycle(n=0)) == ()


def test_distributes_items_in_pipe_into_n_subpipes():
    segments = tuple(_123_pype().dist(2))

    assert tuple(map(tuple, segments)) == ((1, 3), (2,))


def test_distributing_items_into_more_subpipes_than_there_are_items_returns_empty_subpipes():
    segments = tuple(_123_pype().dist(4))

    assert tuple(map(tuple, segments)) == ((1,), (2,), (3,), ())


def test_divides_pipe_into_n_equal_sized_subpipes_when_n_is_multiple_of_size():
    segments = tuple(_123_pype().cat(_654_pype()).divide(3))

    assert tuple(map(tuple, segments)) == ((1, 2), (3, 6), (5, 4))


def test_divides_pipe_into_as_single_item_subpipes_followed_by_empty_pipes_when_n_is_larger_than_size():
    segments = tuple(_123_pype().divide(4))

    assert tuple(map(tuple, segments)) == ((1,), (2,), (3,), ())


def test_divides_pipe_into_same_size_subpipes_plus_excess_subpipe_when_n_is_smaller_than_size_but_not_multiple():
    segments = tuple(_123_pype().cat(_654_pype()).divide(4))

    assert tuple(map(tuple, segments)) == ((1,), (2,), (3,), _654)


def test_produces_a_side_effect_per_item():
    side_effect = create_autospec(lambda n: n)

    assert tuple(_123_pype().do(side_effect)) == _123

    side_effect.assert_has_calls([call(1), call(2), call(3)])


PARALLEL_SUM = Value('i', 0)


def test_produces_a_side_effect_per_item_in_parallel():
    """
    Mocks can't be pickled and only memory-shared objects which are global can be used in multiprocessing
    """

    _123_pype().do(side_effect, now=True, workers=2)

    assert PARALLEL_SUM.value == sum(_123)


def side_effect(n: int):
    with PARALLEL_SUM.get_lock():
        PARALLEL_SUM.value += n


def test_drops_the_given_number_of_first_items():
    assert tuple(_123_pype().drop(1)) == (2, 3)


def test_dropping_zero_items_returns_the_same_pipe():  # FIXME SHOULD THIS BE ALLOWED?
    assert tuple(_123_pype().drop(0)) == (1, 2, 3)


def test_dropping_more_first_items_than_there_are_in_pipe_is_the_same_as_dropping_as_many_as_there_are_in_it():
    assert tuple(_123_pype().drop(10)) == ()


def test_drops_the_given_number_of_last_items():
    assert tuple(_123_pype().drop(-2)) == (1,)


def test_dropping_more_last_items_than_there_are_in_pipe_is_the_same_as_dropping_as_many_as_there_are_in_it():
    assert tuple(_123_pype().drop(-10)) == ()


def test_rejects_items_until_condition_is_true():
    assert tuple(_123_pype().drop_while(lambda n: n != 2)) == _23


def test_makes_pipe_eager():
    pipe = _123_pype().eager()

    pipe.size()

    assert pipe.size() == 3


def test_enumerates_items():
    assert tuple(_a_fun_day_pype().enum(start=1)) == ((1, 'a'), (2, 'fun'), (3, 'day'))


def test_enumerates_items_with_a_swapped_index():
    assert tuple(_a_fun_day_pype().enum(start=1, swap=True)) == (('a', 1), ('fun', 2), ('day', 3))


def test_flattens_pipe_of_iterables_into_a_single_iterable():
    assert tuple(_a_fun_day_pype().flat()) == ('a', 'f', 'u', 'n', 'd', 'a', 'y')


def test_transforms_iterable_items_and_flattens_them_into_a_pipe_of_elements():
    assert tuple(_a_fun_day_pype().flatmap(str.upper)) == ('A', 'F', 'U', 'N', 'D', 'A', 'Y')


def test_computes_item_frequencies_with_total():
    pipe = pype('AFUNDAY')

    assert tuple(pipe.freqs()) == (('A', 2, 2 / 7),) + tuple((char, 1, 1 / 7) for char in 'FUNDY') + ((TOTAL, 7, 1.),)


def test_computes_item_frequencies_without_total():
    pipe = pype('AFUNDAY')

    assert tuple(pipe.freqs(total=False)) == (('A', 2, 2 / 7),) + tuple((char, 1, 1 / 7) for char in 'FUNDY')


def test_groups_items_by_given_key():
    assert tuple(_a_fun_day_pype().group_by(len)) == ((1, ['a']), (3, ['fun', 'day']))


def test_interleaves_items_with_other_iterable():
    assert tuple(pype([[1], [2], [3]]).interleave([[True, False], [True, True], [False, True]])) \
           == ([1], [True, False], [2], [True, True], [3], [False, True])


def test_interleaves_items_with_other_iterable_truncating_to_shortest():
    assert tuple(_123_pype().interleave(_a_fun_day_pype(), n=2)) == (1, 2, 'a', 3, 'fun')


def test_interleaves_items_with_other_iterable_skipping_items_if_other_pipe_is_exhausted():
    assert tuple(_123_pype().interleave(_a_fun_day_pype(), n=2, trunc=False)) == (1, 2, 'a', 3, 'fun', 'day')


def test_interleaves_items_with_other_iterable_skipping_items_this_pipe_is_exhausted():
    assert tuple(_123_pype().interleave(['a', 'fun', 'fun', 'day'], trunc=False)) == (1, 'a', 2, 'fun', 3, 'fun', 'day')


def test_interleaving_with_an_empty_iterable_skipping_items_returns_this_pipe():
    assert tuple(_123_pype().interleave(_empty_pype(), trunc=False)) == _123


def test_interleaving_with_an_empty_iterable_with_truncation_returns_an_empty_pipe():
    assert tuple(_123_pype().interleave(_empty_pype(), trunc=True)) == ()


def test_can_be_iterated_through_concisely():
    pipe = _123_pype().it()

    assert next(pipe) == _123[0]
    assert next(pipe) == _123[1]
    assert next(pipe) == _123[2]


def test_transforms_items():
    assert tuple(_123_pype().map(px(pow, 2), round)) == (2, 4, 8)


def test_transforms_items_in_parallel():
    assert tuple(_123_pype().map(px(pow, 2), workers=2)) == (2, 4, 8)


def test_partitions_items_according_to_predicate_into_a_tuple_of_pipes():
    assert tuple(map(tuple, _123_pype().partition(lambda n: n < 2))) == ((2, 3), (1,))


def test_picks_items_property():
    Person = namedtuple('Person', ['age'])

    pipe = Pype((Person(11), Person(22), Person(33)))

    assert tuple(pipe.pick(Person.age)) == (11, 22, 33)


def test_picks_items_elements_at_the_given_key():
    pipe = Pype(str(n) for n in _123)

    assert tuple(pipe.pick(0)) == ('1', '2', '3')


def test_prints_each_item_using_str():
    mock_stdout = Mock(spec_set=sys.stdout)

    _a_fun_day_pype().print(file=mock_stdout)

    mock_stdout.write.assert_has_calls([call('a'), call('\n'), call('fun'), call('\n'), call('day'), call('\n')])


def test_prints_each_item_as_per_the_given_function():
    mock_stdout = Mock(spec_set=sys.stdout)

    _123_pype().print(lambda n: f'n:{n}', file=mock_stdout)

    mock_stdout.write.assert_has_calls([call('n:1'), call('\n'), call('n:2'), call('\n'), call('n:3'), call('\n')])


def test_reduces_items_to_single_value():
    assert _123_pype().reduce(lambda summation, n: summation + n) == sum(_123)


def test_reduces_items_to_single_value_starting_with_a_initial_item():
    assert _123_pype().reduce(lambda summation, n: summation + n, init=-1) == sum(_23)


def test_rejects_items_that_fulfill_predicate():
    assert tuple(_123_pype().reject(lambda n: n < 2)) == _23


def test_reverses_pipe():
    assert tuple(_123_pype().reverse()) == (3, 2, 1)


def test_returns_iterable_items_elements_in_a_roundrobin_fashion():
    assert tuple(_a_fun_day_pype().roundrobin()) == ('a', 'f', 'd', 'u', 'a', 'n', 'y')


@with_seed(42)
def test_samples_items_with_current_seed():
    assert tuple(_123_pype().sample(2)) == (3, 1)


def test_samples_items_with_given_seed():
    assert tuple(_123_pype().sample(2, seed_=42)) == (3, 1)


def test_selects_items_that_fulfill_predicate():
    assert tuple(_123_pype().select(lambda n: n < 2)) == (1,)


@with_seed(42)
def test_shuffles_items_with_current_seed():
    assert tuple(_123_pype().shuffle()) == (2, 1, 3)


def test_shuffles_items_with_given_seed():
    assert tuple(_123_pype().shuffle(seed_=42)) == (2, 1, 3)


def test_returns_the_size_of_the_pipe():
    assert _123_pype().size() == len(_123)


def test_produces_a_slice_of_the_pipe():
    assert tuple(_123_pype().slice(1, 2)) == (2,)


def test_slicing_with_end_larger_than_the_size_is_the_same_as_slicing_with_end_equal_to_the_size():
    assert tuple(_123_pype().slice(1, 3)) == tuple(_123_pype().slice(1, 4))


def test_slicing_with_start_larger_than_the_size_returns_an_empty_pipe():
    assert tuple(_123_pype().slice(6, 7)) == ()


def test_sorts_items():
    assert tuple(_a_fun_day_pype().sort()) == ('a', 'day', 'fun')


def test_sorts_items_in_reverse_order():
    assert tuple(_a_fun_day_pype().sort(rev=True)) == ('fun', 'day', 'a')


def test_sorts_items_with_key():
    assert tuple(_123_pype().sort(lambda n: -n)) == (3, 2, 1)


def test_splits_pipeline_after_predicate_is_true():
    assert tuple(map(tuple, _123_pype().split(lambda n: n == 2))) == ((1, 2), (3,))


def test_splits_pipeline_before_predicate_is_true():
    assert tuple(map(tuple, _123_pype().split(lambda n: n == 2, mode='before'))) == ((1,), (2, 3,))


def test_splits_pipeline_before_and_after_predicate_is_true_leaving_true_items_out():
    assert tuple(map(tuple, _123_pype().split(lambda n: n == 2, mode='at'))) == ((1,), (3,))


def test_produces_the_first_n_items():
    assert tuple(_123_pype().take(1)) == (1,)


def test_produces_empty_pipe_when_asked_for_first_0_items():
    assert tuple(_123_pype().take(0)) == ()


def test_asking_for_more_first_items_than_the_size_is_the_same_as_asking_for_as_many_first_items_as_the_size():
    assert tuple(_123_pype().take(10)) == tuple(_123_pype().take(3))


def test_produces_the_last_n_items():
    assert tuple(_123_pype().take(-2)) == _23


def test_asking_for_more_last_items_than_the_size_is_the_same_as_asking_for_as_many_last_items_as_the_size():
    assert tuple(_123_pype().take(-10)) == tuple(_123_pype().take(-3))


def test_selects_items_until_condition_is_true():
    assert tuple(_123_pype().take_while(lambda n: n < 3)) == (1, 2)


def test_produces_multiple_copies_of_itself():
    copy1, copy2, copy3 = _123_pype().tee(3)

    assert (tuple(copy1), tuple(copy2), tuple(copy3)) == (_123, _123, _123)


def test_applies_function_to_itself():
    assert _123_pype().to(tuple) == _123


def test_applies_several_functions_to_itself():
    assert _123_pype().to(tuple, pype, Pype.size) == len(_123)


def test_lazily_writes_items_to_file(tmpdir):
    target = join(tmpdir, '123.txt')

    assert tuple(_123_pype().to_file(target, now=False)) == _123

    with open(target) as target:
        assert target.readlines() == ['1\n', '2\n', '3\n']


def test_eagerly_writes_items_to_file(tmpdir):
    target = join(tmpdir, '123.txt')

    pipe = _123_pype().to_file(target)

    with open(target) as target:
        assert target.readlines() == ['1\n', '2\n', '3\n']

    assert tuple(pipe) == ()


def test_writes_items_to_file_without_line_terminator(tmpdir):
    target = join(tmpdir, '123.txt')

    _123_pype().map(str).to_file(target, eol=False, now=True)

    with open(target) as target:
        assert target.readlines() == ['123']


def test_writes_pairs_to_json_file_as_object(tmpdir):
    target = join(tmpdir, 'object.json')

    Pype([('a', 1), ('fun', 2), ('day', 3)]).to_json(target)

    with open(target) as file:
        assert json.load(file) == {'a': 1, 'fun': 2, 'day': 3}


def test_writes_pairs_to_json_file_as_list(tmpdir):
    target = join(tmpdir, 'list.json')

    Pype([('a', 1), ('fun', 2), ('day', 3)]).to_json(target, as_dict=False)

    with open(target) as file:
        assert json.load(file) == [['a', 1], ['fun', 2], ['day', 3]]


def test_writes_items_to_json_file_as_list(tmpdir):
    target = join(tmpdir, 'list.json')

    Pype(['a', 'fun', 'day']).to_json(target, as_dict=False)

    with open(target) as file:
        assert json.load(file) == ['a', 'fun', 'day']


def test_finds_top_items():
    assert tuple(_123_pype().top(1)) == (3,)


def test_finds_top_items_with_key():
    assert tuple(_123_pype().top(1, neg)) == (1,)


def test_produces_unique_items():
    assert tuple(_aba_pype().uniq()) == _ab


def test_creates_multiple_pipes_from_iterable_items_own_items():
    pairs = Pype(((1, -1), (2, -2), (3, -3)))

    lefts = 1, 2, 3
    rights = -1, -2, -3

    assert tuple(map(tuple, pairs.unzip())) == (lefts, rights)


def test_sliding_window_of_size_0_returns_a_pipe_with_a_single_empty_window():  # FIXME SHOULD THIS BE ALLOWED?
    assert tuple(map(tuple, _123_pype().window(0))) == ((),)


def test_produces_windows_slid_over_items():
    assert tuple(map(tuple, _123_pype().window(2))) == ((1, 2), (2, 3))


def test_produces_windows_slid_over_items_with_given_shift_and_padding_value():
    assert tuple(map(tuple, _123_pype().window(2, shift=2, pad='filler'))) == ((1, 2), (3, 'filler'))


def test_zipping_with_a_pipe_of_the_same_size_returns_a_pipe_of_the_same_size_with_paired_items():
    assert tuple(_123_pype().zip(_654_pype())) == ((1, 6), (2, 5), (3, 4))


def test_zipping_with_a_pipe_of_different_size_returns_a_pipe_the_size_of_the_longer_one_with_missing_items_padded():
    assert tuple(_123_pype().zip(Pype('funny'), trunc=False, pad=4)) \
           == ((1, 'f'), (2, 'u'), (3, 'n'), (4, 'n'), (4, 'y'))


def test_self_zipping_when_items_have_the_same_size_returns_pipe_with_the_paired_items_elements():
    assert tuple(_aAfunFUNdayDAY_pype().zip()) == (('a', 'fun', 'day'), ('A', 'FUN', 'DAY'))


def test_self_zipping_with_different_sized_items_gives_pipe_with_items_the_size_of_the_longest_one_with_padding():
    assert tuple(_a_fun_day_pype().zip(trunc=False, pad='?')) == (('a', 'f', 'd'), ('?', 'u', 'a'), ('?', 'n', 'y'))


def test_self_zipping_with_a_function_pairs_items_with_output_of_said_function():
    assert tuple(_a_fun_day_pype().zip_with(len)) == (('a', 1), ('fun', 3), ('day', 3))
