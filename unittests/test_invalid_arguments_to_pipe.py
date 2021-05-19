"""
These tests specify behaviour of an operation when invalid arguments are passed in
"""
from os.path import join
from statistics import median, mode, mean

from pytest import mark, raises

from unittests import _123_pype, _123, _a_fun_day_pype

NON_INTS = 'a', 4.2, lambda n: n, _123, None
NON_PYPES = NON_INTS
NON_BINARY_FUNCTIONS = NON_INTS
NON_CALLABLES = ['a', 4.2, _123, None]


@mark.parametrize('fn', NON_BINARY_FUNCTIONS)
def test_accumulation_fails_with_non_binary_functions(fn):
    """"""
    with raises(TypeError):
        tuple(_123().accum(fn))


@mark.parametrize('pipe', NON_PYPES)
def test_concatenation_fails_with_non_pypes(pipe):
    """"""
    with raises(TypeError):
        tuple(_123_pype().cat(pipe))


@mark.parametrize('n', NON_INTS)
def test_chunking_fails_with_non_int_size(n):
    """"""
    with raises(TypeError):
        tuple(_123_pype().chunk(n))


@mark.parametrize('n', [0, -1])
def test_chunking_fails_with_non_positive_size(n):
    """"""
    with raises(ValueError):
        tuple(_123_pype().chunk(n))


def test_finite_cycling_fails_with_negative_number_of_cycles():
    """"""
    with raises(ValueError):
        tuple(_123_pype().cycle(-1))


@mark.parametrize('n', NON_INTS[:-1])
def test_finite_cycling_fails_with_non_int_cycles(n):
    """"""
    with raises(TypeError):
        tuple(_123_pype().cycle(n))


@mark.parametrize('n', NON_INTS)
def test_distributing_items_fails_with_non_int_n(n):
    """"""
    with raises(TypeError):
        tuple(_123_pype().dist(n))


@mark.parametrize('n', [0, -1])
def test_distributing_items_fails_with_non_positive_n(n):
    """"""
    with raises(ValueError):
        tuple(_123_pype().dist(n))


@mark.parametrize('n', NON_INTS)
def test_dividing_pipe_fails_with_non_int_n(n):
    """"""
    with raises(TypeError):
        tuple(_123_pype().divide(n))


@mark.parametrize('n', [0, -1])
def test_dividing_pipe_fails_with_non_positive_n(n):
    """"""
    with raises(ValueError):
        tuple(_123_pype().divide(n))


@mark.parametrize('fn', NON_CALLABLES)
def test_side_effect_fails_with_non_callables(fn):
    """"""
    with raises(TypeError):
        tuple(_123_pype().do(fn))


@mark.parametrize('workers', NON_INTS)
def test_side_effect_fails_with_non_int_workers(workers):
    """"""
    with raises(TypeError):
        tuple(_123_pype().do(lambda x: x * 2, workers=workers))


def test_side_effect_fails_with_negative_workers():
    """"""
    with raises(ValueError):
        tuple(_123_pype().do(lambda x: x * 2, workers=-1))


@mark.parametrize('chunk_size', NON_INTS)
def test_side_effect_fails_with_non_int_chunk_size(chunk_size):
    """"""
    with raises(TypeError):
        tuple(_123_pype().do(lambda x: x * 2, workers=2, chunk_size=chunk_size))


def test_side_effect_fails_with_non_positive_chunk_size():
    """"""
    with raises(ValueError):
        tuple(_123_pype().do(lambda x: x * 2, workers=2, chunk_size=-1))


@mark.parametrize('n', NON_INTS)
def test_drop_fails_with_non_ints(n):
    """"""
    with raises(TypeError):
        tuple(_123_pype().drop(n))


@mark.parametrize('fn', NON_CALLABLES)
def test_rejecting_while_condition_is_true_fails_with_non_callables(fn):
    """"""
    with raises(TypeError):
        tuple(_123_pype().drop_while(fn))


@mark.parametrize('n', NON_INTS)
def test_enumeration_fails_with_non_ints(n):
    """"""
    with raises(TypeError):
        tuple(_123_pype().enum(n))


def test_flattening_fails_with_non_iterable_items():
    """"""
    with raises(TypeError):
        tuple(_123_pype().flat())


@mark.parametrize('fn', NON_CALLABLES)
def test_flatmapping_fails_with_non_callable_mappings(fn):
    """"""
    with raises(TypeError):
        tuple(_a_fun_day_pype().flatmap(fn))


def test_flatmapping_fails_with_non_iterable_items():
    """"""
    with raises(TypeError):
        tuple(_123_pype().flatmap(str.upper))


@mark.parametrize('fn', NON_CALLABLES)
def test_grouping_fails_with_non_callable_keys(fn):
    """"""
    with raises(TypeError):
        tuple(_123_pype().group_by(fn))


@mark.parametrize('fn', NON_CALLABLES)
def test_mapping_fails_with_non_callable_function(fn):
    """"""
    with raises(TypeError):
        tuple(_123_pype().map(fn))


@mark.parametrize('fn', NON_CALLABLES)
def test_multiple_mapping_fails_with_non_callable_function(fn):
    """"""
    with raises(TypeError):
        tuple(_123_pype().map(lambda x: x * 2, fn))


@mark.parametrize('workers', NON_INTS)
def test_mapping_fails_with_non_int_workers(workers):
    """"""
    with raises(TypeError):
        tuple(_123_pype().map(lambda x: x * 2, workers=workers))


def test_mapping_fails_with_negative_workers():
    """"""
    with raises(ValueError):
        tuple(_123_pype().map(lambda x: x * 2, workers=-1))


@mark.parametrize('chunk_size', NON_INTS)
def test_mapping_fails_with_non_int_chunk_size(chunk_size):
    """"""
    with raises(TypeError):
        tuple(_123_pype().map(lambda x: x * 2, workers=2, chunk_size=chunk_size))


def test_mapping_fails_with_non_positive_chunk_size():
    """"""

    with raises(ValueError):
        tuple(_123_pype().map(lambda x: x * 2, workers=2, chunk_size=-1))


@mark.parametrize('fn', NON_CALLABLES)
def test_partitioning_fails_with_non_callable_predicates(fn):
    """"""
    with raises(TypeError):
        for _ in _123_pype().partition(fn):
            pass


@mark.parametrize('fn', NON_CALLABLES)
def test_printing_fails_with_non_callable_function(fn):
    """"""
    with raises(TypeError):
        tuple(_123_pype().print(fn))


@mark.parametrize('fn', NON_BINARY_FUNCTIONS)
def test_reducing_fails_with_non_binary_reduction_callable(fn):
    """"""
    with raises(TypeError):
        tuple(_123_pype().reduce(fn))


@mark.parametrize('fn', NON_CALLABLES)
def test_rejecting_items_fails_with_non_callable_predicate(fn):
    """"""
    with raises(TypeError):
        tuple(_123_pype().reject(fn))


@mark.parametrize('n', NON_INTS)
def test_sampling_fails_with_non_int_n(n):
    """"""
    with raises(TypeError):
        tuple(_123_pype().sample(n))


def test_sampling_fails_with_negative_n():
    """"""
    with raises(ValueError):
        tuple(_123_pype().sample(-1))


@mark.parametrize('fn', NON_CALLABLES)
def test_selecting_items_fails_with_non_callable_predicate(fn):
    """"""
    with raises(TypeError):
        tuple(_123_pype().select(fn))


@mark.parametrize('n', NON_INTS)
def test_slicing_fails_with_non_int_start(n):
    """"""
    with raises(TypeError):
        tuple(_123_pype().slice(n, 5))


@mark.parametrize('n', NON_INTS)
def test_slicing_fails_with_non_int_end(n):
    """"""
    with raises(TypeError):
        tuple(_123_pype().slice(1, n))


def test_slicing_fails_with_negative_start():
    """"""
    with raises(ValueError):
        tuple(_123_pype().slice(-1, 5))


def test_slicing_fails_with_negative_end():
    """"""
    with raises(ValueError):
        tuple(_123_pype().slice(1, -1))


def test_slicing_fails_with_start_larger_than_end():
    """"""

    with raises(ValueError):
        tuple(_123_pype().slice(1, 0))


@mark.parametrize('fn', NON_CALLABLES[:-1])
def test_sorting_fails_with_non_callable_non_none_key(fn):
    """"""
    with raises(TypeError):
        tuple(_123_pype().sort(fn))


@mark.parametrize('fn', NON_CALLABLES)
def test_splitting_fails_with_non_callable_predicate(fn):
    """"""
    with raises(TypeError):
        tuple(_123_pype().split(fn))


@mark.parametrize('mode', [1, 1., lambda n: n, [1, 2]])
def test_splitting_fails_with_non_string_modes(mode):
    """"""
    with raises(TypeError):
        tuple(_123_pype().split(lambda n: n == 2, mode=mode))


def test_splitting_fails_with_unsupported_modes():
    """"""
    with raises(ValueError):
        tuple(_123_pype().split(lambda n: n == 2, mode='non-existent'))


@mark.parametrize('n', NON_INTS)
def test_asking_for_first_or_last_n_items_fails_with_non_int_n(n):
    """"""
    with raises(TypeError):
        tuple(_123_pype().take(n))


@mark.parametrize('fn', NON_CALLABLES)
def test_selecting_items_while_condition_is_met_fails_with_non_callable_predicate(fn):
    """"""
    with raises(TypeError):
        tuple(_123_pype().take_while(fn))


@mark.parametrize('n', NON_INTS)
def test_teeing_fails_with_non_ints(n):
    """"""
    with raises(TypeError):
        tuple(_123_pype().tee(n))


@mark.parametrize('n', [-1, 0])
def test_teeing_fails_with_non_negatives(n):
    """"""
    with raises(ValueError):
        print(tuple(_123_pype().tee(n)))


@mark.parametrize('fn', NON_CALLABLES)
def test_applying_function_fails_with_non_callable_function(fn):
    """"""
    with raises(TypeError):
        tuple(_123_pype().to(fn))


@mark.parametrize('fn1', [sum, mean, mode, median])
@mark.parametrize('fn2', NON_CALLABLES)
def test_applying_functions_fails_with_at_least_one_non_callable_function(fn1, fn2):
    """"""
    with raises(TypeError):
        tuple(_123_pype().to(fn1, fn2))


@mark.parametrize('path', [4.2, None, abs])
def test_writing_to_file_fails_with_invalid_paths(path):
    """"""
    with raises(TypeError):
        _123_pype().to_file(path, now=True)


@mark.parametrize('mode', ['r', 'rt', 'rb', 'r+', 'r+b'])
def test_writing_to_file_fails_with_invalid_open_modes(mode, tmpdir):
    """"""
    with raises(ValueError):
        _123_pype().to_file(join(tmpdir, '123.txt'), mode=mode, now=True)


@mark.parametrize('n', NON_INTS)
def test_asking_for_top_n_items_fails_with_non_int_n(n):
    """"""
    with raises(TypeError):
        tuple(_123_pype().top(n))


def test_asking_for_top_n_items_fails_with_negative_n():
    """"""
    with raises(ValueError):
        tuple(_123_pype().top(-1))


@mark.parametrize('fn', NON_CALLABLES)
def test_asking_for_top_n_items_fails_with_non_callable_key(fn):
    """"""
    with raises(TypeError):
        tuple(_123_pype().top(1, fn))


def test_unzipping_fails_with_non_iterable_items():
    """"""
    with raises(TypeError):
        tuple(_123_pype().unzip())


@mark.parametrize('size', NON_INTS)
def test_sliding_window_over_items_fails_with_non_ints(size):
    """"""
    with raises(TypeError):
        tuple(_123_pype().window(size))


def test_sliding_window_over_items_fails_with_negative_sizes():
    """"""
    with raises(ValueError):
        tuple(_123_pype().window(-1))


@mark.parametrize('pipe', NON_PYPES)
def test_zipping_fails_with_non_pipes(pipe):
    """"""
    with raises(TypeError):
        tuple(_123_pype().zip(pipe))


@mark.parametrize('fn', NON_CALLABLES)
def test_zipping_with_function_fails_with_non_callable_functions(fn):
    """"""
    with raises(TypeError):
        tuple(_a_fun_day_pype().zip_with(fn))
