"""
These tests specify an operation's laziness, or lack thereof (eagerness). Laziness is defined here as the operation's
ability to execute incrementally, one item at a time, triggered by iterating through it, as opposed to on all items at
once.

Laziness is verified by invoking the operation on a pipe backed by a lazy collection, then iterating through a single
item and finally checking that the pipe has been consumed only by that one item. Laziness is integral to data streaming.

Note that these tests do not specify deferral per se, ie, the ability of an operation's execution to happen after its
invocation has returned (a concept akin to asynchrony). Although all lazy operations are deferred, not all deferred
operations are lazy. Deferral is tested in `test_execution_deferral.py`

"""
from collections import namedtuple
from operator import add
from os.path import join

from pytest import raises, mark

from pypey import Pype
from pypey.pype import SPLIT_MODES
from unittests import _23, _123, _fun_day, _aba_pype, _112233_pype, _112233, _123_pype, _a_fun_day_pype, \
    _aAfunFUNdayDAY_pype


def test_accumulation_does_not_consume_whole_pipe():
    pipe = _123_pype()

    next(iter(pipe.accum(add)))

    assert tuple(pipe) == _23


def test_accumulation_with_initial_value_does_not_consume_whole_pipe():
    pipe = _123_pype()

    next(iter(pipe.accum(add, 0)))

    assert tuple(pipe) == _123


def test_concatenation_does_not_consume_either_pipe():
    pipe = _123_pype()
    other_pipe = _123_pype()

    next(iter(pipe.cat(other_pipe)))

    assert tuple(pipe) == _23
    assert tuple(other_pipe) == _123


def test_chunking_does_not_consume_pipe():
    pipe = _123_pype()

    next(iter(pipe.chunk(2)))

    assert tuple(pipe) == _23


def test_cloning_does_not_consume_pipe():
    pipe = _123_pype()

    next(iter(pipe.clone()))

    assert tuple(pipe) == _123


def test_finite_cycling_does_not_consume_pipe():
    pipe = _123_pype()

    next(iter(pipe.cycle(2)))

    assert tuple(pipe) == _23


def test_infinite_cycling_does_not_consume_pipe():
    pipe = _123_pype()

    next(iter(pipe.cycle()))

    assert tuple(pipe) == _23


def test_distributing_items_does_not_consume_pipe():
    pipe = _123_pype()

    next(iter(next(iter(pipe.dist(2)))))

    assert tuple(pipe) == _23


def test_dividing_pipe_consumes_it():
    pipe = _123_pype()

    next(iter(pipe.divide(2)))

    assert tuple(pipe) == ()


def test_side_effect_does_not_consume_pipe():
    pipe = _123_pype()

    next(iter(pipe.do(print)))

    assert tuple(pipe) == _23


def test_parallel_lazy_side_effect_consumes_pipe_but_tees_it():
    pipe = _123_pype()

    next(iter(pipe.do(print, workers=2)))

    assert tuple(pipe) == _123


def test_side_effect_consumes_pipe_if_immediate():
    pipe = _123_pype()

    pipe.do(print, now=True)

    assert tuple(pipe) == ()


def test_parallel_eager_side_effect_consumes_pipe_but_tees_it():
    pipe = _123_pype()

    pipe.do(print, workers=2, now=True)

    assert tuple(pipe) == _123


def test_dropping_items_while_predicate_is_false_does_not_consume_pipe():
    pipe = _123_pype()

    next(iter(pipe.drop_while(lambda n: n > 2)))

    assert tuple(pipe) == _23


def test_enumeration_does_not_consume_pipe():
    pipe = _123_pype()

    next(iter(pipe.enum()))

    assert tuple(pipe) == _23


def test_enumeration_with_swapped_index_does_not_consume_pipe():
    pipe = _123_pype()

    next(iter(pipe.enum(swap=True)))

    assert tuple(pipe) == _23


def test_flattening_does_not_consume_pipe():
    pipe = _a_fun_day_pype()

    next(iter(pipe.flat()))

    assert tuple(pipe) == _fun_day


def test_flatmapping_does_not_consume_pipe():
    pipe = _a_fun_day_pype()

    next(iter(pipe.flatmap(str.upper)))

    assert tuple(pipe) == _fun_day


def test_grouping_by_key_consumes_pipe():
    pipe = _a_fun_day_pype()

    next(iter(pipe.group_by(len)))

    assert tuple(pipe) == ()


def test_asking_for_first_n_items_does_not_consume_pipe():
    pipe = _123_pype()

    next(iter(pipe.head(1)))

    assert tuple(pipe) == _23


def test_concise_iteration_does_not_consume_pipe():
    pipe = _123_pype()

    next(pipe.it())

    assert tuple(pipe) == _23


def test_mapping_does_not_consume_pipe():
    pipe = _123_pype()

    next(iter(pipe.map(lambda n: n * 2)))

    assert tuple(pipe) == _23


def test_parallel_mapping_does_not_consume_pipe_but_tees_it():
    pipe = _123_pype()

    next(iter(pipe.map(lambda x: x * 2, workers=2)))

    assert tuple(pipe) == _123


def test_partitioning_does_not_consume_pipe():
    pipe = _123_pype()

    next(iter(pipe.partition(lambda n: n > 2)[0]))

    assert tuple(pipe) == _23


def test_picking_a_property_does_not_consume_pipe():
    Person = namedtuple('Person', ['age'])

    pipe = Pype((Person(n) for n in _123))

    next(iter(pipe.pick(Person.age)))

    assert tuple(pipe) == (Person(2), Person(3))


def test_picking_a_key_does_not_consume_pipe():
    pipe = Pype(str(n) for n in _123)

    next(iter(pipe.pick(0)))

    assert tuple(pipe) == ('2', '3')


def test_eager_printing_consumes_pipe():
    pipe = _123_pype()

    with raises(StopIteration):
        next(iter(pipe.print()))


def test_lazy_printing_does_not_consume_pipe():
    pipe = _123_pype()

    next(iter(pipe.print(now=False)))

    assert tuple(pipe) == _23


def test_reducing_consumes_pipe():
    pipe = _123_pype()

    pipe.reduce(add)

    with raises(StopIteration):
        next(iter(pipe))


def test_rejecting_items_does_not_consume_pipe():
    pipe = _123_pype()

    next(iter(pipe.reject(lambda n: n > 1)))

    assert tuple(pipe) == _23


def test_reversing_consumes_pipe():
    pipe = _123_pype()

    next(iter(pipe.reverse()))

    assert tuple(pipe) == ()


def test_roundrobbining_consumes_pipe():
    pipe = _a_fun_day_pype()

    next(iter(pipe.roundrobin()))

    assert tuple(pipe) == ()


def test_sampling_consumes_pipe():
    pipe = _123_pype()

    next(iter(pipe.sample(2)))

    assert tuple(pipe) == ()


def test_selecting_does_not_consume_pipe():
    pipe = _123_pype()

    next(iter(pipe.select(lambda n: n > 0)))

    assert tuple(pipe) == _23


def test_shuffling_consumes_pipe():
    pipe = _123_pype()

    next(iter(pipe.shuffle()))

    assert tuple(pipe) == ()


def test_asking_for_size_consumes_pipe():
    pipe = _123_pype()

    pipe.size()

    with raises(StopIteration):
        next(iter(pipe))


def test_skipping_does_not_consume_pipe():
    pipe = _123_pype()

    next(iter(pipe.skip(1)))

    assert tuple(pipe) == (3,)


def test_slicing_does_not_consume_pipe():
    pipe = _123_pype()

    next(iter(pipe.slice(0, 3)))

    assert tuple(pipe) == _23


def test_sorting_consumes_pipe():
    pipe = _123_pype()

    next(iter(pipe.sort()))

    assert tuple(pipe) == ()


@mark.parametrize('mode', SPLIT_MODES)
def test_splitting_does_not_consume_pipe(mode):
    pipe = _123_pype()

    next(iter(pipe.split(lambda n: n == 2, mode=mode)))

    assert tuple(pipe) == (3,)


def test_asking_for_last_n_items_does_not_consume_pipe():
    pipe = _123_pype()

    next(iter(pipe.tail(3)))

    assert tuple(pipe) == ()


def test_selecting_items_while_condition_is_met_does_not_consume_pipe():
    pipe = _123_pype()

    next(iter(pipe.take_while(lambda n: n < 2)))

    assert tuple(pipe) == _23


def test_teeing_does_not_consume_pipe():
    pipe = _123_pype()

    next(iter(next(iter(pipe.tee(3)))))

    assert tuple(pipe) == _23


def test_applying_function_to_pipe_consumes_pipe_when_function_is_eager():
    pipe = _123_pype()

    next(iter(pipe.to(list)))

    assert tuple(pipe) == ()


def test_applying_function_to_pipe_does_not_consume_pipe_when_function_is_lazy():
    pipe = _123_pype()

    next(pipe.to(enumerate))

    assert tuple(pipe) == _23


def test_applying_functions_to_pipe_consumes_pipe_when_at_least_one_function_is_eager():
    pipe = _123_pype()

    next(iter(pipe.to(enumerate, zip, list)))

    assert tuple(pipe) == ()


def test_eager_writing_to_file_consumes_pipe(tmpdir):
    pipe = _123_pype()

    with raises(StopIteration):
        next(iter(pipe.to_file(join(tmpdir, 'zip.txt'), now=True)))


def test_lazy_writing_to_file_does_not_consume_pipe(tmpdir):
    pipe = _123_pype()

    next(iter(pipe.to_file(join(tmpdir, 'zip.txt'), now=False)))

    assert tuple(pipe) == _23


def test_asking_for_top_n_items_consumes_pipe():
    pipe = _123_pype()

    next(iter(pipe.top(2)))

    assert tuple(pipe) == ()


def test_asking_for_the_unique_items_does_not_consume_pipe():
    pipe = _aba_pype()

    next(iter(pipe.uniq()))

    assert tuple(pipe) == ('b', 'a')


def test_sliding_a_window_over_items_does_not_consume_pipe():
    pipe = _123_pype()

    next(iter(pipe.window(1)))

    assert tuple(pipe) == (2, 3)


def test_unzipping_does_not_consume_pipe():
    pipe = _112233_pype()

    next(iter(tuple(pipe.unzip())[0]))

    assert tuple(pipe) == _112233[1:]


def test_zipping_does_not_consume_pipe():
    pipe = _123_pype()
    other_pipe = _123_pype()

    next(iter(pipe.zip(other_pipe)))

    assert tuple(pipe) == _23
    assert tuple(other_pipe) == _23


def test_self_zipping_consumes_pipe():
    pipe = _aAfunFUNdayDAY_pype()

    next(iter(pipe.zip()))

    assert tuple(pipe) == ()


def test_zipping_with_a_function_does_not_consume_pipe():
    pipe = _a_fun_day_pype()

    next(iter(pipe.zip_with(len)))

    assert tuple(pipe) == _fun_day
