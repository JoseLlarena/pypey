"""
These tests specify whether an operation's effect is deferred after invocation returns and items are iterated through.

The way deferral is verified is by invoking the operation and then checking whether the original fixture pipe and its
backing lazy collection have been consumed and the operation's effect has been evaluated.

Note that this does not verify laziness, ie, the the ability of an operation to be applied incrementally, one item at
a time just as the collection is iterated through. Though all lazy operations are deferred, not all deferred operations
are lazy. Laziness is tested in `test_execution_laziness.py`.

"""
from collections import namedtuple
from math import sqrt
from operator import add
from os.path import join

from pypey.pype import Pype
from unittests import _123, _123_pype, _a_fun_day, _aba_pype, _aba, _112233_pype, _112233, _a_fun_day_pype


def test_accumulation_is_deferred():
    pipe = _123_pype()

    pipe.accum(add)

    assert tuple(pipe) == _123


def test_accumulation_with_initial_value_is_deferred():
    pipe = _123_pype()

    pipe.accum(add, 0)

    assert tuple(pipe) == _123


def test_concatenation_is_deferred():
    pipe = _123_pype()
    other_pipe = _123_pype()

    pipe.cat(other_pipe)

    assert tuple(pipe) == _123
    assert tuple(other_pipe) == _123


def test_chunking_is_deferred():
    pipe = _123_pype()

    pipe.chunk(2)

    assert tuple(pipe) == _123


def test_clone_is_deferred():
    pipe = _123_pype()

    pipe.clone()

    assert tuple(pipe) == _123


def test_finite_cycling_is_deferred():
    pipe = _123_pype()

    pipe.cycle(2)

    assert tuple(pipe) == _123


def test_infinite_cycle_is_deferred():
    pipe = _123_pype()

    pipe.cycle()

    assert tuple(pipe) == _123


def test_distributing_items_is_deferred():
    pipe = _123_pype()

    pipe.dist(2)

    assert tuple(pipe) == _123


def test_dividing_the_pipe_is_deferred():
    pipe = _123_pype()

    pipe.divide(2)

    assert tuple(pipe) == _123


def test_side_effect_can_be_deferred():
    pipe = _123_pype()

    pipe.do(print)

    assert tuple(pipe) == _123


def test_parallel_side_effect_can_be_deferred():
    pipe = _123_pype()

    pipe.do(print, workers=2)

    assert tuple(pipe) == _123


def test_side_effect_can_be_made_immediate():
    pipe = _123_pype()

    pipe.do(print, now=True)

    assert tuple(pipe) == ()


def test_parallel_side_effect_can_be_made_immediate():
    """
    This test doesn't really test deferral because the backing Iterable is teed.
    I can't think of a way to test it properly.
    """
    pipe = _123_pype()

    pipe.do(print, now=True, workers=2)

    assert tuple(pipe) == _123


def test_drop_while_is_deferred():
    pipe = _123_pype()

    pipe.drop_while(lambda n: n < 2)

    assert tuple(pipe) == _123


def test_enumeration_is_deferred():
    pipe = _123_pype()

    pipe.enum()

    assert tuple(pipe) == _123


def test_enumeration_with_swapped_index_is_deferred():
    pipe = _123_pype()

    pipe.enum(swap=True)

    assert tuple(pipe) == _123


def test_flattening_is_deferred():
    pipe = _a_fun_day_pype()

    pipe.flat()

    assert tuple(pipe) == _a_fun_day


def test_flatmapping_is_deferred():
    pipe = _a_fun_day_pype()

    pipe.flatmap(str.upper)

    assert tuple(pipe) == _a_fun_day


def test_grouping_by_key_is_deferred():
    pipe = _a_fun_day_pype()

    pipe.group_by(len)

    assert tuple(pipe) == _a_fun_day


def test_asking_for_first_n_items_is_deferred():
    pipe = _123_pype()

    pipe.head(1)

    assert tuple(pipe) == _123


def test_concise_iteration_is_deferred():
    pipe = _123_pype()

    pipe.it()

    assert tuple(pipe) == _123


def test_mapping_is_deferred():
    pipe = _123_pype()

    pipe.map(lambda n: n * 2)

    assert tuple(pipe) == _123


def test_parallel_mapping_is_immediate_but_teed():
    """
    This test doesnt really test deferral because the backing Iterable is teed.
    I can't think of a way to test it properly.
    """
    pipe = _123_pype()

    pipe.map(lambda x: x * 2, workers=2)

    assert tuple(pipe) == _123


def test_partitioning_is_deferred():
    pipe = _123_pype()

    pipe.partition(lambda n: n > 2)

    assert tuple(pipe) == _123


def test_picking_a_property_is_deferred():
    Person = namedtuple('Person', ['age'])

    pipe = Pype((Person(n) for n in _123))

    pipe.pick(Person.age)

    assert tuple(pipe) == (Person(1), Person(2), Person(3))


def test_picking_a_key_is_deferred():
    pipe = Pype(str(n) for n in _123)

    pipe.pick(0)

    assert tuple(pipe) == ('1', '2', '3')


def test_eager_printing_is_immediate():
    pipe = _123_pype()

    pipe.print(now=True)

    assert tuple(pipe) == ()


def test_lazy_printing_is_deferred():
    pipe = _123_pype()

    pipe.print(now=False)

    assert tuple(pipe) == _123


def test_reducing_is_immediate():
    pipe = _123_pype()

    pipe.reduce(add)

    assert tuple(pipe) == ()


def test_rejecting_items_is_deferred():
    pipe = _123_pype()

    pipe.reject(lambda n: n > 1)

    assert tuple(pipe) == _123


def test_reversing_is_deferred():
    pipe = _123_pype()

    pipe.reverse()

    assert tuple(pipe) == _123


def test_roundrobbining_is_deferred():
    pipe = _a_fun_day_pype()

    pipe.roundrobin()

    assert tuple(pipe) == _a_fun_day


def test_sampling_is_deferred():
    pipe = _123_pype()

    pipe.sample(2)

    assert tuple(pipe) == _123


def test_selecting_is_deferred():
    pipe = _123_pype()

    pipe.select(lambda n: n > 1)

    assert tuple(pipe) == _123


def test_shuffling_is_deferred():
    pipe = _123_pype()

    pipe.shuffle()

    assert tuple(pipe) == _123


def test_returning_size_is_immediate():
    pipe = _123_pype()

    pipe.size()

    assert tuple(pipe) == ()


def test_skipping_is_deferred():
    pipe = _123_pype()

    pipe.skip(1)

    assert tuple(pipe) == _123


def test_slicing_is_deferred():
    pipe = _123_pype()

    pipe.slice(1, 3)

    assert tuple(pipe) == _123


def test_sorting_is_deferred():
    pipe = _123_pype()

    pipe.sort()

    assert tuple(pipe) == _123


def test_splitting_is_deferred():
    pipe = _123_pype()

    pipe.split(lambda n: n == 2)

    assert tuple(pipe) == _123


def test_asking_for_last_n_items_is_deferred():
    pipe = _123_pype()

    pipe.tail(1)

    assert tuple(pipe) == _123


def test_selecting_items_while_condition_is_met_is_deferred():
    pipe = _123_pype()

    pipe.take_while(lambda n: n < 2)

    assert tuple(pipe) == _123


def test_teeing_is_deferred():
    pipe = _123_pype()

    pipe.tee(3)

    assert tuple(pipe) == _123


def test_applying_function_to_pipe_is_immediate_when_function_is_eager():
    pipe = _123_pype()

    pipe.to(sum)

    assert tuple(pipe) == ()


def test_applying_function_to_pipe_is_deferred_when_function_is_lazy():
    pipe = _123_pype()

    pipe.to(enumerate)

    assert tuple(pipe) == _123


def test_applying_function_to_pipe_is_immediate_when_at_least_one_function_is_eager():
    pipe = _123_pype()

    pipe.to(enumerate, zip, list)

    assert tuple(pipe) == ()


def test_eager_writing_to_file_is_immediate(tmpdir):
    pipe = _123_pype()

    pipe.to_file(join(tmpdir, 'zip.txt'), now=True)

    assert tuple(pipe) == ()


def test_lazy_writing_to_file_is_deferred(tmpdir):
    pipe = _123_pype()

    pipe.to_file(join(tmpdir, 'zip.txt'), now=False)

    assert tuple(pipe) == _123


def test_asking_for_top_n_items_is_deferred():
    pipe = _123_pype()

    pipe.top(2)

    assert tuple(pipe) == _123


def test_asking_for_the_unique_items_is_deferred():
    pipe = _aba_pype()

    pipe.uniq()

    assert tuple(pipe) == _aba


def test_sliding_a_window_over_items_is_deferred():
    pipe = _123_pype()

    pipe.window(2)

    assert tuple(pipe) == _123


def test_unzipping_is_deferred():
    pipe = _112233_pype()

    pipe.unzip()

    assert tuple(pipe) == _112233


def test_zipping_is_deferred():
    pipe = _123_pype()
    other_pipe = _123_pype()

    pipe.zip(other_pipe)

    assert tuple(pipe) == _123
    assert tuple(other_pipe) == _123


def test_self_zipping_is_deferred():
    pipe = _a_fun_day_pype()

    pipe.zip()

    assert tuple(pipe) == _a_fun_day


def test_zipping_with_function_is_deferred():
    pipe = _a_fun_day_pype()

    pipe.zip_with(len)

    assert tuple(pipe) == _a_fun_day
