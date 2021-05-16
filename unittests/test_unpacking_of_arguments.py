"""
These tests specify behaviour of item unpacking logic for different signature types
"""

from typing import Tuple

from itertools import zip_longest
from pytest import mark, raises

from pypey import Fn, px
from pypey.func import T
from pypey.pype import _unpack_fn


def t(item: T) -> Tuple[T]:
    return item,


unary_funcs = (lambda n, /: n,
               lambda n=4, /: n,
               lambda n: n,
               lambda n=4: n)

keyword_only_unary_funcs = (lambda *, n: n, lambda *, n=4: n)

dyadic_funcs = (lambda n, m, /: (n, m),
                lambda n=4, m=5, /: (n, m),
                lambda n, m: (n, m),
                lambda n=4, m=5: (n, m),

                px(lambda n, m, /: (n, m), 6),
                px(lambda n=4, m=5, /: (n, m), 6),
                px(lambda n, m: (n, m), 6),
                px(lambda n, m: (n, m), m=7),
                px(lambda n=4, m=5: (n, m), 6),
                px(lambda n=4, m=5: (n, m), m=7))

keyword_only_dyadic_funcs = (lambda *, n, m: (n, m), lambda *, n=4, m=5: (n, m))

variadic_funcs = (lambda n, *args: [n, args],
                  lambda n=8, *args: [n, args],
                  lambda *args, n=8: [args, n],
                  lambda *args: args,

                  lambda n, **kwargs: [n, kwargs],
                  lambda n=8, **kwargs: [n, kwargs],

                  lambda n, *args, **kwargs: [n, args, kwargs],
                  lambda n=8, *args, **kwargs: [n, args, kwargs],
                  lambda *args, n=8, **kwargs: [args, n, kwargs],
                  lambda *args, **kwargs: [args, kwargs],

                  px(lambda n, *args: [n, args], 6),
                  px(lambda n, *args: [n, args], 6, 7),
                  px(lambda n=8, *args: [n, args], 6),
                  px(lambda n=8, *args: [n, args], 6, 7),

                  px(lambda *args: args, 4),
                  px(lambda *args: args, 4, 5),

                  px(lambda *args, n: [args, n], n=6),
                  px(lambda *args, n: [args, n], 6, n=7),
                  px(lambda *args, n=8: [args, n], 6),
                  px(lambda *args, n=8: [args, n], n=6),
                  px(lambda *args, n=8: [args, n], 6, 7),
                  px(lambda *args, n=8: [args, n], 6, n=7),

                  px(lambda *args, **kwargs: [args, kwargs], 4),
                  px(lambda *args, **kwargs: [args, kwargs], n=4),
                  px(lambda *args, **kwargs: [args, kwargs], 4, 5),
                  px(lambda *args, **kwargs: [args, kwargs], 4, m=5),
                  px(lambda *args, **kwargs: [args, kwargs], n=4, m=5),
                  px(lambda *args, n, **kwargs: [args, n, kwargs], n=4),
                  px(lambda *args, n, **kwargs: [args, n, kwargs], n=4, m=5),
                  px(lambda *args, n=8, **kwargs: [args, n, kwargs], 4),
                  px(lambda *args, n=8, **kwargs: [args, n, kwargs], n=4),
                  px(lambda *args, n=8, **kwargs: [args, n, kwargs], 4, 5),
                  px(lambda *args, n=8, **kwargs: [args, n, kwargs], 4, m=5),
                  px(lambda *args, n=8, **kwargs: [args, n, kwargs], n=4, m=5))

keyword_only_variadic_funcs = (lambda **kwargs: kwargs,
                               px(lambda **kwargs: kwargs, n=4),
                               px(lambda **kwargs: kwargs, n=4, m=5),
                               lambda *args, n: [args, n],
                               lambda *args, n, **kwargs: [args, n, kwargs],
                               lambda *args, n, **kwargs: [args, n, kwargs],
                               px(lambda *args, n: [args, n], 6),
                               px(lambda *args, n: [args, n], 6, 7))

dyadic_fn_to_non_iterable_expectation = dict(
    zip_longest(dyadic_funcs, [
        None,  # not enough  arguments
        (1, 5),
        None,  # not enough  arguments
        (1, 5),

        (6, 1),
        (6, 1),
        (6, 1),
        (1, 7),
        (6, 1),
        (1, 7)
    ]))

dyadic_fn_to_iterable_expectation = dict(
    zip_longest(dyadic_funcs, [
        (1, 2),
        ((1, 2), 5),
        (1, 2),
        ((1, 2), 5),

        (6, (1, 2)),
        (6, (1, 2)),
        (6, (1, 2)),
        ((1, 2), 7),
        (6, (1, 2)),
        ((1, 2), 7)
    ]))

variadic_fn_to_non_iterable_expectation = dict(
    zip_longest(variadic_funcs, [
        [1, ()],
        [1, ()],
        [(1,), 8],
        (1,),

        [1, {}],
        [1, {}],

        [1, (), {}],
        [1, (), {}],
        [(1,), 8, {}],
        [(1,), {}],

        [6, (1,)],
        [6, (7, 1)],
        [6, (1,)],
        [6, (7, 1)],

        (4, 1),
        (4, 5, 1),

        [(1,), 6],
        [(6, 1), 7],
        [(6, 1), 8],
        [(1,), 6],
        [(6, 7, 1), 8],
        [(6, 1), 7],

        [(4, 1), {}],
        [(1,), {'n': 4}],
        [(4, 5, 1), {}],
        [(4, 1), {'m': 5}],
        [(1,), {'n': 4, 'm': 5}],
        [(1,), 4, {}],
        [(1,), 4, {'m': 5}],
        [(4, 1), 8, {}],
        [(1,), 4, {}],
        [(4, 5, 1), 8, {}],
        [(4, 1), 8, {'m': 5}],
        [(1,), 4, {'m': 5}]
    ]))

variadic_fn_to_iterable_expectation = dict(
    zip_longest(variadic_funcs, [
        [1, (2, 3)],
        [1, (2, 3)],
        [(1, 2, 3), 8],
        (1, 2, 3),

        [(1, 2, 3), {}],
        [(1, 2, 3), {}],

        [1, (2, 3), {}],
        [1, (2, 3), {}],
        [(1, 2, 3), 8, {}],
        [(1, 2, 3), {}],

        [6, (1, 2, 3)],
        [6, (7, 1, 2, 3)],
        [6, (1, 2, 3)],
        [6, (7, 1, 2, 3)],

        (4, 1, 2, 3),
        (4, 5, 1, 2, 3),

        [(1, 2, 3), 6],
        [(6, 1, 2, 3), 7],
        [(6, 1, 2, 3), 8],
        [(1, 2, 3), 6],
        [(6, 7, 1, 2, 3), 8],
        [(6, 1, 2, 3), 7],

        [(4, 1, 2, 3), {}],
        [(1, 2, 3), {'n': 4}],
        [(4, 5, 1, 2, 3), {}],
        [(4, 1, 2, 3), {'m': 5}],
        [(1, 2, 3), {'n': 4, 'm': 5}],
        [(1, 2, 3), 4, {}],
        [(1, 2, 3), 4, {'m': 5}],
        [(4, 1, 2, 3), 8, {}],
        [(1, 2, 3), 4, {}],
        [(4, 5, 1, 2, 3), 8, {}],
        [(4, 1, 2, 3), 8, {'m': 5}],
        [(1, 2, 3), 4, {'m': 5}]]))


@mark.parametrize('fn', unary_funcs)
def test_unpacking_works_for_unary_signatures_and_non_iterable_items(fn: Fn):
    assert _unpack_fn(fn)(1) == 1


@mark.parametrize('fn', unary_funcs)
def test_unpacking_works_for_unary_signatures_and_iterable_items(fn: Fn):
    assert _unpack_fn(fn)((1, 2)) == (1, 2)


@mark.parametrize('fn', keyword_only_unary_funcs)
def test_unpacking_fails_for_keyword_only_unary_signatures_and_non_iterable_items(fn: Fn):
    with raises(TypeError):
        _unpack_fn(fn)(1)


@mark.parametrize('fn', keyword_only_unary_funcs)
def test_unpacking_fails_for_keyword_only_unary_signatures_and_iterable_items(fn: Fn):
    with raises(TypeError):
        _unpack_fn(fn)((1, 2))


@mark.parametrize('fn', dyadic_funcs)
def test_unpacking_works_for_dyadic_signatures_and_non_iterable_items(fn: Fn):
    if fn == dyadic_funcs[0] or fn == dyadic_funcs[2]:
        return

    assert _unpack_fn(fn)(1) == dyadic_fn_to_non_iterable_expectation[fn]


@mark.parametrize('fn', dyadic_funcs)
def test_unpacking_works_for_dyadic_signatures_and_iterable_items(fn: Fn):
    assert _unpack_fn(fn)((1, 2)) == dyadic_fn_to_iterable_expectation[fn]


@mark.parametrize('fn', keyword_only_dyadic_funcs)
def test_unpacking_fails_for_keyword_only_dyadic_signatures_and_non_iterable_items(fn: Fn):
    with raises(TypeError):
        _unpack_fn(fn)(1)


@mark.parametrize('fn', keyword_only_dyadic_funcs)
def test_unpacking_fails_for_keyword_only_dyadic_signatures_and_iterable_items(fn: Fn):
    with raises(TypeError):
        _unpack_fn(fn)((1, 2))


@mark.parametrize('fn', variadic_funcs)
def test_unpacking_works_for_variadic_signatures_and_non_iterable_items(fn: Fn):
    assert _unpack_fn(fn)(1) == variadic_fn_to_non_iterable_expectation[fn]


@mark.parametrize('fn', variadic_funcs)
def test_unpacking_works_for_variadic_signatures_and_iterable_items(fn: Fn):
    assert _unpack_fn(fn)((1, 2, 3)) == variadic_fn_to_iterable_expectation[fn]


@mark.parametrize('fn', keyword_only_variadic_funcs)
def test_unpacking_fails_for_keyword_only_variadic_signatures_and_non_iterable_items(fn: Fn):
    with raises(TypeError):
        _unpack_fn(fn)(1)


@mark.parametrize('fn', keyword_only_variadic_funcs)
def test_unpacking_fails_for_keyword_only_variadic_signatures_and_iterable_items(fn: Fn):
    with raises(TypeError):
        _unpack_fn(fn)((1, 2))
