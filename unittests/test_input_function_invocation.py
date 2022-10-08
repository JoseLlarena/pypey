"""
These tests specify behaviour of input function invocation as regards item unpacking logic for different signature types
"""
import sys

from pytest import mark

from pypey import px
from pypey.pype import _unpack_fn, UNARY_WITHOUT_SIGNATURE, N_ARY_WITHOUT_SIGNATURE


SUPPORTS_POSITIONAL_ONLY_PARAMS = sys.version_info.minor > 7


class TestClass:

    def __init__(self, param='param'):
        self.param = param

    def an_instance_method(self, first: str = 'first', *rest: str):
        return first, rest

    @classmethod
    def a_class_method(cls, first: str = 'first', *rest: str):
        return first, rest

    @staticmethod
    def a_static_method(first: str = 'first', *rest: str):
        return first, rest

    def __eq__(self, other):
        return isinstance(other, type(self)) and self.param == other.param

    def __hash__(self):
        return id(self)


def legacy_compat_lambda_defs(*lambdas):
    """
    Allow testing lambdas with positional only parameters in the versions
    of python where they are supported.
    """
    safe_lambdas = []
    for l in lambdas:
        if isinstance(l, str):
            if SUPPORTS_POSITIONAL_ONLY_PARAMS:
                eval(f"""safe_lambdas.append({l})""")
        else:
            safe_lambdas.append(l)
    return tuple(safe_lambdas)


unary_funcs = legacy_compat_lambda_defs(
    "lambda n, /: n",
    "lambda n=4, /: n",
    lambda n: n,
    lambda n=4: n)

keyword_only_unary_funcs = (lambda *, n: n, lambda *, n=4: n)

dyadic_funcs = legacy_compat_lambda_defs(
    "lambda n, m, /: [n, m]",
    lambda n, m: [n, m]
)

keyword_only_dyadic_funcs = (lambda *, n, m: (n, m), lambda *, n=4, m=5: (n, m))

effectively_unary_funcs = legacy_compat_lambda_defs(
    "lambda n=4, m=5, /: [n, m]",
    lambda n=4, m=5: [n, m],

    lambda n, **kwargs: [n, kwargs],
    lambda n=8, **kwargs: [n, kwargs],

    "px(lambda n, m, /: [n, m], 6)",
    "px(lambda n=4, m=5, /: [n, m], 6)",
    px(lambda n, m: [n, m], 6),
    px(lambda n, m: [n, m], m=7),
    px(lambda n=4, m=5: [n, m], 6),
    px(lambda n=4, m=5: [n, m], m=7)
)

variadic_funcs = (

    lambda n, *args: [n, args],
    lambda n=8, *args: [n, args],
    lambda *args, n=8: [args, n],
    lambda *args: args,

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

methods = (TestClass.an_instance_method,
           TestClass().an_instance_method,
           TestClass.a_class_method,
           TestClass.a_static_method,
           TestClass)

all_fns = (unary_funcs +
           keyword_only_unary_funcs +
           dyadic_funcs +
           keyword_only_dyadic_funcs +
           effectively_unary_funcs +
           variadic_funcs +
           keyword_only_variadic_funcs +
           methods +
           tuple(UNARY_WITHOUT_SIGNATURE) +
           tuple(N_ARY_WITHOUT_SIGNATURE - {breakpoint}))

non_spreading_fns = (unary_funcs +
                     keyword_only_unary_funcs +
                     keyword_only_dyadic_funcs +
                     effectively_unary_funcs +
                     keyword_only_variadic_funcs +
                     methods[:1] + methods[-1:] +
                     tuple(UNARY_WITHOUT_SIGNATURE) +
                     tuple(N_ARY_WITHOUT_SIGNATURE - {filter, range, slice, zip, breakpoint}))

spreading_fns = (dyadic_funcs + variadic_funcs + methods[1:-1] + (filter, range, slice, zip))


@mark.parametrize('fn', all_fns)
def test_function_is_invoked_normally_with_non_iterable_item(fn):
    iterable = [1, 2]
    unpacked_fn = _unpack_fn(fn, iterable[0])
    item = iterable[-1]

    fn_output = '<NO FN OUTPUT>'
    fn_exception = '<NO FN EXCEPTION>'

    try:

        fn_output = fn(item)

    except Exception as python_exception:

        fn_exception = python_exception

    unpacked_fn_output = '<NO UNPACKED FN OUTPUT>'
    unpacked_fn_exception = '<NO UNPACKED FN EXCEPTION>'

    try:

        unpacked_fn_output = unpacked_fn(item)

    except Exception as unpacked_exception:

        unpacked_fn_exception = unpacked_exception

    if fn_output != '<NO FN OUTPUT>':

        if isinstance(fn_output, staticmethod) and isinstance(unpacked_fn_output, staticmethod) or \
                isinstance(fn_output, classmethod) and isinstance(unpacked_fn_output, classmethod):
            assert type(unpacked_fn_output) == type(fn_output)
        else:
            assert unpacked_fn_output == fn_output

    else:

        assert unpacked_fn_exception.__str__() == fn_exception.__str__()


@mark.parametrize('fn', non_spreading_fns)
def test_function_is_invoked_normally_with_iterable_item_when_it_is_effectively_unary(fn):
    iterable = [[1, 2], [3, 4]]
    unpacked_fn = _unpack_fn(fn, iterable[0])
    item = iterable[-1]

    fn_output = '<NO FN OUTPUT>'
    fn_exception = '<NO FN EXCEPTION>'

    try:

        fn_output = fn(item)

    except Exception as python_exception:

        fn_exception = python_exception

    unpacked_fn_output = '<NO UNPACKED FN OUTPUT>'
    unpacked_fn_exception = '<NO UNPACKED FN EXCEPTION>'

    try:

        unpacked_fn_output = unpacked_fn(item)

    except Exception as unpacked_exception:

        unpacked_fn_exception = unpacked_exception

    if fn_output != '<NO FN OUTPUT>':

        if isinstance(fn_output, staticmethod) and isinstance(unpacked_fn_output, staticmethod) or \
                isinstance(fn_output, classmethod) and isinstance(unpacked_fn_output, classmethod) or \
                isinstance(fn_output, type(iter([]))) and isinstance(unpacked_fn_output, type(iter([]))):
            assert type(unpacked_fn_output) == type(fn_output)
        else:
            assert unpacked_fn_output == fn_output, f'{unpacked_fn_exception}|{fn} -> {unpacked_fn}'

    else:

        assert unpacked_fn_exception.__str__() == fn_exception.__str__(), f'{unpacked_fn_output} |{fn} -> {unpacked_fn}'


@mark.parametrize('fn', spreading_fns)
def test_function_is_invoked_with_argument_unpacking_with_iterable_item_when_it_is_nary(fn):
    iterable = [[1, 2], [3, 4]]
    unpacked_fn = _unpack_fn(fn, iterable[0])
    item = iterable[-1]

    fn_output = '<NO FN OUTPUT>'
    fn_exception = '<NO FN EXCEPTION>'

    try:

        fn_output = fn(*item)

    except Exception as python_exception:

        fn_exception = python_exception

    unpacked_fn_output = '<NO UNPACKED FN OUTPUT>'
    unpacked_fn_exception = '<NO UNPACKED FN EXCEPTION>'

    try:

        unpacked_fn_output = unpacked_fn(item)

    except Exception as unpacked_exception:

        unpacked_fn_exception = unpacked_exception

    if fn_output != '<NO FN OUTPUT>':

        if isinstance(fn_output, staticmethod) and isinstance(unpacked_fn_output, staticmethod) or \
                isinstance(fn_output, classmethod) and isinstance(unpacked_fn_output, classmethod) or \
                isinstance(fn_output, zip) and isinstance(unpacked_fn_output, zip):
            assert type(unpacked_fn_output) == type(fn_output)
        else:
            assert unpacked_fn_output == fn_output, f'{unpacked_fn_exception}|{fn} -> {unpacked_fn}'

    else:

        assert unpacked_fn_exception.__str__() == fn_exception.__str__(), f'{unpacked_fn_output} |{fn} -> {unpacked_fn}'
