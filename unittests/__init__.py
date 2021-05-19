from typing import Tuple

from pypey import Pype, Fn
from random import seed, setstate, getstate

_123 = 1, 2, 3
_23 = _123[1:]
_654 = 6, 5, 4
_aba = 'a', 'b', 'a'
_ab = _aba[:-1]

_a_fun_day = 'a', 'fun', 'day'
_fun_day = _a_fun_day[1:]

_112233 = (1, 1), (2, 2), (3, 3)
_2233 = _112233[1:]

_aAfunFUNdayDAY = ('a', 'A'), ('fun', 'FUN'), ('day', 'DAY')


def _empty_pype() -> Pype:
    return Pype(iter(()))


def _123_pype() -> Pype[int]:
    return Pype(iter(_123))


def _654_pype() -> Pype[int]:
    return Pype(iter(_654))


def _a_fun_day_pype() -> Pype[str]:
    return Pype(iter(_a_fun_day))


def _112233_pype() -> Pype[Tuple[int, int]]:
    return Pype((n, n) for n in _123)


def _aAfunFUNdayDAY_pype() -> Pype[Tuple[str, str]]:
    return Pype((w, w.upper()) for w in _a_fun_day)


def _aba_pype() -> Pype[str]:
    return Pype(iter('aba'))


def with_seed(seed_: int) -> Fn:
    def decorator(function: Fn) -> Fn:
        def wrapper(*args, **kwargs):
            s = getstate()
            seed(seed_)
            ret = function(*args, **kwargs)
            setstate(s)
            return ret

        return wrapper

    return decorator
