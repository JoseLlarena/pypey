import pickle
from os.path import join, dirname

import dill  # type: ignore
from pytest import mark, raises
from pypey.pypes import pype
from unittests import _123_pype, _123

TEXT_PATH = join(dirname(__file__), 'test_file.txt')
BIN_PATH = join(dirname(__file__), '123.bin')
NUMBER_JSON_PATH = join(dirname(__file__), 'number.json')
STRING_JSON_PATH = join(dirname(__file__), 'string.json')
BOOLEAN_JSON_PATH = join(dirname(__file__), 'boolean.json')
LIST_JSON_PATH = join(dirname(__file__), 'list.json')
OBJECT_JSON_PATH = join(dirname(__file__), 'object.json')
NULL_JSON_PATH = join(dirname(__file__), 'null.json')


def test_creates_a_pipe_from_the_lines_of_a_text_file():
    assert tuple(pype.file(TEXT_PATH)) == ('line 1', 'line 2', 'line 3')


def test_creates_a_pipe_from_the_lines_of_a_text_file_keeping_the_line_terminators():
    assert tuple(pype.file(TEXT_PATH, strip=False)) == ('line 1\n', 'line 2\n', 'line 3')


def test_creates_a_pipe_from_a_binary_file():
    assert tuple(pype.file(BIN_PATH, mode='rb', encoding=None, strip=False).to(tuple)[0]) == _123


def test_creates_a_pipe_from_dictionary():
    assert tuple(pype.dict({'fun': 1, 'day': 2})) == (('fun', 1), ('day', 2))


@mark.parametrize('_dict', ['a', 4.2, lambda n: n, _123, None])
def test_pipe_creation_from_dict_fails_when_given_non_dicts(_dict):
    with raises(TypeError):
        pype.dict(_dict)


@mark.parametrize('it', [4.2, lambda n: n, None])
def test_pipe_creation_from_iterable_fails_when_given_non_iterable(it):
    with raises(TypeError):
        pype(it)


def test_pipe_can_be_pickled_and_unpickled(tmpdir: str):
    bin_path = join(tmpdir, '246.bin')

    with open(bin_path, 'wb') as bin_file:
        pickle.dump(_123_pype().map(_x2), bin_file)

    with open(bin_path, 'rb') as bin_file:
        assert tuple(pickle.load(bin_file)) == tuple(map(lambda n: n * 2, _123))


def test_pipe_can_be_dilled_and_undilled(tmpdir: str):
    """
    `dill` is used py `pathos` for extended pickling capabilities.
    """
    bin_path = join(tmpdir, '123.bin')

    with open(bin_path, 'wb') as bin_file:
        dill.dump(_123_pype().map(lambda n: n * 2), bin_file)

    with open(bin_path, 'rb') as bin_file:
        assert tuple(dill.load(bin_file)) == tuple(map(lambda n: n * 2, _123))


def test_creates_pipe_from_number_json(tmpdir: str):
    assert tuple(pype.json(NUMBER_JSON_PATH)) == (42,)


def test_creates_pipe_from_string_json(tmpdir: str):
    assert tuple(pype.json(STRING_JSON_PATH)) == ('forty-two',)


def test_creates_pipe_from_boolean_json(tmpdir: str):
    assert tuple(pype.json(BOOLEAN_JSON_PATH)) == (True,)


def test_creates_pipe_from_null_json(tmpdir: str):
    assert tuple(pype.json(NULL_JSON_PATH)) == (None,)


def test_creates_pipe_from_list_json(tmpdir: str):
    assert tuple(pype.json(LIST_JSON_PATH)) == (1, 2, 3, 5, 8, 13)


def test_creates_pipe_from_object_json(tmpdir: str):
    assert tuple(pype.json(OBJECT_JSON_PATH)) == (('a', 1.), ('fun', 2.), ('day', 3.))


def _x2(n: int) -> int:
    return n * 2
