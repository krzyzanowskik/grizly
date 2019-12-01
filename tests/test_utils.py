import os
from filecmp import cmp

from grizly.etl import (
    to_s3,
    read_s3
)

from grizly.utils import (
    check_if_exists,
    check_if_valid_type,
    set_cwd,
    get_path,
    file_extension
)


def write_out(out):
    with open(
        get_path("output.sql", from_where='here'),
        "w",
    ) as f:
        f.write(out)


def test_check_if_exists():
    assert check_if_exists('fiscal_calendar_weeks','baseviews') == True


def test_check_if_valid_type():
    assert check_if_valid_type('INT')
    assert not check_if_valid_type('string')
    assert check_if_valid_type('varchar(30)')


def test_set_cwd():
    cwd = set_cwd("test")
    user_cwd = os.environ['USERPROFILE']
    user_cwd = os.path.join(user_cwd, "test")
    assert cwd == user_cwd


def test_file_extention():
    file_path = get_path('test.csv')
    assert file_extension(file_path) == '.csv'
    assert file_extension(get_path()) == '' 