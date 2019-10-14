import pytest
import sqlparse
import os
from copy import deepcopy
from filecmp import cmp

from grizly.etl import (
    to_csv,
    create_table,
    to_s3,
    read_s3,
    csv_to_s3,
    s3_to_csv,
    s3_to_rds
)

from grizly.utils import (
    read_config,
    check_if_exists,
    delete_where,
    set_cwd,
    get_path
)


def test_check_if_exists():
    assert check_if_exists('fiscal_calendar_weeks','baseviews') == True


def test_set_cwd():
    cwd = set_cwd("test")
    user_cwd = os.environ['USERPROFILE']
    user_cwd = os.path.join(user_cwd, "test")
    assert cwd == user_cwd


def test_to_s3_and_s3_to_file():
    in_file_path = get_path('grizly', 'grizly', 'tests', 'tables.xlsx')
    to_s3(in_file_path, 'test/tables.xlsx')
    out_file_path = get_path('grizly', 'grizly', 'tests', 'tables_s3.xlsx')
    read_s3(out_file_path, 'test/tables.xlsx')
    assert cmp(in_file_path, out_file_path) == True

    os.remove(out_file_path)