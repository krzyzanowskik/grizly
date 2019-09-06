import pytest
import sqlparse
import os
from copy import deepcopy

from grizly.io.etl import (
    to_csv,
    create_table,
    csv_to_s3,
    s3_to_csv,
    s3_to_rds
)

from grizly.core.utils import (
    read_config,
    check_if_exists,
    delete_where,
    set_cwd
)


def test_check_if_exists():
    assert check_if_exists('fiscal_calendar_weeks','baseviews') == True


def test_set_cwd():
    cwd = set_cwd("test")
    user_cwd = os.environ['USERPROFILE']
    user_cwd = os.path.join(user_cwd, "test")
    assert cwd == user_cwd