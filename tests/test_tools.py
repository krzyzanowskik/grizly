from pandas import DataFrame
import os
from filecmp import cmp
from grizly import (
    AWS
)
from tests import config


def write_out(out):
    with open(
        config.tests_txt_file,
        "w",
    ) as f:
        f.write(out)


def test_df_to_s3_and_s3_to_file():
    aws = AWS(file_name='testing_aws_class.csv')
    df = DataFrame({'col1': [1, 2], 'col2': [3, 4]})
    aws.df_to_s3(df)
   
    first_file_path = os.path.join(aws.file_dir, aws.file_name)
    second_file_path = os.path.join(aws.file_dir, 'testing_aws_class_1.csv')

    os.rename(first_file_path, second_file_path)
    aws.s3_to_file()

    assert cmp(first_file_path, second_file_path) == True
    os.remove(first_file_path)
    os.remove(second_file_path)
