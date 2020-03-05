import csv
import os
from configparser import ConfigParser

import boto3
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

from .config import Config

from .tools.sqldb import SQLDB
from .utils import get_path, read_config, clean, clean_colnames


# config_path = get_path('.grizly', 'config.json')
# config = Config().from_json(config_path)
try:
    os.environ["HTTPS_PROXY"] = read_config()[
        "https"
    ]  # remove the second option once whole team has moved to Config
except:
    pass


def to_csv(
    qf, csv_path, sql, engine=None, sep="\t", chunksize=None, debug=False, cursor=None
):
    """
    Writes table to csv file.
    Parameters
    ----------
    csv_path : string
        Path to csv file.
    sql : string
        SQL query.
    engine : str, optional
        Engine string. Required if cursor is not provided.
    sep : string, default '\t'
        Separtor/delimiter in csv file.
    chunksize : int, default None
        If specified, return an iterator where chunksize is the number of rows to include in each chunk.
    cursor : Cursor, optional
        The cursor to be used to execute the SQL, by default None
    """
    if cursor:
        cursor.execute(sql)
        close_cursor = False

    else:
        engine = create_engine(engine, encoding="utf8", poolclass=NullPool)

        try:
            con = engine.connect().connection
            cursor = con.cursor()
            cursor.execute(sql)
        except:
            try:
                con = engine.connect().connection
                cursor = con.cursor()
                cursor.execute(sql)
            except:
                raise

        close_cursor = True

    with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile, delimiter=sep)
        writer.writerow(qf.data["select"]["sql_blocks"]["select_aliases"])
        cursor_row_count = 0
        if isinstance(chunksize, int):
            if chunksize == 1:
                while True:
                    row = cursor.fetchone()
                    cursor_row_count += 1
                    if not row:
                        break
                    writer.writerow(row)
            else:
                while True:
                    rows = cursor.fetchmany(chunksize)
                    cursor_row_count += len(rows)
                    if not rows:
                        break
                    writer.writerows(rows)
        else:
            writer.writerows(cursor.fetchall())

    if close_cursor:
        cursor.close()
        con.close()

    return cursor_row_count


def s3_to_rds_qf(
    qf,
    table,
    s3_name,
    schema="",
    file_format="csv",
    if_exists="fail",
    sep="\t",
    use_col_names=True,
    redshift_str=None,
    bucket=None,
):
    """
    Writes s3 to Redshift database.

    Parameters:
    -----------
    qf : {None, QFrame}, default None
        QFrame object or None
    table : string
        Name of SQL table.
    s3_name : string

    schema : string, optional
        Specify the schema.
    if_exists : {'fail', 'replace', 'append'}, default 'fail'
            How to behave if the table already exists.
            * fail: Raise a ValueError
            * replace: Clean table before inserting new values. NOTE: It won't drop the table.
            * append: Insert new values to the existing table.
    sep : string, default '\t'
        Separator/delimiter in csv file.
    use_col_names : boolean, default True
        If True the data will be loaded by the names of columns.
    redshift_str : str, optional
        Redshift engine string, if None then 'mssql+pyodbc://Redshift'
    bucket : str, optional
        Bucket name, if None then 'teis-data'
    """
    if if_exists not in ("fail", "replace", "append"):
        raise ValueError("'{}' is not valid for if_exists".format(if_exists))

    redshift_str = redshift_str if redshift_str else "mssql+pyodbc://Redshift"
    bucket_name = bucket if bucket else "teis-data"
    engine = create_engine(redshift_str, encoding="utf8", poolclass=NullPool)

    table_name = f"{schema}.{table}" if schema else f"{table}"

    if check_if_exists(table, schema, redshift_str=redshift_str):
        if if_exists == "fail":
            raise ValueError("Table {} already exists".format(table_name))
        elif if_exists == "replace":
            sql = f"DELETE FROM {table_name}"
            engine.execute(sql)
            print("SQL table has been cleaned up successfully.")
        else:
            pass
    else:
        create_table(qf, table, engine_str=redshift_str, schema=schema)

    if s3_name.split(".")[1] != file_format.lower():
        s3_name += file_format.lower()

    col_names = (
        "(" + ", ".join(qf.data["select"]["sql_blocks"]["select_aliases"]) + ")"
        if use_col_names
        else ""
    )

    config = ConfigParser()
    config.read(get_path(".aws", "credentials"))
    aws_access_key_id = config["default"]["aws_access_key_id"]
    aws_secret_access_key = config["default"]["aws_secret_access_key"]

    print("Loading {} data into {} ...".format(s3_name, table_name))

    if file_format.upper() == "PARQUET":
        sql = f"""
            COPY {table_name} FROM 's3://{bucket_name}/{s3_name}'
            access_key_id '{aws_access_key_id}'
            secret_access_key '{aws_secret_access_key}'
            FORMAT AS PARQUET
            ;commit;
            """
    else:
        sql = f"""
            COPY {table_name} {col_names} FROM 's3://{bucket_name}/{s3_name}'
            access_key_id '{aws_access_key_id}'
            secret_access_key '{aws_secret_access_key}'
            delimiter '{sep}'
            FORMAT AS CSV
            NULL ''
            IGNOREHEADER 1
            ;commit;
            """

    result = engine.execute(sql)
    result.close()
    print("Data has been copied to {}".format(table_name))


def build_copy_statement(
    file_name,
    schema,
    table_name,
    file_format="csv",
    sep="\t",
    time_format=None,
    bucket=None,
    s3_dir=None,
    remove_inside_quotes=False,
):
    """[summary]

    Parameters
    ----------
    file_name : [type]
        [description]
    schema : [type]
        [description]
    table_name : [type]
        [description]
    sep : str, optional
        [description], by default "\t"
    time_format : [type], optional
        [description], by default None
    bucket : [type], optional
        [description], by default None
    s3_dir : str, optional
        s3_dir in the format dir1/dir2, by default None
    remove_inside_quotes : bool, optional
        [description], by default False
    """

    bucket_name = bucket if bucket else "teis-data"

    if not s3_dir:
        s3_key = file_name
    else:
        s3_key = (
            s3_dir + file_name if s3_dir.endswith("/") else s3_dir + "/" + file_name
        )

    config = ConfigParser()
    config.read(get_path(".aws", "credentials"))
    aws_access_key_id = config["default"]["aws_access_key_id"]
    aws_secret_access_key = config["default"]["aws_secret_access_key"]

    sql = f"""
        COPY {schema}.{table_name} FROM 's3://{bucket_name}/{s3_key}'
        access_key_id '{aws_access_key_id}'
        secret_access_key '{aws_secret_access_key}'
        delimiter '{sep}'
        FORMAT AS {file_format.upper()}
        NULL ''
        IGNOREHEADER 1
        ;commit;
        """

    if time_format:
        indent = 9
        last_line_pos = len(sql) - len(";commit;") - indent
        spaces = indent * " "  # print formatting
        time_format_argument = f"timeformat '{time_format}'"
        sql = (
            sql[:last_line_pos]
            + time_format_argument
            + "\n"
            + spaces[:-1]
            + sql[last_line_pos:]
        )

    if remove_inside_quotes:
        sql = sql.replace("REMOVEQUOTES", r"CSV QUOTE AS '\"'")

    return sql


def s3_to_rds(
    file_name,
    table_name=None,
    schema="",
    file_format="csv",
    time_format=None,
    if_exists="fail",
    sep="\t",
    redshift_str=None,
    bucket=None,
    s3_key=None,
    remove_inside_quotes=False,
):
    """
    Writes s3 to Redshift database.

    Parameters:
    -----------
    file_name : string
        Name of the file to be uploaded.
    table_name : string, optional
        The name of the table. By default, equal to file_name.
    schema : string, optional
        Specify the schema.
    if_exists : {'fail', 'replace', 'append'}, default 'fail'
            How to behave if the table already exists.
            * fail: Raise a ValueError.
            * replace: Clean table before inserting new values. NOTE: It won't drop the table.
            * append: Insert new values to the existing table.
    sep : string, default '\t'
        Separator/delimiter in csv file.
    redshift_str : str, optional
        Redshift engine string, if None then 'mssql+pyodbc://Redshift'
    bucket : str, optional
        Bucket name, if None then 'teis-data'
    """

    if if_exists not in ("fail", "replace", "append"):
        raise ValueError(f"'{if_exists}' is not valid for if_exists")

    redshift_str = redshift_str if redshift_str else "mssql+pyodbc://Redshift"
    bucket_name = bucket if bucket else "teis-data"
    engine = create_engine(redshift_str, encoding="utf8", poolclass=NullPool)

    if not table_name:
        table_name = file_name.replace(f".{file_format.lower()}", "")

    if check_if_exists(table_name, schema, redshift_str=redshift_str):
        if if_exists == "fail":
            raise ValueError(f"Table {table_name} already exists")
        elif if_exists == "replace":
            sql = f"DELETE FROM {schema}.{table_name}"
            engine.execute(sql)
            print(f"Table {table_name} has been cleaned up successfully.")
        else:
            pass

    sql = build_copy_statement(
        file_name=file_name,
        schema=schema,
        table_name=table_name,
        file_format=file_format,
        sep=sep,
        time_format=time_format,
        bucket=bucket_name,
        s3_dir=s3_key,
        remove_inside_quotes=remove_inside_quotes,
    )

    print(f"Loading data into {schema}.{table_name}...")
    engine.execute(sql)
    print(f"Data has been copied to {schema}.{table_name}")
