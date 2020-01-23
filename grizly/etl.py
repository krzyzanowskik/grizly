import csv
import os
from configparser import ConfigParser

import boto3
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

from .config import Config

from .utils import check_if_exists, get_path, read_config

# config_path = get_path('.grizly', 'config.json')
# config = Config().from_json(config_path)
try:
    os.environ["HTTPS_PROXY"] = read_config()["https"] # remove the second option once whole team has moved to Config
except:
    pass


def to_csv(qf, csv_path, sql, engine=None, sep='\t', chunksize=None, debug=False, cursor=None):
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
        engine = create_engine(engine, encoding='utf8', poolclass=NullPool)

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

    with open(csv_path, 'w', newline='', encoding = 'utf-8') as csvfile:
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


def create_table(qf, table, engine, schema='', char_size=500):
    """
    Creates a new table in database if the table doesn't exist.

    Parameters
    ----------
    qf : QFrame object
    table : string
        Name of SQL table.
    engine : str
        Engine string.
    schema : string, optional
        Specify the schema.
    char_size : int, size of the VARCHAR field in the database column
    """
    engine = create_engine(engine, encoding='utf8', poolclass=NullPool)

    table_name = f'{schema}.{table}' if schema else f'{table}'

    if check_if_exists(table, schema):
        print("Table {} already exists...".format(table_name))

    else:
        sql_blocks = qf.data["select"]["sql_blocks"]
        columns = []
        for item in range(len(sql_blocks["select_aliases"])):
            if sql_blocks["types"][item] == "VARCHAR(500)":
                column = sql_blocks["select_aliases"][item] + ' ' + 'VARCHAR({})'.format(char_size)
            else:
                column = sql_blocks["select_aliases"][item] + ' ' + sql_blocks["types"][item]
            columns.append(column)

        columns_str = ", ".join(columns)
        sql = "CREATE TABLE {} ({})".format(table_name, columns_str)

        con = engine.connect()
        con.execute(sql)
        con.close()

        print("Table {} has been created successfully.".format(sql))


def s3_to_rds_qf(qf, table, s3_name, schema='', if_exists='fail', sep='\t', use_col_names=True, redshift_str=None, bucket=None):
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
            * fail: Raise a ValueError.
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

    redshift_str = redshift_str if redshift_str else 'mssql+pyodbc://Redshift'
    bucket_name = bucket if bucket else 'teis-data'
    engine = create_engine(redshift_str, encoding='utf8', poolclass=NullPool)

    table_name = f'{schema}.{table}' if schema else f'{table}'

    if check_if_exists(table, schema, redshift_str=redshift_str):
        if if_exists == 'fail':
            raise ValueError("Table {} already exists".format(table_name))
        elif if_exists == 'replace':
            sql = f"DELETE FROM {table_name}"
            engine.execute(sql)
            print('SQL table has been cleaned up successfully.')
        else:
            pass
    else:
        create_table(qf, table, engine=redshift_str, schema=schema)

    if s3_name[-4:] != '.csv': s3_name += '.csv'

    col_names = '(' + ', '.join(qf.data['select']['sql_blocks']['select_aliases']) + ')' if use_col_names else ''

    config = ConfigParser()
    with open("tst.txt", "w") as f:
        f.write(get_path(".aws", "credentials"))
    config.read(get_path('.aws','credentials'))
    if config['default']:
         aws_access_key_id = config['default']['aws_access_key_id']
         aws_secret_access_key = config['default']['aws_secret_access_key']
    #aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    #aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
 
    print("Loading {} data into {} ...".format(s3_name,table_name))

    sql = f"""
        COPY {table_name} {col_names} FROM 's3://{bucket_name}/{s3_name}'
        access_key_id '{aws_access_key_id}'
        secret_access_key '{aws_secret_access_key}'
        delimiter '{sep}'
        NULL ''
        IGNOREHEADER 1
        REMOVEQUOTES
        ;commit;
        """

    result = engine.execute(sql)
    result.close()
    print('Data has been copied to {}'.format(table_name))


def csv_to_s3(csv_path, keep_csv=True, bucket: str=None):
    """
    Writes csv file to s3.

    Parameters
    ----------
    csv_path : string
        Path to csv file.
    keep_csv : bool, optional
        Whether to keep the local csv copy after uploading it to Amazon S3, by default True
    bucket : str, optional
        Bucket name, if None then 'teis-data'
    """
    bucket_name = bucket if bucket else 'teis-data'
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    # if s3_name[-4:] != '.csv': s3_name = s3_name + '.csv'

    s3_name = os.path.basename(csv_path)

    bucket.upload_file(csv_path, s3_name)
    print('{} uploaded to s3 as {}'.format(os.path.basename(csv_path), s3_name))

    if not keep_csv:
        os.remove(csv_path)


def write_to(qf, table, schema, if_exists):
    """
    Inserts values from QFrame object into given table. Name of columns in qf and table have to match each other.
    Warning: QFrame object should not have Denodo defined as an engine.
    Parameters:
    -----
    qf: QFrame object
    table: string
    schema: string
    """
    sql = qf.get_sql().sql
    columns = ', '.join(qf.data['select']['sql_blocks']['select_aliases'])
    if schema!='':
        sql_statement = f"INSERT INTO {schema}.{table} ({columns}) {sql}"
        sql_del_statement = f"DELETE FROM {schema}.{table}"
    else:
        sql_statement = f"INSERT INTO {table} ({columns}) {sql}"
        sql_del_statement = f"DELETE FROM {table}"
    engine = create_engine(qf.engine)

    if check_if_exists(table=table, schema=schema, redshift_str=qf.engine):
        if if_exists=='replace':
            engine.execute(sql_del_statement)
            engine.execute(sql_statement)
            print(f'Data has been owerwritten into {schema}.{table}')
        elif if_exists=='fail':
            raise ValueError("Table already exists")
        elif if_exists=='append':
            engine.execute(sql_statement)
            print(f'Data has been appended to {table}')
    else:
        create_table(qf=qf, table=table, engine=engine, schema=schema)
        engine.execute(sql_statement)


# KM: To be removed
def to_s3(file_path: str, s3_name: str, bucket: str=None, keep_csv=True):
    """Writes local file to s3.

    Parameters
    ----------
    file_path : str
        Path to the file.
    s3_name : str
        Name of s3_file. Should be: 'repo_name/file_name'.
    bucket : str, optional
        Bucket name, if None then 'teis-data'
    """
    bucket_name = bucket if bucket else 'teis-data'
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    bucket.upload_file(file_path, s3_name)
    print('{} uploaded to s3 as {}'.format(os.path.basename(file_path), s3_name))

    if not keep_csv:
        os.remove(file_path)


# KM: To be removed
def read_s3(file_path: str, s3_name: str, bucket: str=None):
    """Downloads s3 file with prefix to local file.

    Parameters
    ----------
    file_path : str
        Path to the file.
    s3_name : str
        Name of s3_file.
    bucket : str, optional
        Bucket name, if None then 'teis-data'
    """
    bucket_name = bucket if bucket else 'teis-data'
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    with open(file_path, 'wb') as data:
        bucket.download_fileobj(s3_name, data)
    print('{} uploaded to {}'.format(s3_name, os.path.basename(file_path)))


# KM: To be removed
def s3_to_csv(csv_path, bucket: str=None):
    """
    Writes s3 to csv file .

    Parameters
    ----------
    csv_path : string
        Path to csv file.
    bucket : str, optional
        Bucket name, if None then 'teis-data'
    """
    bucket_name = bucket if bucket else 'teis-data'
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    # if s3_name[-4:] != '.csv': s3_name = s3_name + '.csv'
    s3_name = os.path.basename(csv_path)

    with open(csv_path, 'wb') as data:
        bucket.download_fileobj(s3_name, data)
    print('{} uploaded to {}'.format(s3_name, csv_path))


def df_to_s3(df, table_name, schema, dtype=None, sep='\t', clean_df=False, keep_csv=True, chunksize=10000,
            if_exists="fail", redshift_str=None, s3_key=None, bucket=None):

    """Copies a dataframe inside a Redshift schema.table
        using the upload via this process:
        df -> local csv -> s3 csv -> redshift table

        NOTE: currently this function performs a delete * in
        the target table, append is in TODO list, also we
        need to add a timestamp column

        COLUMN TYPES: right now you need to do a DROP TABLE to
        change the column type, this needs to be changed TODO

    Parameters
    ----------
    keep_csv : bool, optional
        Whether to keep the local csv copy after uploading it to Amazon S3, by default True
    redshift_str : str, optional
        Redshift engine string, if None then 'mssql+pyodbc://Redshift'
    bucket : str, optional
        Bucket name, if None then 'teis-data'
    """

    redshift_str = redshift_str if redshift_str else 'mssql+pyodbc://Redshift'
    bucket_name = bucket if bucket else 'teis-data'

    engine = create_engine(redshift_str, encoding='utf8', poolclass=NullPool)

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    filename = table_name + '.csv'
    filepath = os.path.join(os.getcwd(), filename)

    if clean_df:
        df = df_clean(df)

    df = clean_colnames(df)
    df.columns = df.columns.str.strip().str.replace(" ", "_") # Redshift won't accept column names with spaces

    df.to_csv(filepath, sep=sep, encoding="utf-8", index=False, chunksize=chunksize)
    print(f'{filename} created in {filepath}')

    if s3_key:
        if s3_key.endswith("/"):
            s3_key = s3_key[:-1]

    bucket.upload_file(filepath, f"{s3_key}/{filename}")
    print(f'{filename} successfully uploaded to {bucket_name}/{s3_key}/{filename}')

    if check_if_exists(table_name, schema, redshift_str=redshift_str):
        if if_exists == 'fail':
            raise ValueError(f"Table {table_name} already exists")
        elif if_exists == 'replace':
            sql = f"DELETE FROM {schema}.{table_name}"
            engine.execute(sql)
            print('SQL table has been cleaned up successfully.')
        else:
            pass
    else:
        df.head(1).to_sql(table_name, schema=schema, index=False, con=engine, dtype=dtype)

    if not keep_csv:
        os.remove(filepath)


def clean_colnames(df):

    reserved_words = ["user"]

    df.columns = df.columns.str.strip().str.replace(" ", "_") # Redshift won't accept column names with spaces
    df.columns = [f'"{col}"' if col.lower() in reserved_words else col for col in df.columns]

    return df


def df_clean(df):

    def remove_inside_quotes(string):
        """ removes double single quotes ('') inside a string,
        e.g. Sam 'Sammy' Johnson -> Sam Sammy Johnson """

        # pandas often parses timestamp values obtained from SQL as objects
        if type(string) == pd.Timestamp:
            return string

        if pd.notna(string):
            if isinstance(string, str):
                if string.find("'") != -1:
                    first_quote_loc = string.find("'")
                    if string.find("'", first_quote_loc+1) != -1:
                        second_quote_loc = string.find("'", first_quote_loc+1)
                        string_cleaned = string[:first_quote_loc] + string[first_quote_loc+1:second_quote_loc] + string[second_quote_loc+1:]
                        return string_cleaned
        return string


    def remove_inside_single_quote(string):
        """ removes a single single quote ('') from the beginning of a string,
        e.g. Sam 'Sammy' Johnson -> Sam Sammy Johnson """
        if type(string) == pd.Timestamp:
            return string

        if pd.notna(string):
            if isinstance(string, str):
                if string.startswith("'"):
                    return string[1:]
        return string


    df_string_cols = df.select_dtypes(object)
    df_string_cols = (
        df_string_cols
        .applymap(remove_inside_quotes)
        .applymap(remove_inside_single_quote)
        .replace(to_replace="\\", value="")
        .replace(to_replace="\n", value="", regex=True) # regex=True means "find anywhere within the string"
    )
    df.loc[:, df.columns.isin(df_string_cols.columns)] = df_string_cols

    bool_cols = df.select_dtypes(bool).columns
    df[bool_cols] = df[bool_cols].astype(int)

    return df

  
def build_copy_statement(file_name, schema, table_name, sep="\t", time_format=None, bucket=None, s3_dir=None,
                        remove_inside_quotes=False):
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

    bucket_name = bucket if bucket else 'teis-data'

    if not s3_dir:
        s3_key = file_name
    else:
        s3_key = s3_dir + file_name if s3_dir.endswith('/') else s3_dir + '/' + file_name

    config = ConfigParser()
    config.read(get_path('.aws','credentials'))
    
    if config["default"]:
        aws_access_key_id = config['default']['aws_access_key_id']
        aws_secret_access_key = config['default']['aws_secret_access_key']

    # aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    # aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    sql = f"""
        COPY {schema}.{table_name} FROM 's3://{bucket_name}/{s3_key}'
        access_key_id '{aws_access_key_id}'
        secret_access_key '{aws_secret_access_key}'
        delimiter '{sep}'
        NULL ''
        IGNOREHEADER 1
        REMOVEQUOTES
        ;commit;
        """

    if time_format:
        indent = 9
        last_line_pos = len(sql) - len(";commit;") - indent
        spaces = indent * " " # print formatting
        time_format_argument = f"timeformat '{time_format}'"
        sql = sql[:last_line_pos] + time_format_argument + "\n" + spaces[:-1] + sql[last_line_pos:]

    if remove_inside_quotes:
        sql = sql.replace("REMOVEQUOTES", r"CSV QUOTE AS '\"'")

    return sql


def s3_to_rds(file_name, table_name=None, schema='', time_format=None, if_exists='fail', sep='\t',
            redshift_str=None, bucket=None, s3_key=None, remove_inside_quotes=False):
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

    redshift_str = redshift_str if redshift_str else 'mssql+pyodbc://Redshift'
    bucket_name = bucket if bucket else 'teis-data'
    engine = create_engine(redshift_str, encoding='utf8', poolclass=NullPool)

    if not table_name:
        table_name = file_name.replace(".csv", "")

    if check_if_exists(table_name, schema, redshift_str=redshift_str):
        if if_exists == 'fail':
            raise ValueError(f"Table {table_name} already exists")
        elif if_exists == 'replace':
            sql = f"DELETE FROM {schema}.{table_name}"
            engine.execute(sql)
            print(f'Table {table_name} has been cleaned up successfully.')
        else:
            pass

    sql = build_copy_statement(file_name=file_name, schema=schema, table_name=table_name, sep=sep, time_format=time_format,
                                    bucket=bucket_name, s3_dir=s3_key, remove_inside_quotes=remove_inside_quotes)

    print(f"Loading data into {table_name}...")
    engine.execute(sql)
    print(f'Data has been copied to {table_name}')
