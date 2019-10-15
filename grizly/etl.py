import boto3
import os
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
import pandas as pd
import csv

from grizly.utils import (
    read_config,
    check_if_exists
)


config = read_config()
try:
    os.environ["HTTPS_PROXY"] = config["https"]
except TypeError:
    pass

def to_csv(qf,csv_path, sql, engine, sep='\t', chunksize=None, compress=False):
    """
    Writes table to csv file.
    Parameters
    ----------
    csv_path : string
        Path to csv file.
    sql : string
        SQL query.
    engine : str
        Engine string.
    sep : string, default '\t'
        Separtor/delimiter in csv file.
    chunksize : int, default None
        If specified, return an iterator where chunksize is the number of rows to include in each chunk.
    """
    engine = create_engine(engine, encoding='utf8', poolclass=NullPool)

    try:
        con = engine.connect().connection
        cursor = con.cursor()
        cursor.execute(sql)
    except:
        con = engine.connect().connection
        cursor = con.cursor()
        cursor.execute(sql)

    with open(csv_path, 'w', newline='', encoding = 'utf-8') as csvfile:
        writer = csv.writer(csvfile, delimiter=sep)
        writer.writerow(qf.data["select"]["sql_blocks"]["select_aliases"])

        if isinstance(chunksize, int):
            if chunksize == 1:
                while True:
                    row = cursor.fetchone()
                    if not row:
                        break
                    writer.writerow(row)
            else:
                while True:
                    rows = cursor.fetchmany(chunksize)
                    if not rows:
                        break
                    writer.writerows(rows)
        else:
            writer.writerows(cursor.fetchall())

    cursor.close()
    con.close()


def to_csv_1(qf,csv_path, sql, engine, sep='\t', chunksize=None, compress=False):
    """
    Writes table to csv file.

    Parameters
    ----------
    csv_path : string
        Path to csv file.
    sql : string
        SQL query.
    engine : str
        Engine string.
    sep : string, default '\t'
        Separtor/delimiter in csv file.
    chunksize : int, default None
        If specified, return an iterator where chunksize is the number of rows to include in each chunk.
    """
    engine = create_engine(engine, encoding='utf8', poolclass=NullPool)

    if chunksize:
        iterator = 0
        limit_reached = False
        if qf.data["select"]["limit"] != '':
            row_limit = qf.data["select"]["limit"]
        while True:
            row_limit -= chunksize
            if row_limit < 0:
                chunksize += row_limit
                limit_reached = True

            qf.limit(f"{chunksize} OFFSET {iterator}")
            iterator += chunksize
            qf.get_sql()

            try:
                con = engine.connect().connection
                cursor = con.cursor()
                cursor.execute(qf.sql)
            except:
                con = engine.connect().connection
                cursor = con.cursor()
                cursor.execute(qf.sql)

            with open(csv_path, 'w', newline='', encoding = 'utf-8') as csvfile:
                writer = csv.writer(csvfile, delimiter=sep)
                writer.writerow(qf.data["select"]["sql_blocks"]["select_aliases"])

                writer.writerows(cursor.fetchall())

            cursor.close()
            con.close()

            if limit_reached:
                break
    else:
        to_csv(qf,csv_path, sql, engine, sep=sep, compress=compress)


def create_table(qf, table, engine, schema=''):
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
    """
    engine = create_engine(engine, encoding='utf8', poolclass=NullPool)

    table_name = f'{schema}.{table}' if schema else f'{table}'

    if check_if_exists(table, schema):
        print("Table {} already exists.".format(table_name))

    else:
        sql_blocks = qf.data["select"]["sql_blocks"]
        columns = []
        for item in range(len(sql_blocks["select_aliases"])):
            column = sql_blocks["select_aliases"][item] + ' ' + sql_blocks["types"][item]
            columns.append(column)

        columns_str = ", ".join(columns)
        sql = "CREATE TABLE {} ({})".format(table_name, columns_str)

        con = engine.connect()
        con.execute(sql)
        con.close()

        print("Table {} has been created successfully.".format(table_name))


def to_s3(file_path: str, s3_name: str):
    """Writes local file to s3 in 'teis-data' bucket with prefix 'bulk/'.

    Examples
    --------
    >>> to_s3('emea_daily.xlsx', 'dbb/ENG_EMEA/emea_daily.xlsx')

    Parameters
    ----------
    file_path : str
        Path to the file.
    s3_name : str
        Name of s3_file. Should be: 'repo_name/file_name'.
    """
    s3 = boto3.resource('s3', aws_access_key_id=config["akey"], aws_secret_access_key=config["skey"], region_name=config["region"])
    bucket = s3.Bucket('teis-data')

    bucket.upload_file(file_path, 'bulk/' + s3_name)
    print('{} uploaded to s3 as {}'.format(os.path.basename(file_path), 'bulk/' + s3_name))


def read_s3(file_path: str, s3_name: str):
    """Downloads s3 file from 'teis-data' bucket with prefix 'bulk/' to local file.

    Parameters
    ----------
    file_path : str
        Path to the file.
    s3_name : str
        Name of s3_file.
    """
    s3 = boto3.resource('s3', aws_access_key_id=config["akey"], aws_secret_access_key=config["skey"], region_name=config["region"])
    bucket = s3.Bucket('teis-data')

    with open(file_path, 'wb') as data:
        bucket.download_fileobj('bulk/' + s3_name, data)
    print('{} uploaded to {}'.format('bulk/' + s3_name, os.path.basename(file_path)))


def csv_to_s3(csv_path):
    """
    Writes csv file to s3 in 'teis-data' bucket.

    Parameters
    ----------
    csv_path : string
        Path to csv file.
    """
    s3 = boto3.resource('s3', aws_access_key_id=config["akey"], aws_secret_access_key=config["skey"], region_name=config["region"])
    bucket = s3.Bucket('teis-data')

    # if s3_name[-4:] != '.csv': s3_name = s3_name + '.csv'

    s3_name = os.path.basename(csv_path)

    bucket.upload_file(csv_path, 'bulk/' + s3_name)
    print('{} uploaded to s3 as {}'.format(os.path.basename(csv_path), 'bulk/' + s3_name))


def s3_to_csv(csv_path):
    """
    Writes s3 in 'teis-data' bucket to csv file .

    Parameters
    ----------
    csv_path : string
        Path to csv file.
    """
    s3 = boto3.resource('s3', aws_access_key_id=config["akey"], aws_secret_access_key=config["skey"], region_name=config["region"])
    bucket = s3.Bucket('teis-data')

    # if s3_name[-4:] != '.csv': s3_name = s3_name + '.csv'
    s3_name = os.path.basename(csv_path)

    with open(csv_path, 'wb') as data:
        bucket.download_fileobj('bulk/' + s3_name, data)
    print('{} uploaded to {}'.format('bulk/' + s3_name, csv_path))


def df_to_s3(df, table_name, schema, dtype=None, sep='\t', engine=None, delete_first=False, clean_df=False, keep_csv=False, chunksize=10000, if_exists="fail"):

    """Copies a dataframe inside a Redshift schema.table
        using the bulk upload via this process:
        df -> local csv -> s3 csv -> redshift table

        NOTE: currently this function performs a delete * in
        the target table, append is in TODO list, also we
        need to add a timestamp column

        COLUMN TYPES: right now you need to do a DROP TABLE to
        change the column type, this needs to be changed TODO
    """

    ACCESS_KEY = config["akey"]
    SECRET_KEY = config["skey"]
    REGION = config["region"]

    if engine is None:
        engine = create_engine('mssql+pyodbc://Redshift')

    s3 = boto3.resource('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, region_name=REGION)
    bucket = s3.Bucket('teis-data')

    filename = table_name + '.csv'
    filepath = os.path.join(os.getcwd(), filename)

    if delete_first:
        remove_from_s3(table_name)
        s3_file = s3.Object('teis-data', filename)
        s3_file.delete()

    if clean_df:
        df = df_clean(df)

    df = clean_colnames(df)
    df.columns = df.columns.str.strip().str.replace(" ", "_") # Redshift won't accept column names with spaces

    df.to_csv(filepath, sep="\t", encoding="utf-8", index=False, chunksize=chunksize)
    print(f'{filename} created in {filepath}')

    bucket.upload_file(filepath, f"bulk/{filename}")
    print(f'bulk/{filename} file uploaded to s3')

    if check_if_exists(table_name, schema):
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


def remove_from_s3(table_name, bucket_name="teis-data", file_extension="csv"):
    """ Requires configuration of AWS CLI (in CMD: >>aws configure) """

    os.system(f"SET HTTPS_PROXY=nyc3.sme.zscalertwo.net:10156 && aws s3api delete-object --bucket {bucket_name} --key bulk/{table_name}.{file_extension}")

    return None


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


def s3_to_rds_qf(qf, table, s3_name, schema='', if_exists='fail', sep='\t', use_col_names=True):
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
    """
    if if_exists not in ("fail", "replace", "append"):
        raise ValueError("'{0}' is not valid for if_exists".format(if_exists))

    engine = create_engine("mssql+pyodbc://Redshift", encoding='utf8', poolclass=NullPool)

    table_name = f'{schema}.{table}' if schema else f'{table}'

    if check_if_exists(table, schema):
        if if_exists == 'fail':
            raise ValueError("Table {} already exists".format(table_name))
        elif if_exists == 'replace':
            sql ="DELETE FROM {}".format(table_name)
            engine.execute(sql)
            print('SQL table has been cleaned up successfully.')
        else:
            pass
    else:
        create_table(qf, table, engine="mssql+pyodbc://Redshift", schema=schema)

    if s3_name[-4:] != '.csv': s3_name += '.csv'

    col_names = '(' + ', '.join(qf.data['select']['sql_blocks']['select_aliases']) + ')' if use_col_names else ''

    print("Loading {} data into {} ...".format('bulk/'+s3_name,table_name))

    sql = """
        COPY {} {} FROM 's3://teis-data/bulk/{}'
        access_key_id '{}'
        secret_access_key '{}'
        delimiter '{}'
        NULL ''
        IGNOREHEADER 1
        REMOVEQUOTES
        ;commit;
        """.format(table_name, col_names, s3_name, config["akey"], config["skey"], sep)

    engine.execute(sql)
    print('Data has been copied to {}'.format(table_name))


def s3_to_rds(file_name, table_name=None, schema='', if_exists='fail', sep='\t'):
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
    """

    if if_exists not in ("fail", "replace", "append"):
        raise ValueError("'{0}' is not valid for if_exists".format(if_exists))

    engine = create_engine("mssql+pyodbc://Redshift", encoding='utf8', poolclass=NullPool)

    if not table_name:
        table_name = file_name.replace(".csv", "")

    if check_if_exists(table_name, schema):
        if if_exists == 'fail':
            raise ValueError(f"Table {table_name} already exists")
        elif if_exists == 'replace':
            sql = f"DELETE FROM {schema}.{table_name}"
            engine.execute(sql)
            print(f'Table {table_name} has been cleaned up successfully.')
        else:
            pass

    print(f"Loading data into {table_name}...")
    sql = f"""
        COPY {schema}.{table_name} FROM 's3://teis-data/bulk/{file_name}'
        access_key_id '{config["akey"]}'
        secret_access_key '{config["skey"]}'
        delimiter '{sep}'
        NULL ''
        IGNOREHEADER 1
        REMOVEQUOTES
        ;commit;
        """

    engine.execute(sql)
    print(f'Data has been copied to {table_name}')


def write_to(qf, table, schema):
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
    else:
        sql_statement = f"INSERT INTO {table} ({columns}) {sql}"
    engine = create_engine(qf.engine)
    engine.execute(sql_statement)
    print(f'Data has been written to {table}')
