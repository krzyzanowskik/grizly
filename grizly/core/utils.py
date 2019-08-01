import os
import json
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy.pool import NullPool


def read_store():
    json_path = os.path.join(os.getcwd(), 'json', 'etl_store.json')
    with open(json_path, 'r') as f:
                store = json.load(f)
    return store


store = read_store()
os.environ["HTTPS_PROXY"] = store["https"]


def check_if_exists(table, schema=''):
    """
    Checks if a table exists in Redshift.
    """
    engine = create_engine(store["redshift"], encoding='utf8', poolclass=NullPool)
    if schema == '':
        table_name = table
        sql_exists = "select * from information_schema.tables where table_name = '{}' ". format(table)
    else:
        table_name = schema + '.' + table
        sql_exists = "select * from information_schema.tables where table_schema = '{}' and table_name = '{}' ". format(schema, table)
        
    return not pd.read_sql_query(sql = sql_exists, con=engine).empty


def delete_where(table, schema='', *argv):
    """
    Removes records from Redshift table which satisfy *argv.
    
    Parameters:
    ----------
    table : string
        Name of SQL table.
    schema : string, optional
        Specify the schema.

    Examples:
    --------
        >>> delete_where('test_table', schema='testing', "fiscal_year = '2019'")

        Will generate and execute query:
        "DELETE FROM testing.test WHERE fiscal_year = '2019'"


        >>> delete_where('test_table', schema='testing', "fiscal_year = '2017' OR fiscal_year = '2018'", "customer in ('Enel', 'Agip')")

        Will generate and execute two queries:
        "DELETE FROM testing.test WHERE fiscal_year = '2017' OR fiscal_year = '2018'"
        "DELETE FROM testing.test WHERE customer in ('Enel', 'Agip')"

    """
    table_name = f'{schema}.{table}' if schema else f'{table}'

    if check_if_exists(table, schema):
        engine = create_engine(store["redshift"], encoding='utf8', poolclass=NullPool)

        if argv is not None:
            for arg in argv:
                sql = f"DELETE FROM {table_name} WHERE {arg} "
                engine.execute(sql)
                print(f'Records from table {table_name} where {arg} has been removed successfully.')
    else:
        print(f"Table {table_name} doesn't exist.")