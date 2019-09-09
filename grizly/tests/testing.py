import pytest
from copy import deepcopy
import pandas
import os
from sqlalchemy import create_engine

from grizly.io.etl import (write_to, csv_to_s3, s3_to_csv)

from grizly.core.qframe import (QFrame)

from grizly.io.sqlbuilder import (get_sql)




"""
List of functions to test:
- write_to
- csv_to_s3
- s3_to_csv
df_to_s3
to_rds
s3_to_rds
s3_to_rds_qf
create_table (?)
"""




def clean_testexpr(testsql):
    testsql = testsql.replace("\n", "")
    testsql = testsql.replace("\t", "")
    testsql = testsql.replace("\r", "")
    testsql = testsql.replace("  ", "")
    testsql = testsql.replace(" ", "")
    testsql = testsql.lower()
    return testsql


def test_write_to():
    excel_path = os.path.join(os.getcwd(), 'grizly', 'grizly', 'tests', 'testing.xlsx')
    qf = QFrame(engine='mssql+pyodbc://Redshift').read_excel(excel_path, sheet_name="data")
    qf.query("assignment_level='CBC2'")
    
    schema="testing"
    table="test_patrycja"

    sql = qf.get_sql().sql
    columns = ', '.join(qf.data['select']['sql_blocks']['select_aliases'])
    if schema!='':
        sql_statement = f"INSERT INTO {schema}.{table} ({columns}) {sql}"
    else:
        sql_statement = f"INSERT INTO {table} ({columns}) {sql}"

    statement="""INSERT INTO testing.test_patrycja (assignment_group, billings, fiscal_year, assignment_code, assignment_name) 
    SELECT assignment_level AS assignment_group, sum(billings) AS billings, fiscal_year,
       assignment_values_fy19.profit_center || ' x ' || assignment_values_fy19.assignment_code_l2 AS assignment_code,
       assignment_values_fy19.profit_center || ' x ' || assignment_values_fy19.assignment_code_l2 AS assignment_name
    FROM testing.assignment_values_fy19 WHERE assignment_level='CBC2'
    GROUP BY assignment_group, fiscal_year, assignment_code, assignment_name, profit_center, assignment_code_l2, assignment_name_l2"""

    assert clean_testexpr(sql_statement) == clean_testexpr(statement)

    # Test with subquery
    qf.query("assignment_level='CBC4'",if_exists='replace')
    qf.select(['assignment_level','fiscal_year'])
    qf.write_to('test_patrycja','testing')

    sql = qf.get_sql().sql
    columns = ', '.join(qf.data['select']['sql_blocks']['select_aliases'])
    if schema!='':
        sql_statement = f"INSERT INTO {schema}.{table} ({columns}) {sql}"
    else:
        sql_statement = f"INSERT INTO {table} ({columns}) {sql}"

    statement = """INSERT INTO testing.test_patrycja (assignment_group, fiscal_year) SELECT sq.assignment_group AS assignment_group, sq.fiscal_year AS fiscal_year
    FROM (SELECT assignment_level AS assignment_group, sum(billings) AS billings, fiscal_year,
          assignment_values_fy19.profit_center || ' x ' || assignment_values_fy19.assignment_code_l2 AS assignment_code,
          assignment_values_fy19.profit_center || ' x ' || assignment_values_fy19.assignment_code_l2 AS assignment_name
    FROM testing.assignment_values_fy19 WHERE assignment_level='CBC4'
    GROUP BY assignment_group, fiscal_year, assignment_code, assignment_name, profit_center, assignment_code_l2, assignment_name_l2) sq"""

    assert clean_testexpr(sql_statement) == clean_testexpr(statement)


def test_csv_to_s3_to_csv():
    engine_string = "sqlite:///" + os.getcwd() + "\\grizly\\grizly\\tests\\chinook.db"
    engine = create_engine(engine_string)
    df = pandas.read_sql(sql="select * from chinook", con=engine)
    csv_path = os.path.join(os.getcwd(), 'csv_to_s3_test.csv')
    s3_name = "'csv_to_s3_test.csv'

    csv = test_df.to_csv(path_or_buf=csv_path, index=False)  

    test_s3 = csv_to_s3(csv_path,s3_name)
    test_csv = s3_to_csv(s3_name,csv_path)

    assert csv.equals(test_csv)


def test_df_to_s3():
    engine_string = "sqlite:///" + os.getcwd() + "\\grizly\\grizly\\tests\\chinook.db"
    engine = create_engine(engine_string)
    df = pandas.read_sql(sql="select * from chinook", con=engine) 

    """..."""



