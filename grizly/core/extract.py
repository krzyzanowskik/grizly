import boto3
import os
import csv
from pandas import (
    ExcelWriter
)
import openpyxl
import win32com.client as win32
from grizly.core.utils import (
    get_path,
    read_config,
    check_if_exists
)
from pandas import DataFrame
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

grizly_config = read_config()
os.environ["HTTPS_PROXY"] = grizly_config["https"]

class Csv():
    def __init__(self, config=None, engine_str:str=None):
        if config == None:
            self.engine_str = engine_str
        else:
            self.engine_str = config.engine_str
        self.deletethis = ""

    def from_sql(self, table, csv_path, schema:str=None, chunksize:int=1000):
        engine = create_engine(self.engine_str, encoding='utf8', poolclass=NullPool)
        conn = engine.connect().connection
        cursor = conn.cursor()
        sql = f"""SELECT count(*) FROM {table};"""
        cursor.execute(sql)
        records_count = cursor.fetchone()[0]
        chunks = 0
        chunks_int = records_count//chunksize
        chunks_float = records_count/chunksize
        if chunks_float>chunks_int:
            chunks += chunks_int + 1
        if chunks_float<1:
            chunks += chunks_int + 1
        #Header Stuff
        if schema != None:
            sql = f"""SELECT * FROM {schema}.{table} LIMIT 1;"""
        else:
            sql = f"""SELECT * FROM {table} LIMIT 1;"""
        cursor.execute(sql)
        column_names = [desc[0] for desc in cursor.description]
        #Data
        for chunk in range(chunks):
            offset = chunk * chunksize
            print(f"start loading {chunksize} records")
            sql = f"""SELECT * FROM {table} LIMIT {chunksize} OFFSET {offset};"""
            cursor.execute(sql)
            rows = cursor.fetchall()
            if not rows:
                break
            if chunk == 0:
                try:
                    os.remove(csv_path)
                except FileNotFoundError:
                    pass
            with open(csv_path, 'a', newline='', encoding = 'utf-8') as csvfile:
                writer = csv.writer(csvfile, delimiter=',')
                if chunk == 0:
                    writer.writerow(column_names)
                writer = csv.writer(csvfile, delimiter=',')
                writer.writerows(rows)
            print(f"loaded {offset + chunksize} records")
        cursor.close()
        conn.close()
        return self

    def from_sfdc(self):
        pass
    
    def from_github(self):
        pass