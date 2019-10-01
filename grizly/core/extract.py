import os
import csv
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

def csv_writer(path, rows):
    with open(path, 'a', newline='', encoding = 'utf-8') as csvfile:
                print("writing...")
                writer = csv.writer(csvfile, delimiter=',')
                writer.writerows(rows)

class Extract():
    """
        Writes to csv file.
        Parameters
        ----------
        csv_path : string
            Path to csv file.
    """
    def __init__(self, config=None, csv_path:str=None, extract_format:str = 'csv'):
        if config == None:
            self.csv_path = csv_path
        else:
            self.csv_path = config.csv_path
    
    def get_path(self):
        return self.csv_path

    def from_sql(self, table, engine_str, chunk_column:str=None, schema:str=None, sep='\t'):
        """
        Writes SQL table to csv file.
        Parameters
        ----------
        sql : string
            SQL query.
        engine : str
            Engine string.
        sep : string, default '\t'
            Separtor/delimiter in csv file.
        """
        engine = create_engine(engine_str, encoding='utf8', poolclass=NullPool)
        try:
            conn = engine.connect().connection
        except:
            conn = engine.connect().connection
        cursor = conn.cursor()

        if chunk_column != None:
            sql = f"""SELECT {chunk_column} FROM {table} GROUP BY {chunk_column};"""
            cursor.execute(sql)
            records = [t[0] for t in cursor.fetchall()]

            for chunk_column_value in records:
                print(f"start loading {chunk_column} = {chunk_column_value} of {records}")
                sql = f"""SELECT * FROM {table} WHERE {chunk_column}={chunk_column_value};"""
                cursor.execute(sql)
                rows = cursor.fetchall()
                if extract_format == 'csv':
                    csv_writer(self.csv_path, rows)
                else:
                    raise "Error format not supported"
        else:
            print(f"start loading records")
            sql = f"""SELECT * FROM {table};"""
            cursor.execute(sql)
            rows = cursor.fetchall()
            if extract_format == 'csv':
                csv_writer(self.csv_path, rows)
            else:
                raise "Error format not supported"
        return self

    def from_qf():
        pass

    def from_sfdc(self):
        """
        Writes Salesforce table to csv file.
        Parameters
        ----------
        username : string
        username_password : string
        tablename : string
        ...?
        """
        pass
    
    def from_github(self):
        pass