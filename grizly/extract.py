import os
import csv
import requests
import dask
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from simple_salesforce import Salesforce
from simple_salesforce.login import SalesforceAuthenticationFailed
from grizly.tools import AWS
from grizly.utils import (
    file_extension, 
    read_config
)
from grizly.store import Store
from grizly.config import (
    Config, 
    _validate_config
)

class Extract():
    """Writes data to file.
    """
    def __init__(self, file_path:str=None, config_key:str=None):
        """
        Parameters
        ----------
        file_path : str, optional
            Path to local output file, by default None
        config_key : str, optional
            Config key , by default 'standard'
        """
        self.file_path = file_path
        self.rows = None
        self.task = None
        self.config_key = config_key if config_key else 'standard'


    def get_path(self):
        """Returns path stored in extract.file_path
        
        Returns
        -------
        str
            Path to local output file
        """
        return self.file_path


    def write(self):
        """Writes extract.rows to csv file.
        """
        assert file_extension(self.file_path) == '.csv', "This method only supports csv files"

        with open(self.file_path, 'w', newline='', encoding = 'utf-8') as csvfile:
            print("writing...")
            writer = csv.writer(csvfile, delimiter='\t')
            writer.writerows(self.rows)
            print("done writing")

    # KM: can we change argument order? 
    def from_sql(self, table, engine_str, chunk_column:str=None, schema:str=None, sep='\t', delayed=False):
        """Writes SQL table to csv file.
        
        Parameters
        ----------
        table : str
            Name of table
        engine_str : str
            Engine string.
        chunk_column : str, optional
            [description], by default None
        schema : str, optional
            Name of schema, by default None
        sep : str, optional
            Separtor/delimiter in csv file, by default '\t'
        delayed : bool, optional
            [description], by default False
        
        Returns
        -------
        Extract
        """
        def from_sql():
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
                    self.rows = cursor.fetchall()
                    self.write()
            else:
                print(f"start loading records")
                sql = f"""SELECT * FROM {table};"""
                cursor.execute(sql)
                self.rows = cursor.fetchall()
                self.write()
        if not delayed:
            from_sql()
        else:
            self.task = dask.delayed(from_sql)()
        return self


    def from_qf(self, qf):
        df = qf.to_df()
        if self.file_path != None:
            self.rows = df.values.tolist()
            self.write()
            return self
        else:
            return df


    def from_sfdc(self, fields, table, where=None, env="prod", delayed=False, output="file", username:str=None, password:str=None, organizationId:str=None, instance_url:str=None):
        """
        Writes Salesforce table to csv file."""
        def from_sfdc():
            if env == "prod":
                if username is None:
                    _validate_config(config=Config.data[self.config_key], 
                                    services='sfdc', 
                                    env='prod')
                    username_prod = Config.data[self.config_key]['sfdc']['prod']['username']
                else:
                    username_prod = username

                if password is None:
                    _validate_config(config=Config.data[self.config_key], 
                                    services='sfdc', 
                                    env='prod')
                    password_prod = Config.data[self.config_key]['sfdc']['prod']['password']
                else:
                    password_prod = password

                if organizationId is None:
                    _validate_config(config=Config.data[self.config_key], 
                                    services='sfdc', 
                                    env='prod')
                    organizationId_prod = Config.data[self.config_key]['sfdc']['prod']['organizationId']
                else:
                    organizationId_prod = organizationId
                try:
                    sf = Salesforce(password=password_prod, 
                                    username=username_prod, 
                                    organizationId=organizationId_prod)
                except SalesforceAuthenticationFailed:
                    print("Could not log in to SFDC. Are you sure your password hasn't expired and your proxy is set up correctly?")
                    raise SalesforceAuthenticationFailed

            elif env == "stage":
                if username is None:
                    _validate_config(config=Config.data[self.config_key], 
                                    services='sfdc', 
                                    env='stage')
                    username_stage = Config.data[self.config_key]['sfdc']['stage']['username']
                else:
                    username_stage = username

                if password is None:
                    _validate_config(config=Config.data[self.config_key], 
                                    services='sfdc', 
                                    env='stage')
                    password_stage = Config.data[self.config_key]['sfdc']['stage']['password']
                else:
                    password_stage = password

                if organizationId is None:
                    _validate_config(config=Config.data[self.config_key], 
                                    services='sfdc', 
                                    env='stage')
                    organizationId_stage = Config.data[self.config_key]['sfdc']['stage']['organizationId']
                else:
                    organizationId_stage = organizationId

                if instance_url is None:
                    _validate_config(config=Config.data[self.config_key], 
                                    services='sfdc', 
                                    env='stage')
                    instance_url_stage = Config.data[self.config_key]['sfdc']['stage']['instance_url']
                else:
                    instance_url_stage = instance_url
                try:
                    sf = Salesforce(instance_url=instance_url_stage, 
                                    password=password_stage, 
                                    username=username_stage,
                                    organizationId=organizationId_stage, 
                                    sandbox=True, 
                                    security_token='')
                except SalesforceAuthenticationFailed:
                    print("Could not log in to SFDC. Are you sure your password hasn't expired and your proxy is set up correctly?")
                    raise SalesforceAuthenticationFailed
            else:
                raise ValueError("Please choose one of supported environments: (prod, stage)")

            query = f"SELECT {', '.join(fields)} FROM {table}"
            if where:
                query += f" WHERE {where}"

            data = sf.query_all(query)

            if output == "file":
                rows = []
                colnames = [item for item in data["records"][0] if item != "attributes"]
                rows.append(colnames)
                for item in data['records']:
                    row = []
                    for field in fields:
                        row.append(item[field])
                    rows.append(row)

                self.rows = rows
                self.write()

            elif output == "df":
                import pandas as pd
                import numpy as np

                l = []
                for item in data['records']:
                    row = []
                    for field in fields:
                        row.append(item[field])
                    l.append(row)

                df = (pd
                      .DataFrame(l, columns=fields)
                      .replace(to_replace=["None"], value=np.nan)
                     )
                return df

        if not delayed:
            if output == "df":
                return from_sfdc()
            else:
                from_sfdc()
        else:
            self.task = dask.delayed(from_sfdc)()
        return self


    def from_github(self, username:str=None, username_password:str=None, pages:int=100):
        """Writes GitHub data in csv file.
        
        Parameters
        ----------
        username : str
            [description]
        username_password : str
            [description]
        pages : int, optional
            [description], by default 100
        
        Returns
        -------
        Extract
        """
        proxies = {
            "http": "http://restrictedproxy.tycoelectronics.com:80",
            "https": "https://restrictedproxy.tycoelectronics.com:80",
            }
        records = []
        if username is None:
            username = _validate_config(config=Config.data[self.config_key], 
                                        services='github')['github']['username']
        if username_password is None:
            username_password = _validate_config(config=Config.data[self.config_key], 
                                                services='github')['github']['username_password']

        for page in range(pages):
            page += 1
            issues = f'https://api.github.com/orgs/tedcs/issues?page={page}&filter=all'
            data = requests.get(issues, auth=(username,username_password), proxies = proxies)
            if len(data.json()) == 0:
                break
            if page == 1:
                records.append(["url","repository_name", "user_login", "assignees_login"
                , "milestone_title", "title", "created_at", "updated_at", "state", "labels"])
            for i in range(len(data.json())):
                record = []
                record.append(data.json()[i]["url"])
                record.append(data.json()[i]["repository"]["name"])
                record.append(data.json()[i]["user"]["login"])
                record.append(', '.join([assignee["login"] for assignee in data.json()[i]["assignees"]]))
                try:
                    record.append(data.json()[i]["milestone"]["title"])
                except:
                    record.append("no_milestone")
                record.append(data.json()[i]["title"])
                record.append(data.json()[i]["created_at"])
                record.append(data.json()[i]["updated_at"])
                record.append(data.json()[i]["state"])
                record.append(', '.join([label["name"] for label in data.json()[i]["labels"]]))
                records.append(record)
        self.rows = records
        self.write()
        return self


    def from_s3(self, s3_key:str, bucket:str=None, redshift_str:str=None):
        """Writes s3 to local file.

        Parameters
        ----------
        s3_key : str
            Name of s3 key
        bucket : str, optional
            Bucket name, if None then 'teis-data'
        redshift_str : str, optional
            Redshift engine string, if None then 'mssql+pyodbc://Redshift'

        Returns
        -------
        Extract
        """
        file_name = os.path.basename(self.file_path)
        file_dir = os.path.dirname(self.file_path)
        aws = AWS(
                file_name=file_name,
                s3_key=s3_key,
                bucket=bucket,
                file_dir=file_dir,
                redshift_str=redshift_str
                )
        aws.s3_to_file()
        return self