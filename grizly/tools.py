from boto3 import resource
import os
import openpyxl
from grizly.utils import (
    get_path,
    check_if_exists,
    _validate_config
)
from pandas import (
    DataFrame
)
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from io import StringIO
from csv import reader
from configparser import ConfigParser
from copy import deepcopy



class AWS:
    """Class that represents a file in S3.

        Examples
        --------
        >>> from grizly import get_path, AWS
        >>> s3 = AWS('emea_daily.xlsx', s3_key='dbb/ENG_EMEA/', file_dir=get_path('acoe_projects', 'dbb', 'ENG_EMEA'))

        Parameters
        ----------
        file_name : str, optional
            Name of the file, if None then 'test.csv'
        s3_key : str, optional
            Name of s3 key, if None then 'bulk/'
        bucket : str, optional
            Bucket name, if None then 'acoe-s3'
        file_dir : str, optional
            Path to local folder to store the file, if None then '%UserProfile%/s3_loads'
        redshift_str : str, optional
            Redshift engine string, if None then 'mssql+pyodbc://redshift_acoe'
        config : dict, optional
            Config module (imported .py file), by default None
        """
    def __init__(self, file_name:str, s3_key:str=None, bucket:str=None, file_dir:str=None, redshift_str:str=None, config:dict=None):
        if not config:
            config = {
                    's3_key' : '',
                    'bucket' : 'acoe-s3',
                    'file_dir' : get_path('s3_loads'),
                    'redshift_str' : 'mssql+pyodbc://redshift_acoe'
            }
        else:
            config = _validate_config(config=config, 
                                    service='s3')['s3']

        dict_config = deepcopy(config)
        config = _AttrDict()
        config.update(dict_config)

        self.file_name = file_name
        self.s3_key = s3_key if s3_key else config.s3_key
        self.bucket = bucket if bucket else config.bucket
        self.file_dir = file_dir if file_dir else config.file_dir
        self.redshift_str = redshift_str if redshift_str else config.redshift_str
        self.s3_resource = resource('s3')
        os.makedirs(self.file_dir, exist_ok=True)

        if self.s3_key == '':
            raise ValueError("s3_key not specified")

        if not self.s3_key.endswith('/'):
            raise ValueError("s3_key should end with /")

    def info(self):
        """Print a concise summary of a AWS.

        Examples
        --------
        >>> AWS('test.csv', 'bulk/').info()
        file_name: 	'test.csv'
        s3_key: 	'bulk/'
        bucket: 	'acoe-s3'
        file_dir: 	'C:/Users/XXX/s3_loads'
        redshift_str: 	'mssql+pyodbc://redshift_acoe'
        """
        print(f"\033[1m file_name: \033[0m\t'{self.file_name}'")
        print(f"\033[1m s3_key: \033[0m\t'{self.s3_key}'")
        print(f"\033[1m bucket: \033[0m\t'{self.bucket}'")
        print(f"\033[1m file_dir: \033[0m\t'{self.file_dir}'")
        print(f"\033[1m redshift_str: \033[0m\t'{self.redshift_str}'")


    def s3_to_s3(self, file_name:str=None, s3_key:str=None, bucket:str=None):
        """Copies S3 file to another S3 file.

        TODO: For now it moves only one file at a time, we need t improve it to move whole folders.
        
        Parameters
        ----------
        file_name : str, optional
            New file name, if None then the same as in class
        s3_key : str, optional
            New S3 key, if None then the same as in class
        bucket : str, optional
            New bucket, if None then the same as in class

        Examples
        --------
        >>> s3 = AWS('test.csv', 'bulk/')
        >>> s3.info()
        file_name: 	'test.csv'
        s3_key: 	'bulk/'
        bucket: 	'acoe-s3'
        file_dir: 	'C:/Users/XXX/s3_loads'
        redshift_str: 	'mssql+pyodbc://redshift_acoe'
        >>> s3 = s3.s3_to_s3('test_old.csv', s3_key='bulk/test/')
        'bulk/test.csv' copied from 'acoe-s3' to 'acoe-s3' bucket as 'bulk/test/test_old.csv'
        >>> s3.info()
        file_name: 	'test_old.csv'
        s3_key: 	'bulk/test/'
        bucket: 	'acoe-s3'
        file_dir: 	'C:/Users/XXX/s3_loads'
        redshift_str: 	'mssql+pyodbc://redshift_acoe'
        
        Returns
        -------
        AWS
            AWS class with new parameters
        """
        file_name = file_name if file_name else self.file_name
        s3_key = s3_key if s3_key else self.s3_key
        bucket = bucket if bucket else self.bucket

        s3_file = self.s3_resource.Object(bucket, s3_key + file_name)

        source_s3_key = self.s3_key + self.file_name
        copy_source = {
            'Key': source_s3_key,
            'Bucket': self.bucket
        }

        s3_file.copy(copy_source)
        print(f"'{source_s3_key}' copied from '{self.bucket}' to '{bucket}' bucket as '{s3_key + file_name}'")

        return AWS(file_name=file_name, s3_key=s3_key, bucket=bucket, file_dir=self.file_dir, redshift_str=self.redshift_str)


    def file_to_s3(self):
        """Writes local file to S3.

        Examples
        --------
        >>> from grizly import get_path
        >>> file_dir=get_path('acoe_projects', 'analytics_project_starter', '01_workflows')
        >>> aws = AWS('test_table.csv', s3_key='analytics_project_starter/test/', file_dir=file_dir)
        >>> aws.file_to_s3()
        """
        file_path = os.path.join(self.file_dir, self.file_name)

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File '{file_path}' does not exist.")

        s3_key = self.s3_key + self.file_name
        s3_file = self.s3_resource.Object(self.bucket, s3_key)
        s3_file.upload_file(file_path)

        print(f"'{self.file_name}' uploaded to '{self.bucket}' bucket as '{s3_key}'")


    def s3_to_file(self):
        """Writes S3 to local file.

        Examples
        --------
        >>> aws = AWS('test.csv', 'bulk/')
        >>> aws.s3_to_file()
        """
        file_path = os.path.join(self.file_dir, self.file_name)

        s3_key = self.s3_key + self.file_name
        s3_file = self.s3_resource.Object(self.bucket, s3_key)
        s3_file.download_file(file_path)

        print(f"'{s3_key}' uploaded to '{file_path}'")


    def df_to_s3(self, df:DataFrame, sep:str='\t'):
        """Saves DataFrame in S3.
        
        Examples
        --------
        >>> from pandas import DataFrame
        >>> df = DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        >>> AWS('test.csv', 'bulk/').df_to_s3(df)

        Parameters
        ----------
        df : DataFrame
            DataFrame object
        sep : str, optional
            Separator, by default '\t'
        """
        if not isinstance(df, DataFrame):
            raise ValueError("'df' must be DataFrame object")

        file_path = os.path.join(self.file_dir, self.file_name)

        if not file_path.endswith('.csv'):
            raise ValueError("Invalid file extention - 'file_name' attribute must end with '.csv'")

        df.to_csv(file_path, index=False, sep=sep)
        print(f"DataFrame saved in '{file_path}'")

        self.file_to_s3()


    def s3_to_rds(self, table:str, schema:str=None, if_exists:{'fail', 'replace', 'append'}='fail', sep:str='\t', types:dict=None) :
        """Writes S3 to Redshift table.    

        Parameters
        ----------
        table : str
            Table name
        schema : str, optional
            Schame name, by default None
        if_exists : {'fail', 'replace', 'append'}, default 'fail'
            How to behave if the table already exists.

            * fail: Raise a ValueError.
            * replace: Clean table before inserting new values.
            * append: Insert new values to the existing table.

        sep : str, optional
            Separator, by default '\t'
        types : dict, optional
            Data types to force, by default None
        """
        if if_exists not in ("fail", "replace", "append"):
            raise ValueError(f"'{if_exists}' is not valid for if_exists")

        table_name = f'{schema}.{table}' if schema else table

        engine = create_engine(self.redshift_str, encoding='utf8')

        if check_if_exists(table, schema, redshift_str=self.redshift_str):
            if if_exists == 'fail':
                raise AssertionError("Table {} already exists".format(table_name))
            elif if_exists == 'replace':
                sql ="DELETE FROM {}".format(table_name)
                engine.execute(sql)
                print('SQL table has been cleaned up successfully.')
            else:
                pass
        else:
            self._create_table_like_s3(table_name, sep, types)
        
        config = ConfigParser()
        config.read(get_path('.aws','credentials'))
        aws_access_key_id = config['default']['aws_access_key_id']
        aws_secret_access_key = config['default']['aws_secret_access_key']

        s3_key = self.s3_key + self.file_name
        print("Loading {} data into {} ...".format(s3_key, table_name))

        sql = f"""
            COPY {table_name} FROM 's3://{self.bucket}/{s3_key}'
            access_key_id '{aws_access_key_id}'
            secret_access_key '{aws_secret_access_key}'
            delimiter '{sep}'
            NULL ''
            IGNOREHEADER 1
            REMOVEQUOTES
            ;commit;
            """

        engine.execute(sql)
        print(f'Data has been copied to {table_name}')


    def df_to_rds(self, df:DataFrame, table:str, schema:str=None, if_exists:{'fail', 'replace', 'append'}='fail', sep:str='\t', types:dict=None):
        """Writes DataFrame to Redshift table.
        
        Parameters
        ----------
        df : DataFrame
            DataFrame object
        table : str
            Table name
        schema : str, optional
            Schema name, by default None
        if_exists : {'fail', 'replace', 'append'}, default 'fail'
            How to behave if the table already exists.

            * fail: Raise a ValueError.
            * replace: Clean table before inserting new values.
            * append: Insert new values to the existing table.

        sep : str, optional
            Separator, by default '\t'
        types : dict, optional
            Data types to force
        """
        self.df_to_s3(df, sep=sep)
        self.s3_to_rds(
            table=table, 
            schema=schema, 
            if_exists=if_exists, 
            sep=sep,
            types=types)


    def _create_table_like_s3(self, table_name, sep, types):
        s3_client = self.s3_resource.meta.client

        obj_content = s3_client.select_object_content(
                        Bucket=self.bucket,
                        Key=self.s3_key + self.file_name,
                        ExpressionType='SQL',
                        Expression="SELECT * FROM s3object LIMIT 21",
                        InputSerialization = {'CSV': {'FileHeaderInfo': 'None', 'FieldDelimiter': sep}},
                        OutputSerialization = {'CSV': {}},
                        )

        records = []
        for event in obj_content['Payload']:
            if 'Records' in event:
                records.append(event['Records']['Payload'])

        file_str = ''.join(r.decode('utf-8') for r in records)
        csv_reader =  reader(StringIO(file_str))

        def isfloat(s):
            try:
                float(s)
                return not s.isdigit()
            except ValueError:
                return False
            
        count = 0
        for row in csv_reader:
            if count == 0:
                column_names = row
                column_isfloat = [[] for i in row]
            else:
                i = 0 
                for item in row:
                    column_isfloat[i].append(isfloat(item))
                    i+=1
            count+=1

        columns = []

        count = 0
        for col in column_names:
            if types and col in types:
                col_type = types[col].upper()
                types.pop(col)
            else:
                if True in set(column_isfloat[count]):
                    col_type = "FLOAT"
                else:
                    col_type = "VARCHAR(500)"
            columns.append(f"{col} {col_type}")
            count += 1
        if types:
            other_cols = list(types.keys())
            print(f"Columns {other_cols} were not found.")
            
        column_str = ", ".join(columns)
        sql = "CREATE TABLE {} ({})".format(table_name, column_str)

        engine = create_engine(self.redshift_str, encoding='utf8')
        engine.execute(sql)

        print("Table {} has been created successfully.".format(table_name))


class _AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(_AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self
