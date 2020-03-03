from boto3 import resource
from botocore.exceptions import ClientError
import os
from datetime import datetime, timezone
from ..utils import get_path, check_if_exists
from ..etl import clean
from pandas import DataFrame, read_csv, read_parquet
from sqlalchemy import create_engine
from io import StringIO
from csv import reader
from configparser import ConfigParser
from functools import wraps
import logging


class S3:

    """Class that represents a file in S3.

        Parameters
        ----------
        file_name : str
            Name of the file
        s3_key : str
            Name of s3 key
        bucket : str, optional
            Bucket name, if None then 'acoe-s3'
        file_dir : str, optional
            Path to local folder to store the file, if None then '%UserProfile%/s3_loads'
        redshift_str : str, optional
            Redshift engine string, if None then 'mssql+pyodbc://redshift_acoe'
        """

    def __init__(
        self,
        file_name: str,
        s3_key: str = None,
        bucket: str = None,
        file_dir: str = None,
        redshift_str: str = None,
        min_time_window: int = 0,
        logger=None,
    ):
        self.file_name = file_name
        self.s3_key = s3_key
        self.bucket = bucket if bucket else "acoe-s3"
        self.file_dir = file_dir if file_dir else get_path("s3_loads")
        self.redshift_str = (
            redshift_str if redshift_str else "mssql+pyodbc://redshift_acoe"
        )
        self.s3_resource = resource("s3")
        folders = s3_key.split("/")
        self.full_s3_key = os.path.join(*folders, self.file_name).replace("\\", "/")
        try:
            self.s3_obj = self.s3_resource.Object(self.bucket, self.full_s3_key)
            self.last_modified = self.s3_obj.last_modified
        except:
            self.s3_obj = None
            self.last_modified = None
        # self.last_modified = self.s3_obj.last_modified
        self.min_time_window = min_time_window
        os.makedirs(self.file_dir, exist_ok=True)

        if self.s3_key == "":
            raise ValueError("s3_key not specified")

        if not self.s3_key.endswith("/"):
            self.s3_key += "/"
        self.logger = logger or logging.getLogger(__name__)

    def _check_if_s3_exists(f):
        @wraps(f)
        def wrapped(self, *args, **kwargs):
            s3_key = self.s3_key + self.file_name
            try:
                self.s3_resource.Object(self.bucket, s3_key).load()
            except ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    raise FileNotFoundError(f"{s3_key} file not found")

            return f(self, *args, **kwargs)

        return wrapped

    def _can_upload(self):
        if not self.s3_obj or self.min_time_window == 0:
            return True
        now_utc = datetime.now(timezone.utc)
        diff = now_utc - self.last_modified
        diff_seconds = diff.seconds
        if diff_seconds < self.min_time_window:
            return False
        return True

    def info(self):
        """Print a concise summary of a S3.

        Examples
        --------
        >>> S3('test.csv', 'bulk/', file_dir=r'C:\\Users').info()
        file_name:      'test.csv'
        s3_key:         'bulk/'
        bucket:         'acoe-s3'
        file_dir:       'C:\\Users'
        redshift_str:   'mssql+pyodbc://redshift_acoe'
        """
        print(f"file_name: \t'{self.file_name}'")
        print(f"s3_key: \t'{self.s3_key}'")
        print(f"bucket: \t'{self.bucket}'")
        print(f"file_dir: \t'{self.file_dir}'")
        try:
            print(f"last_modified: \t {self.last_modified}")
        except:
            pass
        print(f"redshift_str: \t'{self.redshift_str}'")

    def list(self):
        """Returns the list of files that are in S3.s3_key
        """
        files = []
        key_list = self.s3_key.split("/")[:-1]
        data = self.s3_resource.meta.client.list_objects(
            Bucket=self.bucket, Prefix=self.s3_key
        )
        if "Contents" in data:
            for file in self.s3_resource.meta.client.list_objects(
                Bucket=self.bucket, Prefix=self.s3_key
            )["Contents"]:
                file_list = file["Key"].split("/")
                for item in key_list:
                    file_list.pop(0)
                if len(file_list) == 1:
                    files.append(file_list[0])
        else:
            files = []
        return files

    @_check_if_s3_exists
    def delete(self):
        """Removes S3 file.
        """
        s3_key = self.s3_key + self.file_name
        self.s3_resource.Object(self.bucket, s3_key).delete()

        print(f"'{s3_key}' has been removed successfully")
        return self

    @_check_if_s3_exists
    def copy_to(
        self,
        file_name: str = None,
        s3_key: str = None,
        bucket: str = None,
        keep_file: bool = True,
    ):
        """Copies S3 file to another S3 file.

        Parameters
        ----------
        file_name : str, optional
            New file name, if None then the same as in class
        s3_key : str, optional
            New S3 key, if None then the same as in class
        bucket : str, optional
            New bucket, if None then the same as in class
        keep_file:
            Whether to keep the original S3 file after copying it to another S3 file, by default True

        Examples
        --------
        >>> s3 = S3('test.csv', 'bulk/', file_dir=r'C:\\Users')
        >>> s3.info()
        file_name: 	    'test.csv'
        s3_key: 	    'bulk/'
        bucket: 	    'acoe-s3'
        file_dir: 	    'C:\\Users'
        redshift_str: 	'mssql+pyodbc://redshift_acoe'
        >>> s3 = s3.copy_to('test_old.csv', s3_key='bulk/test/')
        'bulk/test.csv' copied from 'acoe-s3' to 'acoe-s3' bucket as 'bulk/test/test_old.csv'
        >>> s3.info()
        file_name: 	    'test_old.csv'
        s3_key: 	    'bulk/test/'
        bucket: 	    'acoe-s3'
        file_dir: 	    'C:\\Users'
        redshift_str: 	'mssql+pyodbc://redshift_acoe'

        Returns
        -------
        S3
            S3 class with new parameters
        """
        file_name = file_name if file_name else self.file_name
        s3_key = s3_key if s3_key else self.s3_key
        bucket = bucket if bucket else self.bucket

        s3_file = self.s3_resource.Object(bucket, s3_key + file_name)

        source_s3_key = self.s3_key + self.file_name
        copy_source = {"Key": source_s3_key, "Bucket": self.bucket}

        s3_file.copy(copy_source)
        print(
            f"'{source_s3_key}' copied from '{self.bucket}' to '{bucket}' bucket as '{s3_key + file_name}'"
        )

        if not keep_file:
            self.delete()

        return S3(
            file_name=file_name,
            s3_key=s3_key,
            bucket=bucket,
            file_dir=self.file_dir,
            redshift_str=self.redshift_str,
        )

    def from_file(self, keep_file=True):
        """Writes local file to S3.

        Parameters
        ----------
        min_time_window:
            The minimum time required to pass between the last and current upload for the file to be uploaded.
            This allows the uploads to be robust to retrying (retries will not re-upload the same file, 
            making the upload almost-idempotent)
        keep_file:
            Whether to keep the local file copy after uploading it to Amazon S3, by default True

        Examples
        --------
        >>> file_dir=get_path('acoe_projects', 'analytics_project_starter', '01_workflows')
        >>> s3 = S3('test_table.csv', s3_key='analytics_project_starter/test/', file_dir=file_dir)
        >>> s3 = s3.from_file()
        'test_table.csv' uploaded to 'acoe-s3' bucket as 'analytics_project_starter/test/test_table.csv'
        """
        file_path = os.path.join(self.file_dir, self.file_name)

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File '{file_path}' does not exist.")

        s3_key = self.s3_key + self.file_name
        s3_file = self.s3_resource.Object(self.bucket, s3_key)
        if not self._can_upload():
            msg = (
                f"File {self.file_name} was not uploaded because a recent version exists."
                + f"\nSet S3.min_time_window to 0 to force the upload (currently set to: {self.min_time_window})."
            )
            self.logger.warning(msg)
            return self
        s3_file.upload_file(file_path)

        print(f"'{self.file_name}' uploaded to '{self.bucket}' bucket as '{s3_key}'")

        if not keep_file:
            os.remove(file_path)
            print(f"'{file_path}' has been removed")

        return self

    @_check_if_s3_exists
    def to_file(self):
        r"""Writes S3 to local file.

        Examples
        --------
        >>> S3('test.csv', 'bulk/', file_dir='C:\\Users').to_file()
        'bulk/test.csv' was successfully downloaded to 'C:\Users\test.csv'
        >>> os.remove('C:\\Users\\test.csv')
        """
        file_path = os.path.join(self.file_dir, self.file_name)

        s3_key = self.s3_key + self.file_name
        s3_file = self.s3_resource.Object(self.bucket, s3_key)
        s3_file.download_file(file_path)

        print(f"'{s3_key}' was successfully downloaded to '{file_path}'")

    def to_df(self, **kwargs):

        if not self.file_name.endswith("csv") or self.file_name.endswith("parquet"):
            raise NotImplementedError(
                "Unsupported file format. Please use CSV or Parquet files."
            )

        file_path = os.path.join(self.file_dir, self.file_name)
        self.to_file()

        if self.file_name.endswith("csv"):
            sep = kwargs.get("sep")
            if not sep:
                sep = "\t"
            df = read_csv(file_path, sep=sep)
        else:
            columns = kwargs.get("columns")
            df = read_parquet(file_path, columns=columns)

        return df

    def from_df(self, df: DataFrame, sep: str = "\t", clean_df=False, keep_file=True):
        """Saves DataFrame in S3.

        Examples
        --------
        >>> from pandas import DataFrame
        >>> df = DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        >>> s3 = S3('test.csv', 'bulk/', file_dir=r'C:\\Users').from_df(df, keep_file=False)
        DataFrame saved in 'C:\\Users\\test.csv'
        'test.csv' uploaded to 'acoe-s3' bucket as 'bulk/test.csv'
        'C:\\Users\\test.csv' has been removed

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

        if not file_path.endswith(".csv"):
            raise ValueError(
                "Invalid file extention - 'file_name' attribute must end with '.csv'"
            )

        if clean_df:
            df = clean(df)

        df.to_csv(file_path, index=False, sep=sep)
        print(f"DataFrame saved in '{file_path}'")

        return self.from_file(keep_file=keep_file)

    @_check_if_s3_exists
    def to_rds(
        self,
        table: str,
        schema: str = None,
        if_exists: {"fail", "replace", "append"} = "fail",
        sep: str = "\t",
        types: dict = None,
    ):
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

        table_name = f"{schema}.{table}" if schema else table

        engine = create_engine(self.redshift_str, encoding="utf8")

        if check_if_exists(table, schema, redshift_str=self.redshift_str):
            if if_exists == "fail":
                raise AssertionError("Table {} already exists".format(table_name))
            elif if_exists == "replace":
                sql = "DELETE FROM {}".format(table_name)
                engine.execute(sql)
                print("SQL table has been cleaned up successfully.")
            else:
                pass
        else:
            self._create_table_like_s3(table_name, sep, types)

        config = ConfigParser()
        config.read(get_path(".aws", "credentials"))
        S3_access_key_id = config["default"]["aws_access_key_id"]
        S3_secret_access_key = config["default"]["aws_secret_access_key"]

        s3_key = self.s3_key + self.file_name
        print("Loading {} data into {} ...".format(s3_key, table_name))

        sql = f"""
            COPY {table_name} FROM 's3://{self.bucket}/{s3_key}'
            access_key_id '{S3_access_key_id}'
            secret_access_key '{S3_secret_access_key}'
            delimiter '{sep}'
            NULL ''
            IGNOREHEADER 1
            REMOVEQUOTES
            ;commit;
            """

        engine.execute(sql)
        print(f"Data has been copied to {table_name}")

    def _create_table_like_s3(self, table_name, sep, types):
        s3_client = self.s3_resource.meta.client

        obj_content = s3_client.select_object_content(
            Bucket=self.bucket,
            Key=self.s3_key + self.file_name,
            ExpressionType="SQL",
            Expression="SELECT * FROM s3object LIMIT 21",
            InputSerialization={
                "CSV": {"FileHeaderInfo": "None", "FieldDelimiter": sep}
            },
            OutputSerialization={"CSV": {}},
        )

        records = []
        for event in obj_content["Payload"]:
            if "Records" in event:
                records.append(event["Records"]["Payload"])

        file_str = "".join(r.decode("utf-8") for r in records)
        csv_reader = reader(StringIO(file_str))

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
                    i += 1
            count += 1

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

        engine = create_engine(self.redshift_str, encoding="utf8")
        engine.execute(sql)

        print("Table {} has been created successfully.".format(table_name))
