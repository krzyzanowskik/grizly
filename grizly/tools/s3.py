from boto3 import resource
from botocore.exceptions import ClientError
import os
from datetime import datetime, timezone
from .sqldb import SQLDB
from ..utils import get_path, clean, clean_colnames, file_extension
from pandas import DataFrame, read_csv, read_parquet
from io import StringIO
from csv import reader
from configparser import ConfigParser
import logging
from functools import wraps, partial
import deprecation

deprecation.deprecated = partial(
    deprecation.deprecated, deprecated_in="0.3", removed_in="0.4"
)


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
        self.s3_key = s3_key if s3_key else ""
        if not self.s3_key.endswith("/") and self.s3_key != "":
            self.s3_key += "/"
        self.full_s3_key = self.s3_key + self.file_name
        self.bucket = bucket if bucket else "acoe-s3"
        self.file_dir = file_dir if file_dir else get_path("s3_loads")
        self.redshift_str = (
            redshift_str if redshift_str else "mssql+pyodbc://redshift_acoe"
        )
        self.min_time_window = min_time_window
        os.makedirs(self.file_dir, exist_ok=True)
        self.logger = logger or logging.getLogger(__name__)

    def _check_if_s3_exists(f):
        @wraps(f)
        def wrapped(self, *args, **kwargs):
            s3_key = self.s3_key + self.file_name
            try:
                resource("s3").Object(self.bucket, s3_key).load()
            except ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    raise FileNotFoundError(f"{s3_key} file not found")

            return f(self, *args, **kwargs)

        return wrapped

    def _can_upload(self):
        try:
            last_modified = (
                resource("s3").Object(self.bucket, self.full_s3_key).last_modified
            )
        except:
            last_modified = None

        if not last_modified or self.min_time_window == 0:
            return True

        now_utc = datetime.now(timezone.utc)
        diff = now_utc - last_modified
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
        # try:
        #     print(
        #         f"last_modified: \t {resource('s3').Object(self.bucket, self.full_s3_key).last_modified}"
        #     )
        # except:
        #     pass
        print(f"redshift_str: \t'{self.redshift_str}'")

    def list(self):
        """Returns the list of files that are in S3.s3_key
        """
        files = []
        key_list = self.s3_key.split("/")[:-1]
        data = resource("s3").meta.client.list_objects(
            Bucket=self.bucket, Prefix=self.s3_key
        )
        if "Contents" in data:
            for file in resource("s3").meta.client.list_objects(
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
        resource("s3").Object(self.bucket, s3_key).delete()

        print(f"'{s3_key}' has been removed successfully")

    @_check_if_s3_exists
    def copy_to(
        self,
        file_name: str = None,
        s3_key: str = None,
        bucket: str = None,
        keep_file: bool = True,
        if_exists: {"fail", "skip", "replace", "archive"} = "replace",
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
        keep_file : bool, optional
            Whether to keep the original S3 file after copying it to another S3 file, by default True
        if_exists : str, optional
            How to behave if the output S3 already exists.

            * fail: Raise ValueError
            * skip: Abort without throwing an error
            * replace: Overwrite existing file
            * archive: Move old S3 to s3_key/archive/file_name(version)

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
        if if_exists not in ("fail", "skip", "replace", "archive"):
            raise ValueError(f"'{if_exists}' is not valid for if_exists")
        file_name = file_name if file_name else self.file_name
        s3_key = s3_key if s3_key else self.s3_key
        bucket = bucket if bucket else self.bucket

        out_s3 = S3(
            file_name=file_name,
            s3_key=s3_key,
            bucket=bucket,
            file_dir=self.file_dir,
            redshift_str=self.redshift_str,
        )
        exists = True if file_name in out_s3.list() else False

        if exists:
            if if_exists == "fail":
                raise ValueError(f"{s3_key + file_name} already exists")
            elif if_exists == "skip":
                return self
            elif if_exists == "archive":
                out_s3.archive()

        s3_file = resource("s3").Object(bucket, s3_key + file_name)

        source_s3_key = self.s3_key + self.file_name
        copy_source = {"Key": source_s3_key, "Bucket": self.bucket}

        s3_file.copy(copy_source)
        print(
            f"'{source_s3_key}' copied from '{self.bucket}' to '{bucket}' bucket as '{s3_key + file_name}'"
        )

        if not keep_file:
            self.delete()

        return out_s3

    def from_file(
        self,
        keep_file: bool = True,
        if_exists: {"fail", "skip", "replace", "archive"} = "replace",
    ):
        """Writes local file to S3.

        Parameters
        ----------
        min_time_window:
            The minimum time required to pass between the last and current upload for the file to be uploaded.
            This allows the uploads to be robust to retrying (retries will not re-upload the same file, 
            making the upload almost-idempotent)
        keep_file:
            Whether to keep the local file copy after uploading it to Amazon S3, by default True
        if_exists : str, optional
            How to behave if the S3 already exists.

            * fail: Raise ValueError
            * skip: Abort without throwing an error
            * replace: Overwrite existing file
            * archive: Move old S3 to s3_key/archive/file_name(version)

        Examples
        --------
        >>> file_dir=get_path('acoe_projects', 'analytics_project_starter', '01_workflows')
        >>> s3 = S3('test_table.csv', s3_key='analytics_project_starter/test/', file_dir=file_dir)
        >>> s3 = s3.from_file()
        'test_table.csv' uploaded to 'acoe-s3' bucket as 'analytics_project_starter/test/test_table.csv'
        """
        if if_exists not in ("fail", "skip", "replace", "archive"):
            raise ValueError(f"'{if_exists}' is not valid for if_exists")
        file_path = os.path.join(self.file_dir, self.file_name)

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File '{file_path}' does not exist.")

        exists = True if self.file_name in self.list() else False

        if exists:
            if if_exists == "fail":
                raise ValueError(f"{self.s3_key + self.file_name} already exists")
            elif if_exists == "skip":
                return self
            elif if_exists == "archive":
                self.archive()

        s3_key = self.s3_key + self.file_name
        s3_file = resource("s3").Object(self.bucket, s3_key)

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
    def to_file(
        self, if_exists: {"fail", "skip", "replace"} = "replace",
    ):
        r"""Writes S3 to local file.

        Parameters
        ----------
        if_exists : str, optional
            How to behave if the local file already exists.

            * fail: Raise ValueError
            * skip: Abort without throwing an error
            * replace: Overwrite existing file

        Examples
        --------
        >>> S3('test.csv', 'bulk/', file_dir='C:\\Users').to_file()
        'bulk/test.csv' was successfully downloaded to 'C:\Users\test.csv'
        >>> os.remove('C:\\Users\\test.csv')
        """
        if if_exists not in ("fail", "skip", "replace"):
            raise ValueError(f"'{if_exists}' is not valid for if_exists")
        file_path = os.path.join(self.file_dir, self.file_name)

        if os.path.exists(file_path):
            if if_exists == "fail":
                raise ValueError(f"File {file_path} already exists")
            elif if_exists == "skip":
                return None

        s3_key = self.s3_key + self.file_name
        s3_file = resource("s3").Object(self.bucket, s3_key)
        s3_file.download_file(file_path)

        print(f"'{s3_key}' was successfully downloaded to '{file_path}'")

    def to_df(self, **kwargs):

        if not file_extension(self.file_name) in ["csv", "parquet"]:
            raise NotImplementedError(
                "Unsupported file format. Please use CSV or Parquet files."
            )

        file_path = os.path.join(self.file_dir, self.file_name)
        self.to_file(if_exists=kwargs.get("if_exists"))

        if file_extension(self.file_name) == "csv":
            sep = kwargs.get("sep")
            if not sep:
                sep = "\t"
            df = read_csv(file_path, sep=sep)
        else:
            columns = kwargs.get("columns")
            df = read_parquet(file_path, columns=columns)

        return df

    def from_df(
        self,
        df: DataFrame,
        sep: str = "\t",
        clean_df: bool = False,
        keep_file: bool = True,
        chunksize: int = 10000,
        if_exists: {"fail", "skip", "replace", "archive"} = "replace",
    ):
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
        clean_df : bool, optional
            Whether to clean DataFrame before loading to s3, by default True
        keep_file : bool, optional
            Whether to keep the local file copy after uploading it to Amazon S3, by default True
        chunksize : int, optional
            Rows to write at a time from DataFrame to csv, by default 10000
        if_exists : str, optional
            How to behave if the S3 already exists.

            * fail: Raise ValueError
            * skip: Abort without throwing an error
            * replace: Overwrite existing file
            * archive: Move old S3 to s3_key/archive/file_name(version)
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
        df = clean_colnames(df)

        df.to_csv(file_path, index=False, sep=sep, chunksize=chunksize)
        print(f"DataFrame saved in '{file_path}'")

        return self.from_file(keep_file=keep_file, if_exists=if_exists)

    @_check_if_s3_exists
    def to_rds(
        self,
        table: str,
        schema: str = None,
        if_exists: {"fail", "replace", "append"} = "fail",
        sep: str = "\t",
        types: dict = None,
        column_order: list = None,
        remove_inside_quotes: bool = False,
        time_format: str = None,
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
        column_order : list, optional
            List of column names in other order than default (more info https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-column-mapping.html)
        remove_inside_quotes : bool, optional
            Whether to add REMOVEQUOTES to copy statement, by default False
        """
        if if_exists not in ("fail", "replace", "append"):
            raise ValueError(f"'{if_exists}' is not valid for if_exists")

        sqldb = SQLDB(db="redshift", engine_str=self.redshift_str)
        table_name = f"{schema}.{table}" if schema else table

        if sqldb.check_if_exists(table, schema):
            if if_exists == "fail":
                raise AssertionError(f"Table {table_name} already exists")
            elif if_exists == "replace":
                sqldb.delete_from(table=table, schema=schema)
            else:
                pass
        else:
            self._create_table_like_s3(table_name, sep, types)

        s3_key = self.s3_key + self.file_name

        config = ConfigParser()
        config.read(get_path(".aws", "credentials"))
        S3_access_key_id = config["default"]["aws_access_key_id"]
        S3_secret_access_key = config["default"]["aws_secret_access_key"]

        if column_order != None:
            column_order = "(" + ", ".join(column_order) + ")"
        else:
            column_order = ""
        remove_inside_quotes = "REMOVEQUOTES" if remove_inside_quotes else ""
        sql = f"""
            COPY {table_name} {column_order} FROM 's3://{self.bucket}/{s3_key}'
            access_key_id '{S3_access_key_id}'
            secret_access_key '{S3_secret_access_key}'
            delimiter '{sep}'
            NULL ''
            IGNOREHEADER 1
            {remove_inside_quotes}
            FORMAT AS {file_extension(self.file_name)}
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

        con = sqldb.get_connection()
        print("Loading {} data into {} ...".format(s3_key, table_name))
        con.execute(sql)
        con.close()
        print(f"Data has been copied to {table_name}")

    def archive(self):
        """Moves S3 to 'archive/' key. It adds also the versions of the file eg. file(0).csv, file(1).csv, ...

        Examples
        --------
        >>> s3 = S3('test_old.csv', 'bulk/test/', file_dir=r'C:\\Users')
        >>> s3_arch = s3.archive()
            'bulk/test/test_old.csv' copied from 'acoe-s3' to 'acoe-s3' bucket as 'archive/bulk/test/test_old(0).csv'
            'bulk/test/test_old.csv' has been removed successfully
        >>> s3_arch.delete()
            'archive/bulk/test/test_old(0).csv' has been removed successfully
        """
        s3_archive = S3(
            file_name=self.file_name,
            s3_key="archive/" + self.s3_key,
            bucket=self.bucket,
        )

        version = 0
        while True:
            file_name = (
                s3_archive.file_name.split(".")[0]
                + f"({version})."
                + s3_archive.file_name.split(".")[1]
            )
            if file_name not in s3_archive.list():
                return self.copy_to(
                    file_name=file_name,
                    s3_key="archive/" + self.s3_key,
                    keep_file=False,
                )
            else:
                version += 1

    def _create_table_like_s3(self, table_name, sep, types):
        s3_client = resource("s3").meta.client

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

        sqldb = SQLDB(db="redshift", engine_str=self.redshift_str)
        con = sqldb.get_connection()
        con.execute(sql)
        con.close()

        print("Table {} has been created successfully.".format(table_name))


@deprecation.deprecated(details="Use S3.to_file function instead",)
def s3_to_csv(csv_path, bucket: str = None):
    """
    Writes s3 to csv file.

    Parameters
    ----------
    csv_path : string
        Path to csv file.
    bucket : str, optional
        Bucket name, if None then 'teis-data'
    """
    s3 = S3(
        file_name=os.path.basename(csv_path),
        bucket=bucket,
        file_dir=os.path.dirname(csv_path),
    )
    s3.to_file()


@deprecation.deprecated(details="Use S3.from_file function instead",)
def csv_to_s3(csv_path, s3_key: str = None, keep_csv=True, bucket: str = None):
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
    s3 = S3(
        file_name=os.path.basename(csv_path),
        s3_key=s3_key,
        bucket=bucket,
        file_dir=os.path.dirname(csv_path),
    )
    return s3.from_file(keep_file=keep_csv)


@deprecation.deprecated(
    details="Use S3.from_df and S3.to_rds functions instead. Parameters: schema, dtype, if_exists are ignored.",
)
def df_to_s3(
    df,
    table_name,
    schema=None,
    dtype=None,
    sep="\t",
    clean_df=False,
    keep_csv=True,
    chunksize=10000,
    if_exists="fail",
    redshift_str=None,
    s3_key=None,
    bucket=None,
):
    s3 = S3(
        file_name=table_name + ".csv",
        s3_key=s3_key,
        bucket=bucket,
        file_dir=os.getcwd(),
        redshift_str=redshift_str,
    )

    return s3.from_df(
        df=df, sep=sep, clean_df=clean_df, keep_file=keep_csv, chunksize=chunksize
    )


@deprecation.deprecated(details="Use S3.to_rds function instead.",)
def s3_to_rds(
    file_name,
    table_name=None,
    schema="",
    time_format=None,
    if_exists="fail",
    sep="\t",
    redshift_str=None,
    bucket=None,
    s3_key=None,
    remove_inside_quotes=False,
):

    if not table_name:
        table = file_name.replace(".csv", "")
    else:
        table = table_name
    s3 = S3(
        file_name=file_name, s3_key=s3_key, bucket=bucket, redshift_str=redshift_str
    )
    s3.to_rds(
        table=table,
        schema=schema,
        if_exists=if_exists,
        sep=sep,
        remove_inside_quotes=remove_inside_quotes,
        time_format=time_format,
    )
