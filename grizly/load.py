import os
import dask
from grizly.tools import S3
from grizly.utils import file_extension


class Load():
    """Loads file to s3 or database (only csv files)
    """
    def __init__(self, file_path:str):
        """
        Parameters
        ----------
        file_path : str, optional
            Path to local file, by default None
        """
        self.file_path = file_path


    def to_s3(self, s3_key:str=None, bucket:str=None, redshift_str:str=None):
        """Writes local file to s3.
        
        Parameters
        ----------
        s3_key : str, optional
            Name of s3 key, default 'bulk/'
        bucket : str, optional
            Bucket name, default 'teis-data'
        redshift_str : str, optional
            Redshift engine string, default 'mssql+pyodbc://Redshift'
        
        Returns
        -------
        S3
            S3 class
        """
        file_name = os.path.basename(self.file_path)
        file_dir = os.path.dirname(self.file_path)
        s3 = S3(
                file_name=file_name, 
                s3_key=s3_key, 
                bucket=bucket, 
                file_dir=file_dir,
                redshift_str=redshift_str
                )
        s3.from_file()
        return s3


    def to_rds(self, table:str, schema:str=None, if_exists:{'fail', 'replace', 'append'}='fail', sep:str='\t', s3_key:str=None, bucket:str=None, redshift_str:str=None, types:dict=None):
        """Writes local csv file to Redshift.
        
        Parameters
        ----------
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
        s3_key : str, optional
            Name of s3 key, if None then 'bulk/'
        bucket : str, optional
            Bucket name, if None then 'teis-data'
        redshift_str : str, optional
            Redshift engine string, if None then 'mssql+pyodbc://Redshift'
        types : dict, optional
            Data types to force, by default None
        """

        assert file_extension(self.file_path) == '.csv', "This method only supports csv files"

        s3 = self.to_s3(
                s3_key=s3_key,
                bucket=bucket,
                redshift_str=redshift_str
                )

        s3.to_rds(
            table=table,
            schema=schema,
            if_exists=if_exists,
            sep=sep,
            types=types
        )