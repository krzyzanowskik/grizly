import os
import dask
from grizly.tools import AWS
from grizly.utils import file_extension


class Load():
    """Loads file to s3 or database (only csv files)"""
    def __init__(self, file_path:str=None, config=None):
        """
        Parameters
        ----------
        file_path : str, optional
            Path to local file, by default None
        config : module, optional
            Config module, by default None
        """
        self.config = config
        if file_path:
             self.file_path = file_path
        elif config:
            self.file_path = config.file_path
        else:
            raise ValueError("Missing file_path argument.")


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
        AWS
            AWS class
        """
        file_name = os.path.basename(self.file_path)
        file_dir = os.path.dirname(self.file_path)
        aws = AWS(
                file_name=file_name, 
                s3_key=s3_key, 
                bucket=bucket, 
                file_dir=file_dir,
                redshift_str=redshift_str,
                config=self.config
                )
        aws.file_to_s3()
        return aws


    def to_rds(self, table:str, schema:str=None, if_exists:{'fail', 'replace', 'append'}='fail', sep:str='\t', s3_key:str=None, bucket:str=None, redshift_str:str=None, types:dict=None):

        assert file_extension(self.file_path) == '.csv', "This method only supports csv files"

        aws = self.to_s3(
                s3_key=s3_key,
                bucket=bucket,
                redshift_str=redshift_str
                )

        aws.s3_to_rds(
            table=table,
            schema=schema,
            if_exists=if_exists,
            sep=sep,
            types=types
        )
        return aws