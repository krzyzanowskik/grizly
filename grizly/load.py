import pandas

class Load():
    """Loads file to s3 or database (only csv files)"""
    def __init__(self, file_path:str=None, config=None):
        self.config = config
        self.file_path = file_path


    def to_s3(self, s3_key:str=None, bucket:str=None, redshift_str:str=None):
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


    def to_rds(self, table:str, schema:str=None, if_exists:{'fail', 'replace', 'append'}='fail', sep:str='\t', s3_key:str=None, bucket:str=None, redshift_str:str=None):

        assert os.path.splitext(self.file_path)[1] == '.csv', "This method only supports csv files"

        aws = self.to_s3(
                s3_key=s3_key,
                bucket=bucket,
                redshift_str=redshift_str
                )

        aws.s3_to_rds(
            table=table,
            schema=schema,
            if_exists=if_exists,
            sep=sep
        )
        return aws