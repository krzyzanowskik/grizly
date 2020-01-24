from .tools.qframe import QFrame
from .etl import df_to_s3 as grizly_df_to_s3
import logging
import dask
import boto3
from .orchestrate import retry
from .utils import get_path
from .config import Config
from .tools.s3 import S3

config_path = get_path('.grizly', 'config.json')
Config().from_json(config_path)

@dask.delayed
def load_qf(engine_str, store_path, subquery, upstream=None):
    return QFrame(engine_str=engine_str).read_json(json_path=store_path, subquery=subquery)


@dask.delayed
@retry(Exception, tries=3, delay=10)
def csv_to_s3(csv_path, s3_dir, keep_csv=True, logger=None, upstream=None):
    """
    Writes csv file to s3 in 'acoe-s3' bucket.
    Parameters
    ----------
    csv_path : string
        Path to csv file.
    """
    if not logger:
        logger = logging.getLogger(__name__)

    Config
    s3 = boto3.resource('s3', aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                        region_name=os.getenv("AWS_DEFAULT_REGION"))
    bucket = s3.Bucket('acoe-s3')

    s3_name=os.path.join(s3_dir, os.path.basename(csv_path))

    try:
        bucket.upload_file(csv_path, s3_name)
    except:
        logger.exception("Error loading data to s3")
        raise
    logger.info(f"{os.path.basename(csv_path)} uploaded to s3 as '{s3_name}")

    if not keep_csv:
        os.remove(csv_path)

    return None


@dask.delayed
@retry(Exception, tries=3, delay=10)
def qf_to_rds(qf, csv_path, s3_dir, schema, table, if_exists='fail', redshift_str=None, bucket=None, logger=None, upstream=None):

    if not logger:
        logger = logging.getLogger(__name__)

    try:
        qf.s3_to_rds(
            schema=schema,
            table=table,
            s3_name=os.path.join(s3_dir, os.path.basename(csv_path)),
            if_exists=if_exists,
            redshift_str=redshift_str,
            bucket=bucket,
        )
        logger.info(f"{os.path.basename(csv_path)} uploaded to Redshift")
    except:
        logger.exception("Error loading data to Redshift")
        raise

    return None


# @dask.delayed
# def df_to_s3(df, schema, table_name, dtype=None, clean_df=True, keep_csv=False, if_exists="fail", s3_key=None):
#
#     logger = logging.getLogger(__name__)
#
#     logger.info(f"Uploading {table_name} to S3...")
#
#     grizly_df_to_s3(df, schema=schema, table_name=table_name, dtype=dtype, clean_df=clean_df, keep_csv=keep_csv, if_exists=if_exists,
#             redshift_str='mssql+pyodbc://redshift_acoe', bucket="acoe-s3", s3_key=s3_key)
#
#     logger.info(f"Table {table_name} was successfully uploaded to S3...")
#
#     return None


# @dask.delayed
# def s3_to_rds(file_name, schema, table_name=None, time_format="YYYY-MM-DDTHH:MI:SS", remove_inside_quotes=None, s3_key=None, if_exists="replace", upstream=None):
#
#     logger = logging.getLogger(__name__)
#
#     if not table_name:
#         table_name = file_name.replace('.csv', '')
#
#     s3_to_rds(file_name=file_name, schema=schema, table_name=table_name, time_format=time_format, if_exists=if_exists, remove_inside_quotes=remove_inside_quotes,
#             redshift_str='mssql+pyodbc://redshift_acoe', bucket="acoe-s3", s3_key=s3_key)
#
#     logger.info(f"Table {table_name} was successfully uploaded to Redshift...")
#
#     return None


@dask.delayed
@retry(Exception, tries=3, delay=10)
def df_to_s3(df, file_name, s3_key, bucket=None, file_dir=None, clean_df=False, keep_file=False):
    logger = logging.getLogger(__name__)
    logger.info(f"Uploading {file_name} to S3...")
    s3 = S3(file_name=file_name, s3_key=s3_key, bucket=bucket, file_dir=file_dir).from_df(df=df, clean_df=clean_df, keep_file=keep_file)
    logger.info(f"Table {file_name} was successfully uploaded to S3...")
    return s3


@dask.delayed
@retry(Exception, tries=3, delay=10)
def s3_to_rds(s3: S3, schema, table_name, if_exists="fail", sep='\t', dtype=None):
    logger = logging.getLogger(__name__)
    logger.info(f"Uploading {table_name} to Redshift...")
    s3.to_rds(schema=schema, table=table_name, if_exists=if_exists, sep=sep, types=dtype)
    logger.info(f"Table {table_name} was successfully uploaded to Redshift...")
    return None
