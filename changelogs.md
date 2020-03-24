# 0.2 to 0.3

### Overall changes:

- Removed 'bulk/' prefix from all functions which uses s3
- Set default engine string 'mssql+pyodbc://redshift_acoe' for redshift and default s3 bucket to 'acoe-s3'
- Changed folder structure. This may affect those of you who does for example `from grizly.orchestrate import ...`.
  Please check the new structure after dowloading 0.3 grizly to your local folder.

### What can go wrong?

- CONFIGURATION: For the S3 we use AWS configuration so if you don't have it in .aws folder please add it. Also for parquet files you need iam_role specified in .aws/credentials file. Config class deals with other configuration (for example Email configuration) so in each workflow you have to specify Config first (check docs).
- PROXY: You can get some connection errors if you don't have at least one of HTTPS_PROXY or HTTP_PROXY specified in env variables. Some libraries may not be installed if you don't have HTTPS_PROXY specified.

### QFrame:

- Changed get_sql() output, now it returns SQL string not QFrame

## NEW CLASSES AND FUNCTIONS

- SFDC
- Github
- S3 (replaced AWS)
- Config (used to set configuration)
- Crosstab (used to bulid crosstab tables)
- SQLDB (contains all functions which interacts with databases)

## REMOVED CLASSES AND FUNCTIONS

### Published

- Excel
- AWS (moved to S3 class, names of the methods also changed)
- Extract (WARNING!!: This class still exists but has completely different attributes and methods)
- Load
- to_s3
- read_s3
- s3_to_rds_qf
- QFrame.read_excel
- Store.get_redshift_columns

### Not published

- initialize_logging
- get_last_working_day
- get_denodo_columns (moved to SQLDB method)
- get_redshift_columns (moved to SQLDB method)

## DEPRECATED FUNCTIONS (WILL BE ROMOVED IN 0.4)

** old function -> what should be used instead **

- QFrame.create_table -> SQLDB.create_table
- QFrame.to_rds -> QFrame.to_csv, S3.from_file and S3.to_rds
- QFrame.csv_to_s3 -> S3.from_file
- QFrame.s3_to_rds -> S3.to_rds
- s3_to_csv -> S3.to_file
- csv_to_s3 -> S3.from_file
- df_to_s3 -> S3.from_df and S3.to_rds
- s3_to_rds -> S3.to_rds
- check_if_exists -> SQLDB.check_if_exists
- create_table -> SQLDB.create_table
- write_to -> SQLDB.write_to
- get_columns -> SQLDB.get_columns
- delete_where -> SQLDB.delete_where
- copy_table -> SQLDB.copy_table
- read_config -> Config().from_json function or in case of AWS credentials - start using S3 class !!!
