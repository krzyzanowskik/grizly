# 0.2 to 0.3

### Overall changes:
- Removed 'bulk/' prefix from all functions which uses s3

### QFrame:
- Removed read_excel
- Changed get_sql() output, now it returns SQL string not QFrame

### Store:
- Removed get_redshift_columns

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

### Not published
- initialize_logging
- get_last_working_day
- get_denodo_columns (moved to SQLDB method)
- get_redshift_columns (moved to SQLDB method)
