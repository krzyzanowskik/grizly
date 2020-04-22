# 0.3.1 to 0.3.2

### SQLDB:
- Added parameter `logger` 
- Added parameter `interface` with options: "sqlalchemy", "turbodbc", "pyodbc"
- check_if_exists() - added option `column`

### S3:
- Added parameter `interface`
- to_rds() - works now with `.parquet` files
- breaking change: when skipping upload, s3.status is now 'skipped' rather than 'failed'

### QFrame:
- Added parameter `interface`
- to_parquet() - fixed bugs
- copy() - logger is now copied as well
#### new methods
- to_arrow()
- offset()
- cut()
- window()

