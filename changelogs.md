# 0.3.1 to 0.3.2

### SQLDB:
- Added parameter `logger` 
- Added parameter `interface` with options: "sqlalchemy", "turbodbc", "pyodbc"
- check_if_exists() - added option `column`

### S3:
- to_rds() - works now with `.parquet` files

### QFrame:
- to_parquet() - fixed bugs