# 0.2 to 0.3

### Overall changes:
- Removed 'bulk/' prefix from all functions which uses s3 

### QFrame:
- Removed read_excel()
- Changed get_sql() output, now it returns SQL string not QFrame

### AWS:
- Removed, switched to S3 class, names of the methods also changed

### S3:
- New class instead of AWS

### Config:
- New class, used to set configuration

