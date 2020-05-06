# 0.3.2 to 0.3.3


## QFrame
- `cut()` - fixed bug with omitting first row #408
- `copy()` - interface is now copied as well
- `to_csv()` - interface parameter is now used if specified

## SQLDB
- `get_columns()` - added `varchar` precision while retriving the types from redshift
