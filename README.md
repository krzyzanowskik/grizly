Grizly is a highly experimental library to generate SQL statements using the python pandas api. 


# **Getting started**
The main class in grizly is called QFrame. You can load basic table information using a dictionary or an excel file.

```python
from grizly import QFrame
```
### Loading data from dictionary

```python
data = {'select': {
           'fields': {
                      'Customer_ID': {'type': 'dim', 'as': 'ID'},
                      'Customer_Name': {'type': 'dim'},
                      'Country': {'type': 'dim'},
                      'Cur_Year':{'type': 'dim'},
                      'FY18_Sales': {'type': 'num'}, 
                      'FY19_Sales': {'type': 'num'}
            },
           'schema': 'sales_schema',
           'table': 'sales_table'
           }
          }

q = QFrame().from_dict(data)
q.get_sql()
print(q.sql)
```


Which will generate this SQL:
```sql
SELECT Customer_ID AS ID,
       Customer_Name,
       Country,
       Cur_Year,
       FY18_Sales,
       FY19_Sales
FROM sales_schema.sales_table
```
### Data manipulation
* Renaming fields
```python
q.rename({'Country': 'Territory', 'Cur_Year': 'Fiscal_Year'})
q.get_sql()
print(q.sql)
```
```sql
SELECT Customer_ID AS ID,
       Customer_Name,
       Country AS Territory,
       Cur_Year AS Fiscal_Year,
       FY18_Sales,
       FY19_Sales
FROM sales_schema.sales_table
```
* Removing fields
```python
q.remove(['Customer_Name','Cur_Year'])
q.get_sql()
print(q.sql)
```
```sql
SELECT Customer_ID AS ID,
       Country AS Territory,
       FY18_Sales,
       FY19_Sales
FROM sales_schema.sales_table 
```
* Adding WHERE clause
``` python
q.query("Country IN ('France', 'Germany')")
q.get_sql()
print(q.sql)
```
```sql
SELECT Customer_ID AS ID,
       Country AS Territory,
       FY18_Sales,
       FY19_Sales
FROM sales_schema.sales_table
WHERE Country IN ('France',
                  'Germany')
```
* Aggregation
``` python

q.groupby(['Customer_ID', 'Country'])['FY18_Sales'].agg('sum')['FY19_Sales'].agg('sum')
q.get_sql()
print(q.sql)
```
```sql
SELECT Customer_ID AS ID,
       Country AS Territory,
       sum(FY18_Sales) AS FY18_Sales,
       sum(FY19_Sales) AS FY19_Sales
FROM sales_schema.sales_table
WHERE Country IN ('France',
                  'Germany')
GROUP BY ID,
         Territory
```
* Expressions
```python
q.assign(type='num', group_by='sum',FY18_Sales_France="CASE WHEN Country='France' THEN FY18_Sales ELSE 0 END" )
q.get_sql()
print(q.sql)
```
```sql
SELECT Customer_ID AS ID,
       Country AS Territory,
       sum(FY18_Sales) AS FY18_Sales,
       sum(FY19_Sales) AS FY19_Sales,
       sum(CASE
               WHEN Country='France' THEN FY18_Sales
               ELSE 0
           END) AS FY18_Sales_France
FROM sales_schema.sales_table
WHERE Country IN ('France',
                  'Germany')
GROUP BY ID,
         Territory
```
### But why?
Currently Pandas does not support building interactive SQL queries with its api. Pandas is a great library with a great api so why not use the same api to generate SQL statements? This would make the data ecosystem more consistent for analysts and reduce their cognitive load when moving from databases to dataframes. And so here we are.

### Future

Of course any contribution is welcome, but right now it is all very experimental. Ideally in the future we:

* add more sql capabilities (expressions, joins, etc.)
* add support for various databases (now it's only tested on redshift)
* add some visualizations with Altair
