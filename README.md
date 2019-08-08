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
                      'Current_Year':{'type': 'dim'},
                      'Sales': {'type': 'num'}
            },
           'schema': 'sales_schema',
           'table': 'sales_table'
           }
          }

q = QFrame().from_dict(data)
q.get_sql()
print(q.sql)
```
```sql
SELECT Customer_ID AS ID,
       Customer_Name,
       Country,
       Current_Year,
       Sales
FROM sales_schema.sales_table
```
### SQL manipulation
* Renaming fields
```python
q.rename({'Country': 'Territory', 'Current_Year': 'Fiscal_Year'})
q.get_sql()
print(q.sql)
```
```sql
SELECT Customer_ID AS ID,
       Customer_Name,
       Country AS Territory,
       Current_Year AS Fiscal_Year,
       Sales
FROM sales_schema.sales_table
```
* Removing fields
```python
q.remove(['Customer_Name', 'Current_Year'])
q.get_sql()
print(q.sql)
```
```sql

SELECT Customer_ID AS ID,
       Country AS Territory,
       Sales
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
       Sales
FROM sales_schema.sales_table
WHERE Country IN ('France',
                  'Germany')
```
* Aggregating fields
``` python

q.groupby(['Customer_ID', 'Country'])['Sales']agg('sum')
q.get_sql()
print(q.sql)
```
```sql
SELECT Customer_ID AS ID,
       Country AS Territory,
       sum(Sales) AS Sales
FROM sales_schema.sales_table
WHERE Country IN ('France',
                  'Germany')
GROUP BY ID,
         Territory
```
* Adding expressions
```python
q.assign(type='num', group_by='sum', Sales_div="Sales/100")
q.get_sql()
print(q.sql)
```
```sql
SELECT Customer_ID AS ID,
       Country AS Territory,
       sum(Sales) AS Sales,
       sum(Sales/100) AS Sales_div
FROM sales_schema.sales_table
WHERE Country IN ('France',
                  'Germany')
GROUP BY ID,
         Territory
```
```python
q.assign(group_by='group', Customer="Customer_Name || '_' || Customer_ID")
q.get_sql()
print(q.sql)
```
```sql
SELECT Customer_ID AS ID,
       Country AS Territory,
       sum(Sales) AS Sales,
       sum(Sales/100) AS Sales_div,
       Customer_Name || '_' || Customer_ID AS Customer
FROM sales_schema.sales_table
WHERE Country IN ('France',
                  'Germany')
GROUP BY ID,
         Territory,
         Customer
```
* Adding DISTINCT statement
```python
q.distinct()
```
```sql
SELECT DISTINCT Customer_ID AS ID,
                Country AS Territory,
                sum(Sales) AS Sales,
                sum(Sales/100) AS Sales_div,
                Customer_Name || '_' || Customer_ID AS Customer
FROM sales_schema.sales_table
WHERE Country IN ('France',
                  'Germany')
GROUP BY ID,
         Territory,
         Customer
```

### But why?
Currently Pandas does not support building interactive SQL queries with its api. Pandas is a great library with a great api so why not use the same api to generate SQL statements? This would make the data ecosystem more consistent for analysts and reduce their cognitive load when moving from databases to dataframes. And so here we are.

### Future

Of course any contribution is welcome, but right now it is all very experimental. Ideally in the future we:

* add more sql capabilities (expressions, joins, etc.)
* add support for various databases (now it's only tested on redshift)
* add some visualizations with Altair
