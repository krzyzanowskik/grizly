Grizly is a highly experimental library to generate SQL statements using the python pandas api. 


# **Getting started**
The main class in grizly is called QFrame. You can load basic table information using a dictionary or an excel file.

```python
from grizly import QFrame
```
## Loading data from dictionary

```python
data = {'select': {
           'fields': {
                      'CustomerId': {'type': 'dim', 'as': 'ID'},
                      'CustomerName': {'type': 'dim'},
                      'Country': {'type': 'dim'},
                      'FiscalYear':{'type': 'dim'},
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
SELECT CustomerId AS Id,
       CustomerName,
       Country,
       FiscalYear,
       Sales
FROM sales_schema.sales_table
```
### SQL manipulation
* Renaming fields
```python
q.rename({'Country': 'Territory', 'FiscalYear': 'FY'})
q.get_sql()
print(q.sql)
```
```sql
SELECT CustomerId AS Id,
       CustomerName,
       Country AS Territory,
       FiscalYear AS FY,
       Sales
FROM sales_schema.sales_table
```
* Removing fields
```python
q.remove(['CustomerName', 'FiscalYear'])
q.get_sql()
print(q.sql)
```
```sql

SELECT CustomerId AS Id,
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
SELECT CustomerId AS Id,
       Country AS Territory,
       Sales
FROM sales_schema.sales_table
WHERE Country IN ('France',
                  'Germany')
```
* Aggregating fields
``` python

q.groupby(['CustomerId', 'Country'])['Sales'].agg('sum')
q.get_sql()
print(q.sql)
```
```sql
SELECT CustomerId AS Id,
       Country AS Territory,
       sum(Sales) AS Sales
FROM sales_schema.sales_table
WHERE Country IN ('France',
                  'Germany')
GROUP BY Id,
         Territory
```
* Adding expressions
```python
q.assign(type='num', group_by='sum', Sales_Div="Sales/100")
q.get_sql()
print(q.sql)
```
```sql
SELECT CustomerId AS Id,
       Country AS Territory,
       sum(Sales) AS Sales,
       sum(Sales/100) AS Sales_Div
FROM sales_schema.sales_table
WHERE Country IN ('France',
                  'Germany')
GROUP BY Id,
         Territory
```
```python
q.assign(group_by='group', Sales_Positive="CASE WHEN Sales>0 THEN 1 ELSE 0 END")
q.get_sql()
print(q.sql)
```
```sql
SELECT CustomerId AS Id,
       Country AS Territory,
       sum(Sales) AS Sales,
       sum(Sales/100) AS Sales_Div,
       CASE
           WHEN Sales>0 THEN 1
           ELSE 0
       END AS Sales_Positive
FROM sales_schema.sales_table
WHERE Country IN ('France',
                  'Germany')
GROUP BY Id,
         Territory,
         Sales_Positive
```
* Adding DISTINCT statement
```python
q.distinct()
```
```sql
SELECT DISTINCT CustomerId AS Id,
                Country AS Territory,
                sum(Sales) AS Sales,
                sum(Sales/100) AS Sales_Div,
                CASE
                    WHEN Sales>0 THEN 1
                    ELSE 0
                END AS Sales_Positive
FROM sales_schema.sales_table
WHERE Country IN ('France',
                  'Germany')
GROUP BY Id,
         Territory,
         Sales_Positive

```
* Adding ORDER BY statement
```python
q.orderby("Sales")
```
```sql
SELECT DISTINCT CustomerId AS Id,
                Country AS Territory,
                sum(Sales) AS Sales,
                sum(Sales/100) AS Sales_Div,
                CASE
                    WHEN Sales>0 THEN 1
                    ELSE 0
                END AS Sales_Positive
FROM sales_schema.sales_table
WHERE Country IN ('France',
                  'Germany')
GROUP BY Id,
         Territory,
         Sales_Positive
ORDER BY Sales
```
```python
q.orderby(["Country", "Sales"], False)
```
```sql
SELECT DISTINCT CustomerId AS Id,
                Country AS Territory,
                sum(Sales) AS Sales,
                sum(Sales/100) AS Sales_Div,
                CASE
                    WHEN Sales>0 THEN 1
                    ELSE 0
                END AS Sales_Positive
FROM sales_schema.sales_table
WHERE Country IN ('France',
                  'Germany')
GROUP BY Id,
         Territory,
         Sales_Positive
ORDER BY Territory DESC,
         Sales DESC
```

## Loading data from excel file
Now we will be loading fields information from excel file. Your excel file must contain following columns:
* **column** - Name of the column in **table**.
* **column_type** - Type of the column. Possibilities:

     * **dim** - VARCHAR(500)  
     * **num** - FLOAT
     
     Every column has to have specified type. If you want to sepcify another type check **custom_type**.
* **expression** - Expression, eg. CASE statement, column operation, CONCAT statement, ... . In the case of expression **column** should be empty and the alias (name) of the expression should be placed in **column_as**.
* **column_as** - Column alias (name).
* **group_by** - Aggregation type. Possibilities:

     * **group** - This field will go to GROUP BY statement.
     * **{sum, count, min, max, avg}** - This field will by aggregated in specified way.
     
     Please make sure that every field that is placed in SELECT statement (**select** !=0) is also placed in GROUP BY. 
     If you don't want to aggregate fields leave **group_by** empty.
* **select** - Set 0 to remove this field from SELECT statement.
* **custom_type** - Specify custom SQL data type, eg. DATE.
* **schema** - Name of the schema. Always in the first row.
* **table** - Name of the table. Always in the first row.

Now let's take a look at following example.

![alt text](https://github.com/kfk/grizly/blob/0.2/grizly/docs/sales_fields_excel.png)

In this case we also have a column **scope** which is used to filter rows in excel file and in that we are be able to create two different QFrames using **sales_table**. First QFrame contains information about the customer:
```python
customer_qf = QFrame().read_excel('sales_fields.xlsx', sheet_name='sales', query="scope == 'customer'")
customer_qf.get_sql()
print(customer_qf.sql)
```
```sql
SELECT CustomerId AS Id,
       LastName || FirstName AS CustomerName,
       Email,
       Country
FROM sales_schema.sales_table
```
Second QFrame contains information about the customer's sales:
```python
sales_qf = QFrame().read_excel('sales_fields.xlsx', sheet_name='sales', query="scope == 'sales'")
sales_qf.get_sql()
print(sales_qf.sql)
```
```sql
SELECT CustomerId AS Id,
       TransactionDate,
       sum(Sales) AS Sales,
       count(CASE
                 WHEN Sales > 0 THEN 1
                 ELSE 0
             END) AS Sales_Positive
FROM sales_schema.sales_table
GROUP BY Id,
         TransactionDate
```

# Joining data


### But why?
Currently Pandas does not support building interactive SQL queries with its api. Pandas is a great library with a great api so why not use the same api to generate SQL statements? This would make the data ecosystem more consistent for analysts and reduce their cognitive load when moving from databases to dataframes. And so here we are.

