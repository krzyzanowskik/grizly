Grizly is a highly experimental library to generate SQL statements using the python pandas api. You can do this:

```python
columns = {'FY18_Sales':{'type':'num'}, 'FY19_Sales':{'type':'num'}
           , 'Country':{'type':'dim'}, 'Year':{'type':'dum'}
          }

q = QFrame(schema='sales_schema', table='sales_table').from_dict(columns)

q['FY18_Orders'] = {'type':'num'} #add new column

q.groupby(['Country'])['FY18_Sales', 'FY19_Sales'].agg('sum')
q[((q['Country'] != 'France') & (q['Year'] != 2016))]
sql(q)
```
Which will generate this SQL:
```sql
SELECT FY18_Sales, FY19_Sales, Country, Year, FY18_Orders, sum(sales_table.FY18_Sales), sum(sales_table.FY19_Sales) 
FROM sales_schema.sales_table 
WHERE Country<>'France' and Year<>2016 
GROUP BY sales_table.Country
```
You can also do simple column calculations like so:

```python
q['YoY_Sales'] = q['FY18_Sales'] - q['FY19_Sales']
q.fields['YoY_Sales']['expr']
```
Which for now does not generate valid SQL but will be in the future. For now it generates this expression:

```sql
sales_table.FY18_Sales - sales_table.FY19_Sales
```
### But why?
Currently Pandas does not support building interactive SQL queries with its api. Pandas is a great library with a great api so why not use the same api to generate SQL statements? This would make the data ecosystem more consistent for analysts and reduce their cognitive load when moving from databases to dataframes. And so here we are.

### Future

Of course any contribution is welcome, but right now it is all very experimental. Ideally in the future we:

* add more sql capabilities (expressions, joins, etc.)
* add support for various databases (now it's only tested on redshift)
* add some visualizations with Altair