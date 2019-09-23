Grizly is a highly experimental library to generate SQL statements using the python pandas api. 


<<<<<<< HEAD
# **Getting started!**
=======

# **Getting started!**

>>>>>>> bba8b8ba45ac1daabd173b5145f93b58157c90d8
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
We will be using **chinook.db** to visualize data.
```python
engine_string = "sqlite:///" + os.getcwd() + "\\chinook.db"
```
First table is **tracks** table. We will order by **Name** to mix the rows and we will take only 10 records for better visual effect . 
```python
tracks = {  'select': {
                'fields': {
                    'TrackId': { 'type': 'dim'},
                    'Name': {'type': 'dim'},
                    'AlbumId': {'type': 'dim'},
                    'Composer': {'type': 'dim'},
                    'UnitPrice': {'type': 'num'}
                },
                'table': 'tracks'
            }
}
tracks_qf = QFrame(engine=engine_string).from_dict(tracks).orderby('Name').limit(10)
print(tracks_qf.get_sql().sql)
display(tracks_qf.to_df())
```
```sql
SELECT TrackId,
       Name,
       AlbumId,
       Composer,
       UnitPrice
FROM tracks
ORDER BY Name
LIMIT 10
```
<table>
  <thead>
    <tr style="text-align: right;">
      <th>TrackId</th>
      <th>Name</th>
      <th>AlbumId</th>
      <th>Composer</th>
      <th>UnitPrice</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>3027</td>
      <td>"40"</td>
      <td>239</td>
      <td>U2</td>
      <td>0.99</td>
    </tr>
    <tr>
      <td>2918</td>
      <td>"?"</td>
      <td>231</td>
      <td>None</td>
      <td>1.99</td>
    </tr>
    <tr>
      <td>3412</td>
      <td>"Eine Kleine Nachtmusik" Serenade In G, K. 525...</td>
      <td>281</td>
      <td>Wolfgang Amadeus Mozart</td>
      <td>0.99</td>
    </tr>
    <tr>
      <td>109</td>
      <td>#1 Zero</td>
      <td>11</td>
      <td>Cornell, Commerford, Morello, Wilk</td>
      <td>0.99</td>
    </tr>
    <tr>
      <td>3254</td>
      <td>#9 Dream</td>
      <td>255</td>
      <td>None</td>
      <td>0.99</td>
    </tr>
    <tr>
      <td>602</td>
      <td>'Round Midnight</td>
      <td>48</td>
      <td>Miles Davis</td>
      <td>0.99</td>
    </tr>
    <tr>
      <td>1833</td>
      <td>(Anesthesia) Pulling Teeth</td>
      <td>150</td>
      <td>Cliff Burton</td>
      <td>0.99</td>
    </tr>
    <tr>
      <td>570</td>
      <td>(Da Le) Yaleo</td>
      <td>46</td>
      <td>Santana</td>
      <td>0.99</td>
    </tr>
    <tr>
      <td>3045</td>
      <td>(I Can't Help) Falling In Love With You</td>
      <td>241</td>
      <td>None</td>
      <td>0.99</td>
    </tr>
    <tr>
      <td>3057</td>
      <td>(Oh) Pretty Woman</td>
      <td>242</td>
      <td>Bill Dees/Roy Orbison</td>
      <td>0.99</td>
    </tr>
  </tbody>
</table>

The second table is **playlist_track** table. It contains more than 8k records, here you can see only sample 10 records.
```python
playlist_track = { "select": {
                        "fields":{
                            "PlaylistId": {"type" : "dim"},
                            "TrackId": {"type" : "dim"}
                        },
                        "table" : "playlist_track"
                    }
                }

playlist_track_qf = QFrame(engine=engine_string).from_dict(playlist_track)
print(playlist_track_qf.get_sql().sql)
display(playlist_track_qf.to_df().sample(10))
```
```sql
SELECT PlaylistId,
       TrackId
FROM playlist_track
```
<table>
  <thead>
    <tr style="text-align: right;">
      <th>PlaylistId</th>
      <th>TrackId</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>8</td>
      <td>2667</td>
    </tr>
    <tr>
      <td>1</td>
      <td>1771</td>
    </tr>
    <tr>
      <td>5</td>
      <td>2404</td>
    </tr>
    <tr>
      <td>1</td>
      <td>1136</td>
    </tr>
    <tr>
      <td>1</td>
      <td>2083</td>
    </tr>
    <tr>
      <td>8</td>
      <td>198</td>
    </tr>
    <tr>
      <td>1</td>
      <td>1440</td>
    </tr>
    <tr>
      <td>8</td>
      <td>1689</td>
    </tr>
    <tr>
      <td>1</td>
      <td>2330</td>
    </tr>
    <tr>
      <td>5</td>
      <td>2808</td>
    </tr>
  </tbody>
</table>

Now let's join them on **TrackId**.

```python
joined_qf = join([tracks_qf,playlist_track_qf], join_type="left join", on="sq1.TrackId=sq2.TrackId")

print(joined_qf.get_sql().sql)
display(joined_qf.to_df())
```
```sql
SELECT sq1.TrackId AS TrackId,
       sq1.Name AS Name,
       sq1.AlbumId AS AlbumId,
       sq1.Composer AS Composer,
       sq1.UnitPrice AS UnitPrice,
       sq2.PlaylistId AS PlaylistId
FROM
  (SELECT TrackId,
          Name,
          AlbumId,
          Composer,
          UnitPrice
   FROM tracks
   ORDER BY Name
   LIMIT 10) sq1
LEFT JOIN
  (SELECT PlaylistId,
          TrackId
   FROM playlist_track) sq2 ON sq1.TrackId=sq2.TrackId
```
<table>
  <thead>
    <tr style="text-align: right;">
      <th>TrackId</th>
      <th>Name</th>
      <th>AlbumId</th>
      <th>Composer</th>
      <th>UnitPrice</th>
      <th>PlaylistId</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>3027</td>
      <td>"40"</td>
      <td>239</td>
      <td>U2</td>
      <td>0.99</td>
      <td>1</td>
    </tr>
    <tr>
      <td>3027</td>
      <td>"40"</td>
      <td>239</td>
      <td>U2</td>
      <td>0.99</td>
      <td>8</td>
    </tr>
    <tr>
      <td>2918</td>
      <td>"?"</td>
      <td>231</td>
      <td>None</td>
      <td>1.99</td>
      <td>3</td>
    </tr>
    <tr>
      <td>2918</td>
      <td>"?"</td>
      <td>231</td>
      <td>None</td>
      <td>1.99</td>
      <td>10</td>
    </tr>
    <tr>
      <td>3412</td>
      <td>"Eine Kleine Nachtmusik" Serenade In G, K. 525...</td>
      <td>281</td>
      <td>Wolfgang Amadeus Mozart</td>
      <td>0.99</td>
      <td>1</td>
    </tr>
    <tr>
      <td>3412</td>
      <td>"Eine Kleine Nachtmusik" Serenade In G, K. 525...</td>
      <td>281</td>
      <td>Wolfgang Amadeus Mozart</td>
      <td>0.99</td>
      <td>8</td>
    </tr>
    <tr>
      <td>3412</td>
      <td>"Eine Kleine Nachtmusik" Serenade In G, K. 525...</td>
      <td>281</td>
      <td>Wolfgang Amadeus Mozart</td>
      <td>0.99</td>
      <td>12</td>
    </tr>
    <tr>
      <td>3412</td>
      <td>"Eine Kleine Nachtmusik" Serenade In G, K. 525...</td>
      <td>281</td>
      <td>Wolfgang Amadeus Mozart</td>
      <td>0.99</td>
      <td>15</td>
    </tr>
    <tr>
      <td>109</td>
      <td>#1 Zero</td>
      <td>11</td>
      <td>Cornell, Commerford, Morello, Wilk</td>
      <td>0.99</td>
      <td>1</td>
    </tr>
    <tr>
      <td>109</td>
      <td>#1 Zero</td>
      <td>11</td>
      <td>Cornell, Commerford, Morello, Wilk</td>
      <td>0.99</td>
      <td>8</td>
    </tr>
    <tr>
      <td>3254</td>
      <td>#9 Dream</td>
      <td>255</td>
      <td>None</td>
      <td>0.99</td>
      <td>1</td>
    </tr>
    <tr>
      <td>3254</td>
      <td>#9 Dream</td>
      <td>255</td>
      <td>None</td>
      <td>0.99</td>
      <td>8</td>
    </tr>
    <tr>
      <td>602</td>
      <td>'Round Midnight</td>
      <td>48</td>
      <td>Miles Davis</td>
      <td>0.99</td>
      <td>1</td>
    </tr>
    <tr>
      <td>602</td>
      <td>'Round Midnight</td>
      <td>48</td>
      <td>Miles Davis</td>
      <td>0.99</td>
      <td>8</td>
    </tr>
    <tr>
      <td>1833</td>
      <td>(Anesthesia) Pulling Teeth</td>
      <td>150</td>
      <td>Cliff Burton</td>
      <td>0.99</td>
      <td>1</td>
    </tr>
    <tr>
      <td>1833</td>
      <td>(Anesthesia) Pulling Teeth</td>
      <td>150</td>
      <td>Cliff Burton</td>
      <td>0.99</td>
      <td>8</td>
    </tr>
    <tr>
      <td>570</td>
      <td>(Da Le) Yaleo</td>
      <td>46</td>
      <td>Santana</td>
      <td>0.99</td>
      <td>1</td>
    </tr>
    <tr>
      <td>570</td>
      <td>(Da Le) Yaleo</td>
      <td>46</td>
      <td>Santana</td>
      <td>0.99</td>
      <td>5</td>
    </tr>
    <tr>
      <td>570</td>
      <td>(Da Le) Yaleo</td>
      <td>46</td>
      <td>Santana</td>
      <td>0.99</td>
      <td>8</td>
    </tr>
    <tr>
      <td>3045</td>
      <td>(I Can't Help) Falling In Love With You</td>
      <td>241</td>
      <td>None</td>
      <td>0.99</td>
      <td>1</td>
    </tr>
    <tr>
      <td>3045</td>
      <td>(I Can't Help) Falling In Love With You</td>
      <td>241</td>
      <td>None</td>
      <td>0.99</td>
      <td>5</td>
    </tr>
    <tr>
      <td>3045</td>
      <td>(I Can't Help) Falling In Love With You</td>
      <td>241</td>
      <td>None</td>
      <td>0.99</td>
      <td>8</td>
    </tr>
    <tr>
      <td>3057</td>
      <td>(Oh) Pretty Woman</td>
      <td>242</td>
      <td>Bill Dees/Roy Orbison</td>
      <td>0.99</td>
      <td>1</td>
    </tr>
    <tr>
      <td>3057</td>
      <td>(Oh) Pretty Woman</td>
      <td>242</td>
      <td>Bill Dees/Roy Orbison</td>
      <td>0.99</td>
      <td>8</td>
    </tr>
  </tbody>
</table>

## Multiple join
Third table is **playlists** table.
```python
playlists = { "select": {
                    "fields": {
                        "PlaylistId": {"type" : "dim"},
                        "Name": {"type" : "dim"}
                    },
                    "table" : "playlists"
                }
            }

playlists_qf = QFrame(engine=engine_string).from_dict(playlists)
print(playlists_qf.get_sql().sql)
display(playlists_qf.to_df())
```
```sql
SELECT PlaylistId,
       Name
FROM playlists
```
<table>
  <thead>
    <tr style="text-align: right;">
      <th>PlaylistId</th>
      <th>Name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>1</td>
      <td>Music</td>
    </tr>
    <tr>
      <td>2</td>
      <td>Movies</td>
    </tr>
    <tr>
      <td>3</td>
      <td>TV Shows</td>
    </tr>
    <tr>
      <td>4</td>
      <td>Audiobooks</td>
    </tr>
    <tr>
      <td>5</td>
      <td>90’s Music</td>
    </tr>
    <tr>
      <td>6</td>
      <td>Audiobooks</td>
    </tr>
    <tr>
      <td>7</td>
      <td>Movies</td>
    </tr>
    <tr>
      <td>8</td>
      <td>Music</td>
    </tr>
    <tr>
      <td>9</td>
      <td>Music Videos</td>
    </tr>
    <tr>
      <td>10</td>
      <td>TV Shows</td>
    </tr>
    <tr>
      <td>11</td>
      <td>Brazilian Music</td>
    </tr>
    <tr>
      <td>12</td>
      <td>Classical</td>
    </tr>
    <tr>
      <td>13</td>
      <td>Classical 101 - Deep Cuts</td>
    </tr>
    <tr>
      <td>14</td>
      <td>Classical 101 - Next Steps</td>
    </tr>
    <tr>
      <td>15</td>
      <td>Classical 101 - The Basics</td>
    </tr>
    <tr>
      <td>16</td>
      <td>Grunge</td>
    </tr>
    <tr>
      <td>17</td>
      <td>Heavy Metal Classic</td>
    </tr>
    <tr>
      <td>18</td>
      <td>On-The-Go 1</td>
    </tr>
  </tbody>
</table>

Now if we want to join **tracks**, **playlist_track** and **playlists** tables we can use **TrackId** and **PlaylistId**. The problem is that in **tracks** and **playlists** tables we have the same column **Name**. By default **join** function is taking all fields from the first QFrame, then all the fields from the second QFrame which are not in the first and so on. If we still want to keep all fields from each QFrame we have to set **unique_col=False**.
```python
joined_qf = join(qframes=[tracks_qf, playlist_track_qf, playlists_qf], join_type=
                ['left join', 'left join'], on=[
                 'sq1.TrackId=sq2.TrackId', 'sq2.PlaylistId=sq3.PlaylistId'], unique_col=False)
                 
joined_qf.show_duplicated_columns()
```
![image](https://user-images.githubusercontent.com/52569986/63426683-b6189d80-c413-11e9-91eb-5241937a7b5a.png)

```python
joined_qf.remove(['sq2.TrackId', 'sq2.PlaylistId']).rename({'sq1.Name': 'TrackName', 'sq3.Name': 'PlaylistType'})
print(joined_qf.get_sql().sql)
display(joined_qf.to_df())
```
```sql
SELECT sq1.TrackId AS TrackId,
       sq1.Name AS TrackName,
       sq1.AlbumId AS AlbumId,
       sq1.Composer AS Composer,
       sq1.UnitPrice AS UnitPrice,
       sq3.PlaylistId AS PlaylistId,
       sq3.Name AS PlaylistType
FROM
  (SELECT TrackId,
          Name,
          AlbumId,
          Composer,
          UnitPrice
   FROM tracks
   ORDER BY Name
   LIMIT 10) sq1
LEFT JOIN
  (SELECT PlaylistId,
          TrackId
   FROM playlist_track) sq2 ON sq1.TrackId=sq2.TrackId
LEFT JOIN
  (SELECT PlaylistId,
          Name
   FROM playlists) sq3 ON sq2.PlaylistId=sq3.PlaylistId
```
<table>
  <thead>
    <tr style="text-align: right;">
      <th>TrackId</th>
      <th>TrackName</th>
      <th>AlbumId</th>
      <th>Composer</th>
      <th>UnitPrice</th>
      <th>PlaylistId</th>
      <th>PlaylistType</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>3027</td>
      <td>"40"</td>
      <td>239</td>
      <td>U2</td>
      <td>0.99</td>
      <td>1</td>
      <td>Music</td>
    </tr>
    <tr>
      <td>3027</td>
      <td>"40"</td>
      <td>239</td>
      <td>U2</td>
      <td>0.99</td>
      <td>8</td>
      <td>Music</td>
    </tr>
    <tr>
      <td>2918</td>
      <td>"?"</td>
      <td>231</td>
      <td>None</td>
      <td>1.99</td>
      <td>3</td>
      <td>TV Shows</td>
    </tr>
    <tr>
      <td>2918</td>
      <td>"?"</td>
      <td>231</td>
      <td>None</td>
      <td>1.99</td>
      <td>10</td>
      <td>TV Shows</td>
    </tr>
    <tr>
      <td>3412</td>
      <td>"Eine Kleine Nachtmusik" Serenade In G, K. 525...</td>
      <td>281</td>
      <td>Wolfgang Amadeus Mozart</td>
      <td>0.99</td>
      <td>1</td>
      <td>Music</td>
    </tr>
    <tr>
      <td>3412</td>
      <td>"Eine Kleine Nachtmusik" Serenade In G, K. 525...</td>
      <td>281</td>
      <td>Wolfgang Amadeus Mozart</td>
      <td>0.99</td>
      <td>8</td>
      <td>Music</td>
    </tr>
    <tr>
      <td>3412</td>
      <td>"Eine Kleine Nachtmusik" Serenade In G, K. 525...</td>
      <td>281</td>
      <td>Wolfgang Amadeus Mozart</td>
      <td>0.99</td>
      <td>12</td>
      <td>Classical</td>
    </tr>
    <tr>
      <td>3412</td>
      <td>"Eine Kleine Nachtmusik" Serenade In G, K. 525...</td>
      <td>281</td>
      <td>Wolfgang Amadeus Mozart</td>
      <td>0.99</td>
      <td>15</td>
      <td>Classical 101 - The Basics</td>
    </tr>
    <tr>
      <td>109</td>
      <td>#1 Zero</td>
      <td>11</td>
      <td>Cornell, Commerford, Morello, Wilk</td>
      <td>0.99</td>
      <td>1</td>
      <td>Music</td>
    </tr>
    <tr>
      <td>109</td>
      <td>#1 Zero</td>
      <td>11</td>
      <td>Cornell, Commerford, Morello, Wilk</td>
      <td>0.99</td>
      <td>8</td>
      <td>Music</td>
    </tr>
    <tr>
      <td>3254</td>
      <td>#9 Dream</td>
      <td>255</td>
      <td>None</td>
      <td>0.99</td>
      <td>1</td>
      <td>Music</td>
    </tr>
    <tr>
      <td>3254</td>
      <td>#9 Dream</td>
      <td>255</td>
      <td>None</td>
      <td>0.99</td>
      <td>8</td>
      <td>Music</td>
    </tr>
    <tr>
      <td>602</td>
      <td>'Round Midnight</td>
      <td>48</td>
      <td>Miles Davis</td>
      <td>0.99</td>
      <td>1</td>
      <td>Music</td>
    </tr>
    <tr>
      <td>602</td>
      <td>'Round Midnight</td>
      <td>48</td>
      <td>Miles Davis</td>
      <td>0.99</td>
      <td>8</td>
      <td>Music</td>
    </tr>
    <tr>
      <td>1833</td>
      <td>(Anesthesia) Pulling Teeth</td>
      <td>150</td>
      <td>Cliff Burton</td>
      <td>0.99</td>
      <td>1</td>
      <td>Music</td>
    </tr>
    <tr>
      <td>1833</td>
      <td>(Anesthesia) Pulling Teeth</td>
      <td>150</td>
      <td>Cliff Burton</td>
      <td>0.99</td>
      <td>8</td>
      <td>Music</td>
    </tr>
    <tr>
      <td>570</td>
      <td>(Da Le) Yaleo</td>
      <td>46</td>
      <td>Santana</td>
      <td>0.99</td>
      <td>1</td>
      <td>Music</td>
    </tr>
    <tr>
      <td>570</td>
      <td>(Da Le) Yaleo</td>
      <td>46</td>
      <td>Santana</td>
      <td>0.99</td>
      <td>5</td>
      <td>90’s Music</td>
    </tr>
    <tr>
      <td>570</td>
      <td>(Da Le) Yaleo</td>
      <td>46</td>
      <td>Santana</td>
      <td>0.99</td>
      <td>8</td>
      <td>Music</td>
    </tr>
    <tr>
      <td>3045</td>
      <td>(I Can't Help) Falling In Love With You</td>
      <td>241</td>
      <td>None</td>
      <td>0.99</td>
      <td>1</td>
      <td>Music</td>
    </tr>
    <tr>
      <td>3045</td>
      <td>(I Can't Help) Falling In Love With You</td>
      <td>241</td>
      <td>None</td>
      <td>0.99</td>
      <td>5</td>
      <td>90’s Music</td>
    </tr>
    <tr>
      <td>3045</td>
      <td>(I Can't Help) Falling In Love With You</td>
      <td>241</td>
      <td>None</td>
      <td>0.99</td>
      <td>8</td>
      <td>Music</td>
    </tr>
    <tr>
      <td>3057</td>
      <td>(Oh) Pretty Woman</td>
      <td>242</td>
      <td>Bill Dees/Roy Orbison</td>
      <td>0.99</td>
      <td>1</td>
      <td>Music</td>
    </tr>
    <tr>
      <td>3057</td>
      <td>(Oh) Pretty Woman</td>
      <td>242</td>
      <td>Bill Dees/Roy Orbison</td>
      <td>0.99</td>
      <td>8</td>
      <td>Music</td>
    </tr>
  </tbody>
</table>

### But why?
Currently Pandas does not support building interactive SQL queries with its api. Pandas is a great library with a great api so why not use the same api to generate SQL statements? This would make the data ecosystem more consistent for analysts and reduce their cognitive load when moving from databases to dataframes. And so here we are.

