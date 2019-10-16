===========================
Getting Started
===========================

------------
QFrame Intro
------------

You can quickly get started with a QFrame() instance by creating a simple .json file. The most basic json file has this structure:

.. code:: json

    {
    "sales_flash": {
        "select": {
        "fields": {
            "dimension_column_1": {
            "type": "dim",
            "group_by": "group"
            },
            "value_column_1": {
            "type": "dim",
            "as": "some as name",
            "group_by": "sum"
            }
        },
        "schema": "schema",
        "table": "table",
        "limit": 10
        }
    }

The above json file has a simple table definition with a simple limit of 10 records. Remove limit if you want all records.

You can now use the above json file in the python code like so:

 .. code:: python

    from grizly import get_cwd, QFrame
    json = get_cwd("folder", "subfolder", "subquery.json")
    qf = QFrame().read_json(json_path = json, subquery="somesubquery")

Now you have a QFrame() instance. This means you can use your *qf* object like so:

.. code:: python

    qf.get_sql() #<- will generate the sql
    qf.get_sql().sql #<- will return the sql string

You can now also create a dataframe:

.. code:: python

    qf.to_df()

Or you can create a csv file like so:

 .. code:: python

    from grizly import get_cwd, QFrame
    csv_path = get_cwd("folder", "subfolder", "subquery.json")
    qf.to_csv(csv_path)

--------------------
In database workflow
--------------------

Sometimes all the data is in 1 database. Like for instance it is all in Redshift.

In this case we want to materialize the new transformed data inside a new Redshift table. This will make our workflow a lot faster as the data will not need to come back to our desktop and then uploaded again into the database.

We need to keep in mind a couple of things. First, we need to make sure to run *to_sql()* on the qframe, this way grizly will generate the internal sql necessary to create the table. Second, we need to create the table. Finally we can upload the qframe sql into our new table. Note, if the table already exists we don't need to run get_sql or create_table.

.. code:: python

    from grizly import set_cwd, QFrame
    json = set_cwd("acoe_projects", "training", "subquery.json")
    qf = QFrame(engine="mssql+pyodbc://Redshift").read_json(json_path = json, subquery="sandbox")
    qf.get_sql()
    qf.create_table("testing2", schema="z_sandbox_ac")
    qf.to_table(table = "testing2", schema = "z_sandbox_ac", if_exists="replace")

