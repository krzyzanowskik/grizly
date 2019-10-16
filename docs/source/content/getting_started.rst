===========================
Getting Started
===========================


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