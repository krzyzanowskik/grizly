
Tutorial - the Store
--------------------

The idea of store.py is to put both qframe definitions and other type of
definitions in the store for easy handling of many names and variables.

The first thing you do is instantiating the Store()

.. code:: ipython3

    from grizly import Store, get_path, QFrame
    from grizly import get_path
    
    json_path = get_path("acoe_projects","dev", "grizly",
                                "notebooks","store.json")
    
    store = Store(json_path)

The above code will create a “store.json” if it does not exist already
and it will read it into a Python dictionary if it exists.

Adding an SQL query to the json
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Adding a query to the json manually by editing the file can be
cumbersome. You can create a simple query with the ``add_query()``
function

.. code:: ipython3

    store.add_query(["col1", "col2"], "schema", "table", "query_2")
    store.to_store()

The above will add a query named *query_1* into the store. The *query_1*
will not be saved back into the store.json file until you do
*to_json()*. You can, however, check the data by doing store.new_data

Use case, adding a conf key
~~~~~~~~~~~~~~~~~~~~~~~~~~~

We can add a configuration key to use later like:

.. code:: ipython3

    store.add_key(engine_string_rds="some_engine_string")
    store.new_data

We can read any key back by doing (the below code will fail):

.. code:: ipython3

    store.get_key("engine_string_rds")

OK the above code will give you an error because the key
*engine_string_rds* has not yet been saved to our json file. So first we
need to do *to_store()* to save the data and then we can read

.. code:: ipython3

    store.to_store(key="engine_string_rds")

You can return also a list or key from the get_key() to be used in
another functions like QFrame.select() or QFrame.rename() like so:

.. code:: ipython3

    q = QFrame().read_json(json_path=json_path, subquery="query_1")
    q.get_sql(print_sql=False)
    select_names = q.data["select"]["sql_blocks"]["select_names"]
    select_aliases = q.data["select"]["sql_blocks"]["select_aliases"]
    column_keys = dict(zip(select_names, select_aliases)) #<- creating our dictionary
    
    store.add_key(selects_on_query_1 = column_keys)#<- adding our dictionary
    store.to_store()#<- saving

Remember get_columns()
~~~~~~~~~~~~~~~~~~~~~~

You can do

.. code:: python

   from grizly import get_columns()

   columns = get_columns(schema, table, etc.)
   columns_as_dict = dict(zip(columns, columns))

   store.add_key(columns_as_dict)

   store.to_store()

The above can take columns from a denodo or redshift database and put
them automagically into your json file. You can then change your json as
you which and use this dict in your code with store.get_key()

.. code:: ipython3

    store.get_key("selects_on_query_1", oftype="dict")

Key concepts of the store
~~~~~~~~~~~~~~~~~~~~~~~~~

-  You only update the store.json if you do .to_store()
-  You can clean up your non saved data by doing ``store.new_data = {}``
-  You only use the store for building your json, you don’t use much the
   store as part of your workflow
