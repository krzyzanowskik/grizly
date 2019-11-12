New Tutorial - QFrame
=====================

QFrame is a class which generates an SQL statement. It stores fields
info ``QFrame.data`` parameter which is a dictionary.

How to create a QFrame
----------------------

We have three basic ways to create a QFrame. We can use a dictionary, an
Excel file or a JSON file.

.. code:: ipython3

    from grizly import (
        get_path, 
        QFrame
    )

Using dictionary
~~~~~~~~~~~~~~~~

This method is the most direct method of creating a QFrame - to use it
you need to know the structure of ``QFrame.data``. From following
dictionary

.. code:: ipython3

    data = {'select': {'table': 'table',
              'schema': 'schema',
              'fields': {'col': {'type': 'dim'}}}}

QFrame will generate a simple sql

.. code:: ipython3

    qf = QFrame().read_dict(data)
    qf.get_sql()


.. parsed-literal::

    SELECT col
    FROM schema.table
    



.. parsed-literal::

    <grizly.qframe.QFrame at 0x1f6d6feba48>



So each dictinary have to have ``select`` key in which we have
``fields`` which we want to have in our SQL statement. Each key have to
have specified ``type`` which can be ‘dim’ if the varibale is a
dimension variable or ‘num’ if the variable is a numeric variable. Let’s
take a look at all options that we can have under ``select`` and
``fields`` keys.

.. code:: ipython3

    data = {'select': {'table': 'table',
              'schema': 'schema',
              'fields': {'column_1': {'type': 'dim',
                'as': '',
                'group_by': '',
                'order_by': '',
                'expression': '',
                'select': '',
                'custom_type': ''}},
              'where': '',
              'distinct': '',
              'having': '',
              'limit': ''}}

-  ``table`` - Name of the table.
-  ``schema`` - Name of the schema.
-  ``fields``, in each field:

   -  ``type`` - Type of the column. Options:

      -  ‘dim’ - VARCHAR(500)
      -  ‘num’ - FLOAT

   Every column has to have specified type. If you want to sepcify
   another type check ``custom_type``.

   -  ``as`` - Column alias (name).

   -  ``group_by`` - Aggregation type. Possibilities:

      -  ‘group’ - This field will go to GROUP BY statement.
      -  {‘sum’, ‘count’, ‘min’, ‘max’, ‘avg’} - This field will by
         aggregated in specified way.

   If you don’t want to aggregate fields leave ``group_by`` empty in
   each field.

   -  ``order_by`` - Put the field in order by statement. Options:

      -  ‘ASC’
      -  ‘DESC’

   -  ``expression`` - Expression, eg. CASE statement, column operation,
      CONCAT statement, … .
   -  ``select`` - Set 0 if you don’t want to put this field in SELECT
      statement.
   -  ``custom_type`` - Specify custom SQL data type, eg. DATE.

-  ``where`` - Add where statement, eg. ‘sales>100’
-  ``distinct`` - Set 1 to add distinct to select
-  ``having`` - Add having statement, eg. ‘sum(sales)>100’
-  ``limit`` - Add limit, eg. 100

Using Excel file
~~~~~~~~~~~~~~~~

Using JSON file
~~~~~~~~~~~~~~~

Read QFrame content
-------------------

Printing SQL
~~~~~~~~~~~~

.. code:: ipython3

    qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
    qf.get_sql()


.. parsed-literal::

    SELECT CustomerId,
           Sales
    FROM schema.table
    



.. parsed-literal::

    <grizly.qframe.QFrame at 0x1f6d6fce3c8>



Getting fields
~~~~~~~~~~~~~~

.. code:: ipython3

    qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
    qf.get_fields()




.. parsed-literal::

    ['CustomerId', 'Sales']



SQL manupulation
----------------

Renaming columns
~~~~~~~~~~~~~~~~

.. code:: ipython3

    qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
    
    qf.rename({'Sales': 'Billings'})
    qf.get_sql()


.. parsed-literal::

    SELECT CustomerId,
           Sales AS Billings
    FROM schema.table
    



.. parsed-literal::

    <grizly.qframe.QFrame at 0x1a62b3b4c48>



Removing fields
~~~~~~~~~~~~~~~

.. code:: ipython3

    qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
    
    qf.remove(['Sales'])
    qf.get_sql()


.. parsed-literal::

    SELECT CustomerId
    FROM schema.table
    



.. parsed-literal::

    <grizly.qframe.QFrame at 0x1a62b55ac88>



Rearranging fields
~~~~~~~~~~~~~~~~~~

.. code:: ipython3

    qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
    
    qf.rearrange(['Sales', 'CustomerId'])
    qf.get_sql()


.. parsed-literal::

    SELECT Sales,
           CustomerId
    FROM schema.table
    



.. parsed-literal::

    <grizly.qframe.QFrame at 0x1f6d6fce348>



Adding WHERE clause
~~~~~~~~~~~~~~~~~~~

.. code:: ipython3

    qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
    
    qf.query("Sales != 0")
    qf.get_sql()


.. parsed-literal::

    SELECT CustomerId,
           Sales
    FROM schema.table
    WHERE Sales != 0
    



.. parsed-literal::

    <grizly.qframe.QFrame at 0x1a62b563408>



Aggregating fields
~~~~~~~~~~~~~~~~~~

.. code:: ipython3

    qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
    
    qf.groupby(['CustomerId'])['Sales'].agg('sum')
    qf.get_sql()


.. parsed-literal::

    SELECT CustomerId,
           sum(Sales) AS Sales
    FROM schema.table
    GROUP BY CustomerId
    



.. parsed-literal::

    <grizly.qframe.QFrame at 0x1a62b56a588>



Adding expressions
~~~~~~~~~~~~~~~~~~

.. code:: ipython3

    qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
    
    qf.assign(Sales_Div="Sales/100", type='num')
    qf.get_sql()


.. parsed-literal::

    SELECT CustomerId,
           Sales,
           Sales/100 AS Sales_Div
    FROM schema.table
    



.. parsed-literal::

    <grizly.qframe.QFrame at 0x1a62b54e0c8>



.. code:: ipython3

    qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
    
    qf.assign(Sales_Positive="CASE WHEN Sales>0 THEN 1 ELSE 0 END")
    qf.get_sql()


.. parsed-literal::

    SELECT CustomerId,
           Sales,
           CASE
               WHEN Sales>0 THEN 1
               ELSE 0
           END AS Sales_Positive
    FROM schema.table
    



.. parsed-literal::

    <grizly.qframe.QFrame at 0x1a62b54ec48>



Adding DISTINCT statement
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: ipython3

    qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
    
    qf.distinct()
    qf.get_sql()


.. parsed-literal::

    SELECT DISTINCT CustomerId,
                    Sales
    FROM schema.table
    



.. parsed-literal::

    <grizly.qframe.QFrame at 0x1a62b563748>



Adding ORDER BY statement
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: ipython3

    qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
    
    qf.orderby(["Sales"])
    qf.get_sql()
    qf.data


.. parsed-literal::

    SELECT CustomerId,
           Sales
    FROM schema.table
    ORDER BY Sales
    



.. parsed-literal::

    {'select': {'fields': {'CustomerId': {'type': 'dim'},
       'Sales': {'type': 'num', 'order_by': 'ASC'}},
      'schema': 'schema',
      'table': 'table',
      'sql_blocks': {'select_names': ['CustomerId', 'Sales'],
       'select_aliases': ['CustomerId', 'Sales'],
       'group_dimensions': [],
       'group_values': [],
       'order_by': ['Sales '],
       'types': ['VARCHAR(500)', 'FLOAT(53)']}}}



.. code:: ipython3

    qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
    
    qf.orderby(["Sales"], ascending=False)
    qf.get_sql()


.. parsed-literal::

    SELECT CustomerId,
           Sales
    FROM schema.table
    ORDER BY Sales DESC
    



.. parsed-literal::

    <grizly.qframe.QFrame at 0x1a62b56fc48>



Adding HAVING statement
~~~~~~~~~~~~~~~~~~~~~~~

.. code:: ipython3

    qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
    
    qf.groupby(['CustomerId'])['Sales'].agg('sum')
    qf.having("sum(sales)>100")
    qf.get_sql()


.. parsed-literal::

    SELECT CustomerId,
           sum(Sales) AS Sales
    FROM schema.table
    GROUP BY CustomerId
    HAVING sum(sales)>100
    



.. parsed-literal::

    <grizly.qframe.QFrame at 0x1f6d6f36308>



Adding LIMIT
~~~~~~~~~~~~

.. code:: ipython3

    qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
    
    qf.limit(100)
    qf.get_sql()


.. parsed-literal::

    SELECT CustomerId,
           Sales
    FROM schema.table
    LIMIT 100
    



.. parsed-literal::

    <grizly.qframe.QFrame at 0x1f6d6edf988>


