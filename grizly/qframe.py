from IPython.display import HTML, display
import pandas
import re
import os
import sqlparse
from copy import deepcopy
import json
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

from grizly.sqlbuilder import (
    get_duplicated_columns,
    get_sql,
    build_column_strings
)

from grizly.excel import (
    read_excel,
    copy_df_to_excel
)

from grizly.etl import (
    to_csv,
    create_table,
    csv_to_s3,
    s3_to_rds_qf,
    write_to
)

from .ui import(
    SubqueryUI,
    FieldUI
)

from grizly.utils import(
    check_if_valid_type
)

import openpyxl


def prepend_table(data, expression):
    field_regex = r"\w+[a-z]"
    escapes_regex = r"""[^"]+"|'[^']+'|and\s|or\s"""
    column_names = re.findall(field_regex, expression)
    columns_to_escape = " ".join(re.findall(escapes_regex, expression))
    for column_name in column_names:
        if column_name in columns_to_escape:
            pass
        else:
            _column_name = data["table"] + "." + column_name
            expression = expression.replace(column_name, _column_name)
            columns_to_escape += " {}".format(column_name)
    return expression


class QFrame:
    """Class which genearates a SQL statement.

    Parameters
    ----------
    data : dict
        Dictionary structure holding fields, schema, table, sql information.

    engine : str
        Engine string. If empty then the engine string is "mssql+pyodbc://DenodoODBC".
        Other engine strings:

        * DenodoPROD: "mssql+pyodbc://DenodoPROD",
        * Redshift: "mssql+pyodbc://Redshift",
        * MariaDB: "mssql+pyodbc://retool_dev_db"
    """
    # KM: can we delete sql argument?
    def __init__(self, data={}, engine='', sql='', getfields=[]):
        self.engine =  engine if engine!='' else "mssql+pyodbc://DenodoODBC"
        self.data = data
        self.sql = sql
        self.getfields = getfields
        self.fieldattrs = ["type", "as", "group_by", "expression", "select", "custom_type", "order_by"]
        self.fieldtypes = ["dim", "num"]
        self.metaattrs = ["limit", "where", "having"]

    def create_sql_blocks(self):
        """Creates blocks which are used to generate an SQL"""
        if self.data == {}:
            print("Your QFrame is empty.")
            return self
        else:
            self.data['select']['sql_blocks'] = build_column_strings(self.data)
            return self


    def validate_data(self, data):
        """Validates loaded data.

        Parameters
        ----------
        data : dict
            Dictionary structure holding fields, schema, table, sql information.

        Returns
        -------
        dict
            Dictionary with validated data.
        """
        return _validate_data(data)


    def show_duplicated_columns(self):
        """Shows duplicated columns.

        Returns
        -------
        QFrame
        """
        duplicates = show_duplicated_columns(self.data)

        if duplicates != {}:
            print("\033[1m", "DUPLICATED COLUMNS: \n", "\033[0m")
            for key in duplicates.keys():
                print("\033[1m", key, "\033[0m", ":\t", duplicates[key], "\n")
            print("Use your_qframe.remove() to remove or your_qframe.rename() to rename columns.")

        else:
            print("There are no duplicated columns.")
        return self


    def save_json(self, json_path, subquery=''):
        """Saves QFrame.data to json file.

        Parameters
        ----------
        json_path : str
            Path to json file.
        subquery : str, optional
            Key in json file, by default ''
        """
        if os.path.isfile(json_path):
            with open(json_path, 'r') as f:
                json_data = json.load(f)
                if json_data =="":
                    json_data = {}
        else:
            json_data = {}

        if subquery != '':
            json_data[subquery] = self.data
        else:
            json_data = self.data

        with open(json_path, 'w') as f:
            json.dump(json_data, f, indent=4)
        print(f"Data saved in {json_path}")


    def read_excel(self, excel_path, sheet_name="", query=""):
        """Reads fields information from excel file.

        Parameters
        ----------
        excel_path : str
            Path to excel file.
        sheet_name : str, optional
            Sheet name, by default ""
        query : str, optional
            Filter for rows in excel file, by default ""

        Returns
        -------
        QFrame
        """
        schema, table, columns_qf = read_excel(excel_path, sheet_name, query)

        data = {"select": {
                    "fields": columns_qf,
                    "schema": schema,
                    "table": table
                }}

        self.data = self.validate_data(data)
        return self

    def build_subquery(self, store_path):
        return SubqueryUI(store_path=store_path).build_subquery()

    def build_field(self, store_path):
        return FieldUI(store_path=store_path).build_field(store_path, self)

    def from_json(self, json_path, subquery=''):
        """Reads QFrame.data from json file.

        Parameters
        ----------
        json_path : str
            Path to json file.
        subquery : str, optional
            Key in json file, by default ''

        Returns
        -------
        QFrame
        """
        with open(json_path, 'r') as f:
            data = json.load(f)
            if data != {}:
                if subquery == '':
                    self.data = self.validate_data(data)
                    #self.engine = data["engine"]
                else:
                    self.data = self.validate_data(data[subquery])
                    #self.engine = data[subquery]["select"]["engine"]
            else:
                self.data = data
        return self

    def read_json(self, json_path, subquery=''):
        """Warning: this function is obsoleted, use from_json instead.
        
        Reads QFrame.data from json file.

        Parameters
        ----------
        json_path : str
            Path to json file.
        subquery : str, optional
            Key in json file, by default ''

        Returns
        -------
        QFrame
        """
        self.from_json(json_path, subquery)
        return self

    def read_dict(self, data):
        """Reads QFrame.data from dictionary.

        Parameters
        ----------
        data : dict
            Dictionary structure holding fields, schema, table, sql information.

        Examples
        --------
        >>> qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
        >>> qf.get_sql()
            SELECT CustomerId,
                Sales
            FROM schema.table

        Returns
        -------
        QFrame
        """
        self.data = self.validate_data(data)
        return self


    def select(self, fields):
        """Creates a subquery that looks like "SELECT sq.col1, sq.col2 FROM (some sql) sq".

        NOTE: Selected fields will be placed in the new QFrame. Names of new fields are created
        as a concat of "sq." and alias in the parent QFrame.

        Examples
        --------
        q -> fields : customer_id as 'customer', date, order

        >>> q.select(["customer_id", "order"])

        q -> fields : sq.customer_id, sq.order

        Parameters
        ----------
        fields : list or str
            Fields in list or field as a string.
            If Fields is * then Select will contain all columns

        Returns
        -------
        QFrame
        """
        self.create_sql_blocks()
        sq_fields = deepcopy(self.data["select"]["fields"])
        new_fields = {}

        if isinstance(fields, str): 
            if fields == "*":
                fields = sq_fields
            else:
                fields = [fields]

        for field in fields:
            if field not in sq_fields:
                print(f"Field {field} not found")

            elif "select"  in sq_fields[field] and sq_fields[field]["select"] == 0:
                print(f"Field {field} is not selected in subquery.")

            else:
                if "as" in sq_fields[field] and sq_fields[field]["as"] != '':
                    alias = sq_fields[field]["as"]
                else:
                    alias = field
                new_fields[f"sq.{alias}"] = {"type": sq_fields[field]["type"], "as": alias}
                if "custom_type" in sq_fields[field] and sq_fields[field]['custom_type'] !='':
                    new_fields[f"sq.{alias}"]["custom_type"] = sq_fields[field]["custom_type"]

        if new_fields:
            data = {"select": {"fields": new_fields }, "sq": self.data}
            self.data = data

        return self


    def rename(self, fields):
        """Renames columns (changes the field alias).

        Parameters
        ----------
        fields : dict
            Dictionary of columns and their new names.

        Examples
        --------
        >>> qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
        >>> qf.rename({'Sales': 'Billings'})
        >>> qf.get_sql()
        SELECT CustomerId,
            Sales AS Billings
        FROM schema.table

        Returns
        -------
        QFrame
        """
        for field in fields:
            if field in self.data["select"]["fields"]:
                self.data["select"]["fields"][field]["as"] = fields[field].replace(" ", "_")
        return self


    def remove(self, fields):
        """Removes fields.

        Parameters
        ----------
        fields : list
            List of fields to remove.

        Examples
        --------
        >>> qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
        >>> qf.remove(['Sales'])
        >>> qf.get_sql()
        SELECT CustomerId
        FROM schema.table

        Returns
        -------
        QFrame
        """
        if isinstance(fields, str) : fields = [fields]

        for field in fields:
            self.data["select"]["fields"].pop(field, f"Field {field} not found.")

        return self


    def distinct(self):
        """Adds DISTINCT statement.

        Examples
        --------
        >>> qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
        >>> qf.distinct()
        >>> qf.get_sql()
        SELECT DISTINCT CustomerId,
                        Sales
        FROM schema.table

        Returns
        -------
        QFrame
        """
        self.data["select"]["distinct"] = 1

        return self


    def query(self, query, if_exists='append', operator='and'):
        """Adds WHERE statement.

        Parameters
        ----------
        query : str
            Where statement.
        if_exists : {'append', 'replace'}, optional
            How to behave when the where clause already exists, by default 'append'
        operator : {'and', 'or'}, optional
            How to add another condition to existing one, by default 'and'

        Examples
        --------
        >>> qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
        >>> qf.query("Sales != 0")
        >>> qf.get_sql()
        SELECT CustomerId,
            Sales
        FROM schema.table
        WHERE Sales != 0

        Returns
        -------
        QFrame
        """
        if if_exists not in ["append", "replace"]:
            raise ValueError("Invalid value in if_exists. Valid values: 'append', 'replace'.")
        if operator not in ['and', 'or']:
            raise ValueError("Invalid value in operator. Valid values: 'and', 'or'.")

        if "union" in self.data["select"]:
            print("You can't add where clause inside union. Use select() method first.")
        else:
            if 'where' not in self.data['select'] or self.data['select']['where'] == '' or if_exists=='replace':
                self.data["select"]["where"] = query
            elif if_exists=='append':
                self.data["select"]["where"] += f" {operator} {query}"
        return self


    def having(self, having, if_exists='append', operator='and'):
        """Adds HAVING statement.

        Parameters
        ----------
        having : str
            Having statement.
        if_exists : {'append', 'replace'}, optional
            How to behave when the having clause already exists, by default 'append'
        operator : {'and', 'or'}, optional
            How to add another condition to existing one, by default 'and'

        
        Examples
        --------
        >>> qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
        >>> qf.groupby(['CustomerId'])['Sales'].agg('sum')
        >>> qf.having("sum(sales)>100")
        >>> qf.get_sql()
        SELECT CustomerId,
            sum(Sales) AS Sales
        FROM schema.table
        GROUP BY CustomerId
        HAVING sum(sales)>100

        Returns
        -------
        QFrame
        """
        if if_exists not in ["append", "replace"]:
            raise ValueError("Invalid value in if_exists. Valid values: 'append', 'replace'.")
        if operator not in ['and', 'or']:
            raise ValueError("Invalid value in operator. Valid values: 'and', 'or'.")

        if "union" in self.data["select"]:
            print("""You can't add having clause inside union. Use select() method first.
            (The GROUP BY and HAVING clauses are applied to each individual query, not the final result set.)""")

        else:
            if if_exists=='replace':
                self.data["select"]["having"] = having
            else:
                if 'having' in self.data['select']:
                    self.data["select"]["having"] += f" {operator} {having}"
                else:
                    self.data["select"]["having"] = having
        return self


    def assign(self, type="dim", group_by='', order_by='', custom_type='',  **kwargs):
        """Assigns expressions.

        Parameters
        ----------
        type : {'dim', 'num'}, optional
            Column type, by default "dim"

            * dim: VARCHAR(500)
            * num: FLOAT(53)
        group_by : {group, sum, count, min, max, avg, ""}, optional
            Aggregation type, by default ""
        order_by : {'ASC','DESC'}, optional
            Sort ascending or descending, by default ''
        custom_type : str, optional
            Column type, by default ''

        Examples
        --------
        >>> qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
        >>> qf.assign(Sales_Div="Sales/100", type='num')
        >>> qf.get_sql()
        SELECT CustomerId,
            Sales,
            Sales/100 AS Sales_Div
        FROM schema.table

        >>> qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
        >>> qf.assign(Sales_Positive="CASE WHEN Sales>0 THEN 1 ELSE 0 END")
        >>> qf.get_sql()
        SELECT CustomerId,
            Sales,
            CASE
                WHEN Sales>0 THEN 1
                ELSE 0
            END AS Sales_Positive
        FROM schema.table

        Returns
        -------
        QFrame
        """
        if type not in ["dim", "num"] and custom_type=='':
            raise ValueError("Custom type is not provided and invalid value in type. Valid values: 'dim', 'num'.")
        if group_by.lower() not in ["group", "sum", "count", "min", "max", "avg", ""]:
            raise ValueError("Invalid value in group_by. Valid values: 'group', 'sum', 'count', 'min', 'max', 'avg', ''.")
        if order_by.lower() not in ["asc", "desc", ""]:
            raise ValueError("Invalid value in order_by. Valid values: 'ASC', 'DESC', ''.")
        if "union" in self.data["select"]:
            print("You can't assign expressions inside union. Use select() method first.")
        else:
            if kwargs is not None:
                for key in kwargs:
                    expression = kwargs[key]
                    self.data["select"]["fields"][key] = {
                        "type": type,
                        "as": key,
                        "group_by": group_by,
                         "order_by": order_by,
                        "expression": expression,
                        "custom_type": custom_type
                        }
        return self


    def groupby(self, fields):
        """Adds GROUP BY statement.

        Parameters
        ----------
        fields : list or string
            List of fields or a field.

        Examples
        --------
        >>> qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
        >>> qf.groupby(['CustomerId'])['Sales'].agg('sum')
        >>> qf.get_sql()
        SELECT CustomerId,
            sum(Sales) AS Sales
        FROM schema.table
        GROUP BY CustomerId

        Returns
        -------
        QFrame
        """
        assert "union" not in self.data["select"], "You can't group by inside union. Use select() method first."

        if isinstance(fields, str) : fields = [fields]

        for field in fields:
            self.data["select"]["fields"][field]["group_by"] = "group"

        return self


    def agg(self, aggtype):
        """Aggregates fields.

        Parameters
        ----------
        aggtype : {'sum', 'count', 'min', 'max', 'avg'}
            Aggregation type.
        
        Examples
        --------
        >>> qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
        >>> qf.groupby(['CustomerId'])['Sales'].agg('sum')
        >>> qf.get_sql()
        SELECT CustomerId,
            sum(Sales) AS Sales
        FROM schema.table
        GROUP BY CustomerId

        Returns
        -------
        QFrame
        """
        if aggtype.lower() not in ["group", "sum", "count", "min", "max", "avg"]:
            raise ValueError("Invalid value in aggtype. Valid values: 'group', 'sum', 'count', 'min', 'max', 'avg'.")

        if "union" in self.data["select"]:
            print("You can't aggregate inside union. Use select() method first.")
        else:
            if isinstance(*self.getfields, tuple):
                self.getfields = list(*self.getfields)

            for field in self.getfields:
                if field in self.data["select"]["fields"]:
                    self.data["select"]["fields"][field]["group_by"] = aggtype
                else:
                    print("Field not found.")

        return self


    def orderby(self, fields, ascending=True):
        """Adds ORDER BY statement.

        Parameters
        ----------
        fields : list or str
            Fields in list or field as a string.
        ascending : bool or list, optional
            Sort ascending vs. descending. Specify list for multiple sort orders, by default True
        
        Examples
        --------
        >>> qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
        >>> qf.orderby(["Sales"])
        >>> qf.get_sql()
        SELECT CustomerId,
            Sales
        FROM schema.table
        ORDER BY Sales

        >>> qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
        >>> qf.orderby(["Sales"], ascending=False)
        >>> qf.get_sql()
        SELECT CustomerId,
            Sales
        FROM schema.table
        ORDER BY Sales DESC

        Returns
        -------
        QFrame
        """
        if isinstance(fields, str) : fields = [fields]
        if isinstance(ascending, bool) : ascending = [ascending for item in fields]

        assert len(fields) == len(ascending), "Incorrect list size."

        iterator = 0
        for field in fields:
            if field in self.data["select"]["fields"]:
                order = 'ASC' if ascending[iterator] else 'DESC'
                self.data["select"]["fields"][field]["order_by"] = order
            else:
                print(f"Field {field} not found.")

            iterator+=1

        return self


    def limit(self, limit):
        """Adds LIMIT statement.

        Parameters
        ----------
        limit : int or str
            Number of rows to select.
        
        Examples
        --------
        >>> qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
        >>> qf.limit(100)
        >>> qf.get_sql()
        SELECT CustomerId,
            Sales
        FROM schema.table
        LIMIT 100

        Returns
        -------
        QFrame
        """
        self.data["select"]["limit"] = str(limit)

        return self


    def rearrange(self, fields):
        """Changes order of the columns.

        Parameters
        ----------
        fields : list or str
            Fields in list or field as a string.
        
        Examples
        --------
        >>> qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
        >>> qf.rearrange(['Sales', 'CustomerId'])
        >>> qf.get_sql()
        SELECT Sales,
            CustomerId
        FROM schema.table

        Returns
        -------
        QFrame
        """
        if isinstance(fields, str) : fields = [fields]

        old_fields = deepcopy(self.data['select']['fields'])
        assert set(old_fields) == set(fields) and len(old_fields) == len(fields), "Fields are not matching, make sure that fields are the same as in your QFrame."

        new_fields = {}
        for field in fields :
            new_fields[field] = old_fields[field]

        self.data['select']['fields'] = new_fields

        self.create_sql_blocks()

        return self


    def get_fields(self):
        """Returns list of QFrame fields.

        Examples
        --------
        >>> qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
        >>> qf.get_fields()
        ['CustomerId', 'Sales']

        Returns
        -------
        list
            List of field names
        """

        fields = list(self.data['select']['fields'].keys()) if self.data else []

        return fields


    def get_sql(self, print_sql=True):
        """Overwrites the SQL statement inside the class and prints saved string.

        Parameters
        ----------
        print_sql : bool, optional
            If True prints generated SQL statement, by default True

        Examples
        --------
        >>> qf = QFrame().read_dict(data = {'select': {'fields': {'CustomerId': {'type': 'dim'}, 'Sales': {'type': 'num'}}, 'schema': 'schema', 'table': 'table'}})
        >>> qf.get_sql()
        SELECT CustomerId,
            Sales
        FROM schema.table

        Returns
        -------
        QFrame
        """
        self.create_sql_blocks()
        self.sql = get_sql(self.data)

        if print_sql:
            print(self.sql)

        return self


    def create_table(self, table, schema='', char_size=500):
        """Creates a new empty QFrame table in database if the table doesn't exist.

        Parameters
        ----------
        table : str
            Name of SQL table.
        engine : str
            Engine string (where we want to create table).
        schema : str, optional
            Specify the schema.

        Returns
        -------
        QFrame
        """
        create_table(qf=self, table=table, engine=self.engine, schema=schema, char_size=char_size)
        return self

    ## Non SQL Processing

    def to_csv(self, csv_path, chunksize=None, debug=False, cursor=None):
        """Writes QFrame table to csv file.

        Parameters
        ----------
        csv_path : str
            Path to csv file.
        chunksize : int, default None
            If specified, return an iterator where chunksize is the number of rows to include in each chunk.
        cursor : Cursor, optional
            The cursor to be used to execute the SQL, by default None

        Returns
        -------
        QFrame
        """
        self.create_sql_blocks()
        self.sql = get_sql(self.data)

        row_count = to_csv(qf=self, csv_path=csv_path, sql=self.sql, engine=self.engine, chunksize=chunksize, cursor=cursor)
        if debug:
            return row_count
        return self


    def to_rds(self, table, csv_path, schema='', if_exists='fail', sep='\t', use_col_names=True, chunksize=None, keep_csv=True, cursor=None, redshift_str=None, bucket=None):
        """Writes QFrame table to Redshift database.

        Parameters
        ----------
        table : str
            Name of SQL table
        csv_path : str
            Path to csv file
        schema : str, optional
            Specify the schema
        if_exists : {'fail', 'replace', 'append'}, optional
            How to behave if the table already exists, by default 'fail'

            * fail: Raise a ValueError
            * replace: Clean table before inserting new values.
            * append: Insert new values to the existing table

        sep : str, optional
            Separator/delimiter in csv file, by default '\t'
        use_col_names : bool, optional
            If True the data will be loaded by the names of columns, by default True
        chunksize : int, optional
            If specified, return an iterator where chunksize is the number of rows to include in each chunk, by default None
        keep_csv : bool, optional
            Whether to keep the local csv copy after uploading it to Amazon S3, by default True
        cursor : Cursor, optional
            The cursor to be used to execute the SQL, by default None
        redshift_str : str, optional
            Redshift engine string, by default 'mssql+pyodbc://Redshift'
        bucket : str, optional
            Bucket name, if None then 'teis-data'

        Examples
        --------
        With defaults:

        >>> q = QFrame(
        >>>   ).to_rds(table='some_table', csv_path='some_path')

        With schema:

        >>> q = QFrame(
        >>>   ).to_rds(schema='some_schema', table='some_table', csv_path='some_path')

        Returns
        -------
        QFrame
        """
        self.create_sql_blocks()
        self.sql = get_sql(self.data)

        to_csv(self,csv_path, self.sql, engine=self.engine, sep=sep, chunksize=chunksize, cursor=cursor)
        csv_to_s3(csv_path, keep_csv=keep_csv, bucket=bucket)

        s3_to_rds_qf(self, 
                    table, 
                    s3_name=os.path.basename(csv_path), 
                    schema=schema, 
                    if_exists=if_exists, 
                    sep=sep, 
                    use_col_names=use_col_names, 
                    redshift_str=redshift_str,
                    bucket=bucket)

        return self

    
    def to_table(self, table, schema='', if_exists='fail'):
        """Inserts values from QFrame object into given table. Name of columns in qf and table have to match each other.
        
        Parameters
        ----------
        table: str
            Name of SQL table
        schema: str
            Specify the schema
        if_exists : {'fail', 'replace', 'append'}, optional
            How to behave if the table already exists, by default 'fail'

            * fail: Raise a ValueError.
            * replace: Clean table before inserting new values.
            * append: Insert new values to the existing table.

        Returns
        -------
        QFrame
        """
        write_to(qf=self,table=table,schema=schema, if_exists=if_exists)
        return self


    def to_df(self):
        """Writes QFrame to DataFrame. Uses pandas.read_sql.

        TODO: DataFarme types should correspond to types defined in QFrame data.

        Returns
        -------
        DataFrame
            Data generated from sql.
        """
        self.create_sql_blocks()
        self.sql = get_sql(self.data)

        con = create_engine(self.engine, encoding='utf8', poolclass=NullPool)
        df = pandas.read_sql(sql=self.sql, con=con)
        return df

      
    def to_sql(self, table, engine, schema='', if_exists='fail', index=True,
                index_label=None, chunksize=None, dtype=None, method=None):
        """Writes QFrame to DataFarme and then DataFarme to SQL database. Uses pandas.to_sql.

        Parameters
        ----------
        table : str
            Name of SQL table.
        engine : str
            Engine string.
        schema : string, optional
            Specify the schema.
        if_exists : {'fail', 'replace', 'append'}, default 'fail'
            How to behave if the table already exists.

            * fail: Raise a ValueError.
            * replace: Drop the table before inserting new values.
            * append: Insert new values to the existing table.

        index : bool, default True
            Write DataFrame index as a column. Uses `index_label` as the column
            name in the table.
        index_label : str or sequence, default None
            Column label for index column(s). If None is given (default) and
            `index` is True, then the index names are used.
            A sequence should be given if the DataFrame uses MultiIndex.
        chunksize : int, optional
            Rows will be written in batches of this size at a time. By default,
            all rows will be written at once.
        dtype : dict, optional
            Specifying the datatype for columns. The keys should be the column
            names and the values should be the SQLAlchemy types or strings for
            the sqlite3 legacy mode.
        method : {None, 'multi', callable}, default None
            Controls the SQL insertion clause used:

            * None : Uses standard SQL ``INSERT`` clause (one per row).
            * 'multi': Pass multiple values in a single ``INSERT`` clause.
            * callable with signature ``(pd_table, conn, keys, data_iter)``.
        """
        df = self.to_df()
        con = create_engine(self.engine, encoding='utf8', poolclass=NullPool)

        df.to_sql(name=table, con=con, schema=schema, if_exists=if_exists,
        index=index, index_label=index_label, chunksize= chunksize, dtype=dtype, method=method)
        return self

    def to_excel(self, input_excel_path, output_excel_path, sheet_name='', startrow=0, startcol=0, index=False, header=False):
        """Saves data to Excel file.

        Parameters
        ----------
        input_excel_path : str
            Path to template Excel file
        output_excel_path : str
            Path to Excel file in which we want to save data
        sheet_name : str, optional
            Sheet name, by default ''
        startrow : int, optional
            Upper left cell row to dump data, by default 0
        startcol : int, optional
            Upper left cell column to dump data, by default 0
        index : bool, optional
            Write row index, by default False
        header : bool, optional
            Write header, by default False

        Returns
        -------
        QFrame
        """
        df = self.to_df()
        copy_df_to_excel(df=df, input_excel_path=input_excel_path, output_excel_path=output_excel_path, sheet_name=sheet_name, startrow=startrow, startcol=startcol,index=index, header=header)

        return df

    #AC: this probably needs to be removed
    def csv_to_s3(self, csv_path, keep_csv=True, bucket=None):
        """Writes csv file to s3.

        Parameters
        ----------
        csv_path : str
            Path to csv file.
        keep_csv : bool, optional
            Whether to keep the local csv copy after uploading it to Amazon S3, by default True
        bucket : str, optional
            Bucket name, if None then 'teis-data'

        Returns
        -------
        QFrame
        """
        csv_to_s3(csv_path, keep_csv=keep_csv, bucket=bucket)
        return self

    #AC: this probably needs to be removed
    def s3_to_rds(self, table, s3_name, schema='', if_exists='fail', sep='\t', use_col_names=True, redshift_str=None, bucket=None):
        """Writes s3 to Redshift database.

        Parameters
        ----------
        table : str
            Name of SQL table.
        s3_name : str
            Name of s3 file from which we want to load data.
        schema : str, optional
            Specify the schema.
        if_exists : {'fail', 'replace', 'append'}, default 'fail'
            How to behave if the table already exists.

            * fail: Raise a ValueError.
            * replace: Clean table before inserting new values.
            * append: Insert new values to the existing table.

        sep : str, default '\t'
            Separator/delimiter in csv file.
        use_col_names : bool, optional
            If True the data will be loaded by the names of columns, by default True
        redshift_str : str, optional
            Redshift engine string, by default 'mssql+pyodbc://Redshift'
        bucket : str, optional
            Bucket name, if None then 'teis-data'

        Returns
        -------
        QFrame
        """
        self.create_sql_blocks()
        self.sql = get_sql(self.data)

        s3_to_rds_qf(self, 
                    table, 
                    s3_name=s3_name, 
                    schema=schema , 
                    if_exists=if_exists, 
                    sep=sep, 
                    use_col_names=use_col_names, 
                    redshift_str=redshift_str,
                    bucket=bucket)
        return self


    def copy(self):
        """Makes a copy of QFrame.

        Returns
        -------
        QFrame
        """
        data = deepcopy(self.data)
        engine = deepcopy(self.engine)
        sql = deepcopy(self.sql)
        getfields = deepcopy(self.getfields)
        return QFrame(data=data, engine=engine, sql=sql, getfields=getfields)


    def __getitem__(self, getfields):
        self.getfields = []
        self.getfields.append(getfields)
        return self


def join(qframes=[], join_type=None, on=None, unique_col=True):
    """Joins QFrame objects. Returns QFrame.

    Name of each field is a concat of: "sq" + position of parent QFrame in qframes + "." + alias in their parent QFrame.
    If the fields have the same aliases in their parent QFrames they will have the same aliases in joined QFrame.

    By default the joined QFrame will contain all fields from the first QFrame and all fields from the other QFrames
    which are not in the first QFrame. This approach prevents duplicates. If you want to choose the columns, set unique_col=False and
    after performing join please remove fields with the same aliases or rename the aliases.

    Parameters
    ----------
    qframes : list
        List of qframes
    join_type : str or list
        Join type or a list of join types.
    on : str or list
        List of on join conditions. In case of CROSS JOIN set the condition on 0.
        NOTE: Structure of the elements of this list is very specific. You always have to use prefix "sq{qframe_position}."
        if you want to refer to the column. Check examples.
    unique_col : boolean, optional
        If True the joined QFrame will cotain all fields from the first QFrame and all fields from other QFrames which
        are not repeated. If False the joined QFrame will contain all fields from every QFrame, default True


    NOTE: Order of the elements in join_type and on list is important.

    TODO: Add validations on engines. QFarmes engines have to be the same.

    Examples
    --------
    qframes:
    q1 -> fields: customer_id, orders
    q2 -> fields: customer_id, orders as 'ord'

    >>> q_joined = join(qframes=[q1,q2], join_type="LEFT JOIN", on="sq1.customer_id=sq2.customer_id")

    q_joined -> fields: sq1.customer_id as 'customer_id', sq1.orders as 'orders',
                        sq2.ord as 'ord'

    >>> q_joined.get_sql()
        SELECT  sq1.customer_id as 'customer_id',
                sq1.orders as 'orders',
                sq2.ord as 'ord'
        FROM
            (q1.sql) sq1
        LEFT JOIN
            (q2.sql) sq2
        ON sq1.customer_id=sq2.customer_id


    qframes:
    q1 -> fields: customer_id, orders
    q2 -> fields: customer_id, orders as 'ord'
    q3 -> fields: id, orders, date

    >>> q_joined = join(qframes=[q1,q2,q3], join_type=["CROSS JOIN", "inner join"], on=[0, "sq2.customer_id=sq3.id"], unique_col=False)

    q_joined -> fields: sq1.customer_id as 'customer_id', sq1.orders as 'orders',
                        sq2.customer_id as 'customer_id', sq2.ord as 'ord',
                        sq3.id as 'id', sq3.orders as 'orders', sq3.date as 'date'

    >>> q_joined.show_duplicated_columns()
        DUPLICATED COLUMNS:
            customer_id : ['sq1.customer_id', 'sq2.customer_id']
            orders : ['sq1.orders', 'sq3.orders']

    >>> q_joined.remove(['sq2.customer_id', 'sq3.id'])
    >>> q_joined.rename({'sq1.orders': 'orders_1', 'sq2.ord': 'orders_2', 'sq3.orders' : 'orders_3})

    q_joined -> fields: sq1.customer_id as 'customer_id', sq1.orders as 'orders_1',
                        sq2.ord as 'orders_2',
                        sq3.orders as 'orders_3', sq3.date as 'date

    >>> q_joined.get_sql()
        SELECT  sq1.customer_id as 'customer_id',
                sq1.orders as 'orders_1',
                sq2.ord as 'orders_2',
                sq3.orders as 'orders_3',
                sq3.date as 'date
        FROM
            (q1.sql) sq1
        CROSS JOIN
            (q2.sql) sq2
        INNER JOIN
            (q3.sql) sq3 ON sq2.customer_id=sq3.id

    Returns
    -------
    QFrame
    """
    assert len(qframes) == len(join_type)+1 or len(qframes)==2 and isinstance(join_type,str), "Incorrect list size."
    assert len(qframes)==2 and isinstance(on,(int,str)) or len(join_type) == len(on) , "Incorrect list size."

    data = {'select': {'fields': {} }}
    aliases = []

    iterator = 0
    for q in qframes:
        q.create_sql_blocks()
        iterator += 1
        data[f"sq{iterator}"] = deepcopy(q.data)
        sq = deepcopy(q.data['select'])

        for alias in sq["sql_blocks"]["select_aliases"]:
            if unique_col and alias in aliases:
                continue
            else:
                aliases.append(alias)
                for field in sq["fields"]:
                    if field == alias or "as" in sq["fields"][field] and sq["fields"][field]["as"] == alias:
                        data["select"]["fields"][f"sq{iterator}.{alias}"] = {"type": sq["fields"][field]["type"], "as": alias}
                        if "custom_type" in sq["fields"][field] and sq["fields"][field]["custom_type"] != "":
                            data["select"]["fields"][f"sq{iterator}.{alias}"]["custom_type"] = sq["fields"][field]["custom_type"]
                        break

    if isinstance(join_type, str) : join_type = [join_type]
    if isinstance(on, (int,str)) : on = [on]

    data["select"]["join"] = { "join_type": join_type, "on": on}

    print("Data joined successfully.")
    if not unique_col:
        print("Please remove or rename duplicated columns. Use your_qframe.show_duplicated_columns() to check duplicates.")
    return QFrame(data=data, engine=qframes[0].engine)


def union(qframes=[], union_type=None, union_by='position'):
    """Unions QFrame objects. Returns QFrame.

    Parameters
    ----------
    qframes : list
        List of qframes
    union_type : str or list
        Type or list of union types. Valid types: 'UNION', 'UNION ALL'.
    union_by : {'position', 'name'}, optional
        How to union the qframe, by default 'position'

        * position: union by position of the field
        * name: union by the field aliases
    
    Examples
    --------
    >>> q_unioned = union(qframes=[q1, q2, q3], union_type=["UNION ALL", "UNION"])
    >>> q_unioned.get_sql()
        q1.sql
        UNION ALL
        q2.sql
        UNION
        q3.sql


    Returns
    -------
    QFrame
    """
    if isinstance(union_type, str) : union_type = [union_type]

    assert len(qframes) >= 2, "You have to specify at least 2 qframes to perform a union."
    assert len(qframes) == len(union_type)+1, "Incorrect list size."
    assert set(item.upper() for item in union_type) <= {"UNION", "UNION ALL"}, "Incorrect union type. Valid types: 'UNION', 'UNION ALL'."
    if union_by not in {'position', 'name'}:
        raise ValueError("Invalid value for union_by. Valid values: 'position', 'name'.")
    
    data = {'select': {'fields': {}}}

    main_qf = qframes[0]
    main_qf.create_sql_blocks()
    data["sq1"] = deepcopy(main_qf.data)
    old_fields = deepcopy(main_qf.data["select"]["fields"])
    new_fields = deepcopy(main_qf.data["select"]["sql_blocks"]["select_aliases"])
    new_types = deepcopy(main_qf.data["select"]["sql_blocks"]["types"])
    qframes.pop(0)

    iterator = 2
    for qf in qframes:
        assert main_qf.engine == qf.engine, "QFrames have different engine strings."
        qf.create_sql_blocks()
        qf_aliases = qf.data["select"]["sql_blocks"]["select_aliases"]
        assert len(new_fields) == len(qf_aliases), f"Amount of fields in {iterator}. QFrame doesn't match amount of fields in 1. QFrame."
        
        if union_by == 'name':
            field_diff_1 = set(new_fields) - set(qf_aliases)
            field_diff_2 = set(qf_aliases) - set(new_fields)
            assert field_diff_1 == set() and field_diff_2 == set(), f"""Aliases {field_diff_2} not found in 1. QFrame, aliases {field_diff_1} not found in {iterator}. QFrame. Use qf.rename() to rename fields or set option union_by='position'"""
            ordered_fields = []
            for new_field in new_fields:
                fields = deepcopy(qf.data['select']['fields'])
                for field in fields:
                    if field == new_field or "as" in fields[field] and fields[field]["as"] == new_field:
                        ordered_fields.append(field)
                        break
            qf.rearrange(ordered_fields)
            qf.create_sql_blocks()

        for new_field in new_fields:
            field_position = new_fields.index(new_field)
            new_type = new_types[field_position]
            qf_alias = qf.data['select']['sql_blocks']['select_aliases'][field_position]
            qf_type = qf.data['select']['sql_blocks']['types'][field_position]
            assert qf_type == new_type, f"Types don't match. 1. QFrame alias: {new_field} type: {new_type}, {iterator}. QFrame alias: {qf_alias} type: {qf_type}."

        data[f"sq{iterator}"] = deepcopy(qf.data)
        iterator += 1

    for field in old_fields:
        if "select" in old_fields[field] and old_fields[field]["select"] == 0:
            continue
        else:
            if "as" in old_fields[field] and old_fields[field]["as"] != "":
                alias = old_fields[field]["as"]
            else:
                alias = field

            data["select"]["fields"][alias] = {"type": old_fields[field]["type"]}
            if "custom_type" in old_fields[field] and old_fields[field]["custom_type"] != "":
                data["select"]["fields"][alias]["custom_type"] = old_fields[field]["custom_type"]

    data["select"]["union"] = {"union_type": union_type}

    print("Data unioned successfully.")
    return QFrame(data=data, engine=qframes[0].engine)

def _validate_data(data):
    if data == {}:
        raise AttributeError("Your data is empty.")

    if "select" not in data:
        raise AttributeError("Missing 'select' attribute.")

    select = data["select"]

    if "table" not in select and "join" not in select and "union" not in select and "sq" not in data:
        raise AttributeError("Missing 'table' attribute.")

    if "fields" not in select:
        raise AttributeError("Missing 'fields' attribute.")

    fields = select["fields"]


    for field in fields:
        for key_attr in fields[field]:
            if key_attr not in {"type", "as", "group_by", "expression", "select", "custom_type", "order_by"}:
                raise AttributeError(f"""Field '{field}' has invalid attribute '{key_attr}'. Valid attributes:
                                        'type', 'as', 'group_by', 'order_by', 'expression', 'select', 'custom_type'""")
        if "type" in fields[field]:
            field_type = fields[field]["type"]
            if "custom_type" in fields[field]:
                field_custom_type = fields[field]["custom_type"]
                if field_custom_type != '':
                    if not check_if_valid_type(field_custom_type):
                        raise ValueError(f"""Field '{field}' has invalid value '{field_custom_type}' in custom_type. Check valid types for Redshift tables.""")
                elif field_type not in ["dim", "num"] and field_custom_type=='':
                    raise ValueError(f"""Field '{field}' doesn't have custom_type and has invalid value in type: '{field_type}'. Valid values: 'dim', 'num'.""")
            else:
                if field_type not in ["dim", "num"]:
                    raise ValueError(f"""Field '{field}' has invalid value in type: '{field_type}'. Valid values: 'dim', 'num'.""")
        else:
            raise AttributeError(f"Missing type attribute in field '{field}'.")

        if "as" in fields[field]:
            fields[field]["as"] = fields[field]["as"].replace(" ", "_")

        if "group_by" in fields[field] and fields[field]["group_by"] != "":
            group_by = fields[field]["group_by"]
            if group_by.upper() not in ["GROUP", "SUM", "COUNT", "MAX", "MIN", "AVG"]:
                raise ValueError(f"""Field '{field}' has invalid value in  group_by: '{group_by}'. Valid values: '', 'group', 'sum', 'count', 'max', 'min', 'avg'""")
            elif group_by.upper() in ["SUM", "COUNT", "MAX", "MIN", "AVG"] and field_type != 'num':
                raise ValueError(f"Field '{field}' has value '{field_type}' in type and value '{group_by}' in group_by. In case of aggregation type should be 'num'.")

        if "order_by" in fields[field] and fields[field]["order_by"] != "":
            order_by = fields[field]["order_by"]
            if order_by.upper() not in ["DESC", "ASC"]:
                raise ValueError(f"""Field '{field}' has invalid value in order_by: '{order_by}'. Valid values: '', 'desc', 'asc'""")

        if "select" in fields[field] and fields[field]["select"] != "":
            is_selected = fields[field]["select"]
            if str(is_selected) not in {"0", "0.0"}:
                raise ValueError(f"""Field '{field}' has invalid value in select: '{is_selected}'.  Valid values: '', '0', '0.0'""")

    if "distinct" in select and select["distinct"] != "":
        distinct = select["distinct"]
        if str(int(distinct)) != "1":
            raise ValueError(f"""Distinct attribute has invalid value: '{distinct}'.  Valid values: '', '1'""")

    if "limit" in select and select["limit"] != "":
        limit = select["limit"]
        try:
            int(limit)
        except:
            raise ValueError(f"""Limit attribute has invalid value: '{limit}'.  Valid values: '', integer """)

    return data


def initiate(columns, schema, table, json_path, engine_str="", subquery="", col_types=None):
    """Creates a dictionary with fields information for a Qframe and saves the data in json file.

    Parameters
    ----------
    columns : list
        List of columns.
    schema : str
        Name of schema.
    table : str
        Name of table.
    json_path : str
        Path to output json file.
    subquery : str, optional
        Name of the query in json file. If this name already exists it will be overwritten, by default ''
    col_type : list
        List of data types of columns (in 'columns' list)
    """
    if os.path.isfile(json_path):
        with open(json_path, 'r') as f:
            json_data = json.load(f)
            if json_data =="":
                json_data = {}
    else:
        json_data = {}

    fields = {}

    if col_types == None:
        for col in columns:
            type = "num" if "amount" in col else "dim"
            field = {
                        "type": type,
                        "as": "",
                        "group_by": "",
                        "order_by": "",
                        "expression": "",
                        "select": "",
                        "custom_type": ""
                    }
            fields[col] = field

    elif isinstance(col_types, list):
        for index, col in enumerate(columns):
            custom_type = col_types[index]
            field = {
                        "type": "",
                        "as": "",
                        "group_by": "",
                        "order_by": "",
                        "expression": "",
                        "select": "",
                        "custom_type": custom_type
                    }
            fields[col] = field

    data = {
            "select": {
                "table": table,
                "schema": schema,
                "fields": fields,
                "engine": engine_str,
                "where": "",
                "distinct": "",
                "having": "",
                "limit": ""
            }
            }


    if subquery != '':
        json_data[subquery] = data
    else:
        json_data = data

    with open(json_path, 'w') as f:
        json.dump(json_data, f)

    print(f"Data saved in {json_path}")
