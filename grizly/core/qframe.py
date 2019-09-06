from IPython.display import HTML, display
import pandas
import re
import os
import sqlparse
from copy import deepcopy
import json
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

from grizly.io.sqlbuilder import (
    get_sql,
    build_column_strings
)

from grizly.io.excel import (
    read_excel,
    copy_df_to_excel
)

from grizly.io.etl import *
from grizly.core.utils import *

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
    """
    Parameters
    ----------
    data: dict
        Dictionary structure holding fields, schema, table, sql information.

    engine : str
        Engine string. If empty then the engine string is "mssql+pyodbc://DenodoODBC"

    field : Each field is a dictionary with these keys. For instance, a
            query field inside fields definition could look like

            data = {"Country":{"as":"country_name", "group_by":"group"
            , "table":"countries", "schema":"salestables"}}

            In the above case "Country" is a field inside the data
            dictionary

            Field attributes (keys):
            * as: the 'as' of the database column
            * group_by: If this column is grouped. If it's a dim it's always a
            group. If it's a num it can be any group agg (sum, count, max, min, etc.)
            * expression: if this is a calculated field, this is an sql expression like
            column_name * 2 or 'string_value' etc.
    """

    def __init__(self, data={}, engine='', sql='', getfields=[]):
        self.engine =  engine if engine!='' else "mssql+pyodbc://DenodoODBC"
        self.data = data
        self.sql = sql
        self.getfields = getfields
        self.fieldattrs = ["type", "as", "group_by", "expression", "select", "custom_type", "order_by"]
        self.fieldtypes = ["dim", "num"]
        self.metaattrs = ["limit", "where", "having"]


    def validate_data(self, data):
        """Validate loaded data.
        
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


    def save_json(self, json_path, subquery=''):
        """Saves QFrame.data to json file.
        
        Parameters
        ----------
        json_path : str
            Path to json file.
        subquery : str, optional
            [description], by default ''   
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
            json.dump(json_data, f)
        print(f"Data saved in {json_path}")


    def read_json(self, json_path, subquery=''):
        """Reads QFrame.data from json file.
        
        Parameters
        ----------
        json_path : str
            Path to json file.
        subquery : str, optional
            [description], by default ''
        
        Returns
        -------
        QFrame
        """
        with open(json_path, 'r') as f:
            data = json.load(f)
            if data != {}:
                if subquery == '':
                    self.data = self.validate_data(data)
                else:
                    self.data = self.validate_data(data[subquery])
            else:
                self.data = data
        return self


    def from_dict(self, data):
        """Reads QFrame.data from dictionary.
        
        Parameters
        ----------
        data : dict
            Dictionary structure holding fields, schema, table, sql information.
        
        Returns
        -------
        QFrame
        """
        self.data = self.validate_data(data)
        return self


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


    def create_sql_blocks(self):
        if self.data == {}:
            print("Your QFrame is empty.")
            return self
        else:
            self.data['select']['sql_blocks'] = build_column_strings(self.data)
            return self


    def rename(self, fields):
        """Renames columns.
        
        Examples
        --------
        >>> q.rename({"sq1.customer_id" : "customer_id", "sq2.customer_id" : "supplier_id"})
        
        Parameters
        ----------
        fields : dict
            Dictionary of columns and their new names.
        
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

        Examples
        --------
        >>> q.remove(["sq1.customer_id", "sq2.customer_id"])

        Parameters
        ----------
        fields : list
            List of fields to remove.
        
        Returns
        -------
        QFrame

        """
        if isinstance(fields, str) : fields = [fields]

        for field in fields:
            self.data["select"]["fields"].pop(field, f"Field {field} not found.")

        return self


    def distinct(self):
        """
        Adds DISTINCT statement.

        Examples:
        --------
            >>> q.distinct()
        """
        self.data["select"]["distinct"] = 1

        return self


    def query(self, query, if_exists='append', operator='and'):
        """Adds WHERE statement.
        
        Examples
        --------
        >>> q.query("country!='Italy'")

        Parameters
        ----------
        query : str
            Where statement.
        if_exists : {'append', 'replace'}, optional
            How to behave when the where clause already exists, by default 'append'
        operator : {'and', 'or'}, optional
            How to add another condition to existing one, by default 'and'
        
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
            if 'where' not in self.data['select'] or if_exists=='replace':
                self.data["select"]["where"] = query
            elif if_exists=='append':
                self.data["select"]["where"] += f" {operator} {query}"
        return self


    def having(self, having, if_exists='append', operator='and'):
        """Adds HAVING statement.
        
        Examples
        --------
        >>> q.having("sum(Value)>1000 and count(Customer)<=65")      

        Parameters
        ----------
        having : str
            Having statement.
        if_exists : {'append', 'replace'}, optional
            How to behave when the having clause already exists, by default 'append'
        operator : {'and', 'or'}, optional
            How to add another condition to existing one, by default 'and'
        """
        if if_exists not in ["append", "replace"]:
            raise ValueError("Invalid value in if_exists. Valid values: 'append', 'replace'.")
        if operator not in ['and', 'or']:
            raise ValueError("Invalid value in operator. Valid values: 'and', 'or'.")

        if "union" in self.data["select"]:
            print("""You can't add having clause inside union. Use select() method first.
            (The GROUP BY and HAVING clauses are applied to each individual query, not the final result set.)""")

        else:
            if 'having' not in self.data['select'] or if_exists=='replace':
                self.data["select"]["having"] = having
            elif if_exists=='append':
                self.data["select"]["having"] += f" {operator} {having}"
        return self

    def assign(self, type="dim", group_by="", **kwargs):
        """Assigns expressions.
        
        Examples
        --------
        >>> q.assign(value_x_two="Value * 2")

        Parameters
        ----------
        type : {'dim', 'num'}, optional
            Column type, by default "dim"

            * dim: VARCHAR(500)
            * num: FLOAT(53)
        group_by : {group, sum, count, min, max, avg, ""}, optional
            [description], by default ""
        
        Returns
        -------
        QFrame
        """
        if type not in ["dim", "num"]:
            raise ValueError("Invalid value in type. Valid values: 'dim', 'num'.")
        if group_by.lower() not in ["group", "sum", "count", "min", "max", "avg", ""]:
            raise ValueError("Invalid value in group_by. Valid values: 'group', 'sum', 'count', 'min', 'max', 'avg', ''.")

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
                        "expression": expression}

        return self


    def groupby(self, fields):
        """Adds GROUP BY statement.
        
        Examples
        --------
        >>> q.groupby(["Order", "Customer"])["Value"].agg("sum")

        >>> q.groupby("Order")["Value"].agg("sum")

        Parameters
        ----------
        fields : list or string
            List of fields or a field.
            NOTE : You have to pass the name of the field not the alias.
        
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

        Examples
        --------
        >>> q.groupby(["Order", "Customer"])["Value"].agg("sum")

        Parameters
        ----------
        aggtype : {'sum', 'count', 'min', 'max', 'avg'}
            Aggregation type.

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
        
        Examples
        --------
        >>> q.orderby(["customer_id", "date"], [False, True])

        >>> q.orderby("customer_id")

        Parameters
        ----------
        fields : list or str
            Fields in list or field as a string.
        ascending : bool or list, optional
            Sort ascending vs. descending. Specify list for multiple sort orders, by default True
        
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

        Examples
        --------
        >>> q.limit(100)

        Parameters
        ----------
        limit : int or str
            Number of rows to select.
        """
        self.data["select"]["limit"] = str(limit)

        return self


    def select(self, fields):
        """Creates a subquery that looks like "SELECT sq.col1, sq.col2 FROM (some sql) sq".

        NOTE: Selected fields will be placed in the new QFrame. Names of new fields are created
        as a concat of "sq." and alias in the parent QFrame.
        
        Examples
        --------
        qframe :
        q -> fields : customer_id as 'customer', date, order

        >>> q.select(["customer_id", "order"])

        q -> fields : sq.customer_id, sq.order

        Parameters
        ----------
        fields : list or str
            Fields in list or field as a string.
        
        Returns
        -------
        QFrame
        """

        self.create_sql_blocks()
        sq_fields = deepcopy(self.data["select"]["fields"])
        new_fields = {}

        if isinstance(fields, str) : fields = [fields]

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


    def show_duplicated_columns(self):
        """
        Shows duplicated columns.
        """
        columns = {}
        fields = self.data["select"]["fields"]

        for field in fields:
            alias =  field if  "as" not in fields[field] else fields[field]["as"]
            if alias in columns.keys():
                columns[alias].append(field)
            else:
                columns[alias] = [field]

        duplicates = deepcopy(columns)
        for alias in columns.keys():
            if len(columns[alias]) == 1:
                duplicates.pop(alias)

        if duplicates != {}:
            print("\033[1m", "DUPLICATED COLUMNS: \n", "\033[0m")
            for key in duplicates.keys():
                print("\033[1m", key, "\033[0m", ":\t", duplicates[key], "\n")
            print("Use your_qframe.remove() to remove or your_qframe.rename() to rename columns.")

        else:
            print("There are no duplicated columns.")
        return self

    
    def rearrange(self, fields):
        """
        Changes order of the columns.

        Parameters:
        ----------
        fields : list or string
            Fields in list or field as a string.

        Examples:
        ---------
            qframe :
            q -> fields : customer_id, date, order

            >>> q.rearrange(["customer_id", "order", "date])

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
        """
        Returns list of fields names.
        """ 
        fields = list(self.data['select']['fields'].keys()) if self.data else []

        return fields 

    def get_sql(self):
        """
        Overwrites the sql statement inside the class.

        Examples:
        --------

            >>> q = QFrame().read_excel(excel_path, sheet_name, query)
            >>> q.get_sql()
            >>> sql = q.sql
            >>> print(sql)
        """
        self.create_sql_blocks()

        self.sql = get_sql(self.data)

        print(self.sql)

        return self


    def create_table(self, table, engine, schema=''):
        """
        Creates a new empty QFrame table in database if the table doesn't exist.

        Parameters:
        ----------
        table : string
            Name of SQL table.
        engine : string
            Engine string (where we want to create table).
        schema : string, optional
            Specify the schema.
        """
        create_table(qf=self, table=table, engine=engine, schema=schema)
        return self


    def to_csv(self, csv_path, chunksize=None):
        """Writes QFrame table to csv file.

        Parameters
        ----------
        csv_path : string
            Path to csv file.
        chunksize : int, default None
            If specified, return an iterator where chunksize is the number of rows to include in each chunk.
        """

        self.create_sql_blocks()

        to_csv(qf=self,csv_path=csv_path,sql=self.sql,engine=self.engine,chunksize=chunksize)
        return self


    def csv_to_s3(self, csv_path):
        """
        Writes csv file to s3 in 'teis-data' bucket.

        Parameters:
        ----------
        csv_path : string
            Path to csv file.
        """
        csv_to_s3(csv_path)
        return self


    def s3_to_rds(self, table, s3_name, schema='', if_exists='fail', sep='\t', use_col_names=True):
        """
        Writes s3 to Redshift database.

        Parameters:
        -----------
        table : string
            Name of SQL table.
        schema : string, optional
            Specify the schema.
        if_exists : {'fail', 'replace', 'append'}, default 'fail'
            How to behave if the table already exists.
            * fail: Raise a ValueError.
            * replace: Clean table before inserting new values.
            * append: Insert new values to the existing table.
        sep : string, default '\t'
            Separator/delimiter in csv file.
        """
        s3_to_rds_qf(self, table, s3_name=s3_name, schema=schema , if_exists=if_exists, sep=sep, use_col_names=use_col_names)
        return self


    def to_rds(self, table, csv_path, schema='', if_exists='fail', sep='\t', use_col_names=True, chunksize=None):
        """Writes QFrame table to Redshift database.

        Examples
        --------
        With defaults:

        >>> q = QFrame(
        >>>   ).to_rds(table='some_table', csv_path='some_path')

        With schema:

        >>> q = QFrame(
        >>>   ).to_rds(schema='some_schema', table='some_table', csv_path='some_path')

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
        
        Returns
        -------
        QFrame
        """
        self.create_sql_blocks()

        self.sql = get_sql(self.data)

        to_csv(self,csv_path, self.sql, engine=self.engine, sep=sep, chunksize=chunksize)
        csv_to_s3(csv_path)

        s3_to_rds_qf(self, table, s3_name=os.path.basename(csv_path), schema=schema, if_exists=if_exists, sep=sep, use_col_names=use_col_names)


        return self

    def write_to(self, table, schema=''):
        """
        Inserts values from QFrame object into given table. Name of columns in qf and table have to match each other.

        Parameters:
        -----
        qf: QFrame object
        table: string
        schema: string
        """
        write_to(qf=self,table=table,schema=schema)
        return self


    def to_df(self):
        """
        Writes QFrame to DataFrame. Uses pandas.read_sql. Returns DataFrame.

        TODO: DataFarme types should correspond to types defined in QFrame data.
        """
        self.create_sql_blocks()

        self.sql = get_sql(self.data)

        con = create_engine(self.engine, encoding='utf8', poolclass=NullPool)
        df = pandas.read_sql(sql=self.sql, con=con)
        return df


    def to_sql(self, table, engine, schema='', if_exists='fail', index=True,
                index_label=None, chunksize=None, dtype=None, method=None):
        """
        Writes QFrame to DataFarme and then DataFarme to SQL database. Uses pandas.read_sql.

        Parameters
        ----------
        table : string
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
        index_label : string or sequence, default None
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

        df.to_sql(self, name=table, con=con, schema=schema, if_exists=if_exists,
        index=index, index_label=index_label, chunksize= chunksize, dtype=dtype, method=method)
        return self


    def to_excel(self, input_excel_path, output_excel_path, sheet_name='', startrow=0, startcol=0, index=False, header=False):
        """[summary]
        
        Parameters
        ----------
        input_excel_path : [type]
            [description]
        output_excel_path : [type]
            [description]
        sheet_name : str, optional
            [description], by default ''
        startrow : int, optional
            [description], by default 0
        startcol : int, optional
            [description], by default 0
        index : bool, optional
            [description], by default False
        header : bool, optional
            [description], by default False
        
        Returns
        -------
        QFrame
        """
        df = self.to_df()
        copy_df_to_excel(df=df, input_excel_path=input_excel_path, output_excel_path=output_excel_path, sheet_name=sheet_name, startrow=startrow, startcol=startcol,index=index, header=header)

        return df
    
    
    def copy(self):
        """
        Copies QFrame.
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

    # old
    def to_html(self):
        from IPython.display import HTML, display

        html_table = "<table>"
        header = "\n".join(["<th>{}</th>".format(th) for th in self.fieldattrs])
        html_table += "<tr><th>{}</th></tr>".format(header)
        for field in self.fields:
            html_table += """<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>
                """.format(
                field,
                self.fields[field]["type"],
                self.fields[field]["group_by"],
                self.fields[field]["where"],
                self.fields[field]["having"],
            )
        html_table += "</table>"
        display(HTML(html_table))


def join(qframes=[], join_type=None, on=None, unique_col=True):
    """
    Joins QFrame objects. Returns QFrame.

    Name of each field is a concat of: "sq" + position of parent QFrame in qframes + "." + alias in their parent QFrame.
    If the fields have the same aliases in their parent QFrames they will have the same aliases in joined QFrame.

    By default the joined QFrame will contain all fields from the first QFrame and all fields from the other QFrames
    which are not in the first QFrame. This approach prevents duplicates. If you want to choose the columns set unique_col=False and
    after performing join please remove fields with the same aliases or rename the aliases.

    Parameters:
    ----------
    qframes : list
        List of qframes
    join_type : str or list
        Join type or a list of join types.
    on : str or list
        List of on join conditions. In case of CROSS JOIN set the condition on 0.
        NOTE: Structure of the elements of this list is very specific. You always have to use prefix "sq{qframe_position}."
        if you want to refer to the column. Check examples.
    unique_col : boolean, default True
        If True the joined QFrame will cotain all fields from the first QFrame and all fields from other QFrames which
        are not repeated. If False the joined QFrame will contain all fields from every QFrame.

    NOTE: Order of the elements in join_type and on list is important.

    TODO: Add validations on engines. QFarmes engines have to be the same.

    Examples:
    --------
        qframes:
        q1 -> fields: customer_id, orders
        q2 -> fields: customer_id, orders as 'ord'

        >>> q_joined = join(qframes=[q1,q2], join_type="LEFT JOIN", on="sq1.customer_id=sq2.customer_id")

        q_joined -> fields: sq1.customer_id as 'customer_id', sq1.orders as 'orders',
                            sq2.ord as 'ord'

        >>> q_joined.get_sql()
        >>> print(q_joined.sql)
            SELECT  sq1.customer_id as 'customer_id',
                    sq1.orders as 'orders',
                    sq2.ord as 'ord'
            FROM
                (q1.sql) sq1
            LEFT JOIN
                (q2.sql) sq2
            ON sq1.customer_id=sq2.customer_id

        ------------------------
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
        >>> print(q_joined.sql)
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


def union(qframes=[], union_type=None):
    """
    Unions QFrame objects. Returns QFrame.

    TODO: Add validations on columns and an option to check unioned columns.
    TODO: Add validations on engines.

    Parameters:
    ----------
    qframes : list
        List of qframes
    union_type : str or list
        Type or list of union types. Valid types: 'UNION', 'UNION ALL'.

    Examples:
    --------
        qframes:
        q1 -> fields: customer_id, customer_name, orders
        q2 -> fields: customer_id, customer, orders
        q2 -> fields: id, customer, orders

        >>> q_unioned = union(qframes=[q1, q2, q3], union_type=["UNION ALL", "UNION"])

        q_unioned -> fields: customer_id, customer_name, orders

        >>> q_unioned.get_sql()
        >>> print(q_unioned.sql)

            q1.sql
            UNION ALL
            q2.sql
            UNION
            q3.sql

    """
    if isinstance(union_type, str) : union_type = [union_type]

    assert len(qframes) == len(union_type)+1, "Incorrect list size."
    assert set(item.upper() for item in union_type) <= {"UNION", "UNION ALL"}, "Incorrect union type. Valid types: 'UNION', 'UNION ALL'."
    data = {'select': {'fields': {}}}

    iterator = 0
    for q in qframes:
        q.create_sql_blocks()
        iterator += 1
        data[f"sq{iterator}"] = deepcopy(q.data)

    fields = deepcopy(data["sq1"]["select"]["fields"])

    for field in fields:
        if "select" in fields[field] and fields[field]["select"] == 0:
            continue
        else:
            if "as" in fields[field] and fields[field]["as"] != "":
                alias = fields[field]["as"]
            else:
                alias = field

            data["select"]["fields"][alias] = {"type": fields[field]["type"]}
            if "custom_type" in fields[field] and fields[field]["custom_type"] != "":
                data["select"]["fields"][alias]["custom_type"] = fields[field]["custom_type"]

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
            type_ = fields[field]["type"]
            if type_ not in ["dim", "num"]:
                raise ValueError(f"""Field '{field}' has invalid value in type: '{type_}'. Valid values: 'dim', 'num'.""")
        else:
            raise AttributeError(f"Missing type attribute in field '{field}'.")
            
        if "as" in fields[field]:
            fields[field]["as"] = fields[field]["as"].replace(" ", "_")
            
        if "group_by" in fields[field] and fields[field]["group_by"] != "":
            group_by = fields[field]["group_by"]
            if group_by.upper() not in ["GROUP", "SUM", "COUNT", "MAX", "MIN", "AVG"]:
                raise ValueError(f"""Field '{field}' has invalid value in  group_by: '{group_by}'. Valid values: '', 'group', 'sum', 'count', 'max', 'min', 'avg'""")
            elif group_by.upper() in ["SUM", "COUNT", "MAX", "MIN", "AVG"] and type_ != 'num':
                raise ValueError(f"Field '{field}' has value '{_type}' in type and value '{group_by}' in group_by. In case of aggregation type should be 'num'.")          
                
        if "order_by" in fields[field] and fields[field]["order_by"] != "":
            order_by = fields[field]["order_by"]
            if order_by.upper() not in ["DESC", "ASC"]:
                raise ValueError(f"""Field '{field}' has invalid value in order_by: '{order_by}'. Valid values: '', 'desc', 'asc'""")
                
        if "select" in fields[field] and fields[field]["select"] != "":
            is_selected = fields[field]["select"]
            if str(int(is_selected)) != "0":
                raise ValueError(f"""Field '{field}' has invalid value in select: '{is_selected}'.  Valid values: '', '0'""")
                
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


def initiate(table, schema, columns, json_path, subquery=''):
    """Creates a dictionary with fields information for a Qframe and saves the data in json file.
    
    Parameters
    ----------
    table : str
        Name of table.
    schema : str
        Name of schema.
    columns : list
        List of columns.
    json_path : str
        Path to output json file.
    subquery : str, optional
        Name of the query in json file. If this name already exists it will be overwritten, by default ''
    """
    if os.path.isfile(json_path):
        with open(json_path, 'r') as f:
            json_data = json.load(f)
            if json_data =="":
                json_data = {} 
    else:
        json_data = {}

    fields = {}
    
    for col in columns:
        field = {
                    "type": "dim",
                    "as": "",
                    "group_by": "",
                    "order_by": "",
                    "expression": "",
                    "select": "",
                    "custom_type": ""
                }
        fields[col] = field

    data = {
            "select": {
                "table": table,
                "schema": schema,
                "fields": fields,
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