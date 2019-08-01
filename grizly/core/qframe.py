from IPython.display import HTML, display
import pandas
import re
import os
import sqlparse
import copy

from grizly.io.sqlbuilder import (
    get_sql, 
    to_sql, 
    build_column_strings
)

from grizly.io.excel import read_excel
from grizly.io.etl import *
from grizly.core.utils import *

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
    data: dictionary structure holding fields, schema, table, sql
          information

    db : {'Denodo', 'Redshift', 'MariaDB'}, default 'Denodo'
        Name of database.

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
    other data attributes:
            * schema: the schema name
            * table: the table name, this will be the table alias if the field is from
            a subquery
    """

    def __init__(self, data={}, sql="", db='Denodo', getfields=[]):
        self.data = data
        self.sql = sql
        self.db = db
        self.getfields = getfields  # remove this and put in data
        self.fieldattrs = ["type","as","group_by","expression","select","custom_type"]
        self.fieldtypes = ["dim","num"]
        self.metaattrs = ["limit", "where"]

    def validate_data(self, data):
        # validating fields, need to validate other stuff too
        data = data["select"]

        for field_key in data['fields']:
            for key_attr in data['fields'][field_key]:
                if key_attr not in set(self.fieldattrs):
                    raise AttributeError("Your columns have invalid attributes.")

        for field_key in data["fields"]:
            if "type" in data["fields"][field_key]:
               if data["fields"][field_key]["type"] not in self.fieldtypes:
                    raise ValueError("Your columns have invalid types.")
            else:
                raise KeyError("Some of your columns don't have types.")

    def from_dict(self, data):
        self.validate_data(data)
        self.data = data
        return self

    def read_excel(self, excel_path, sheet_name="", query=""):
        schema, table, columns_qf = read_excel(excel_path, sheet_name, query)

        data = {"select": {
                    "fields": columns_qf,
                    "schema": schema,
                    "table": table
                }}

        self.validate_data(data)
        return QFrame(data=data)

    def create_sql_blocks(self):
          return build_column_strings(self)

    def query(self, query):
        """
        Query
        -----
        Creates a "where" attribute inside the data dictionary.
        Prepends the table name to each column field. So
        Country = 'Italy' becomes Orders.Country = 'Italy'

        >>> orders = dict with order table fields
        >>> q = QFrame().from_dict(orders)
        >>> expr = q.query(
                            "country!='Italy' 
                                and (Customer='Enel' or Customer='Agip')
                                or Value>1000
                            ")
        >>> assert expr.data["where"] == "
                                    Orders.country!='Italy' 
                                    and (Orders.Customer='Enel' or Orders.Customer='Agip')
                                    or Orders.Value>1000
                                    "

        """
        if "union" in self.data["select"]:
            print("You can't add where clause inside union. Use select() method first.")
        else:
            self.data["select"]["where"] = query
        return self

    def assign(self, type="dim", group_by="", **kwargs):
        """
        Assign expressions.

        Parameters:
        ----------
        type: {'dim', 'num'}, default 'dim
            Column type.
            * dim: VARCHAR(500)
            * num: FLOAT(53)
        group_by : {group, sum, count, min, max, avg, ""}, default ""

        Examples:
        --------
            >>> value_x_two = "Value * 2"
            >>> q.assign(value_x_two=value_x_two)
            >>> assert q.data["fields"]["value_x_two"]["expression"] == 
                    "Value * 2"

        """
        if "union" in self.data["select"]:
            print("You can't assign expressions inside union. Use select() method first.")
        else:
            if kwargs is not None:
                for key in kwargs:
                    expression = kwargs[key]
                    self.data["select"]["fields"][key] = {"type": type, "as": key, "group_by": group_by, "expression": expression}
        return self

    def groupby(self, fields):
        if "union" in self.data["select"]:
            print("You can't group by inside union. Use select() method first.")
        else:
            for field in fields:
                self.data["select"]["fields"][field]["group_by"] = "group"
        return self

    def agg(self, aggtype):
        if "union" in self.data["select"]:
            print("You can't aggregate inside union. Use select() method first.")
        else:
            if isinstance(self.getfields, str):
                self.getfields = [self.getfields]
            if aggtype in ["sum", "count"]:
                for field in self.getfields:
                    self.data["select"]["fields"][field]["group_by"] = aggtype
                    alias = field if "as" not in self.data["select"]["fields"][field] else self.data["select"]["fields"][field]["as"]
                    self.data["select"]["fields"][field]["as"] = alias
            else:
                return print("Aggregation type must be sum or count")
        return self

    def limit(self, limit):
        self.data["select"]["limit"] = str(limit)
        return self

    def select(self, fields):
        """
        Creates a subquery that looks like select col1, col2 from (some sql)

        Parameters:
        ----------
        fields : list
            List of fields to select.
        """
        sq_fields = copy.deepcopy(self.data["select"]["fields"])
        new_fields = {}

        for field in fields:
            if field not in sq_fields:
                print(f"Field {field} not found")

            elif "select"  in sq_fields[field] and sq_fields[field]["select"] == 0:
                print(f"Field {field} is not selected in subquery.")

            else:
                alias = field if "as" not in sq_fields[field] else sq_fields[field]["as"]
                new_fields[f"sq.{alias}"] = {"type": sq_fields[field]["type"], "as": alias}
                if "custom_type" in sq_fields[field]:
                    new_fields[f"sq.{alias}"]["custom_type"] = sq_fields[field]["custom_type"]

        if new_fields: 
            data = {"select": {"fields": new_fields }, "sq": self.data}
            self.data = data

        return self

    def rename(self, fields):
        """
        Renames columns.

        Parameters:
        -----------
        fields : dict
            Dictionary of columns and their new names.

        Examples:
        --------
            >>> q.rename({"sq1.customer_id" : "customer_id", "sq2.customer_id" : "supplier_id"})
    
        """
        for field in fields:
            if field in self.data["select"]["fields"]:
                self.data["select"]["fields"][field]["as"] = fields[field]
        return self

    def remove(self, fields):
        """
        Removes fields.

        Parameters:
        -----------
        fields : list
            List of fields to remove.

        Examples:
        --------
            >>> q.remove(["sq1.customer_id", "sq2.customer_id"])
    
        """
        for field in fields:
            self.data["select"]["fields"].pop(field, f"Field {field} not found.")
        return self

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
            )
        html_table += "</table>"
        display(HTML(html_table))

    def to_sql(self, engine_string=""):  # put engine_string in fields as meta
        sql = self.sql
        if engine_string != "":
            df = to_sql(sql, engine_string)
        else:
            df = to_sql(sql, self.data["engine_string"])
        return df

    def get_sql(self, subquery=False):
        """
        Overwrites the sql statement inside the class. To get sql use your_class_name.sql

                >>> q = QFrame().read_excel(excel_path, sheet_name, query)
                >>> q.get_sql()
                >>> sql = q.sql
                >>> print(sql)
        """
        self.sql = get_sql(self).sql
        return self


    def create_table(self, table, schema=''):
        """
        Creates a new table in database if the table doesn't exist.

        Parameters:
        ----------
        table : string
            Name of SQL table.
        schema : string, optional
            Specify the schema.
        """
        create_table(self, table, schema)
        return self


    def to_csv(self, csv_path):
        """
        Writes table to csv file.

        Parameters:
        ----------
        csv_path : string
            Path to csv file.
        """
        if self.sql == '':
            self.create_sql_blocks()
            self.get_sql()

        to_csv(self,csv_path,self.sql,self.db)
        return self


    def csv_to_s3(self, csv_path, s3_name):
        """
        Writes csv file to s3 in 'teis-data' bucket.

        Parameters:
        ----------
        csv_path : string
            Path to csv file.
        s3_name : string
            Name of s3. 
        """
        csv_to_s3(csv_path,s3_name)
        return self


    def s3_to_rds(self, table, s3_name, schema='', if_exists='fail', sep='\t'):
        """
        Writes s3 to Redshift database.

        Parameters:
        -----------
        table : string
            Name of SQL table.
        s3_name : string
            Name of s3. 
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
        s3_to_rds(self, table, s3_name, schema=schema , if_exists=if_exists, sep=sep)
        return self

        
    def to_rds(self, table, csv_path, s3_name, schema='', if_exists='fail', sep='\t'):
        """
        Writes table to Redshift database.

        Parameters:
        ----------
        table : string
            Name of SQL table.
        csv_path : string
            Path to csv file.
        s3_name : string
            Name of s3.
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
        if self.sql == '':
            self.create_sql_blocks()
            self.get_sql()
            
        to_csv(self,csv_path, self.sql, db=self.db, sep=sep)
        csv_to_s3(csv_path, s3_name)
        s3_to_rds(self, table, s3_name, schema=schema, if_exists=if_exists, sep='\t')

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

        duplicates = copy.deepcopy(columns)
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


    def __getitem__(self, getfields):
        self.getfields = []
        self.getfields.append(getfields)
        return self


def join(qframes=[], join_type=[], on=[], unique_col=True):
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
    join_type : list
        List of join types.
    on : list
        List of on join conditions. In case of CROSS JOIN set the condition on 0. 
        NOTE: Structure of the elements of this list is very specific. You always have to use prefix "sq{qframe_position}" 
        if you want to refer to the column. Check examples. 
    unique_col : boolean, default True
        If True the joined QFrame will cotain all fields from the first QFrame and all fields from other QFrames which 
        are not repeated. If False the joined QFrame will contain all fields from every QFrame.

    NOTE: Order of the elements in join_type and on list is important.

    Examples:
    --------
        qframes:
        q1 -> fields: customer_id, orders
        q2 -> fields: customer_id, orders as 'ord'

        >>> q_joined = join(qframes=[q1,q2], join_type=["LEFT JOIN"], on=["sq1.customer_id=sq2.customer_id"])

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

        >>> q_joined = join(qframes=[q1,q2,q3], join_type=["CROSS JOIN", INNER JOIN"], on=[0, "sq2.customer_id=sq3.id"], unique_col=False)

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
    assert len(qframes) == len(join_type)+1 and len(join_type) == len(on), "Incorrect list size."

    data = {'select': {'fields': {} }}
    aliases = []

    iterator = 0
    for q in qframes:
        iterator += 1
        data[f"sq{iterator}"] = q.data
        sq = q.data['select']
            
        for alias in sq["sql_blocks"]["select_aliases"]:
            if unique_col and alias in aliases:
                continue
            else:
                aliases.append(alias)
                for field in sq["fields"]:
                    if field == alias or "as" in sq["fields"][field] and sq["fields"][field]["as"] == alias:
                        data["select"]["fields"][f"sq{iterator}.{alias}"] = {"type": sq["fields"][field]["type"], "as": alias}
                        if "custom_type" in sq["fields"][field]:
                            data["select"]["fields"][f"sq{iterator}.{alias}"]["custom_type"] = sq["fields"][field]["custom_type"]
                        break
                    
    data["select"]["join"] = { "join_type": join_type, "on": on}

    print("Data joined successfully.")
    if not unique_col:
        print("Please remove or rename duplicated columns. Use your_qframe.show_duplicated_columns() to check duplicates.")
    return QFrame(data = data)


def union(qframes=[], union_type=[]):
    """
    Unions QFrame objects. Returns QFrame.

    TODO: Add validations on columns and an option to check unioned columns.

    Parameters:
    ----------
    qframes : list
        List of qframes
    union_type : list
        List of union types. Valid types: 'UNION', 'UNION ALL'.

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
    assert len(qframes) == len(union_type)+1, "Incorrect list size."
    assert set(item.upper() for item in union_type) <= {"UNION", "UNION ALL"}, "Incorrect union type. Valid types: 'UNION', 'UNION ALL'."
    data = {'select': {'fields': {}}}

    iterator = 0
    for q in qframes:
        iterator += 1
        data[f"sq{iterator}"] = q.data 
                
    fields = copy.deepcopy(data["sq1"]["select"]["fields"])
    
    for field in fields:
        if "select" in fields[field] and fields[field]["select"] == 0:
            continue
        else:
            alias = field if "as" not in fields[field] else fields[field]["as"]  
            data["select"]["fields"][alias] = {"type": fields[field]["type"]}
            if "custom_type" in fields[field]:
                data["select"]["fields"][alias]["custom_type"] = fields[field]["custom_type"]
                
    data["select"]["union"] = {"union_type": union_type}

    print("Data unioned successfully.")
    return QFrame(data = data)





    