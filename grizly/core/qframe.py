from IPython.display import HTML, display
import pandas
import re
from grizly.io.sqlbuilder import get_sql, to_sql, get_sql2
from grizly.io.excel import read_excel
import sqlparse


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

    def __init__(self, data={}, getfields=[]):
        self.data = data
        self.getfields = getfields  # remove this and put in data
        self.fieldattrs = ["type","as","group_by"]
        self.fieldtypes = ["dim","num"]
        self.metaattrs = ["limit", "where"]

    def validate_data(self, data):
        # validating fields, need to validate other stuff too

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
        data = {}
        data["fields"] = columns_qf
        data["schema"] = schema
        data["table"] = table
        return QFrame(data=data)

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
        query = prepend_table(self.data, query)
        self.data["where"] = query
        return self

    def assign(self, attribute=False, **kwargs):
        """
            attribute: is False it adds creates a new field in fields. 
                The new field is a field calculated with an SQL expression
                TODO: prepend table name to table fields inside the 
                expression

                >>> value_x_two = "Value * 2"
                >>> q.assign(value_x_two=value_x_two)
                >>> assert q.data["fields"]["value_x_two"] == 
                        "Value * 2"
            
            attribute: is True
                If attribute is True it adds a new attribute in data
                >>> engine_string = "some sqlalchemy engine string"
                >>> q.assign(engine_string=engine_string, attribute=True)
                >>> assert q.data["engine_string"] == "some sqlalchemy engine string"
        """
        if kwargs is not None:
            if attribute != True:
                for key in kwargs:
                    expression = prepend_table(self.data, kwargs[key])
                    self.data["expressions"] = {key: expression}
            else:
                for key in kwargs:
                    self.data[key] = kwargs[key]
        return self

    def groupby(self, fields):
        for field in fields:
            self.data["fields"][field]["group_by"] = "group"
        return self

    def agg(self, aggtype):
        if isinstance(self.getfields, str):
            self.getfields = [self.getfields]
        if aggtype in ["sum", "count"]:
            for field in self.getfields:
                self.data["fields"][field]["group_by"] = aggtype
                self.data["fields"][field]["as"] = "sum_{}".format(field)
            return self
        else:
            return print("Aggregation type must be sum or count")

    def limit(self, limit):
        self.data["limit"] = str(limit)
        return self

    def select(self):
        """
        Creates a subquery that looks like select col1, col2 from (some sql)
        """
        sql = get_sql()

    def rename(self, fields):
        for field in fields:
            self.data["fields"][field]["as"] = fields[field]
        self

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
        sql = self.get_sql()
        if engine_string != "":
            df = to_sql(sql, engine_string)
        else:
            df = to_sql(sql, self.data["engine_string"])
        return df

    def get_sql(self, subquery=False):
        sql = get_sql2(self)
        return sql

    def __getitem__(self, getfields):
        self.getfields = []
        self.getfields.append(getfields)
        return self



def join(l_q, r_q, on, l_table="l_table", r_table="r_table"):
    onstring = ""
    count = 0
    for tup in on:
        count += 1
        if count < len(on):
            onstring += l_table + "." + tup[0] + "=" + r_table + "." + tup[1] + " and "
        else:
            onstring += l_table + "." + tup[0] + "=" + r_table + "." + tup[1]
    l_q_sql = "({}) as {}".format(l_q.get_sql(), l_table)
    r_q_sql = "({}) as {}".format(r_q.get_sql(), r_table)
    sql = "({} JOIN {} ON {})".format(l_q_sql, r_q_sql, onstring)
    sql = sqlparse.format(sql, reindent=True, keyword_case="upper")
    attrs = {"sql": sql}
    d = {}
    for l_field in l_q.fields:
        field_new = l_table + "." + l_field
        d[field_new] = l_q.fields[l_field]
        # d[field_new]["as"] = field_new
    for r_field in r_q.fields:
        if r_field not in l_q.fields:
            field_new = r_table + "." + r_field
            d[field_new] = r_q.fields[r_field]
    return QFrame(fields=d, attrs=attrs)


def union(*args, alias="union"):
    """
    union -> q1, q2, q3
    fields["union"]
    keys: [field_name, type, group]
    """
    sql = ""
    counter = 0
    d = {}
    for arg in args:
        d = {**d, **arg.fields}
        counter += 1
        sql += "({})".format(arg.get_sql())
        if counter < len(args):
            sql += " UNION "
        else:
            sql = "({}) AS {}".format(sql, alias)
    sql = sqlparse.format(sql, reindent=True, keyword_case="upper")
    attrs = {"sql": sql}
    q = QFrame(table=alias, fields=d, attrs=attrs)
    return q
