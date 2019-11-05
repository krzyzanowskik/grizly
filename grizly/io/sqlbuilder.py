import sqlparse
import pandas
from sqlalchemy import create_engine


def get_sql(qf, subquery=False):
    select_fields = []
    group_by_fields = []
    fields = {k: v for k, v in qf.fields.items() if k not in qf.metaattrs}
    if qf.table != "":
        table_str = qf.table + "."
    else:
        table_str = ""
    if qf.schema != "":
        schema_str = qf.schema + "."
    else:
        schema_str = ""

    for field in fields:
        if subquery:
            col_name = fields[field]["as"]
        else:
            col_name = field
        if "expression" not in fields[field]:
            select_value_str = table_str + field
            if "as" in fields[field]:
                select_value_str = select_value_str + " as {}".format(
                    fields[field]["as"]
                )
            else:
                select_value_str = select_value_str
            select_fields.append(select_value_str)

            if "group_by" in fields[field]:
                group_agg = fields[field]["group_by"]
                if fields[field]["type"] == "dim":
                    group_by_fields.append(table_str + field)
                if group_agg != "group":
                    select_fields.remove(select_value_str)
                    select_value_str = (
                        "{}({}".format(group_agg, table_str) + col_name + ")"
                    )
                    select_value_str += " as {}_{}".format(group_agg, col_name)
                    select_fields.append(select_value_str)
        else:
            select_str = "({}) as {}".format(fields[field]["expression"], field)
            select_fields.append(select_str)

    select_fields = ", ".join(select_fields)
    group_by_fields = ", ".join(group_by_fields)

    sql = "SELECT {} ".format(select_fields)

    if "sql" not in qf.attrs:
        sql += "FROM {}{} ".format(schema_str, qf.table)
    else:
        sql += "FROM {}".format(qf.attrs["sql"])

    if "where" in qf.fields:
        sql += " WHERE {}".format(qf.fields["where"])
    if group_by_fields != "":
        sql += " GROUP BY {}".format(group_by_fields)

    if "limit" in qf.fields:
        sql += " LIMIT {}".format(str(qf.fields["limit"]))

    sql = sqlparse.format(sql, reindent=True, keyword_case="upper")
    return sql


def to_col_name(data, field, agg="", noas=False):
    col_name = data["table"] + "." + field
    if agg != "":
        col_name = "{}({}) as {}_{}".format(agg, col_name, agg, field)
    else:
        if "as" in data["fields"][field] and noas is False:
            col_name += " as {}".format(data["fields"][field]["as"])
    return col_name


def get_sql2(qf):
    data = qf.data
    fields = qf.data["fields"]
    sel_cols = []
    for field in fields:
        agg = ""
        col_group_name = ""
        if "group_by" in fields[field]:
            agg = fields[field]["group_by"]
            if "group" in fields[field]["group_by"]:
                col_group_name = to_col_name(data, field, noas=True)
                col_name = to_col_name(data, field)
            else:
                col_name = to_col_name(data, field, agg=agg)
        else:
            col_name = to_col_name(data, field)
        sel_cols.append((col_name, col_group_name, agg))
    if "expressions" in data:
        for expression in data["expressions"]:
            col_name = "({}) as {}".format(data["expressions"][expression], expression)
            sel_cols.append((col_name, col_group_name, agg))

    sel_cols_str = ", ".join([item[0] for item in sel_cols])
    group_cols_str = ", ".join([item[1] for item in sel_cols if item[2] == "group"])

    sql = "SELECT {}".format(sel_cols_str)
    if "sql" not in data:
        if "schema" in data:
            if data["schema"] != "":
                sql += " FROM {}.{} ".format(data["schema"], data["table"])
            else:
                sql += " FROM {} ".format(data["table"])
        else:
            sql += " FROM {} ".format(data["table"])
    else:
        sql += " FROM {}".format(data["sql"])

    if "where" in data:
        sql += " WHERE {}".format(data["where"])
    if group_cols_str != "":
        sql += " GROUP BY {}".format(group_cols_str)
    if "limit" in data:
        sql += " LIMIT {}".format(str(data["limit"]))
    sql = sqlparse.format(sql, reindent=True, keyword_case="upper")
    return sql


def write(qf, table, drop=False):
    """
    qf: q frame
    table: database table to save into

    drop: if drop is True then the table data will be deleted and
        new data inserted. The SQL looks like so:
        DELETE FROM TABLE
        insert into prices (group, id, price)
            select 
            7, articleId, 1.50
            from article where name like 'ABC%';
    """
    fields = qf.data["fields"]
    columns = []
    if drop == False:
        for field in fields:
            col_name = field
            if fields[field]["type"] == "num":
                col_name += " double"
            if fields[field]["type"] == "dim":
                col_name += " varchar(10000)"
            columns.append(col_name)
        columns_str = ", ".join(columns)
        sql = "CREATE TABLE {} ({})".format(table, columns_str)
    else:
        sql = "DELETE FROM {}".format(table)
        pass  # do insert into instead with append
    sql = sqlparse.format(sql, reindent=True, keyword_case="upper")
    return sql


def to_sql(sql, engine_string):
    engine = create_engine(engine_string)
    df = pandas.read_sql(sql=sql, con=engine)
    for col in df:
        coltype = df[col].dtype
        if coltype in ["float64"]:
            df[col] = df[col].map("{:,.0f}".format)
    return df
