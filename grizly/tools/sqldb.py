from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from pandas import read_sql_query
import os
from functools import partial
import deprecation

from ..config import Config
from ..utils import get_sfdc_columns

deprecation.deprecated = partial(
    deprecation.deprecated, deprecated_in="0.3", removed_in="0.4"
)


class SQLDB:
    def __init__(self, db: str, engine_str: str = None, config_key: str = None):
        if config_key:
            config = Config().get_service(config_key=config_key, service="sqldb")
        if db not in {"denodo", "redshift"}:
            raise NotImplementedError(
                f"DB {db} not supported yet. Supported DB's: 'redshift', 'denodo'"
            )
        self.db = db
        self.engine_str = engine_str or os.getenv(db) or config.get(db)

    def get_connection(self):
        engine = create_engine(self.engine_str, encoding="utf8", poolclass=NullPool)
        try:
            con = engine.connect().connection
        except:
            con = engine.connect().connection
        return con

    def check_if_exists(self, table, schema=None):
        """Checks if a table exists in Redshift."""
        if self.db == "redshift":
            con = self.get_connection()
            if schema == None:
                sql_exists = f"select * from information_schema.tables where table_name='{table}'"
            else:
                sql_exists = f"select * from information_schema.tables where table_schema='{schema}' and table_name='{table}'"

            return not read_sql_query(sql=sql_exists, con=con).empty
        else:
            print("Works only with db='redshift'")

    def create_table(self, table, columns, types, schema=None, char_size=500):
        """
        Creates a new table in Redshift if the table doesn't exist.

        Parameters
        ----------
        columns : list
        types : list
        char_size : int, size of the VARCHAR field in the database column
        """
        table_name = f"{schema}.{table}" if schema else table

        if self.check_if_exists(table=table, schema=schema):
            print("Table {} already exists...".format(table_name))
        else:
            col_tuples = []
            for item in range(len(columns)):
                if types[item] == "VARCHAR(500)":
                    column = columns[item] + " " + "VARCHAR({})".format(char_size)
                else:
                    column = columns[item] + " " + types[item]
                col_tuples.append(column)

            columns_str = ", ".join(col_tuples)
            sql = "CREATE TABLE {} ({})".format(table_name, columns_str)
            con = self.get_connection()
            con.execute(sql)
            con.close()

            print("Table {} has been created successfully.".format(sql))
        return self

    def copy_table(
        self, in_table, out_table, in_schema=None, out_schema=None, if_exists="fail"
    ):
        """
        Paramaters
        ----------
        if_exists : str, optional
            How to behave if the output table already exists.
            * fail: Raise a ValueError
            * drop: Drop table
        """
        if self.db == "redshift":
            con = self.get_connection()
            in_table_name = f"{in_schema}.{in_table}" if in_schema else in_table
            if not self.check_if_exists(table=in_table, schema=in_schema):
                print(f"Table {in_table_name} doesn't exist.")
            else:
                out_table_name = (
                    f"{out_schema}.{out_table}" if out_schema else out_table
                )
                if self.check_if_exists(table=out_table, schema=out_schema):
                    if if_exists == "drop":
                        sql = f"""
                        DROP TABLE IF EXISTS {out_table_name};
                        CREATE TABLE {out_table_name} AS
                        SELECT * FROM {in_table_name}
                        """
                    elif if_exists == "fail":
                        raise ValueError(f"Table {in_table_name} already exists")
                con.execute(sql)
            con.close()
        return self

    def delete_from(self, table, schema=None, where=None):
        """
        Removes records from Redshift table which satisfy where.
        """
        if self.db == "redshift":
            con = self.get_connection()
            if self.check_if_exists(table=table, schema=schema):
                table_name = f"{schema}.{table}" if schema else table
                sql = f"DELETE FROM {table_name}"
                if where is None:
                    con.execute(sql)
                    print(
                        f"Records from table {table_name} has been removed successfully."
                    )
                else:
                    sql += f" WHERE {where} "
                    con.execute(sql)
                    print(
                        f"Records from table {table_name} where {where} has been removed successfully."
                    )
            else:
                print(f"Table {table_name} doesn't exist.")
            con.close()
        return self

    def insert_into(self, table, columns, sql, schema=None):
        """
        Inserts records into redshift table.
        """
        if self.db == "redshift":
            con = self.get_connection()
            if self.check_if_exists(table=table, schema=schema):
                table_name = f"{schema}.{table}" if schema else table
                columns = ", ".join(columns)
                sql = f"INSERT INTO {table_name} ({columns}) {sql}"
                con.execute(sql)
            else:
                print(f"Table {table_name} doesn't exist.")
            con.close()
        return self

    def write_to(self, table, columns, sql, schema=None, if_exists="fail"):
        """
        Performs DELETE FROM (if table exists) and INSERT INTO queries in Redshift directly.
        """
        if self.db == "redshift":
            if self.check_if_exists(table=table, schema=schema):
                if if_exists == "replace":
                    self.insert_into(
                        table=table, columns=columns, sql=sql, schema=schema
                    )
                    self.delete_from(table=table, schema=schema)
                    print(f"Data has been owerwritten into {schema}.{table}")
                elif if_exists == "fail":
                    raise ValueError("Table already exists")
                elif if_exists == "append":
                    self.insert_into(
                        table=table, columns=columns, sql=sql, schema=schema
                    )
                    print(f"Data has been appended to {schema}.{table}")
            else:
                raise ValueError("Table doesn't exist. Use create_table first")

    def get_denodo_columns(
        self, table, schema=None, column_types=False, columns=None, date_format="DATE",
    ):
        """Get column names (and optionally types) from Denodo view.
        Parameters
        ----------
        table : str
            Name of table.
        schema : str
            Name of schema.
        column_types : bool
            True means user wants to get also data types.
        date_format : str
            Denodo date format differs from those from other databases. User can choose which format is desired.
        """
        where = (
            f"view_name = '{table}' AND database_name = '{schema}' "
            if schema
            else f"view_name = '{table}' "
        )
        if column_types == False:
            sql = f"""
                SELECT column_name
                FROM get_view_columns()
                WHERE {where}
                """
        else:
            sql = f"""
                SELECT distinct column_name,  column_sql_type, column_size
                FROM get_view_columns()
                WHERE {where}
        """
        con = self.get_connection()
        cursor = con.cursor()
        cursor.execute(sql)
        col_names = []

        if column_types == False:
            while True:
                column = cursor.fetchone()
                if not column:
                    break
                col_names.append(column[0])
            cursor.close()
            con.close()
            return col_names
        else:
            col_types = []
            while True:
                column = cursor.fetchone()
                if not column:
                    break
                col_names.append(column[0])
                if column[1] in ("VARCHAR", "NVARCHAR"):
                    col_types.append(column[1] + "(" + str(min(column[2], 1000)) + ")")
                elif column[1] == "DATE":
                    col_types.append(date_format)
                else:
                    col_types.append(column[1])
            cursor.close()
            con.close()
            if columns:
                col_names_and_types = {
                    col_name: col_type
                    for col_name, col_type in zip(col_names, col_types)
                    if col_name in columns
                }
                col_names = [col for col in col_names_and_types]
                col_types = [type for type in col_names_and_types.values()]
            return col_names, col_types

    def get_redshift_columns(
        self, table, schema=None, column_types=False, columns=None
    ):
        """Get column names (and optionally types) from a Redshift table.
        Parameters
        ----------
        table : str
            Name of table.
        schema : str
            Name of schema.
        column_types : bool
            Whether to retrieve field types.
        """
        con = self.get_connection()
        cursor = con.cursor()
        where = (
            f"table_name = '{table}' AND table_schema = '{schema}' "
            if schema
            else f"table_name = '{table}' "
        )
        sql = f"""
            SELECT ordinal_position AS position, column_name, data_type,
            CASE WHEN character_maximum_length IS NOT NULL
            THEN character_maximum_length
            ELSE numeric_precision END AS max_length
            FROM information_schema.columns
            WHERE {where}
            ORDER BY ordinal_position;
            """
        cursor.execute(sql)

        col_names = []

        if column_types:
            col_types = []
            while True:
                column = cursor.fetchone()
                if not column:
                    break
                col_name = column[1]
                col_type = column[2]
                col_names.append(col_name)
                col_types.append(col_type)
            # leave only the cols provided in the columns argument
            if columns:
                col_names_and_types = {
                    col_name: col_type
                    for col_name, col_type in zip(col_names, col_types)
                    if col_name in columns
                }
                col_names = [col for col in col_names_and_types]
                col_types = [type for type in col_names_and_types.values()]
            to_return = (col_names, col_types)
        else:
            while True:
                column = cursor.fetchone()
                if not column:
                    break
                col_name = column[1]
                col_names.append(col_name)
            # leave only the cols provided in the columns argument
            if columns:
                col_names = [col for col in col_names if col in columns]
            to_return = col_names

        cursor.close()
        con.close()

        return to_return

    def get_columns(
        self, table, schema=None, column_types=False, date_format="DATE", columns=None,
    ):
        """ Retrieves column names and optionally other table metadata """
        if self.db == "denodo":
            return self.get_denodo_columns(
                schema=schema,
                table=table,
                column_types=column_types,
                date_format=date_format,
                columns=columns,
            )
        elif self.db == "redshift":
            return self.get_redshift_columns(
                schema=schema, table=table, column_types=column_types, columns=columns
            )


def check_if_valid_type(type: str):
    """Checks if given type is valid in Redshift.

    Parameters
    ----------
    type : str
        Input type

    Returns
    -------
    bool
        True if type is valid, False if not
    """
    valid_types = [
        "SMALLINT",
        "INT2",
        "INTEGER",
        "INT",
        "INT4",
        "BIGINT",
        "INT8",
        "DECIMAL",
        "NUMERIC",
        "REAL",
        "FLOAT4",
        "DOUBLE PRECISION",
        "FLOAT8",
        "FLOAT",
        "BOOLEAN",
        "BOOL",
        "CHAR",
        "CHARACTER",
        "NCHAR",
        "BPCHAR",
        "VARCHAR",
        "CHARACTER VARYING",
        "NVARCHAR",
        "TEXT",
        "DATE",
        "TIMESTAMP",
        "TIMESTAMP WITHOUT TIME ZONE",
        "TIMESTAMPTZ",
        "TIMESTAMP WITH TIME ZONE",
    ]

    for valid_type in valid_types:
        if type.upper().startswith(valid_type):
            return True
    return False


@deprecation.deprecated(details="Use SQLDB.check_if_exists function instead",)
def check_if_exists(table, schema=""):
    sqldb = SQLDB(db="redshift", engine_str="mssql+pyodbc://redshift_acoe")
    return sqldb.check_if_exists(table=table, schema=schema)


@deprecation.deprecated(details="Use SQLDB.create_table function instead",)
def create_table(table, columns, types, schema="", engine_str=None, char_size=500):
    pass


@deprecation.deprecated(details="Use SQLDB.write_to function instead",)
def write_to(table, columns, sql, schema="", engine_str=None, if_exists="fail"):
    pass


@deprecation.deprecated(details="Use SQLDB.get_columns function instead",)
def get_columns(
    table,
    schema=None,
    column_types=False,
    date_format="DATE",
    db="denodo",
    columns=None,
    engine_str: str = None,
):
    db = db.lower()
    if db == "denodo" or db == "redshift":
        sqldb = SQLDB(db=db, engine_str=engine_str)
        sqldb.get_columns(
            table=table,
            schema=schema,
            column_types=column_types,
            date_format=date_format,
            columns=columns,
        )
    elif db == "sfdc":
        return get_sfdc_columns(table=table, column_types=column_types, columns=columns)
    else:
        raise NotImplementedError("This db is not yet supported")


@deprecation.deprecated(details="Use SQLDB.delete_where function instead",)
def delete_where(table, schema="", redshift_str=None, *argv):
    sqldb = SQLDB(db="redshift", engine_str=redshift_str)
    if argv is not None:
        for arg in argv:
            sqldb.delete_from(table=table, schema=schema, where=arg)
    else:
        sqldb.delete_from(table=table, schema=schema)


@deprecation.deprecated(details="Use SQLDB.copy_table function instead",)
def copy_table(schema, copy_from, to, redshift_str=None):
    sqldb = SQLDB(db="redshift", engine_str=redshift_str)
    sqldb.copy_table(
        in_table=copy_from, out_table=to, in_schema=schema, out_schema=schema
    )
    return "Success"
