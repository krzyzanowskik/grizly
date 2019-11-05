import pytest
import sqlparse
from ..api import QFrame, union, join
from ..io.sqlbuilder import get_sql2, write


def write_out(out):
    with open(
        "C:\\Users\\TE386850\\grizly\\grizly\\tests\\output.sql",
        "w",
    ) as f:
        f.write(out)


def clean_testexpr(testsql):
    testsql = testsql.replace("\n", "")
    testsql = testsql.replace("\t", "")
    testsql = testsql.replace("\r", "")
    testsql = testsql.replace("  ", "")
    testsql = testsql.replace(" ", "")
    return testsql


def test_from_dict():
    customers = {
        "fields": {
            "Country": {"type": "dim", "as": "Country"},
            "Customer": {"type": "dim", "as": "Customer"},
        }
    }
    orders = {
        "fields": {
            "Order": {"type": "dim", "as": "Bookings"},
            "Part": {"type": "dim", "as": "Part"},
            "Customer": {"type": "dim", "as": "Customer"},
            "Value": {"type": "num"},
        },
        "table": "Orders",
    }

    q = QFrame().from_dict(customers)

    assert q.data["fields"]["Country"] == {"type": "dim", "as": "Country"}

def test_from_dict_2():
    customers = {
        "fields": {
            "Country": {"type": "dim", "as": "Country"},
            "Customer": {"type": "dim", "as": "Customer"},
        }
    }
    orders = {
        "fields": {
            "Order": {"type": "dim", "as": "Bookings"},
            "Part": {"type": "dim", "as": "Part"},
            "Customer": {"type": "dim", "as": "Customer"},
            "Value": {"type": "num"},
        },
        "table": "Orders",
    }

    q = QFrame().from_dict(orders)

    assert q.data["fields"]["Value"] == {"type": "num"}

def test_read_excel():
    q = QFrame().read_excel(
        "C:\\Users\\TE386850\\grizly\\grizly\\tests\\tables.xlsx",
        sheet_name="orders",
    )
    assert q.data["fields"]["Order"] == {
        "type": "dim",
        "group_by": "group",
        "as": "Order Number",
    }


def test_query():
    orders = {
        "fields": {
            "Order": {"type": "dim", "as": "Bookings"},
            "Part": {"type": "dim", "as": "Part"},
            "Customer": {"type": "dim", "as": "Customer"},
            "Value": {"type": "num"},
        },
        "table": "Orders",
    }

    q = QFrame().from_dict(orders)
    expr = q.query(
        """country!='Italy' 
                and (Customer='Enel' or Customer='Agip')
                or Value>1000
            """
    )
    testexpr = """Orders.country!='Italy' 
                and (Orders.Customer='Enel' or Orders.Customer='Agip')
                or Orders.Value>1000
            """
    assert expr.data["where"] == testexpr


def test_assign():
    orders = {
        "fields": {
            "Order": {"type": "dim", "as": "Bookings"},
            "Part": {"type": "dim", "as": "Part"},
            "Customer": {"type": "dim", "as": "Customer"},
            "Value": {"type": "num"},
        },
        "table": "Orders",
    }

    q = QFrame().from_dict(orders)
    value_x_two = "Value * 2"
    q.assign(value_x_two=value_x_two)
    assert q.data["expressions"]["value_x_two"] == "Orders.Value * 2"


def test_assign_attribute():
    orders = {
        "fields": {
            "Order": {"type": "dim", "as": "Bookings"},
            "Part": {"type": "dim", "as": "Part"},
            "Customer": {"type": "dim", "as": "Customer"},
            "Value": {"type": "num"},
        },
        "table": "Orders",
    }

    q = QFrame().from_dict(orders)
    engine_string = "some sqlalchemy engine string"
    q.assign(engine_string=engine_string, attribute=True)
    assert q.data["engine_string"] == "some sqlalchemy engine string"


def test_group_by():
    orders = {
        "fields": {
            "Order": {"type": "dim", "as": "Bookings"},
            "Part": {"type": "dim", "as": "Part"},
            "Customer": {"type": "dim", "as": "Customer"},
            "Value": {"type": "num"},
        },
        "table": "Orders",
    }
    q = QFrame().from_dict(orders)
    q.groupby(["Order", "Customer"])
    order = {"type": "dim", "as": "Bookings", "group_by": "group"}
    customer = {"type": "dim", "as": "Customer", "group_by": "group"}
    assert q.data["fields"]["Order"] == order
    assert q.data["fields"]["Customer"] == customer


def test_groupby_agg():
    orders = {
        "fields": {
            "Order": {"type": "dim", "as": "Bookings"},
            "Part": {"type": "dim", "as": "Part"},
            "Customer": {"type": "dim", "as": "Customer"},
            "Value": {"type": "num"},
        },
        "table": "Orders",
    }
    q = QFrame().from_dict(orders)
    q.groupby(["Order", "Customer"])["Value"].agg("sum")
    value = {"type": "num", "group_by": "sum", "as": "sum_Value"}
    assert q.data["fields"]["Value"] == value


def test_get_sql_1():
    orders = {
        "fields": {
            "Order": {"type": "dim", "as": "Bookings"},
            "Part": {"type": "dim", "as": "Part"},
            "Customer": {"type": "dim", "as": "Customer"},
            "Value": {"type": "num"},
        },
        "table": "Orders",
    }
    q = QFrame().from_dict(orders)
    q.groupby(["Order", "Customer"])["Value"].agg("sum")
    sql = get_sql2(q)
    testsql = """SELECT Orders.Order AS Bookings,
                    Orders.Part AS Part,
                    Orders.Customer AS Customer,
                    sum(Orders.Value) AS sum_Value
                FROM Orders
                GROUP BY Orders.Order,
                        Orders.Customer"""
    assert clean_testexpr(sql) == clean_testexpr(testsql)


def test_get_sql_2():
    orders = {
        "fields": {
            "Order": {"type": "dim", "as": "Bookings"},
            "Part": {"type": "dim", "as": "Part"},
            "Customer": {"type": "dim"},
            "Value": {"type": "num"},
        },
        "table": "Orders",
    }
    q = QFrame().from_dict(orders)
    q.groupby(["Order", "Customer"])["Value"].agg("sum")
    q.query("Value > 1000")
    value_x_two = "Value * 2"
    q.assign(value_x_two=value_x_two)
    q.limit(10)
    q.assign(table="tabledata", attribute=True)
    sql = get_sql2(q)
    testsql = """SELECT tabledata.Order AS Bookings,
                    tabledata.Part AS Part,
                    tabledata.Customer,
                    sum(tabledata.Value) AS sum_Value,
                    (Orders.Value * 2) AS value_x_two
                FROM tabledata
                WHERE Orders.Value > 1000
                GROUP BY tabledata.Order,
                        tabledata.Customer
                LIMIT 10"""
    assert clean_testexpr(sql) == clean_testexpr(testsql)


def test_write_sql_table():
    orders = {
        "fields": {
            "Order_Nr": {"type": "dim", "as": "Bookings"},
            "Part": {"type": "dim", "as": "Part"},
            "Customer": {"type": "dim"},
            "Value": {"type": "num"},
        },
        "table": "Orders",
    }
    q = QFrame().from_dict(orders)
    #sql = write(q, "Orders", drop=True)

def test_to_sql():
    q = QFrame().read_excel(
        "C:\\Users\\TE386850\\grizly\\grizly\\tests\\tables.xlsx",
        sheet_name="cb_invoices",
    )
    engine = 'sqlite:///C:\\Users\\TE386850\\grizly\\grizly\\tests\\chinook.db'
    q.assign(sales="Quantity*UnitPrice")
    q.groupby(["TrackId"])[("Quantity")].agg("sum")
    sql = q.get_sql
    df = q.to_sql(engine_string=engine)
    write_out(str(q.data))

def test_validation_data():
    orders = {
        "fields": {
            "Order_Nr": {"type": "dim", "as": "Bookings"},
            "Part": {"type": "dim", "as": "Part"},
            "Customer": {"type": "num"},
            "Value": {"type": "num"},
        },
        "table": "Orders",
    }

    result = QFrame().validate_data(orders)

