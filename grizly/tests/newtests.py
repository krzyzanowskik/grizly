import pytest
import sqlparse
from ..api import QFrame, union, join
from ..io.sqlbuilder import write, build_column_strings, get_sql
import os
from ..io.etl import *



def write_out(out):
    with open(
        os.getcwd() + "\\grizly\\grizly\\tests\\output.sql",
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
        os.getcwd() + "\\grizly\\grizly\\tests\\tables.xlsx",
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
    testexpr = """country!='Italy' 
                and (Customer='Enel' or Customer='Agip')
                or Value>1000
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
        os.getcwd() + "\\grizly\\grizly\\tests\\tables.xlsx",
        sheet_name="cb_invoices",
    )
    engine = "sqlite:///" + os.getcwd() + "\\grizly\\grizly\\tests\\chinook.db"
    q.assign(sales="Quantity*UnitPrice")
    q.groupby(["TrackId"])[("Quantity")].agg("sum")
    q.get_sql()
    df = q.to_sql(engine_string=engine)
    testdata = str({'fields': {'InvoiceLineId': {'type': 'dim', 'group_by': ''}, 
            'InvoiceId': {'type': 'dim', 'group_by': ''}, 'TrackId': {'type': 'dim', 'group_by': 'group'}, 
            'UnitPrice': {'type': 'num', 'group_by': ''}, 
            'Quantity': {'type': 'num', 'group_by': 'sum', 'as': 'sum_Quantity'}, 
            'sales': {'type': 'num', 'as': 'sales', 'group_by': '', 'expression': 'invoice_items.Quantity*invoice_items.UnitPrice'}}, 
            'schema': '', 
            'table': 'invoice_items', 
        'sql_blocks': {'select_names': ['invoice_items.InvoiceLineId', 'invoice_items.InvoiceId', 'invoice_items.TrackId', 'invoice_items.UnitPrice', 'sum(invoice_items.Quantity) as sum_Quantity', 'invoice_items.Quantity*invoice_items.UnitPrice as sales'], 
            'select_aliases': ['InvoiceLineId', 'InvoiceId', 'TrackId', 'UnitPrice', 'sum_Quantity', 'sales'], 
            'group_dimensions': ['invoice_items.TrackId'], 
            'group_values': ['sum(invoice_items.Quantity) as sum_Quantity'], 
            'types': ['VARCHAR(500)', 'VARCHAR(500)', 'VARCHAR(500)', 'FLOAT(53)', 'FLOAT(53)', 'FLOAT(53)']}})
    
    # write_out(str(q.data))
    assert testdata == str(q.data)

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
    QFrame().validate_data(orders)

def test_build_column_strings():
    orders = {
        "fields": {
            "Order_Nr": {"type": "dim", "as": "Bookings"},
            "Value": {"type": "num"},
            "Value_div": {"type": "num", "as": "Value_div", "group_by": "", "expression": "Orders.Value/100"},
        },
        "table": "Orders",
    }
    q = QFrame().from_dict(orders)
    assert build_column_strings(q).data["sql_blocks"]["select_names"] == ["Orders.Order_Nr as Bookings","Orders.Value", "Orders.Value/100 as Value_div"]
    assert build_column_strings(q).data["sql_blocks"]["select_aliases"] == ["Bookings", "Value","Value_div"]

def test_create_sql_blocks():
    orders = {
        "fields": {
            "Order_Nr": {"type": "dim", "as": "Bookings"},
            "Part": {"type": "dim", "as": "Part"},
            "Customer": {"type": "dim"},
            "Value": {"type": "num"},
            "Value_div": {"type": "num", "as": "Value_div", "group_by": "", "expression": "Orders.Value/100"},
        },
        "table": "Orders",
    }
    q = QFrame().from_dict(orders)
    assert q.create_sql_blocks().data == build_column_strings(q).data

def test_assign():
    orders = {
        "fields": {
            "Order_Nr": {"type": "dim", "as": "Bookings"},
            "Part": {"type": "dim", "as": "Part"},
            "Customer": {"type": "dim"},
            "Value": {"type": "num"},
        },
        "table": "Orders",
    }
    q = QFrame().from_dict(orders).assign(Value_div="Value/100")
    assert q.data["fields"]["Value_div"] == {"type": "num", "as": "Value_div", "group_by": "", "expression": "Orders.Value/100"}

def test_get_sql():
    orders = {
        "fields": {
            "Order_Nr": {"type": "dim", "as": "Bookings"},
            "Part": {"type": "dim", "as": "Part"},
            "Customer": {"type": "dim"},
            "Value": {"type": "num"},
            "New_case": {"type": "num", "as": "New_case", "group_by": "", "expression": "CASE WHEN Bookings = 100 THEN 1 ELSE 0 END"},
        },
        "table": "Orders",
    }
    q = QFrame().from_dict(orders)
    q.limit(5)
    q.groupby(q.data["fields"])["Value"].agg("sum")
    testsql = """SELECT Orders.Order_Nr AS Bookings,
                    Orders.Part AS Part,
                    Orders.Customer,
                    sum(Orders.Value) AS sum_Value,
                    CASE
                        WHEN Bookings = 100 THEN 1
                        ELSE 0
                    END AS New_case
                FROM Orders
                GROUP BY Orders.Order_Nr,
                        Orders.Part,
                        Orders.Customer
                LIMIT 5
            """
    sql = get_sql(q).sql
    assert clean_testexpr(sql) == clean_testexpr(testsql)
    # write_out(str(sql))

def test_get_sql_with_select_attr():
    q = QFrame().read_excel(
        os.getcwd() + "\\grizly\\grizly\\tests\\tables.xlsx",
        sheet_name="orders",
    )
    testsql = """
        SELECT orders.Order AS ORDER Number, 
                orders.Part,
                orders.CustomerID_1,
                sum(orders.Value) AS sum_Value,
                CASE
                    WHEN CustomerID_1 <> NULL THEN CustomerID_1
                    ELSE CustomerID_2
                END AS CustomerID
        FROM orders_schema.orders
        GROUP BY orders.Order,
                orders.Part,
                orders.CustomerID_1,
                orders.CustomerID_2
            """

    sql = q.get_sql().sql
    # write_out(str(sql))
    assert clean_testexpr(sql) == clean_testexpr(testsql)


def test_check_if_exists():
    assert check_if_exists('fiscal_calendar_weeks','baseviews') == True