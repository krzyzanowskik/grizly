import pytest
import sqlparse
from ..api import QFrame, union, join


def get_query():
    columns = {
        "FY18_Sales": {"type": "num"},
        "FY19_Sales": {"type": "num"},
        "Country": {"type": "dim"},
        "Year": {"type": "dim"},
    }
    q = QFrame(schema="te_industrial", table="sales_weekly_flash").from_dict(columns)
    return q


def write_out(out):
    with open(
        "C:\\Users\\eg013949\\Documents\\PythonP\\grizly\\grizly\\tests\\output.sql",
        "w",
    ) as f:
        f.write(out)


def clean_testexpr(testsql):
    testsql = testsql.replace("\n", "")
    testsql = testsql.replace("  ", "")
    return testsql


def test_rename_fields():
    q = get_query()
    q.rename(fields={"Country": "Country As"})
    assert q.fields["Country"]["as"] == "Country As"


def test_limit():
    q = get_query()
    q.limit(10)
    testexpr1 = """SELECT sales_weekly_flash.FY18_Sales,
                        sales_weekly_flash.FY19_Sales,
                        sales_weekly_flash.Country,
                        sales_weekly_flash.Year
                    FROM te_industrial.sales_weekly_flash
                    LIMIT 10
    """
    testexpr1 = sqlparse.format(testexpr1, reindent=True, keyword_case="upper")
    expr1 = sqlparse.format(q.get_sql(), reindent=True, keyword_case="upper")
    assert expr1 == testexpr1


def test_query_with_group_by():
    q = get_query()
    q.rename(fields={"Country": "RenamedCountry"})
    q.groupby(["Country"])["FY18_Sales", "FY19_Sales"].agg("sum")
    q.groupby(["Country"])["Country"].agg("count")
    testexpr1 = """SELECT sum(sales_weekly_flash.FY18_Sales) AS sum_FY18_Sales,
                    sum(sales_weekly_flash.FY19_Sales) AS sum_FY19_Sales,
                    count(sales_weekly_flash.Country) AS count_Country,
                    sales_weekly_flash.Year
                    FROM te_industrial.sales_weekly_flash
                    GROUP BY sales_weekly_flash.Country
    """
    testexpr1 = sqlparse.format(testexpr1, reindent=True, keyword_case="upper")
    expr1 = sqlparse.format(q.get_sql(), reindent=True, keyword_case="upper")
    assert expr1 == testexpr1


def test_field_calculations():
    q = get_query()
    q["2_Year_Sales"] = q["FY18_Sales"] + q["FY19_Sales"]
    expr1 = q.fields["2_Year_Sales"]["expr"]
    testexpr1 = "sales_weekly_flash.FY18_Sales + sales_weekly_flash.FY19_Sales"

    q["YoY_Delta"] = q["FY18_Sales"] - q["FY19_Sales"]
    expr2 = q.fields["YoY_Delta"]["expr"]
    testexpr2 = "sales_weekly_flash.FY18_Sales - sales_weekly_flash.FY19_Sales"

    q["Sales_and_Integers"] = (
        q["FY18_Sales"] - q["FY20_Sales"] - 1 + q["FY20_Sales"] + 2
    )
    expr3 = q.fields["Sales_and_Integers"]["expr"]
    testexpr3 = "sales_weekly_flash.FY18_Sales - sales_weekly_flash.FY20_Sales - 1 + sales_weekly_flash.FY20_Sales + 2"

    assert expr1 == testexpr1
    assert expr2 == testexpr2
    assert expr3 == testexpr3


def test_read_excel():
    q = QFrame().read_excel(
        "C:\\Users\\eg013949\\Documents\\PythonP\\grizly\\grizly\\tests\\tables.xlsx",
        sheet_name="sales_weekly_flash",
    )
    expr1 = q.get_sql()
    testexpr1 = """SELECT sum(sales_weekly_flash.FY18_Sales) AS sum_FY18_Sales,
                    sum(sales_weekly_flash.FY19_Sales) AS sum_FY19_Sales,
                    sales_weekly_flash.Country,
                    sales_weekly_flash.Year
                    FROM te_industrial.sales_weekly_flash
                    GROUP BY sales_weekly_flash.Country,
                            sales_weekly_flash.Year
                """
    testexpr1 = sqlparse.format(testexpr1, reindent=True, keyword_case="upper")
    expr1 = sqlparse.format(expr1, reindent=True, keyword_case="upper")
    assert expr1 == testexpr1


def test_query():
    q = get_query()
    q["Region"] = {"type": "dim"}
    q.query("Country = 'Italy' and Region = 'France'")
    expr = q.fields["where"]
    testexpr = """SELECT sales_weekly_flash.Region
            FROM te_industrial.sales_weekly_flash
            WHERE Country = 'Italy'
            AND sales_weekly_flash.Region = 'France'"""
    testexpr = sqlparse.format(testexpr, reindent=True, keyword_kase="upper")
    expr = sqlparse.format(q.get_sql(), reindent=True, keyword_case="upper")
    assert expr == testexpr


def test_assign():
    q = get_query()
    testexpr1 = """SELECT sales_weekly_flash.FY18_Sales,
                    sales_weekly_flash.FY19_Sales,
                    sales_weekly_flash.Country,
                    sales_weekly_flash.Year
                    FROM te_industrial.sales_weekly_flash
    """
    q.assign(delta="FY19_Sales - FY18_Sales", group="somegroup")
    testexpr = sqlparse.format(testexpr1, reindent=True, keyword_case="upper")
    expr = sqlparse.format(q.get_sql(), reindent=True, keyword_case="upper")
    assert expr == testexpr


# Verify and add Asserts
def test_union():
    q = get_query()
    q.query("Country = France")
    q2 = get_query()
    uq = union(q, q2, alias="someunion")
    uq.limit(10)
    uq.groupby(["Country"])["FY18_Sales"].agg("count")
    uq.query("FY18_Sales > 1000")
    expr = uq.get_sql()
    # expr = expr + uq.attrs["sql"]
    expr = sqlparse.format(expr, reindent=True, keyword_case="upper")


def test_join():
    customers = {
        "Country": {"type": "dim", "as": "Country"},
        "Customer": {"type": "dim", "as": "Customer"},
    }
    orders = {
        "Order": {"type": "dim", "as": "Bookings"},
        "Part": {"type": "dim", "as": "Part"},
        "Customer": {"type": "dim", "as": "Customer"},
        "Value": {"type": "num"},
    }
    q_customer = QFrame(table="customer").from_dict(customers)
    q_orders = QFrame(table="customer").from_dict(orders)
    q_orders.groupby(["Order", "Customer"])["Value"].agg("sum")
    on = [("Customer", "Customer")]
    expr = join(q_customer, q_orders, on=on).get_sql(subquery=True)
    write_out(expr)
    testexpr = """SELECT l_table.Country,
                    l_table.Customer,
                    r_table.Order,
                    r_table.Value
                FROM (
                        (SELECT customer.Country,
                                customer.Customer
                        FROM customer) AS l_table
                    JOIN
                        (SELECT customer.Order,
                                customer.Customer,
                                customer.Value
                        FROM customer) AS r_table ON l_table.Customer=r_table.Customer)
                """
    testexpr = sqlparse.format(testexpr, reindent=True, keyword_case="upper")
    assert expr == testexpr
