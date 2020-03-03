import sqlparse
from copy import deepcopy
import re


def get_duplicated_columns(data):
    columns = {}
    fields = data["select"]["fields"]

    for field in fields:
        alias = (
            field
            if "as" not in fields[field] or fields[field]["as"] == ""
            else fields[field]["as"]
        )
        if alias in columns.keys():
            columns[alias].append(field)
        else:
            columns[alias] = [field]

    duplicates = deepcopy(columns)
    for alias in columns.keys():
        if len(columns[alias]) == 1:
            duplicates.pop(alias)

    return duplicates


def build_column_strings(data):
    if data == {}:
        return {}

    duplicates = get_duplicated_columns(data)
    assert (
        duplicates == {}
    ), f"""Some of your fields have the same aliases {duplicates}. Use your_qframe.remove() to remove or your_qframe.rename() to rename columns."""
    select_names = []
    select_aliases = []
    group_dimensions = []
    group_values = []
    order_by = []
    types = []

    fields = data["select"]["fields"]

    for field in fields:
        expr = (
            field
            if "expression" not in fields[field] or fields[field]["expression"] == ""
            else fields[field]["expression"]
        )
        alias = (
            field
            if "as" not in fields[field] or fields[field]["as"] == ""
            else fields[field]["as"]
        )

        if "group_by" in fields[field]:
            if fields[field]["group_by"].upper() == "GROUP":
                prefix = re.search(r"^sq\d*[.]", field)
                if prefix is not None:
                    group_dimensions.append(field[len(prefix.group(0)) :])
                else:
                    group_dimensions.append(field)

            elif fields[field]["group_by"] == "":
                pass

            elif fields[field]["group_by"].upper() in [
                "SUM",
                "COUNT",
                "MAX",
                "MIN",
                "AVG",
            ]:
                agg = fields[field]["group_by"]
                expr = f"{agg}({expr})"
                group_values.append(alias)

        if (
            "select" not in fields[field]
            or "select" in fields[field]
            and fields[field]["select"] == ""
        ):
            select_name = field if expr == alias else f"{expr} as {alias}"

            if "custom_type" in fields[field] and fields[field]["custom_type"] != "":
                type = fields[field]["custom_type"].upper()
            elif fields[field]["type"] == "dim":
                type = "VARCHAR(500)"
            elif fields[field]["type"] == "num":
                type = "FLOAT(53)"

            if "order_by" in fields[field] and fields[field]["order_by"] != "":
                if fields[field]["order_by"].upper() == "DESC":
                    order = fields[field]["order_by"]
                elif fields[field]["order_by"].upper() == "ASC":
                    order = ""
                order_by.append(f"{alias} {order}")

            select_names.append(select_name)
            select_aliases.append(alias)
            types.append(type)
        else:
            pass

    sql_blocks = {
        "select_names": select_names,
        "select_aliases": select_aliases,
        "group_dimensions": group_dimensions,
        "group_values": group_values,
        "order_by": order_by,
        "types": types,
    }

    return sql_blocks


def get_sql(data):
    if data == {}:
        return ""

    data["select"]["sql_blocks"] = build_column_strings(data)
    sql = ""

    if "union" in data["select"]:
        iterator = 1
        sq_data = deepcopy(data[f"sq{iterator}"])
        sql += get_sql(sq_data)

        for union in data["select"]["union"]["union_type"]:
            union_type = data["select"]["union"]["union_type"][iterator - 1]
            sq_data = deepcopy(data[f"sq{iterator+1}"])
            right_table = get_sql(sq_data)

            sql += f" {union_type} {right_table}"
            iterator += 1

    elif "union" not in data["select"]:
        sql += "SELECT"

        if "distinct" in data["select"] and str(data["select"]["distinct"]) == "1":
            sql += " DISTINCT"

        selects = ", ".join(data["select"]["sql_blocks"]["select_names"])
        sql += f" {selects}"

        if "table" in data["select"]:
            if "schema" in data["select"] and data["select"]["schema"] != "":
                sql += " FROM {}.{}".format(
                    data["select"]["schema"], data["select"]["table"]
                )
            else:
                sql += " FROM {}".format(data["select"]["table"])

        elif "join" in data["select"]:
            iterator = 1
            sq_data = deepcopy(data[f"sq{iterator}"])
            left_table = get_sql(sq_data)
            sql += f" FROM ({left_table}) sq{iterator}"

            for join in data["select"]["join"]["join_type"]:
                join_type = data["select"]["join"]["join_type"][iterator - 1]
                sq_data = deepcopy(data[f"sq{iterator+1}"])
                right_table = get_sql(sq_data)
                on = data["select"]["join"]["on"][iterator - 1]

                sql += f" {join_type} ({right_table}) sq{iterator+1}"
                if not on in {0, "0"}:
                    sql += f" ON {on}"
                iterator += 1

        elif (
            "table" not in data["select"]
            and "join" not in data["select"]
            and "sq" in data
        ):
            sq_data = deepcopy(data["sq"])
            sq = get_sql(sq_data)
            sql += f" FROM ({sq}) sq"

        if "where" in data["select"] and data["select"]["where"] != "":
            sql += " WHERE {}".format(data["select"]["where"])

        if data["select"]["sql_blocks"]["group_dimensions"] != []:
            group_names = ", ".join(data["select"]["sql_blocks"]["group_dimensions"])
            sql += f" GROUP BY {group_names}"

        if "having" in data["select"] and data["select"]["having"] != "":
            sql += " HAVING {}".format(data["select"]["having"])

    if data["select"]["sql_blocks"]["order_by"] != []:
        order_by = ", ".join(data["select"]["sql_blocks"]["order_by"])
        sql += f" ORDER BY {order_by}"

    if "limit" in data["select"] and data["select"]["limit"] != "":
        sql += " LIMIT {}".format(data["select"]["limit"])

    sql = sqlparse.format(sql, reindent=True, keyword_case="upper")
    return sql
