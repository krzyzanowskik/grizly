import pandas


def read_excel(excel_path, sheet_name="", query=""):
    if sheet_name != "":
        fields = pandas.read_excel(excel_path, sheet_name=sheet_name).fillna("")
    else:
        fields = pandas.read_excel(excel_path).fillna("")
    if "schema" in fields:
        schema = fields["schema"][0]
    table = fields["table"][0]

    if query != "":
        fields = fields.query(query)

    columns_qf = {}
    for index, row in fields.iterrows():
        columns_qf[row["column"]] = {}
        columns_qf[row["column"]]["type"] = row["column_type"]
        columns_qf[row["column"]]["group_by"] = row["group_by"]
        if row["column_as"] != "":
            columns_qf[row["column"]]["as"] = row["column_as"]
    if "schema" in fields:
        return schema, table, columns_qf
    else:
        return "", table, columns_qf
