import pandas as pd
import openpyxl


def read_excel(excel_path, sheet_name="", query=""):
    if sheet_name != "":
        fields = pd.read_excel(excel_path, sheet_name=sheet_name).fillna("")
    else:
        fields = pd.read_excel(excel_path).fillna("")

    schema = "" if "schema" not in fields else fields["schema"][0]
    table = fields["table"][0]

    if query != "":
        fields = fields.query(query)

    columns_qf = {}
    for index, row in fields.iterrows():
        if row["column"] == "":
            attr = row["column_as"]
        else:
            attr = row["column"]
        columns_qf[attr] = {}
        columns_qf[attr]["type"] = row["column_type"]
        columns_qf[attr]["group_by"] = row["group_by"]
        try:
            if row["expression"] != "":
                columns_qf[attr]["expression"] = row["expression"]
        except:
            pass
        if row["column_as"] != "":
            columns_qf[attr]["as"] = row["column_as"]
        try:
            if row["select"] != "":
                columns_qf[attr]["select"] = row["select"]
        except:
            pass
        try:
            if row["custom_type"] != "":
                columns_qf[attr]["custom_type"] = row["custom_type"]
        except:
            pass

    return schema, table, columns_qf


def copy_df_to_excel(df, input_excel_path, output_excel_path, sheet_name='', startrow=0, startcol=0, index=False, header=False):
    writer = pd.ExcelWriter(input_excel_path, engine='openpyxl')
    book = openpyxl.load_workbook(input_excel_path)
    writer.book = book

    writer.sheets = dict((ws.title, ws) for ws in book.worksheets)

    df.to_excel(writer, sheet_name=sheet_name,startrow=startrow,startcol=startcol,index=index,header=header)

    writer.path = output_excel_path
    writer.save()
    writer.close()
