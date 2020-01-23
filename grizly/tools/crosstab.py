def get_row(row_span):
    html = ''
    for value, span in row_span:
        rowspan = f" rowspan={span}" if span != 1 else ""
        html += f"    <td{rowspan}> {value} </td>\n"
    return html


def get_header(row_span):
    html = ''
    for value, span in row_span:
        rowspan = f" rowspan={span}" if span != 1 else ""
        html += f"    <th{rowspan}> {value} </th>\n"
    return html


def get_rowspan(df, subtotal):
    """Returns a nested dictionary with 'content' and 'lines' in each key.
    For now we only use 'lines' number so in fact it could return only this number."""
    rows = set(df[df.columns[0]])
    data = {}
    if df.columns[0] in subtotal:
        lines = len(rows)
    else:
        lines = 0
    if len(df.columns) != 1:
        for row in rows:
            df_new = df.query(f"{df.columns[0]}=='{row}'")[df.columns[1:]]
            content, lines_new = get_rowspan(df_new, subtotal)
            lines += lines_new
            data[row] = {'content': content, 'lines': lines_new}
    else:
        for row in rows:
            data[row] = {}
            data[row]['lines'] = len(df.query(f"{df.columns[0]}=='{row}'"))
            lines += len(df.query(f"{df.columns[0]}=='{row}'"))
    return data, lines


def html_body(df, columns, values, subtotals, row_span, html):
#     df = df[columns+values].groupby(columns).sum().reset_index()
    if len(columns) != 1:
        for row in set(df[columns[0]]):
            df_new = df.copy().query(f"{columns[0]}=='{row}'")
            row_span.append((row, get_rowspan(df[columns], subtotals)[0][row]['lines']))
            html = html_body(df_new, columns[1:], values, subtotals, row_span, html)
            if columns[0] in subtotals:
                colspan = len(columns)
                row_span = []
                for value in df_new[values].sum():
                    row_span.append((value, 1))
                html += f"  <tr>\n  <td colspan={colspan}>Total</td>\n" + get_row(row_span) + "  </tr>\n"
            row_span = []
    else:
        rem_items = list(df[columns[0]])
        row_span.append((rem_items[0], 1))
        for value in df.query(f"{columns[0]}=='{rem_items[0]}'")[values].sum():
            row_span.append((value, 1))
        html += "  <tr>\n" + get_row(row_span) + "  </tr>\n"
        for item in rem_items[1:]:
            row_span = [(item, 1)]
            for value in df.query(f"{columns[0]}=='{item}'")[values].sum():
                row_span.append((value, 1))
            html += "  <tr>\n" + get_row(row_span) + "  </tr>\n"
    return html


def get_crosstab(df, columns, values, subtotals=[]):
    thead = "<thead>\n  <tr>\n" + get_header([(column, 1) for column in columns+values]) + "  </tr>\n</thead>\n"
    if columns[-1] in subtotals:
        subtotals.remove(columns[-1])
        print("You can't do subtotals on the last non value column.")
    tbody = html_body(df=df,
                     columns=columns,
                     values=values,
                     subtotals=subtotals,
                     row_span=[],
                     html=''
                    )
    tbody = "<tbody>\n" + tbody + "</tbody>\n"
    table = "<table>\n" + thead + tbody + "</table>"
    return table