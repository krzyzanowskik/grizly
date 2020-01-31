class Crosstab():
    def __init__(self, df, columns, values):
        self.df = df
        self.columns = columns
        self.values = values
        self.subtotals = {}
        self.subtotal_columns = []

    def add_subtotals(self, columns):
        self.subtotal_columns = columns
        subtotals = {}
        groups = []

        def get_groups(df, columns, group):
            if len(columns) != 0:
                rows = []
                for item in df[columns[0]]:
                    if item not in rows:
                        rows.append(item)
                for item in rows:
                    group.append(item)
                    group = get_groups(df, columns[1:], group)
                    groups.append([i for i in group])
                    group.pop(-1)
            return group

        get_groups(self.df, columns, [])

        for group in groups:
            colspan = len(self.df.columns) - len(self.values) - len(group) + 1
            filters = [f"{column}=='{row}'" for column, row in zip(columns[:len(group)], group)]
            query = " and ".join(filters)
            group_values = {value: self.df.query(query)[value].sum() for value in self.values}
            group = tuple(group)
            subtotals[group] = {"colspan": colspan,
                                "values": group_values}

        self.subtotals = subtotals
        return self


    def to_html(self):
        def get_header(row_span):
            html = ''
            for value, span in row_span:
                rowspan = f" rowspan={span}" if span != 1 else ""
                html += f"    <th{rowspan}> {value} </th>\n"
            return html

        def get_row(row_span):
            html = ''
            for value, span in row_span:
                rowspan = f" rowspan={span}" if span != 1 else ""
                html += f"    <td{rowspan}> {value} </td>\n"
            return html

        def get_body(d, row_span, html, group, subtotals):
            if 'content' in d.keys():
                for key in d['content'].keys():
                    group.append(key)
                    row_span.append((key, d['content'][key]['rowspan']))
                    html = get_body(d['content'][key], row_span, html, group, subtotals)
                    row_span = []
                    if tuple(group) in subtotals:
                        colspan = subtotals[tuple(group)]['colspan']
                        values = subtotals[tuple(group)]['values']
                        for value in values.keys():
                            row_span.append((values[value], 1))
                        html += f"  <tr>\n  <td colspan={colspan}>Total</td>\n" + get_row(row_span) + "  </tr>\n"
                        row_span = []
                    group.pop(-1)
            elif 'values' in d.keys():
                for value in d['values'].keys():
                    row_span.append((d['values'][value], 1))
                html += "  <tr>\n" + get_row(row_span) + "  </tr>\n"
            return html

        def get_content(df, columns, values, subtotals):
            rows = []
            for item in df[columns[0]]:
                    if item not in rows:
                        rows.append(item)
            data = {}
            if columns[0] in subtotals:
                lines = len(rows)
            else:
                lines = 0
            if len(columns) != 1:
                for row in rows:
                    df_new = df.query(f"{columns[0]}=='{row}'")[df.columns[1:]]
                    data[row] = get_content(df_new, columns[1:], values, subtotals)
                    lines += data[row]['rowspan']
            else:
                for row in rows:
                    data[row] = {}
                    data[row]['rowspan'] = len(df.query(f"{columns[0]}=='{row}'"))
                    data[row]['values'] = {}
                    for value in values:
                        data[row]['values'][value] = df.query(f"{columns[0]}=='{row}'")[value].values[0]
                    lines += data[row]['rowspan']
            return {'content': data, 'rowspan': lines}


        thead = "<thead>\n" + get_header([(column, 1) for column in self.columns+self.values]) + "</thead>\n"

        content = get_content(df=self.df,
                              columns=self.columns,
                              values=self.values,
                              subtotals=self.subtotal_columns)

        tbody = "<tbody>\n" + get_body(d=content, row_span=[], html='', group=[], subtotals=self.subtotals) + "</tbody>\n"
        table = "<table>\n" + thead + tbody + "</table>"

        return table


    def _repr_html_(self):
        return self.to_html()