from numpy import isnan

class Crosstab():
    def __init__(self, formatter={}, mapping={}, na_rep=0):
        self.formatter = formatter
        self.mapping = mapping
        self.na_rep = na_rep
        self.dimensions = []
        self.measures = []
        self.columns = []
        self.content = {}
        self.subtotals = {}
        self.subtotals_names = {}


    def from_df(self, df, dimensions, measures):
        self.dimensions = dimensions
        self.measures = measures
        self.columns = dimensions + measures

        content = {}
        for group in df[dimensions].values:
            filters = [f"{column}=='{item}'" for column, item in zip(dimensions, group)]
            query = " and ".join(filters)
            group = tuple(group)
            content[group] = {}
            for measure in measures:
                value = df.query(query)[measure].values[0]
                if isnan(value):
                    value = self.na_rep
                content[group].update({measure: value})
        self.content = content
        return self


    def sum(self, group, measures):
        csum = []
        for measure in measures:
            counter = 0
            for item in self.content:
                if group == item[:len(group)]:
                    counter += self.content[item][measure]
            csum.append(counter)
        csum = csum[0] if len(measures) == 1 else csum
        return csum


    def count(self, group, measures):
        ccount = []
        for measure in measures:
            counter = 0
            for item in self.content:
                if group == item[:len(group)]:
                    counter += 1
            ccount.append(counter)
        ccount = ccount[0] if len(measures) == 1 else ccount
        return ccount


    def avg(self, group, measures):
        csum = self.sum(group, measures)
        ccount = self.count(group, measures)
        cavg = csum / ccount if len(measures) == 1 else [i / j for i, j in zip(csum, ccount)]
        return cavg


    def agg(self, group, measures, func):
        if func == "sum":
            return self.sum(group, measures)
        elif func == "avg":
            return self.avg(group, measures)
        elif func == "count":
            return self.count(group, measures)
        else:
            raise ValueError("Wrong aggregation type")


    def format(self, formatter):
        self.formatter = formatter
        return self


    def map(self, mapping, level=["content", "subtotals"]):
        for item in level:
            self.mapping[item] = mapping
        return self


    def append(self, group, values, axis=0):
        if axis == 0:
            self.content[group] = {}
            for measure, value in zip(self.measures, values):
                self.content[group].update({measure: value})
        elif axis == 1:
            if group in self.measures:
                raise ValueError(f"Column {group} already exists")
            else:
                self.measures.append(group)
                self.columns.append(group)
                for item, value in zip(self.content, values):
                    self.content[item].update({group: value})
        return self


    def add_subtotals(self, columns, aggregation={}, names={}):
        self.subtotals_names = names
        subtotals = {}

        def get_subtotals(columns, group):
            if len(columns) != 0:
                items = []
                for row in self.content:
                    if tuple(group) == row[:len(group)] and row[len(group)] not in items:
                        items.append(row[len(group)])
                for item in items:
                    group.append(item)
                    if self.dimensions[len(group)-1] in columns:
                        subtotals[tuple(group)] = {}
                        for measure in self.measures:
                            func = aggregation.get(measure) or "sum"
                            value = self.agg(tuple(group), [measure], func)
                            subtotals[tuple(group)].update({measure: value})
                        group = get_subtotals(columns[1:], group)
                    else:
                        group = get_subtotals(columns, group)
                    group.pop(-1)
            return group

        get_subtotals(columns, [])

        self.subtotals = subtotals
        return self


    def to_html(self):
        def get_row(row_def):
            html = ''
            for t, rowspan, style, value in row_def:
                rowspan = f" rowspan={rowspan}" if rowspan != 1 else ""
                html += f"    <t{t}{rowspan} {style}> {value} </t{t}>\n"
            return html

        def get_rowspan(group):
            counter = 0
            for i in self.content:
                if group == i[:len(group)]:
                    counter += 1
            for i in self.subtotals:
                if group == i[:len(group)] and group != i:
                    counter += 1
            return counter

        def get_cell_def(group, measure, level):
            if level == "content":
                value = self.content[group][measure]
            elif level == "subtotals":
                value = self.subtotals[group][measure]

            style = ""
            if level in self.mapping and measure in self.mapping[level]:
                style = self.mapping[level][measure](value)

            if measure in self.formatter:
                value = self.formatter[measure].format(value)

            return style, value

        def get_body(columns, group, row_def, html):
            if len(columns) != 0:
                items = []
                for row in self.content:
                    if tuple(group) == row[:len(group)] and row[len(group)] not in items:
                        items.append(row[len(group)])
                for item in items:
                    group.append(item)
                    row_def.append(('h', get_rowspan(tuple(group)), "", item))
                    html = get_body(columns[1:], group, row_def, html)
                    row_def = []
                    if tuple(group) in self.subtotals:
                        colspan = len(self.dimensions) - len(group) +1
                        values = self.subtotals[tuple(group)]
                        for measure in values:
                            style, value = get_cell_def(tuple(group), measure, "subtotals")
                            row_def.append(('d', 1, style, value))
                        name = self.subtotals_names.get(tuple(group)) or "Total"
                        html += f"  <tr>\n  <th colspan={colspan}>{name}</th>\n" + get_row(row_def) + "  </tr>\n"
                        row_def = []
                    group.pop(-1)
            else:
                values = self.content[tuple(group)]
                for measure in values:
                    style, value = get_cell_def(tuple(group), measure, "content")
                    row_def.append(('d', 1, style, value))
                html += "  <tr>\n" + get_row(row_def) + "  </tr>\n"
            return html

        thead = "<thead>\n" + get_row([('h', 1, "", column) for column in self.columns]) + "</thead>\n"

        html = get_body(self.dimensions, [], [], "")
        tbody = "<tbody>\n" + html + "</tbody>\n"
        table = "<table>\n" + thead + tbody + "</table>"

        return table


    def _repr_html_(self):
        return self.to_html()