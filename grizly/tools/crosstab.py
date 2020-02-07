from numpy import isnan


class Crosstab():
    def __init__(self, caption = "", formatter={}, styling={}, na_rep=0, css_class=""):
        self.caption = caption
        self.formatter = formatter
        self.styling = styling
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
            filters = [f"`{column}`=='{item}'""" for column, item in zip(dimensions, group)]
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


    def apply_style(self, mapping, level=["content", "subtotals"]):
        for item in level:
            self.styling[item] = mapping
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


    def append_header(self, values, pos=0):
        if len(values) != len(self.columns):
            raise ValueError("Length of row does not match length of table")
        new_dimensions = []
        for dim, new_dim in zip(self.dimensions, values[:len(self.dimensions)]):
            dim = dim if isinstance(dim, tuple) else (dim,)
            new_dim = dim[:pos] + (new_dim,) + dim[pos:]
            new_dimensions.append(new_dim)
        self.dimensions = new_dimensions

        new_measures = []
        for measure, new_measure in zip(self.measures, values[len(self.dimensions):]):
            measure_t = measure if isinstance(measure, tuple) else (measure,)
            new_measure = measure_t[:pos] + (new_measure,) + measure_t[pos:]
            new_measures.append(new_measure)
            for item in self.content:
                self.content[item][new_measure] = self.content[item][measure]
                self.content[item].pop(measure)

            for item in self.subtotals:
                self.subtotals[item][new_measure] = self.subtotals[item][measure]
                self.subtotals[item].pop(measure)

            if measure in self.formatter:
                self.formatter[new_measure] = self.formatter[measure]
                self.formatter.pop(measure)

            for level in self.styling:
                if measure in self.styling[level]:
                    self.styling[level][new_measure] = self.styling[level][measure]
                    self.styling[level].pop(measure)
        self.measures = new_measures
        self.columns = self.dimensions + self.measures

        return self


    def remove(self, group, axis=0):
        if axis == 0:
            if group in self.content:
                self.content.pop(group)
            elif group in self.subtotals:
                self.subtotals.pop(group)
        elif axis == 1:
            self.columns.remove(group)
            if group in self.measures:
                self.measures.remove(group)
                for item in self.content:
                    self.content[item].pop(group)
                for item in self.subtotals:
                    self.subtotals[item].pop(group)
                if group in self.formatter:
                    self.formatter.pop(group)
                for level in self.styling:
                    if group in self.styling[level]:
                        self.styling[level].pop(group)
            elif group in self.dimensions:
                self.dimensions.remove(group)
        return self


    def rearrange(self, groups:list, axis=0):
        if axis == 0:
            pass
        elif axis == 1:
            if set(groups) != set(self.columns):
                raise ValueError("List of groups does not match list of crosstab columns")
            self.columns = groups
            measures = []
            dimensions = []
            for group in groups:
                if group in self.measures:
                    measures.append(group)
                    for item in self.content:
                        self.content[item][group] = self.content[item].pop(group)
                    for item in self.subtotals:
                        self.subtotals[item][group] = self.subtotals[item].pop(group)
                    if group in self.formatter:
                        self.formatter[group] = self.formatter.pop(group)
                    for level in self.styling:
                        if group in self.styling[level]:
                            self.styling[level][group] = self.styling[level].pop(group)
                elif group in self.dimensions:
                    dimensions.append(group)
            self.measures = measures
            self.dimensions = dimensions
        return self

    def rename(self, groups, axis=0):
        if axis == 0:
            pass
        elif axis == 1:
            unknown_keys = set(groups.keys()) - set(self.columns)
            if unknown_keys != set():
                raise ValueError(f"Keys {unknown_keys} not found in columns")
            columns = []
            dimensions = []
            for group in self.dimensions:
                new_group = group if group not in groups else groups[group]
                dimensions.append(new_group)
                columns.append(new_group)
            measures = []
            content = {item: {} for item in self.content}
            subtotals = {item: {} for item in self.subtotals}
            formatter = {}
            styling = {level: {} for level in self.styling}
            for group in self.measures:
                new_group = group if group not in groups else groups[group]
                measures.append(new_group)
                columns.append(new_group)
                for item in self.content:
                    content[item][new_group] = self.content[item][group]
                for item in self.subtotals:
                    subtotals[item][new_group] = self.subtotals[item][group]
                if group in self.formatter:
                    formatter[new_group] = self.formatter[group]
                for level in self.styling:
                    if group in self.styling[level]:
                        styling[level][new_group] = self.styling[level][group]
            self.columns = columns
            self.measures = measures
            self.dimensions = dimensions
            self.content = content
            self.subtotals = subtotals
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
            for t, rowspan, colspan, cssclass, style, value in row_def:
                rowspan = f" rowspan={rowspan}" if rowspan != 1 else ""
                colspan = f" colspan={colspan}" if colspan != 1 else ""
                cssclass = f" class={cssclass}" if cssclass != "" else ""
                style = style if style == "" else f" {style}"
                html += f"    <t{t}{rowspan}{colspan}{cssclass}{style}> {value} </t{t}>\n"
            return html

        def get_rowspan(group):
            counter = 0
            for i in self.content:
                if group == i[:len(group)]:
                    counter += 1
            for i in self.subtotals:
                if group == i[:len(group)]:
                    counter += 1
            return counter

        def get_cell_def(group, measure, level):
            if level == "content":
                value = self.content[group][measure]
            elif level == "subtotals":
                value = self.subtotals[group][measure]

            style = ""
            if level in self.styling and measure in self.styling[level]:
                style = self.styling[level][measure](value)

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
                    row_def.append(('h', get_rowspan(tuple(group)), 1, "", "", item))
                    html = get_body(columns[1:], group, row_def, html)
                    if tuple(group) in self.subtotals:
                        name = self.subtotals_names.get(tuple(group)) or "Total"
                        colspan = len(self.dimensions) - len(group)
                        cssclass = "total" if len(group) == 1 else "subtotal"
                        row_def = [('h', 1, colspan, cssclass, "", name)]
                        values = self.subtotals[tuple(group)]
                        for measure in values:
                            style, value = get_cell_def(tuple(group), measure, "subtotals")
                            row_def.append(('d', 1, 1, cssclass, style, value))
                        html += f"  <tr>\n" + get_row(row_def) + "  </tr>\n"
                    row_def = []
                    group.pop(-1)
            else:
                values = self.content[tuple(group)]
                for measure in values:
                    style, value = get_cell_def(tuple(group), measure, "content")
                    row_def.append(('d', 1, 1, "", style, value))
                html += "  <tr>\n" + get_row(row_def) + "  </tr>\n"
            return html

        def get_header():
            columns = []
            for column in self.columns:
                columns.append(column if isinstance(column, tuple) else (column,))

            def get_colspan(row, col):
                counter = 1
                for item in columns[col+1:]:
                    if item[row] == columns[col][row]:
                        counter += 1
                    else:
                        break
                return counter

            html = ""
            for row in range(len(columns[0])):
                row_def = []
                for col in range(len(columns)):
                    if (row, col) == (0, 0) or columns[col][row] != columns[col-1][row]:
                        row_def.append(('h', 1, get_colspan(row, col), "", "", columns[col][row]))
                html += "  <tr>\n" + get_row(row_def) + "  </tr>\n"

            return html

        caption = "<caption>\n" + self.caption + "</caption>\n" if self.caption != "" else ""
        thead = "<thead>\n" + get_header() + "</thead>\n"
        tbody = "<tbody>\n" + get_body(self.dimensions, [], [], "") + "</tbody>\n"

        table = f"<table>\n" + caption + thead + tbody + "</table>\n"

        return  table


    def save_html(self, html_path):
        html = self.to_html()
        with open(html_path, 'w') as file:
            file.write(html)


    def _repr_html_(self):
        return self.to_html()