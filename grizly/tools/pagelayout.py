from yattag import Doc
from html5print import HTMLBeautifier

doc, tag, text = Doc().tagtext()


class Page:
    def __init__(self, content):
        self.content = content

    def to_html(self):
        doc.asis("<!DOCTYPE html>")
        with tag("html"):
            with tag("head"):
                doc.asis(
                    """<link rel="shortcut icon" href="static/images/acoefavicon.ico" type="image/x-icon">
                        <link rel="icon" href="static/images/acoefavicon.ico" type="image/x-icon">
                        <meta charset="utf-8">
                        <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
                        <link href="https://fonts.googleapis.com/css?family=Poppins:300,400,500,600,700,800,900" rel="stylesheet">
                        <title>Basic HTML File</title>
                        <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/font-awesome/4.7.0/css/font-awesome.min.css">
                        <link rel="stylesheet" href="static/css/style.css">"""
                )
            with tag("body"):
                for container in self.content:
                    container.to_html()
        return doc.getvalue()


class Text:
    def __init__(self, content: str = None, style: str = None, content_format: str = None):
        self.content = content or ""
        self.style = style or ""
        self.content_format = content_format or "text"

    def from_json(self, json_path):
        pass

    def to_html(self):
        if self.content_format == "text":
            with tag("div", style=self.style):
                doc.text(self.content)
        else:
            with tag("div"):
                doc.text(self.content)


class FinanceLayout:
    def __init__(self, header=None, body=None, footer=None, style=""):
        self.header = header
        self.body = body
        self.footer = footer
        self.style = style

    def from_json(self, json_path):
        pass

    def to_html(self):
        html = f"""{self.style}"""
        if self.header:
            html += f""" {self.header.to_html()} """
        if self.body:
            html += f"""  {self.body.to_html()} """
        if self.footer:
            html += f""" {self.footer.to_html()} """
        return html


class GridLayout:
    def __init__(self, header=None, body=None):
        self.header = header
        self.body = body

    def to_html(self):
        with tag("div", klass="container-fluid"):
            if self.header:
                with tag("div", klass="row justify-content-md-center"):
                    with tag("h2", klass="mb-4 underline"):
                        text(self.header)
            if self.body:
                with tag("div"):
                    self.body.to_html()
                    doc.asis("<hr>")
        return


class GridCardItem:
    def __init__(
        self,
        header: str = "",
        href: str = "#",
        paragraph: str = "",
        img: str = "",
        width: str = "",
        button: str = "",
        padding_content: str = "",
        padding_between: str = "",
    ):
        self.header = header
        self.paragraph = paragraph
        self.href = href
        self.img = img
        self.width = width
        self.button = button
        self.padding_content = padding_content
        self.padding_between = padding_between

    def to_html(self):
        with tag("div", klass="card", style="width:{self.width};padding:{self.padding_between}"):
            with tag("a", href=self.href):
                doc.asis(f"""<img class="card-img-top img-fluid" src="{self.img}" alt="Card image cap">""")
            with tag("div", klass="card-body", style="padding:{self.padding_content}"):
                with tag("h5", klass="card-title"):
                    doc.text(self.header)
                with tag("p", klass="card-text"):
                    doc.text(self.paragraph)
                    doc.asis("<br>")
                    if self.button:
                        with tag("a", klass="btn btn-primary", href=self.href):
                            doc.text(self.button)
                    else:
                        with tag("a", klass="btn btn-primary", href=self.href):
                            doc.text("Go To")
        return


class Row:
    def __init__(self, content: str = "", style: str = ""):
        self.content = content
        self.style = style

    def to_html(self):
        for row in self.content:
            with tag("div", klass=f"""row {self.style}"""):
                row.to_html()
        return


class Column:
    def __init__(self, content, size=None):
        self.size = size
        self.content = content

    def to_html(self):
        if self.size:
            # with array of size
            if len(self.size) == len(self.content):
                for i in range(0, len(self.content)):
                    with tag("div", klass=f"""col-sm-{self.size[i]}"""):
                        self.content[i].to_html()
            # with one size for all column
            else:
                for column in self.content:
                    with tag("div", klass=f"""col-sm-{self.size}"""):
                        column.to_html()
        # automatic size
        else:
            for column in self.content:
                with tag("div", klass=f"""col-md"""):
                    column.to_html()
        return
