class Page():
    def __init__(self, content):
        self.content = content
        
    def to_html(self):
        html="""<!DOCTYPE html>
                <html lang="en">
                <head>
                    <link rel="shortcut icon" href="static/images/acoefavicon.ico" type="image/x-icon">
                    <link rel="icon" href="static/images/acoefavicon.ico" type="image/x-icon">
                    <meta charset="utf-8">
                    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
                    <link href="https://fonts.googleapis.com/css?family=Poppins:300,400,500,600,700,800,900" rel="stylesheet">
                    <title>Basic HTML File</title>
                    <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/font-awesome/4.7.0/css/font-awesome.min.css">
                    <link rel="stylesheet" href="static/css/style.css">
                </head>
                <body>"""
        for container in self.content:
            html+=f"""\n {container.to_html()}"""
        html+=f"""</body> \n </html>"""
        return html

class Text():
    def __init__(self, content:str=None, style:str=None, content_format:str=None):
        self.content = content or ""
        self.style = style or ""
        self.content_format = content_format or "text"
        
    def from_json(self, json_path):
        pass

    def to_html(self):
        if self.content_format == "text":
            return f"""<div style="{self.style}">\n {self.content}</div>"""
        else:
            return self.content
        
class FinanceLayout():
    def __init__(self, header=None, body=None, footer=None, style= ""):
        self.header = header
        self.body = body
        self.footer = footer
        self.style = style
        
    def from_json(self, json_path):
        pass
    
    def to_html(self):
        html = f"""{style}"""
        if self.header:
            html +=f""" {self.header.to_html()} """
        if self.body:
            html += f"""  {self.body.to_html()} """
        if self.footer:
            html += f""" {self.footer.to_html()} """
        return html
    
class GridLayout():
    def __init__(self, header=None, body=None):
        self.header = header
        self.body = body
        
    def to_html(self):
        html = ""
        if self.header:
            html +=f'''\n <div class="row justify-content-md-center"> <h2 class="mb-4 underline">{self.header}</h2></div> </div>'''
        if self.body:
            html += f'''\n </div> {self.body.to_html()}<hr> '''
        return html
    
class GridCardItem():
    def __init__(self,
                 header: str="",
                 href: str="",
                 paragraph: str="",
                 img: str="",
                 width: str="",
                 button: str="",
                 padding_content: str="",
                 padding_between: str=""):
        
        self.header = header
        self.paragraph = paragraph
        self.href = href
        self.img = img
        self.width = width
        self.button = button
        self.padding_content = padding_content
        self.padding_between = padding_between

    def to_html(self):
        if self.href:
            if self.button:
                button = f"""<a href="{self.href}" class="btn btn-primary">{self.button}</a>"""
            else:
                button= f"""<a href="{self.href}" class="btn btn-primary">Go To</a>"""
        else: 
            href="#"
            button=""
            
        html = f"""<div class="card" style="width:{self.width};padding:{self.padding_between}">
                    <a href="{self.href}">
                        <img class="card-img-top img-fluid" src="{self.img}" alt="Card image cap">
                    </a>
                    <div class="card-body" style="padding:{self.padding_content};">
                        <h5 class="card-title">{self.header}</h5>
                        <p class="card-text">{self.paragraph}</p>
                        {button}
                    </div>
                    </div>
                """
        return html
        
class GridEmptyItem():
    def __init__(self,size: str = ""):
        self.size=size
    def to_html(self):
        html = ""
        return html
        
class Row():
    def __init__(self,content: str = "", style: str= ""):
        self.content=content
        self.style=style

    def to_html(self):
        html=""
        for row in self.content:
                html+=f"""<div class="row {self.style}">  {row.to_html()}  </div>"""
        return html
    
class Column():
    def __init__(self,content, size=None):
        self.size=size
        self.content=content

    def to_html(self):
        html=""
        if self.size:
            #with array of size
            if len(self.size)==len(self.content):
                for i in range(0,len(self.content)):
                    html+=f"""<div class="col-sm-{self.size[i]}">  {self.content[i].to_html()}  </div>"""
            #with one size for all column
            else:
                for column in self.content:
                    html+=f"""<div class="col-sm-{self.size}"> {column.to_html()} </div>"""
        #automatic size
        else:
            for column in self.content:
                html+=f"""<div class="col-sm">  {column.to_html()}  </div>"""
        return html