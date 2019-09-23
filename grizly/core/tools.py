import os
from pandas import (
    ExcelWriter
)
import openpyxl
import win32com.client as win32

class Excel:
    """Class which deals with Excel files.
    """
    def __init__(self, excel_path, output_excel_path=''):
        """
        Parameters
        ----------
        excel_path : str
            Path to input Excel file.
        output_excel_path : str, optional
            Path to output Excel.
        """

        self.input_excel_path = excel_path
        self.filename = os.path.basename(self.input_excel_path)
        if output_excel_path != '':
            self.output_excel_path = output_excel_path
        else:
            self.output_excel_path = os.path.join(os.path.split(excel_path)[0], 
                                        os.path.splitext(self.filename)[0] + '_working' + os.path.splitext(self.filename)[1])
        #self.book = book = openpyxl.load_workbook(self.input_excel_path)
    
    def write_df(self, df, sheet, row=1, col=1, index=False, header=False):
        """Saves DatFrame in Excel file.
        
        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame to be saved in Excel
        sheet: str
            Name of sheet
        row : int, optional
            Upper left cell row to dump DataFrame, by default 1
        col : int, optional
            Upper left cell column to dump DataFrame, by default 1
        index : bool, optional
            Write row names (index), by default False
        header : bool, optional
            Write column names (header), by default False
        """

        writer = ExcelWriter(self.input_excel_path, engine='openpyxl')
        book = openpyxl.load_workbook(self.input_excel_path)
        writer.book = book

        writer.sheets = dict((ws.title, ws) for ws in book.worksheets)

        df.to_excel(writer, sheet_name=sheet,startrow=row-1,startcol=col-1,index=index,header=header)

        writer.path = self.output_excel_path
        writer.save()
        writer.close()

        self.input_excel_path = self.output_excel_path
        self.filename = os.path.basename(self.output_excel_path)

        return self
        
    def write_value(self, sheet, row, col, value):
        """Writes cell value to Excel file.
        
        Parameters
        ----------
        sheet : str
            Name of sheet
        row : int
            Cell row
        col : int
            Cell column

        Returns
        -------
        float
            Cell value
        """
        book = openpyxl.load_workbook(self.input_excel_path)

        worksheet = book.get_sheet_by_name(sheet)
        worksheet.cell(row=row, column=col, value=value)
        book.save(filename = self.output_excel_path)
        
        print("Written value {} in sheet {}".format(value, sheet))

        return self
     
    def save(self):
        """save to workbook"""
        #self.book.save()
        pass

    def get_value(self, sheet, row, col):
        """Extracts cell value from Excel file.
        
        Parameters
        ----------
        sheet : str
            Name of sheet
        row : int
            Cell row
        col : int
            Cell column

        Returns
        -------
        float
            Cell value
        """
        xlApp = win32.Dispatch('Excel.Application')
        wb = xlApp.Workbooks.Open(self.input_excel_path)
        ws = wb.Worksheets(sheet)
        value = ws.Cells(row,col).Value
        wb.Close()
        xlApp.Quit()
        
        return value

    def open(self, input=False):

        if input == False:
            path = self.input_excel_path
        else:
            path = self.output_excel_path

        try:
            excel = win32.gencache.EnsureDispatch('Excel.Application')
            try:
                xlwb = excel.Workbooks(path)
            except Exception as e:
                try:
                    xlwb = excel.Workbooks.Open(path)
                except Exception as e:
                    print(e)
                    xlwb = None
            ws = wb.Worksheets('blaaaa') 
            excel.Visible = True

        except Exception as e:
            print(e)

        finally:
            ws = None
            wb = None
            excel = None

        return self

