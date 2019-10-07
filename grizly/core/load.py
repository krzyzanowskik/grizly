import pandas

class Load():
    """
        Loads data into a database
        Parameters
        ----------
        engine_str : string
    """
    def __init__(self, csv_path):
        self.csv_path = csv_path

    def to_s3(self):
        pass

    def deletethis(self):
        print(x)

    def to_rds(self, table, s3_bucket, engine_str, schema:str=None, sep='\t'):
        type = "VARCHAR(500)"
        type = "FLOAT(53)"
        pandas.read_csv(csv_path, nrows=50)
        columns = ''
        for column in df.columns:
            column_type = df[column].dtype
            if column_type == object:
                db_column_type = "VARCHAR(500)"
            else:
                db_column_type = "FLOAT(53)"
            columns += column + ' ' + db_column_type + ', \n'
            
            t = (column, column_type, db_column_type)
        print(columns)