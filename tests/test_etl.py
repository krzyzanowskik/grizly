from ..grizly.etl import build_copy_statement
from configparser import ConfigParser
from ..grizly.utils import get_path

def test_build_copy_statement():

    # config = ConfigParser()
    # config.read(get_path('.aws','credentials'))
    # akey = config['default']['aws_access_key_id']
    # skey = config['default']['aws_secret_access_key']
    akey = os.getenv("AWS_ACCESS_KEY_ID")
    skey = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    def test_time_format():
        params = {
            "file_name": "test.csv", 
            "schema": "z_sandbox_mz", 
            "table_name": "test_table", 
            "time_format": "some_time_format"
        }
        assert build_copy_statement(**params) == f"""\n        COPY z_sandbox_mz.test_table FROM \'s3://teis-data/test.csv\'\n        access_key_id \'{akey}\'\n        secret_access_key \'{skey}\'\n        delimiter \'\t\'\n        NULL \'\'\n        IGNOREHEADER 1\n        REMOVEQUOTES\n        timeformat \'some_time_format\'\n        ;commit;\n        """
    
    def test_remove_inside_quotes():
        params = {
            "file_name": "test.csv", 
            "schema": "z_sandbox_mz", 
            "table_name": "test_table", 
            "remove_inside_quotes": True
        }
        assert build_copy_statement(**params) == f"""\n        COPY z_sandbox_mz.test_table FROM \'s3://teis-data/test.csv\'\n        access_key_id \'{akey}\'\n        secret_access_key \'{skey}\'\n        delimiter \'\t\'\n        NULL \'\'\n        IGNOREHEADER 1\n        CSV QUOTE AS \'\\"\'\n        ;commit;\n        """
    
    test_time_format()
    test_remove_inside_quotes()
