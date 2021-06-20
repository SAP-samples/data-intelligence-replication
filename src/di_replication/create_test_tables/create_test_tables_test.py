import create_test_tables
from utils.mock_di_api import mock_api
from utils.operator_test import operator_test
        
api = mock_api(__file__)     # class instance of mock_api
mock_api.print_send_msg = False  # set class variable for printing api.send

optest = operator_test(__file__)
# config parameter 
api.config.base_tablename = 'REPLICATION.TEST_TABLE'    # datatype : string
api.config.num_new_tables = 5   # datatype : integer
api.config.num_drop_tables = 5   # datatype : integer
api.config.table_repos = 'REPLICATION.TABLE_REPOS'    # datatype : string

create_test_tables.gen()

  