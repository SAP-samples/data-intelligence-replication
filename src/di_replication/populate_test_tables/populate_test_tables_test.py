import populate_test_tables
from utils.mock_di_api import mock_api
from utils.operator_test import operator_test
        
api = mock_api(__file__)     # class instance of mock_api
mock_api.print_send_msg = False  # set class variable for printing api.send

optest = operator_test(__file__)
# config parameter 
api.config.num_rows = 100   # datatype : integer

msg = optest.get_message('msg_1.json')
#msg = api.Message(attributes={'operator':'di_replication.populate_test_tables'},body = None)

populate_test_tables.on_data(msg)

  