import gate
from utils.mock_di_api import mock_api
from utils.operator_test import operator_test
        
api = mock_api(__file__)     # class instance of mock_api
mock_api.print_send_msg = False  # set class variable for printing api.send

optest = operator_test(__file__)
# config parameter 


msg = api.Message(attributes={'operator':'di_replication.gate'},body = 'test1')
gate.on_data(msg)
msg = api.Message(attributes={'operator':'di_replication.gate','message.lastBatch':False},body = 'test2')
gate.on_data(msg)
msg = api.Message(attributes={'operator':'di_replication.gate','message.lastBatch':True},body = 'test3')
gate.on_data(msg)


  