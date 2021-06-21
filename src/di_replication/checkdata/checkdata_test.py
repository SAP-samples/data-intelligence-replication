import checkdata
from utils.mock_di_api import mock_api
from utils.operator_test import operator_test
        
api = mock_api(__file__)     # class instance of mock_api
mock_api.print_send_msg = True  # set class variable for printing api.send

optest = operator_test(__file__)
# config parameter 


msg = optest.get_msgtable('bytetest.csv')
msg.body = [ [b[0],b[1],b[2].encode('cp1250'),b[3],b[4]] for b in msg.body ]
msg.attributes['table']['columns'][2]['type']['hana'] = 'VARBINARY'
checkdata.on_input(msg)