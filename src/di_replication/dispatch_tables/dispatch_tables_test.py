import dispatch_tables
from utils.mock_di_api import mock_api
from utils.operator_test import operator_test
        
api = mock_api(__file__)     # class instance of mock_api
mock_api.print_send_msg = True  # set class variable for printing api.send

optest = operator_test(__file__)
# config parameter 
api.config.mode = 'R'    # datatype : string


msg = optest.get_msgtable('TABLE_REPOS.csv')
dispatch_tables.on_tables(msg)

#msg1 = api.Message(attributes={'operator':'di_replication.dispatch_tables'},body = None)
#dispatch_tables.on_data(msg1)

#msg2 = api.Message(attributes={'operator':'di_replication.dispatch_tables'},body = None)
#dispatch_tables.on_nodata(msg2)
# print result list
for mt in mock_api.msg_list :
  print('*********************')
  print('Port: {}'.format(mt['port']))
  print('Data: {}'.format(mt['data'].attributes))
  print('Data: {}'.format(mt['data'].body))
  #print(optest.msgtable2df(mt['data']))  
  