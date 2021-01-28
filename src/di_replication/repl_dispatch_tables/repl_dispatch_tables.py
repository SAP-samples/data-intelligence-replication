

import os
import io
import pandas as pd
import logging

import subprocess

import sdi_utils.gensolution as gs

try:
    api
except NameError:
    class api:
        class Message:
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes
                
        def send(port,msg) :
            if port == outports[1]['name'] :
                print(msg.body)
            elif port == outports[2]['name'] :
                print('Limit reached - Exit')
                #exit(0)
        class config:
            ## Meta data
            config_params = dict()
            tags = {}
            version = "0.1.0"

            operator_description = "Dispatch Tables"
            operator_name = 'repl_dispatch_tables'
            operator_description_long = "Send next table to process."
            add_readme = dict()

            stop_no_changes = True
            config_params['stop_no_changes'] = {'title': 'Stops on no changes',
                                           'description': 'Stops when no changes.',
                                           'type': 'boolean'}

        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
        logger = logging.getLogger(name=config.operator_name)


# catching logger messages for separate output
log_stream = io.StringIO()
sh = logging.StreamHandler(stream=log_stream)
sh.setFormatter(logging.Formatter('%(asctime)s |  %(levelname)s | %(name)s | %(message)s', datefmt='%H:%M:%S'))
api.logger.addHandler(sh)


df_tables = pd.DataFrame()
pointer = 0
no_changes_counter = 0
num_roundtrips = 0
num_batch = 1
first_call = True

def set_replication_tables(msg) :
    global df_tables
    global first_call
    global num_batch

    header = [c["name"] for c in msg.attributes['table']['columns']]
    df_tables = pd.DataFrame(msg.body,columns=header)

    att = {'num_tables':df_tables.shape[0],'data_outcome':False,'num_batch':num_batch,'table_repository': msg.attributes['table']['name']}
    process(api.Message(attributes=att,body=df_tables))

def process(msg) :

    global df_tables
    global pointer
    global no_changes_counter
    global num_roundtrips
    global first_call
    global num_batch

    att = dict(msg.attributes)

    att['operator'] = 'repl_dispatch_tables'

    # case no repl tables provided
    if df_tables.empty :
        api.logger.warning('No replication tables yet provided!')
        api.send(outports[0]['name'], log_stream.getvalue())
        return 0

    if att['data_outcome'] == True :
        api.logger.debug('Reset \"number of changes\"-counter')
        no_changes_counter =  0
    elif first_call :
        first_call = False;
    else: # only when changes and not first_call in loop
        no_changes_counter += 1
        api.logger.debug('Changes counter: {}'.format(no_changes_counter))

    # end pipeline if there were no changes in all tables
    if no_changes_counter >=  df_tables.shape[0] :
        api.logger.info('Number of roundtrips without changes: {} - ending loop'.format(no_changes_counter))
        api.send(outports[0]['name'], log_stream.getvalue())
        msg = api.Message(attributes=att, body=no_changes_counter)
        api.send(outports[2]['name'], msg)
        return 0

    repl_table = df_tables.iloc[pointer]

    att['replication_table'] = repl_table['TABLE_NAME']
    if 'CHECKSUM_COL' in repl_table :
        att['checksum_col'] = repl_table['CHECKSUM_COL']
    if 'SLICE_PERIOD' in repl_table :
        att['slice_period'] = repl_table['SLICE_PERIOD']

    # split table from schema
    if '.' in repl_table['TABLE_NAME']  :
        att['table_name'] = repl_table['TABLE_NAME'].split('.')[1]
        att['schema_name'] = repl_table['TABLE_NAME'].split('.')[0]
    else :
        att['table_name'] = repl_table['TABLE_NAME']
    table_msg = api.Message(attributes= att, body = repl_table)
    api.send(outports[1]['name'], table_msg)

    api.logger.info('Dispatch table: {}'.format(att['replication_table']))
    api.send(outports[0]['name'], log_stream.getvalue())

    pointer = (pointer + 1) % df_tables.shape[0]



inports = [{'name': 'tables', 'type': 'message.table',"description":"List of tables"},
           {'name': 'trigger', 'type': 'message.*',"description":"Trigger"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'trigger', 'type': 'message',"description":"trigger"},
            {'name': 'limit', 'type': 'message',"description":"limit"}]


#api.set_port_callback(inports[1]['name'], process)
#api.set_port_callback(inports[0]['name'], set_replication_tables)

def test_operator() :

    api.config.debug_mode = True
    api.config.round_trips_to_stop = 1
    api.config.parallelization = 1

    att = dict()
    att['table'] = {"columns": [{"class": "string", "name": "TABLE_NAME", "nullable": True, "size": 50,"type": {"hana": "NVARCHAR"}}, \
                                {"class": "string", "name": "CHECKSUM_COL", "nullable": True, "size": 50,"type": {"hana": "NVARCHAR"}}, \
                                {"class": "timestamp", "name": "LATEST_CONSISTENCY_CHECK", "nullable": True, "type": {"hana": "TIMESTAMP"}} ,
                                {"class": "integer", "name": "CONSISTENCY_CODE", "nullable": True,"type": {"hana": "INTEGER"}}], \
                   "version": 1,"name":"repl_table"}

    data = [["REPLICATION.TABLE_1","INDEX",'2020-08-01',0],\
            ["REPLICATION.TABLE_2","INDEX",'2020-08-01',0],\
            ["REPLICATION.TABLE_3","INDEX",'2020-08-01',0],\
            ["REPLICATION.TABLE_4","INDEX",'2020-08-01',0]]

    #data = [{'TABLE':'repl_TABLE1', 'LATENCY':0},{'TABLE':'repl_TABLE2', 'LATENCY':0},{'TABLE':'repl_TABLE3', 'LATENCY':0},
    #        {'TABLE':'repl_TABLE4', 'LATENCY':0},{'TABLE':'repl_TABLE5', 'LATENCY':0},{'TABLE':'repl_TABLE6', 'LATENCY':0}]

    msg = api.Message(attributes=att, body=data)
    set_replication_tables(msg)

    trigger = api.Message(attributes={'table':'test','data_outcome':False}, body='go')
    for i in range(0,20) :
        process(trigger)

if __name__ == '__main__':
    test_operator()
    if True:
        basename = os.path.basename(__file__[:-3])
        package_name = os.path.basename(os.path.dirname(os.path.dirname(__file__)))
        project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        solution_name = '{}_{}.zip'.format(basename,api.config.version)
        package_name_ver = '{}_{}'.format(package_name,api.config.version)

        solution_dir = os.path.join(project_dir,'solution/operators',package_name_ver)
        solution_file = os.path.join(project_dir,'solution/operators',solution_name)

        # rm solution directory
        subprocess.run(["rm", '-r',solution_dir])

        # create solution directory with generated operator files
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)

        # Bundle solution directory with generated operator files
        subprocess.run(["vctl", "solution", "bundle", solution_dir, "-t",solution_file])


