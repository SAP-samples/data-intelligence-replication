

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
                logging.info(msg.body.iloc[0])
            elif port == outports[2]['name'] :
                logging.info('Limit reached - Exit')
                #exit(0)
        class config:
            ## Meta data
            config_params = dict()
            tags = {}
            version = "0.1.0"

            operator_description = "Dispatch Tables"
            operator_name = 'dispatch_tables'
            operator_description_long = "Send next table to process."
            add_readme = dict()

            mode = 'F'
            config_params['mode'] = {'title': 'Mode: (F)or-Loop, (C)onditional or (I)ndefinite',
                                           'description': 'Mode: (\'F\'or, \'C\'onditional or \'I\'indefinite.',
                                           'type': 'string'}

        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
        logger = logging.getLogger(name=config.operator_name)


# catching logger messages for separate output
log_stream = io.StringIO()
sh = logging.StreamHandler(stream=log_stream)
sh.setFormatter(logging.Formatter('%(asctime)s |  %(levelname)s | %(name)s | %(message)s', datefmt='%H:%M:%S'))
api.logger.addHandler(sh)


df_tables = pd.DataFrame()
pointer = 0
num_roundtrips = 0
last_data_outcome = 0

def set_replication_tables(msg) :
    global df_tables

    header = [c["name"] for c in msg.attributes['table']['columns']]
    df_tables = pd.DataFrame(msg.body,columns=header)
    att = {'num_tables':df_tables.shape[0],
           'table_repository': msg.attributes['table']['name'],
           'data_outcome': True}
    process(api.Message(attributes=att,body=df_tables))


def process(msg) :

    global pointer
    global num_roundtrips
    global last_data_outcome

    att = dict(msg.attributes)

    att['operator'] = 'dispatch_tables'

    # case no repl tables provided
    if df_tables.empty :
        api.logger.warning('No replication tables yet provided!')
        api.send(outports[0]['name'], log_stream.getvalue())
        return 0

    # end pipeline if there were no changes in all tables
    if  (num_roundtrips - last_data_outcome) > df_tables.shape[0] and  api.config.mode == 'C' :
        api.logger.info('No changes after roundtrips: {}'.format(num_roundtrips))
        api.send(outports[0]['name'], log_stream.getvalue())
        msg = api.Message(attributes=att, body=num_roundtrips)
        api.send(outports[2]['name'], msg)
        return 0

    if att['data_outcome'] == True :
        last_data_outcome = num_roundtrips
        att['data_outcome'] = False

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

    api.logger.info('Dispatch table: {}  step: {} last_action: {}'.format(att['replication_table'],num_roundtrips,last_data_outcome))
    api.send(outports[0]['name'], log_stream.getvalue())

    pointer = (pointer + 1) % df_tables.shape[0]
    if pointer == 0 and api.config.mode == 'F':
        api.logger.info('Input has been processed once: {}/{}'.format(df_tables.shape[0],num_roundtrips))
        api.send(outports[0]['name'], log_stream.getvalue())
        msg = api.Message(attributes=att, body=num_roundtrips)
        api.send(outports[2]['name'], msg)
        return 0
    num_roundtrips += 1



inports = [{'name': 'tables', 'type': 'message.table',"description":"List of tables"},
           {'name': 'trigger', 'type': 'message.*',"description":"Trigger"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'trigger', 'type': 'message',"description":"trigger"},
            {'name': 'limit', 'type': 'message',"description":"limit"}]


#api.set_port_callback(inports[1]['name'], process)
#api.set_port_callback(inports[0]['name'], set_replication_tables)

def test_operator() :

    api.config.stop_no_changes = True

    att = dict()
    att['table'] = {"columns": [{"class": "string", "name": "TABLE_NAME", "nullable": True, "size": 50,"type": {"hana": "NVARCHAR"}}, \
                                {"class": "string", "name": "SLICE_PERIOD", "nullable": True, "size": 2,"type": {"hana": "NVARCHAR"}}], \
                   "version": 1,"name":"repl_table"}

    tables = [["REPLICATION.TABLE_1",'H1'],\
            ["REPLICATION.TABLE_2",'H1'],\
            ["REPLICATION.TABLE_3",'H1'],\
            ["REPLICATION.TABLE_4",'H1']]

    msg_tables = api.Message(attributes=att,body=tables)
    set_replication_tables(msg_tables)

    body = 'go'
    att = {'table':'test','data_outcome':False}
    trigger = api.Message(attributes=att, body='go')
    for i in range(0,20) :
        att['data_outcome'] = True if i == 3 else False
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


