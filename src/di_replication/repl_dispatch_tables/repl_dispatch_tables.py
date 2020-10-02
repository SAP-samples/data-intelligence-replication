import io

import os
import time
import pandas as pd



import subprocess

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

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
            tags = {'sdi_utils':'', 'pandas':''}
            version = "0.1.0"

            operator_description = "Dispatch Tables"
            operator_name = 'repl_dispatch_tables'
            operator_description_long = "Send next table to process."
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            periodicity = 0
            config_params['periodicity'] = {'title': 'Periodicity (s)',
                                       'description': 'Periodicity (s).',
                                       'type': 'integer'}

            round_trips_to_stop = 10000000
            config_params['round_trips_to_stop'] = {'title': 'Roundtips to stop',
                                       'description': 'Fraction of tables to parallelize.',
                                       'type': 'integer'}

            count_all_roundtrips = False
            config_params['count_all_roundtrips'] = {'title': 'Count All Roundtrips',
                                           'description': 'Count all roundtrips irrespective to changes.',
                                           'type': 'boolean'}




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

    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)

    # case no repl tables provided
    if df_tables.empty :
        logger.warning('No replication tables yet provided!')
        api.send(outports[0]['name'], log_stream.getvalue())
        return 0


    if api.config.count_all_roundtrips == False and att['data_outcome'] == True:
        logger.debug('Reset \"number of changes\"-counter')
        no_changes_counter =  0

    # end pipeline if there were no changes in all tables AND happened more than round_trips_to_stop
    if no_changes_counter >= api.config.round_trips_to_stop * df_tables.shape[0] :
        logger.info('Number of roundtrips without changes: {} - ending loop'.format(no_changes_counter))
        api.send(outports[0]['name'], log_stream.getvalue())
        msg = api.Message(attributes=att, body=no_changes_counter)
        api.send(outports[2]['name'], msg)
        return 0

    # goes idle if no changes has happened
    if pointer == 0 and not first_call:
        if num_roundtrips > 1:
            logger.info('******** {} **********'.format(num_roundtrips))
            logger.info(
                'Roundtrip completed: {} tables - {} unchanged roundtrips'.format(df_tables.shape[0], no_changes_counter))
            if no_changes_counter >= df_tables.shape[0] :
                logger.info('Goes idle due to no changes: {} s'.format(api.config.periodicity))
                time.sleep(api.config.periodicity)
        num_roundtrips += 1


    repl_table = df_tables.iloc[pointer]

    att['replication_table'] = repl_table['TABLE']
    att['checksum_col'] = repl_table['CHECKSUM_COL']
    # split table from schema
    if '.' in repl_table['TABLE']  :
        att['table_name'] = repl_table['TABLE'].split('.')[1]
        att['schema_name'] = repl_table['TABLE'].split('.')[0]
    else :
        att['table_name'] = repl_table['TABLE']
    table_msg = api.Message(attributes= att, body = repl_table)
    api.send(outports[1]['name'], table_msg)

    logger.info('Dispatch table: {}  ({}/{})'.format(att['replication_table'], \
            no_changes_counter, api.config.round_trips_to_stop * df_tables.shape[0]))
    api.send(outports[0]['name'], log_stream.getvalue())

    # counter is always incremented when all roundtrips are counted
    if api.config.count_all_roundtrips == True or att['data_outcome'] == False:
        no_changes_counter += 1

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
    att['table'] = {"columns": [{"class": "string", "name": "TABLE", "nullable": True, "size": 50,"type": {"hana": "NVARCHAR"}}, \
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
        subprocess.run(["rm", '-r','../../../solution/operators/sdi_replication_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name + '_' + api.config.version
        subprocess.run(["vctl", "solution", "bundle",'../../../solution/operators/sdi_replication_' + api.config.version, \
                        "-t", solution_name])
        subprocess.run(["mv", solution_name + '.zip', '../../../solution/operators'])



