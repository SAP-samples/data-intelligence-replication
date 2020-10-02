import io

import os
import time



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
            tags = {'sdi_utils':''}
            version = "0.1.0"

            operator_description = "Repl. Table Dispatcher"
            operator_name = 'repl_table_dispatcher'
            operator_description_long = "Send next table to replication process."
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            periodicity = 0
            config_params['periodicity'] = {'title': 'Periodicity (s)',
                                       'description': 'Periodicity (s).',
                                       'type': 'integer'}

            parallelization = 0.1
            config_params['parallelization'] = {'title': 'Fraction of tables to parallelize',
                                       'description': 'Periodicity (s).',
                                       'type': 'number'}

            round_trips_to_stop = 10000000
            config_params['round_trips_to_stop'] = {'title': 'Roundtips to stop',
                                       'description': 'Fraction of tables to parallelize.',
                                       'type': 'integer'}

            count_all_roundtrips = False
            config_params['count_all_roundtrips'] = {'title': 'Count All Roundtrips',
                                           'description': 'Count all roundtrips irrespective to changes.',
                                           'type': 'boolean'}

            change_type = 'I'
            config_params['change_type'] = {'title': 'Insert (\'I\') or update (\'U\')' ,
                                       'description': 'Insert (\'I\') or update (\'U\')',
                                       'type': 'string'}


repl_tables = list()
pointer = 0
no_changes_counter = 0
num_roundtrips = 0
num_batch = 1
first_call = True

def set_replication_tables(msg) :
    global repl_tables
    global first_call
    global num_batch

    # header = [c["name"] for c in msg.attributes['table']['columns']]
    repl_tables = [{"TABLE": r[0], "LATENCY": r[1]} for r in msg.body]

    num_batch = int(api.config.parallelization * len(repl_tables))
    if num_batch == 0 :
        num_batch = 1

    att = {'num_tables':len(repl_tables),'data_outcome':True,'num_batch':num_batch,'table_repository': msg.attributes['table']['name']}
    process(api.Message(attributes=att,body=repl_tables))

def process(msg) :

    global repl_tables
    global pointer
    global no_changes_counter
    global num_roundtrips
    global first_call
    global num_batch

    att = dict(msg.attributes)

    att['operator'] = 'repl_table_dispatcher'
    if not 'data_outcome' in msg.attributes :
        att['data_outcome'] = True

    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)
    #logger.debug('Attributes: {} - {}'.format(str(msg.attributes),str(att)))

    # case no repl tables provided
    if len(repl_tables) == 0 :
        logger.warning('No replication tables yet provided!')
        api.send(outports[0]['name'], log_stream.getvalue())
        return 0

    replication_type = api.config.change_type
    if replication_type == 'I' :
        att['append_mode'] = True
        logger.info('Replication type: Insert')
    elif replication_type == 'U' :
        att['append_mode'] = False
        logger.info('Replication type: Update (not append)')
    elif replication_type == 'D' :
        att['append_mode'] = False
        logger.info('Replication type: Delete (not append)')
    else :
        err_stat = 'Unknown Replication type: {} (Valid: I,U,D)'.format(replication_type)
        logger.error(err_stat)
        api.send(outports[0]['name'], log_stream.getvalue())
        raise ValueError(err_stat)

    if api.config.count_all_roundtrips == False and att['data_outcome'] == True:
        # reset counter
        no_changes_counter =  0

    # end pipeline if there were no changes in all tables AND happened more than round_trips_to_stop
    if no_changes_counter >= api.config.round_trips_to_stop * len(repl_tables) :
        logger.info('Number of roundtrips without changes: {}'.format(no_changes_counter))
        logger.info('Parallelization: {}'.format(att['parallelization']))
        api.send(outports[0]['name'], log_stream.getvalue())
        msg = api.Message(attributes=att, body=no_changes_counter)
        api.send(outports[2]['name'], msg)
        return 0

    # goes idle if no changes has happened
    if pointer == 0 and not first_call:
        if num_roundtrips > 1:
            logger.info('******** {} **********'.format(num_roundtrips))
            logger.info(
                'Roundtrip completed: {} tables - {} unchanged roundtrips'.format(len(repl_tables), no_changes_counter))
            if no_changes_counter >= len(repl_tables) :
                logger.info('Goes idle due to no changes: {} s'.format(api.config.periodicity))
                time.sleep(api.config.periodicity)
        num_roundtrips += 1

    # parallelization
    #n Only needed when started. Then a new replication process is started whenever a table replication has been finished.
    if first_call  == True:
        logger.debug('Parallelize: {} -> {}'.format(api.config.parallelization, num_batch))
        first_call = False
    else :
        num_batch = 1

    # Sends output message within a loop but less than number of replicated tables
    # but only on first call.
    # Later the loop back message comes and triggers 1 output message
    for b in range(0, num_batch) :

        # table dispatch carousel
        repl_table = repl_tables[pointer]

        logger.debug('Table: {} - {}({})/{}'.format(repl_table['TABLE'], pointer, b, num_batch))

        att_table = dict(att)

        att_table['latency'] = repl_table['LATENCY']
        att_table['replication_table'] = repl_table['TABLE']
        # split table from schema
        if '.' in repl_table['TABLE']  :
            att_table['base_table'] = repl_table['TABLE'].split('.')[1]
            att_table['table_name'] = repl_table['TABLE'].split('.')[1]
            att_table['schema'] = repl_table['TABLE'].split('.')[0]
            att_table['schema_name'] = repl_table['TABLE'].split('.')[0]
        else :
            att_table['base_table'] = repl_table['TABLE']
            att_table['table_name'] = repl_table['TABLE']
        table_msg = api.Message(attributes= att_table, \
            body = {'TABLE':att_table['replication_table'],'LATENCY':att_table['latency']})
        api.send(outports[1]['name'], table_msg)

        logger.info('Dispatch table: {}  ({}/{})'.format(att_table['replication_table'], \
                no_changes_counter, api.config.round_trips_to_stop * len(repl_tables)))
        api.send(outports[0]['name'], log_stream.getvalue())
        log_stream.seek(0)
        log_stream.truncate()

        if api.config.count_all_roundtrips == True:
            no_changes_counter += 1

        pointer = (pointer + 1) % len(repl_tables)



inports = [{'name': 'tables', 'type': 'message.table',"description":"List of tables"},
           {'name': 'trigger', 'type': 'message',"description":"Trigger"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'trigger', 'type': 'message',"description":"trigger"},
            {'name': 'limit', 'type': 'message',"description":"limit"}]


#api.set_port_callback(inports[1]['name'], process)
#api.set_port_callback(inports[0]['name'], set_replication_tables)

def test_operator() :

    api.config.debug_mode = True
    api.config.round_trips_to_stop = 1
    api.config.parallelization = 1

    att = {"table":{"columns":[{"class":"string","name":"TABLE","nullable":False,"size":100,"type":{"hana":"NVARCHAR"}},\
                               {"class":"integer","name":"LATENCY","nullable":True,"type":{"hana":"INTEGER"}}],\
                    "name":"repository","version":1}}

    data = [["REPLICATION.DOC_METADATA_REPL",0],["REPLICATION.TEXT_WORDS_REPL",2],["REPLICATION.WORD_INDEX_REPL",1],["REPLICATION.WORD_SENTIMENT_REPL",5]]

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



