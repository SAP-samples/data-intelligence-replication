

import os
import io
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
                # logging.info(msg.body)
                pass
            elif port == outports[2]['name'] :
                raise StopIteration

        class config:
            ## Meta data
            config_params = dict()
            tags = {}
            version = "0.1.0"

            operator_description = "Dispatch Tables"
            operator_name = 'dispatch_tables'
            operator_description_long = "Runs iteratively over all tables in table-list. There are 4 modes: \n\n"\
                                        "* F-or: loops once over all tables\n"\
                                        "* C-onditional: stops when for all tables in one iteration there was no processing\n" \
                                        "* R-move: removes table from table list once there was no processing until there is no table left\n" \
                                        "* I-indefinite: never stops\n"

            add_readme = dict()

            mode = 'C'
            config_params['mode'] = {'title': 'Mode: (F)or-Loop, (C)onditional, (R)emove or (I)ndefinite',
                                           'description': 'Mode: (\'F\'or, \'C\'onditional (\'R\')emove or \'I\'indefinite.',
                                           'type': 'string'}

        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
        logger = logging.getLogger(name=config.operator_name)


# catching logger messages for separate output
log_stream = io.StringIO()
sh = logging.StreamHandler(stream=log_stream)
sh.setFormatter(logging.Formatter('%(asctime)s :  %(levelname)s : %(name)s : %(message)s', datefmt='%H:%M:%S'))
api.logger.addHandler(sh)

tables = dict()
pointer = -1
num_roundtrips = 0
last_data_outcome = 0
first_round = True


## Reading the table Repository
def set_replication_tables(msg):
    global tables

    header = [c["name"] for c in msg.attributes['table']['columns']]
    tables = [{header[i]: v for i, v in enumerate(row)} for row in msg.body]

    # split the table name with schema into table and schema
    tables = [dict(t, **{'table_name': t['TABLE_NAME'].split('.')[1], 'schema_name': t['TABLE_NAME'].split('.')[0]}) for
              t in tables]

    att = {'num_tables': len(tables),'table_repository': msg.attributes['table']['name']}
    return process(api.Message(attributes=att, body=tables))


##
def nodata_process(msg):
    global tables
    global pointer

    msg.body = 'NODATA'

    if api.config.mode == 'R':
        # CONDITION: If there is no data processed than delete from table
        removed_table = tables[pointer]['table_name']
        del tables[pointer]
        pointer = pointer - 1 if pointer > 0 else len(tables) -1

        if len(tables) == 0: # No tables in tables-list left
            api.logger.info('Last Table removed from table-list: {}'.format(removed_table))
            api.send(outports[0]['name'], log_stream.getvalue())
            log_stream.seek(0)
            log_stream.truncate()
            msg = api.Message(attributes=msg.attributes, body='Number of roundtrips: {}'.format(num_roundtrips))
            api.send(outports[2]['name'], msg)
            return 0

        if pointer == len(tables) :
            pointer = 0
        api.logger.info('Table removed from table-list: {} '.format(removed_table, tables[pointer]['table_name']))

    process(msg)


## MAIN
def process(msg):
    global pointer
    global num_roundtrips
    global last_data_outcome
    global first_round

    att = dict(msg.attributes)

    att['operator'] = 'dispatch_tables'

    # case no repl tables provided
    if len(tables) == 0:
        api.logger.warning('No replication tables yet provided!')
        api.send(outports[0]['name'], log_stream.getvalue())
        log_stream.seek(0)
        msg = api.Message(attributes=att, body='Number of roundtrips: {}'.format(num_roundtrips))
        api.send(outports[2]['name'], msg)
        return 0

    # in case trigger ports body isn not 'NODATA' (None is not reliable) or ERROR
    if not msg.body == 'NODATA':
        last_data_outcome = num_roundtrips

    # end pipeline if there were no changes in all tables
    if (num_roundtrips - last_data_outcome) >= len(tables) and api.config.mode == 'C':
        api.logger.info('No changes after roundtrips: {}'.format(num_roundtrips))
        api.send(outports[0]['name'], log_stream.getvalue())
        log_stream.seek(0)
        msg = api.Message(attributes=att, body='Number of roundtrips: {}'.format(num_roundtrips))
        api.send(outports[2]['name'], msg)
        return 0

    # Get next table from table list
    pointer = (pointer + 1) % len(tables)
    if pointer == 0 and api.config.mode == 'F' and not first_round:
        api.logger.info('Input has been processed once: {}/{}'.format(len(tables), num_roundtrips))
        api.send(outports[0]['name'], log_stream.getvalue())
        log_stream.seek(0)
        msg = api.Message(attributes=att, body=num_roundtrips)
        api.send(outports[2]['name'], msg)
        return 0
    first_round = False
    repl_table = tables[pointer]
    att['table_name'] = repl_table['table_name']
    att['schema_name'] = repl_table['schema_name']

    # Send data to outport
    api.send(outports[1]['name'], api.Message(attributes=att, body=att['table_name']))

    # Send logging to 'log'- outport
    api.logger.info('Dispatch {}  roundtrip {}   last_action  {}'.format(att['table_name'], \
                                                                         num_roundtrips, \
                                                                         last_data_outcome))
    api.send(outports[0]['name'], log_stream.getvalue())
    log_stream.seek(0)
    log_stream.truncate()

    # Move pointer to next table in table list


    num_roundtrips += 1



inports = [{'name': 'tables', 'type': 'message.table', "description": "List of tables"},
           {'name': 'data', 'type': 'message.*', "description": "Trigger"},
           {'name': 'nodata', 'type': 'message', "description": "Trigger"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'trigger', 'type': 'message', "description": "trigger"},
            {'name': 'limit', 'type': 'message', "description": "limit"}]

#api.set_port_callback(inports[0]['name'], set_replication_tables)
#api.set_port_callback(inports[1]['name'], process)
#api.set_port_callback(inports[2]['name'], nodata_process)


#api.set_port_callback(inports[1]['name'], process)
#api.set_port_callback(inports[0]['name'], set_replication_tables)

def test_operator() :

    api.config.mode = 'R'

    print(api.config.operator_description_long)

    att = dict()
    att['table'] = {"columns": [{"class": "string", "name": "TABLE_NAME", "nullable": True, "size": 50,"type": {"hana": "NVARCHAR"}}], \
                   "version": 1,"name":"repl_table"}
    tables = [["REPLICATION.TABLE_1"],["REPLICATION.TABLE_2"],["REPLICATION.TABLE_3"],["REPLICATION.TABLE_4"]]
    msg_tables = api.Message(attributes=att,body=tables)
    set_replication_tables(msg_tables)  # TABLE_1

    att = {'table_name': 'test', 'data_outcome': True}

    try :
        process(api.Message(attributes=att, body='body')) # TABLE_2
        process(api.Message(attributes=att, body='body')) # TABLE_3
        process(api.Message(attributes=att, body='body')) # TABLE_4
        process(api.Message(attributes=att, body='body')) # TABLE_1
        process(api.Message(attributes=att, body='body')) # TABLE_2
        nodata_process(api.Message(attributes=att, body='NODATA')) # remove TABLE_2, TABLE_3
        nodata_process(api.Message(attributes=att, body='NODATA')) # remove TABLE_3, TABLE_4
        process(api.Message(attributes=att, body='body')) # TABLE_1
        process(api.Message(attributes=att, body='body')) # TABLE_4
        nodata_process(api.Message(attributes=att, body='NODATA')) # remove TABLE_4, TABLE_1
        process(api.Message(attributes=att, body='body')) # TABLE_1
        nodata_process(api.Message(attributes=att, body='NODATA')) # remove TABLE_1 exit
        process(api.Message(attributes=att, body='body'))
        process(api.Message(attributes=att, body='body'))
        process(api.Message(attributes=att, body='body'))
    except StopIteration :
        logging.info('Limit reached - Exit')


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


