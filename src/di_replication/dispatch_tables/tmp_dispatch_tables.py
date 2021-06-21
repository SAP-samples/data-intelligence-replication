#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#


import copy
from datetime import datetime

operator_name = 'dispatch_tables'

def log(log_str,level='info') :
    if level == 'debug' :
        api.logger.debug(log_str)
    elif level == 'warning':
        api.logger.warning(log_str)
    elif level == 'error':
        api.logger.error(log_str)
    else :
        api.logger.info(log_str)

    now = datetime.now().strftime('%H:%M:%S')
    api.send('log','{} | {} | {} | {}'.format(now,level,operator_name,log_str))



tables = dict()
pointer = -1
num_roundtrips = 0
last_data_outcome = 0
first_round = True


## Reading the table Repository
def on_tables(msg):
    global tables

    header = [c["name"] for c in msg.attributes['table']['columns']]
    tables = [{header[i]: v for i, v in enumerate(row)} for row in msg.body]

    # split the table name with schema into table and schema
    tables = [dict(t, **{'table_name': t['TABLE_NAME'].split('.')[1], 'schema_name': t['TABLE_NAME'].split('.')[0]}) for
              t in tables]

    att = {'num_tables': len(tables),'table_repository': msg.attributes['table']['name']}
    return on_input(api.Message(attributes=att, body=tables))


##
def on_nodata(msg):
    global tables
    global pointer

    msg.body = 'NODATA'

    if api.config.mode == 'R':
        # CONDITION: If there is no data processed than delete from table
        removed_table = tables[pointer]['table_name']
        del tables[pointer]
        pointer = pointer - 1 if pointer > 0 else len(tables) -1

        if len(tables) == 0: # No tables in tables-list left
            log('Last Table removed from table-list: {}'.format(removed_table))
            msg = api.Message(attributes=msg.attributes, body='Number of roundtrips: {}'.format(num_roundtrips))
            api.send(outports[2]['name'], msg)
            return 0

        if pointer == len(tables) :
            pointer = 0
        log('Table removed from table-list: {} '.format(removed_table, tables[pointer]['table_name']))

    on_input(msg)


## MAIN
def on_input(msg):
    global pointer
    global num_roundtrips
    global last_data_outcome
    global first_round

    att = copy.deepcopy(msg.attributes)

    # case no repl tables provided
    if len(tables) == 0:
        log('No replication tables yet provided!',level='warning')
        msg = api.Message(attributes=att, body='Number of roundtrips: {}'.format(num_roundtrips))
        api.send('limit', msg)
        return 0

    # in case input ports body isn not 'NODATA' (None is not reliable) or ERROR
    if not msg.body == 'NODATA':
        last_data_outcome = num_roundtrips

    # end pipeline if there were no changes in all tables
    if (num_roundtrips - last_data_outcome) >= len(tables) and api.config.mode == 'C':
        log('No changes after roundtrips: {}'.format(num_roundtrips))
        msg = api.Message(attributes=att, body='Number of roundtrips: {}'.format(num_roundtrips))
        api.send('limit', msg)
        return 0

    # Get next table from table list
    pointer = (pointer + 1) % len(tables)
    if pointer == 0 and api.config.mode == 'F' and not first_round:
        log('Input has been processed once: {}/{}'.format(len(tables), num_roundtrips))
        msg = api.Message(attributes=att, body=num_roundtrips)
        api.send('limit', msg)
        return 0

    first_round = False
    repl_table = tables[pointer]
    att['table_name'] = repl_table['table_name']
    att['schema_name'] = repl_table['schema_name']

    # Send data to outport
    api.send('output', api.Message(attributes=att, body=att['table_name']))

    # Send logging to 'log'- outport
    log('Dispatch {}  roundtrip {}   last_action  {}'.format(att['table_name'], \
                                                                         num_roundtrips, \
                                                                         last_data_outcome))

    # Move pointer to next table in table list
    num_roundtrips += 1


api.set_port_callback('tables', on_tables)
api.set_port_callback('input', on_input)
api.set_port_callback('nodata', on_nodata)


