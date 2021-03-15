
import os
import logging
import io

# catching logger messages for separate output
log_stream = io.StringIO()
sh = logging.StreamHandler(stream=log_stream)
sh.setFormatter(logging.Formatter('%(asctime)s |  %(levelname)s | %(name)s | %(message)s', datefmt='%H:%M:%S'))
api.logger.addHandler(sh)

def process(msg):

    att = dict(msg.attributes)
    operator_name = 'repl_max_index_test_tables'

    table_name = att['schema_name'] + '.' + att['table_name']
    sql = 'SELECT MAX("INDEX") FROM {table}'.format(table = table_name)

    api.logger.info('SQL statement: {}'.format(sql))
    att['sql'] = sql

    api.send(outports[1]['name'], api.Message(attributes=att,body=sql))
    api.send(outports[0]['name'], log_stream.getvalue())


inports = [{'name': 'data', 'type': 'message', "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'sql', 'type': 'message', "description": "msg with sql"}]

api.set_port_callback(inports[0]['name'], process)

