import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

import subprocess
import logging
import os
import random
from datetime import datetime, timezone
import pandas as pd

try:
    api
except NameError:
    class api:

        queue = list()

        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if port == outports[1]['name']:
                api.queue.append(msg)

        class config:
            ## Meta data
            config_params = dict()
            version = '0.0.1'
            tags = {'sdi_utils': ''}
            operator_name = 'repl_get_check_profile'
            operator_description = "Get Check Profile of Table"

            operator_description_long = "Get check profile of table."
            add_readme = dict()
            add_readme["References"] = ""

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}




def process(msg):
    att = dict(msg.attributes)
    att['operator'] = 'repl_get_check_profile'

    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)
    logger.info("Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()

    if msg.body == None:
        logger.warning('No checksum column found: {} (Solution: file not in table repository)'.format(att))
        api.send(outports[0]['name'], log_stream.getvalue())
        att['checksum_col'] = ''
    else:
        att['checksum_col'] = msg.body[0][0]

    table = att['schema_name'] + '.' + att['table_name']

    if len(att['checksum_col']) > 0:
        select_sql = 'SELECT COUNT(*) AS NUM_ROWS, SUM(\"{checksumcol}\") AS CHECKSUM, MAX(\"DIREPL_UPDATED\") AS LATEST  FROM {table}'. \
            format(checksumcol=att['checksum_col'], table=table)
    else:
        select_sql = 'SELECT COUNT(*) AS NUM_ROWS, \'0\' AS CHECKSUM, MAX(\"DIREPL_UPDATED\") AS LATEST  FROM {table}'. \
            format(table=table)

    logger.info('Select statement: {}'.format(select_sql))
    att['select_sql'] = select_sql

    logger.debug('Process ended: {}'.format(time_monitor.elapsed_time()))

    # api.send(outports[1]['name'], update_sql)
    api.send(outports[1]['name'], api.Message(attributes=att, body=select_sql))

    log = log_stream.getvalue()
    if len(log) > 0:
        api.send(outports[0]['name'], log)


inports = [{'name': 'data', 'type': 'message.table', "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'msg', 'type': 'message', "description": "msg with sql statement"}]

#api.set_port_callback(inports[0]['name'], process)

def test_operator():
    api.config.use_package_id = False
    api.config.package_size = 1

    msg = api.Message(attributes={'packageid':4711,'table_name':'repl_table','base_table':'repl_table','latency':30,\
                                  'append_mode' : 'I', 'data_outcome':True, 'schema_name':'REPLICATION'},body=[['INDEX']])
    process(msg)

    for msg in api.queue :
        print(msg.attributes)
        print(msg.body)


if __name__ == '__main__':
    test_operator()
    if True:
        subprocess.run(["rm", '-r','../../../solution/operators/sdi_replication_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name + '_' + api.config.version
        subprocess.run(["vctl", "solution", "bundle",'../../../solution/operators/sdi_replication_' + api.config.version, \
                        "-t", solution_name])
        subprocess.run(["mv", solution_name + '.zip', '../../../solution/operators'])

