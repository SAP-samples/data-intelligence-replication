import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

import subprocess
import logging
import os
import random
from datetime import datetime, timezone, timedelta
import pandas as pd
import numpy as np

pd.set_option('mode.chained_assignment',None)

try:
    api
except NameError:
    class api:

        sql_queue = list()
        csv_queue = list()

        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if port == outports[1]['name']:
                api.sql_queue.append(msg)


        class config:
            ## Meta data
            config_params = dict()
            version = '0.0.1'
            tags = {'sdi_utils': ''}
            operator_name = 'repl_create_test_tables'
            operator_description = "Create Test Tables"

            operator_description_long = "Create Test Tables."
            add_readme = dict()
            add_readme["References"] = ""

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            num_tables = 10
            config_params['num_tables'] = {'title': 'Number of tables',
                                           'description': 'Number of tables.',
                                           'type': 'integer'}

            base_table_name = 'REPLICATION.TEST_TABLE'
            config_params['base_table_name'] = {'title': 'Base Table Name',
                                           'description': 'Base Table Name.',
                                           'type': 'string'}


def process(msg):

    operator_name = 'repl_create_test_tables'
    logger, log_stream = slog.set_logging(operator_name, loglevel=api.config.debug_mode)

    logger.info("Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()

    for i in range (0,api.config.num_tables) :

        table_name = api.config.base_table_name + '_' + str(i)
        lastbatch = False if not i == api.config.num_tables - 1 else True

        ### DROP

        att_drop = {'table':{'name':table_name},'message.batchIndex':i,'message.lastBatch':lastbatch,'sql':'DROP'}
        logger.info("Drop table:")
        drop_sql = "DROP TABLE {table}".format(table = table_name)
        api.send(outports[1]['name'], api.Message(attributes=att_drop, body=drop_sql))
        api.send(outports[0]['name'], log_stream.getvalue())
        log_stream.seek(0)
        log_stream.truncate()

        ### CREATE

        logger.info('Create Table: ')

        create_sql = "CREATE COLUMN TABLE {table} (\"INDEX\" BIGINT , \"NUMBER\" BIGINT,  \"DATE\" DATE,"\
                     "\"DIREPL_PACKAGEID\" BIGINT, \"DIREPL_PID\" BIGINT , \"DIREPL_UPDATED\" LONGDATE, " \
                     "\"DIREPL_STATUS\" NVARCHAR(1), \"DIREPL_TYPE\" NVARCHAR(1), " \
                     "PRIMARY KEY (\"INDEX\"));".format(table = table_name )

        att_create = {"table_name":table_name,'message.batchIndex':i,'message.lastBatch':lastbatch,'sql':create_sql}
        api.send(outports[1]['name'], api.Message(attributes=att_create, body=create_sql))
        api.send(outports[0]['name'], log_stream.getvalue())
        log_stream.seek(0)
        log_stream.truncate()

    logger.debug('Process ended: {}'.format(time_monitor.elapsed_time()))
    api.send(outports[0]['name'], log_stream.getvalue())


inports = [{'name': 'data', 'type': 'message.table', "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'sql', 'type': 'message', "description": "msg with sql"}]

#api.set_port_callback(inports[0]['name'], process)

def test_operator():
    api.config.off_set = 2
    api.config.num_rows = 10
    msg = api.Message(attributes={'packageid':4711,'replication_table':'repl_table'},body='')
    process(msg)


    for st in api.sql_queue :
        print(st.attributes)
        print(st.body)

    for st in api.csv_queue :
        print(st.body)


if __name__ == '__main__':
    test_operator()
    if True:
        print(os.getcwd())
        subprocess.run(["rm", '-r','../../../solution/operators/sdi_replication_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name + '_' + api.config.version
        subprocess.run(["vctl", "solution", "bundle",'../../../solution/operators/sdi_replication_' + api.config.version, \
                        "-t", solution_name])
        subprocess.run(["mv", solution_name + '.zip', '../../../solution/operators'])

