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
            operator_name = 'repl_table_profile'
            operator_description = "Table Profile"

            operator_description_long = "Save table profile to table repository."
            add_readme = dict()
            add_readme["References"] = ""

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}




def process(msg):

    att = dict(msg.attributes)
    att['operator'] = 'repl_table_profile'

    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)

    table_repos = att['table_repository']
    table = att['replication_table']
    checksum = att['checksum_col']

    sql = 'UPDATE {table_repos} SET \"TABLE_CHECKSUM\" = (SELECT SUM(\"{csc}\") FROM {table}), '\
          ' \"TABLE_ROWS\" = (SELECT COUNT(*) FROM {table}), \"TABLE_UPDATED\" = CURRENT_UTCTIMESTAMP ' \
          ' WHERE \"TABLE\" = \'{table}\' ' .format(table=table, table_repos = table_repos,csc = checksum)

    logger.info('Update statement: {}'.format(sql))
    att['sql'] = sql

    api.send(outports[1]['name'], api.Message(attributes=att,body=sql))

    log = log_stream.getvalue()
    if len(log) > 0 :
        api.send(outports[0]['name'], log )


inports = [{'name': 'data', 'type': 'message', "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'msg', 'type': 'message', "description": "msg with sql statement"}]

#api.set_port_callback(inports[0]['name'], process)

def test_operator():

    msg = api.Message(attributes={'packageid':4711,'table_name':'repl_table','schema_name':'schema',\
                                  'data_outcome':True,'table_repository':'TEST_TABLES_REPOS', \
                                  'checksum_col':'NUMBER','replication_table':'REPLICATION.TEST_TABLE_0'},body='')
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

