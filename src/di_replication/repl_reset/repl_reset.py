import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

import subprocess
import logging
import os
from datetime import datetime, timezone
import time

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
            operator_name = 'repl_reset'
            operator_description = "Repl. Reset"

            operator_description_long = "Update replication table status reset."
            add_readme = dict()
            add_readme["References"] = ""

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}


def process(msg):

    att = dict(msg.attributes)
    att['operator'] = 'repl_reset'
    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)

    logger.info("Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()
    logger.debug('Attributes: {} - {}'.format(str(msg.attributes),str(att)))

    data = msg.body

    update_sql = 'UPDATE {table} SET \"DIREPL_STATUS\" = \'W\' WHERE  \"DIREPL_STATUS\" = \'B\' '\
                 'AND \"DIREPL_UPDATED\" < ADD_SECONDS(CURRENT_UTCTIMESTAMP,-{latency}) '.format(table=att['replication_table'],latency=att['latency'])

    logger.info('Update statement: {}'.format(update_sql))
    att['update_sql'] = update_sql

    logger.debug('Process ended: {}'.format(time_monitor.elapsed_time()))
    api.send(outports[1]['name'], api.Message(attributes=att,body=update_sql))

    log = log_stream.getvalue()
    if len(log) > 0 :
        api.send(outports[0]['name'], log )


inports = [{'name': 'data', 'type': 'message', "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'msg', 'type': 'message', "description": "msg with sql statement"}]

#api.set_port_callback(inports[0]['name'], process)

def test_operator():

    msg = api.Message(attributes={'replication_table':'repl_table','base_table':'repl_table','latency':30,'data_outcome':True},body={'TABLE':'repl_table','LATENCY':20})
    process(msg)

    for st in api.queue :
        print(st)


if __name__ == '__main__':
    test_operator()
    if True:
        subprocess.run(["rm", '-r','../../../solution/operators/sdi_replication_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name + '_' + api.config.version
        subprocess.run(["vctl", "solution", "bundle",'../../../solution/operators/sdi_replication_' + api.config.version, \
                        "-t", solution_name])
        subprocess.run(["mv", solution_name + '.zip', '../../../solution/operators'])

