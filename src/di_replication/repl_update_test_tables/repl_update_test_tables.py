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
            operator_name = 'repl_update_test_tables'
            operator_description = "Update Test Tables"

            operator_description_long = "Update test tables with incremental value."
            add_readme = dict()
            add_readme["References"] = ""

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            modulo_factor = 2
            config_params['modulo_factor'] = {'title': 'Modulo Factor',
                                           'description': 'Change each row of modulo(factor) = \'0\'.',
                                           'type': 'integer'}


            max_random_num = 10000
            config_params['max_random_num'] = {'title': 'Maximum Random Number',
                                           'description': 'Maximum random number.',
                                           'type': 'integer'}

def process(msg):

    att = dict(msg.attributes)
    operator_name = 'repl_update_test_tables'
    logger, log_stream = slog.set_logging(operator_name, loglevel=api.config.debug_mode)

    modulo_factor = api.config.modulo_factor
    maxn = api.config.max_random_num
    offset = random.randint(0,modulo_factor-1)

    sql = 'UPDATE {table} SET \"NUMBER\" = floor(RAND() * {maxn}), \"DIREPL_TYPE\" = \'U\', \"DIREPL_STATUS\" =  \'W\', '  \
          ' \"DIREPL_UPDATED\" = CURRENT_UTCTIMESTAMP WHERE MOD(\"INDEX\",{mf}) = {of}' \
        .format(table = att['replication_table'],maxn = maxn, mf= modulo_factor,of= offset)

    logger.info('SQL statement: {}'.format(sql))
    att['sql'] = sql

    api.send(outports[1]['name'], api.Message(attributes=att,body=sql))
    api.send(outports[0]['name'], log_stream.getvalue())


inports = [{'name': 'data', 'type': 'message', "description": "Input data"}]
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

