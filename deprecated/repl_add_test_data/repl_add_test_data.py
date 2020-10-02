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
            operator_name = 'repl_add_test_data'
            operator_description = "Add Test Data"
            operator_description_long = "Add test data."
            add_readme = dict()
            add_readme["References"] = ""

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}


            num_data = 10
            config_params['num_data'] = {'title': 'Number of additional records',
                                           'description': 'Number of additional records (max: 999)',
                                           'type': 'integer'}



def process(msg):
    att = dict(msg.attributes)
    att['operator'] = 'repl_add_test_data'
    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)

    att['table'] = att.pop('repl_table')

    tstamp = int(datetime.utcnow().timestamp())
    col1 = np.arange(0, api.config.num_data) + tstamp * 1000
    col2 = np.random.randint(0,100 * api.config.num_data,api.config.num_data)
    df = pd.DataFrame(np.vstack((col1, col2)).T, columns=['INDEX','INT_NUM'])
    df['DIREPL_PID'] = 0
    df['DIREPL_STATUS'] = 'W'
    df['DIREPL_PACKAGEID'] = tstamp % 10000000
    df['DIREPL_TYPE'] = 'I'
    df['DIREPL_UPDATED'] = datetime.now(timezone.utc).isoformat()
    df = df[sorted(df.columns)]

    table_data = df.values.tolist()
    att['table'] = {'columns':list(),'version':1,'name':''}
    for col in df.columns :
        att['table']['columns'].append({'name' : col })

    api.send(outports[1]['name'], api.Message(attributes=att, body=table_data))
    api.send(outports[0]['name'], log_stream.getvalue())




inports = [{'name': 'data', 'type': 'message.table', "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'table', 'type': 'message.table', "description": "msg with table"}]

#api.set_port_callback(inports[0]['name'], process)

def test_operator():

    att_dict = {'sql':'CREATE','message.batchIndex':1,'message.lastBatch':False,'sql':'CREATE'}
    att_dict['repl_table'] = {
        "columns": [{"class": "integer", "name": "INDEX", "nullable": False, "type": {"hana": "BIGINT"}}, \
                    {"class": "integer", "name": "INT_NUM", "nullable": True, "type": {"hana": "BIGINT"}}, \
                    {"class": "integer", "name": "DIREPL_PACKAGEID", "nullable": False, "type": {"hana": "BIGINT"}}, \
                    {"class": "integer", "name": "DIREPL_PID", "nullable": True, "type": {"hana": "BIGINT"}}, \
                    {"class": "timestamp", "name": "DIREPL_UPDATED", "nullable": True,
                     "type": {"hana": "TIMESTAMP"}}, \
                    {"class": "string", "name": "DIREPL_STATUS", "nullable": True, "size": 1,
                     "type": {"hana": "NVARCHAR"}}, \
                    {"class": "string", "name": "DIREPL_TYPE", "nullable": True, "size": 1,
                     "type": {"hana": "NVARCHAR"}}], \
        "version": 1, "name": 'test_table'}

    msg = api.Message(attributes=att_dict,body='')

    process(msg)

    for st in api.queue :
        print(st.attributes)
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

