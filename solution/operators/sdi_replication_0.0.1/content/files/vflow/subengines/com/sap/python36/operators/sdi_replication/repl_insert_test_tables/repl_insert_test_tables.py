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
            operator_name = 'repl_insert_test_tables'
            operator_description = "Insert Test Tables"

            operator_description_long = "Update test tables with incremental value."
            add_readme = dict()
            add_readme["References"] = ""

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            num_inserts = 10
            config_params['num_inserts'] = {'title': 'Number of inserts',
                                           'description': 'Number of inserts.',
                                           'type': 'integer'}


            max_random_num = 10000
            config_params['max_random_num'] = {'title': 'Maximum Random Number',
                                           'description': 'Maximum random number.',
                                           'type': 'integer'}

def process(msg):

    att = dict(msg.attributes)
    operator_name = 'repl_insert_test_tables'
    logger, log_stream = slog.set_logging(operator_name, loglevel=api.config.debug_mode)

    max_index = msg.body[0][0]
    num_inserts = api.config.num_inserts
    maxn = api.config.max_random_num

    col1 = np.arange(max_index+1, max_index + num_inserts+1)
    df = pd.DataFrame(col1, columns=['INDEX']).reset_index()
    df['NUMBER'] = np.random.randint(0, maxn, num_inserts)
    df['DIREPL_UPDATED'] = 0
    df['DIREPL_UPDATED'] = df['DIREPL_UPDATED'].apply(lambda x: datetime.now(timezone.utc).isoformat())
    df['DIREPL_PID'] = 0
    df['DIREPL_STATUS'] = 'W'
    df['DIREPL_PACKAGEID'] = 0
    df['DIREPL_TYPE'] = 'I'

    table_name = att['replication_table']
    att['table'] = {
        "columns": [{"class": "integer", "name": "INDEX", "nullable": False, "type": {"hana": "BIGINT"}}, \
                    {"class": "integer", "name": "NUMBER", "nullable": True, "type": {"hana": "BIGINT"}}, \
                    {"class": "integer", "name": "DIREPL_PACKAGEID", "nullable": False, "type": {"hana": "BIGINT"}}, \
                    {"class": "integer", "name": "DIREPL_PID", "nullable": True, "type": {"hana": "BIGINT"}}, \
                    {"class": "timestamp", "name": "DIREPL_UPDATED", "nullable": True,
                     "type": {"hana": "TIMESTAMP"}}, \
                    {"class": "string", "name": "DIREPL_STATUS", "nullable": True, "size": 1,
                     "type": {"hana": "NVARCHAR"}}, \
                    {"class": "string", "name": "DIREPL_TYPE", "nullable": True, "size": 1,
                     "type": {"hana": "NVARCHAR"}}],"version": 1, "name": att['replication_table']}
    df = df[['INDEX','NUMBER','DIREPL_PACKAGEID','DIREPL_PID','DIREPL_UPDATED','DIREPL_STATUS','DIREPL_TYPE']]

    table_data = df.values.tolist()

    api.send(outports[1]['name'], api.Message(attributes=att, body=table_data))
    api.send(outports[0]['name'], log_stream.getvalue())


inports = [{'name': 'data', 'type': 'message.table', "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'data', 'type': 'message.table', "description": "msg with sql"}]

api.set_port_callback(inports[0]['name'], process)

def test_operator():
    api.config.off_set = 2
    api.config.num_rows = 10
    msg = api.Message(attributes={'packageid':4711,'replication_table':'REPLICATION.TEST_TABLE_0'},body=[[50]])
    process(msg)


    for st in api.sql_queue :
        print(st.attributes)
        print(st.body)

    for st in api.csv_queue :
        print(st.body)


