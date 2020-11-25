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
            operator_name = 'repl_add_test_tables'
            operator_description = "Add Test Tables to Repository Table"

            operator_description_long = "Add Test Tables to Repository Table (SQL Statement)"
            add_readme = dict()
            add_readme["References"] = ""

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}


def process(msg):

    att = dict(msg.attributes)
    att['operator'] = 'repl_add_test_tables'
    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)

    logger.info("Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()

    now_str = datetime.now(timezone.utc).isoformat()
    rec = [[att['table']['name'],'NUMBER',0,0,None,0,0,now_str]]

    att['table'] =  {"columns": [
        {"class": "string", "name": "TABLE_NAME", "nullable": False, "size": 100, "type": {"hana": "NVARCHAR"}},
        {"class": "string", "name": "CHECKSUM_COL", "nullable": True, "size": 100, "type": {"hana": "NVARCHAR"}},
        {"class": "integer", "name": "FILE_CHECKSUM", "nullable": True, "type": {"hana": "BIGINT"}},
        {"class": "integer", "name": "FILE_ROWS", "nullable": True, "type": {"hana": "BIGINT"}},
        {"name": "FILE_UPDATED", "nullable": True, "type": {"hana": "LONGDATE"}},
        {"class": "integer", "name": "TABLE_CHECKSUM", "nullable": True, "type": {"hana": "BIGINT"}},
        {"class": "integer", "name": "TABLE_ROWS", "nullable": True, "type": {"hana": "BIGINT"}},
        {"name": "TABLE_UPDATED", "nullable": True, "type": {"hana": "LONGDATE"}}],
               "name": "REPLICATION.TABLE_REPOSITORY", "version": 1}

    api.send(outports[1]['name'], api.Message(attributes=att, body=rec))
    api.send(outports[0]['name'], log_stream.getvalue())
    log_stream.seek(0)
    log_stream.truncate()

    logger.debug('Process ended: {}'.format(time_monitor.elapsed_time()))
    api.send(outports[0]['name'], log_stream.getvalue())


inports = [{'name': 'data', 'type': 'message.table', "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'data', 'type': 'message.table', "description": "data"}]

api.set_port_callback(inports[0]['name'], process)

def test_operator():
    msg = api.Message(attributes={'packageid':4711,'table':{'name':'repl_table'}},body='')
    process(msg)

    for st in api.queue :
        print(st.attributes)
        print(st.body)


