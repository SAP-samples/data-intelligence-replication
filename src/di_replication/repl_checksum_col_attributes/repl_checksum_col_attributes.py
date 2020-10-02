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
            operator_name = 'repl_checksum_col_attributes'
            operator_description = "Checksum Column to Attributes"

            operator_description_long = "Save checksum column to attributes."
            add_readme = dict()
            add_readme["References"] = ""

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            file_path_att = 'P'
            config_params['file_path_att'] = {'title': 'File Path Attribute (B/P)',
                                           'description': 'Set File Path Attribute (B-ase or P-rimary Key)',
                                           'type': 'string'}


def process(msg):
    att = dict(msg.attributes)
    att['operator'] = 'repl_checksum_col_attributes'

    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)

    if msg.body == None:
        logger.warning('No checksum column found: {} (Solution: file not in table repository)'.format(att))
        api.send(outports[0]['name'], log_stream.getvalue())
        att['checksum_col'] = ''
    else:
        att['checksum_col'] = msg.body[0][0]

    file_path_att = api.config.file_path_att
    if file_path_att == 'P' and len(att['current_file']['primary_key_file']) > 0:
        att['file']['path'] = os.path.join(att['current_file']['dir'], att['current_file']['primary_key_file'])
    elif file_path_att == 'B' and len(att['current_file']['base_file']) > 0:
        att['file']['path'] = os.path.join(att['current_file']['dir'], att['current_file']['base_file'])
    else:
        err_statement = "File path attribute wrongly set (P or B) not  {}!".format(api.config.file_path_att)
        logger.error(err_statement)
        raise ValueError(err_statement)

   # api.send(outports[1]['name'], update_sql)
    api.send(outports[1]['name'], api.Message(attributes=att, body=msg.body))

    log = log_stream.getvalue()
    if len(log) > 0:
        api.send(outports[0]['name'], log)


inports = [{'name': 'data', 'type': 'message.table', "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'msg', 'type': 'message.*', "description": "msg"}]

#api.set_port_callback(inports[0]['name'], process)

def test_operator():

    msg = api.Message(attributes={'packageid':4711,'table_name':'repl_table','base_table':'repl_table','latency':30,\
                                  'append_mode' : 'I', 'data_outcome':True, 'schema_name':'REPLICATION',\
                                  'file':{'path':'/replication/TEST_TABLE_0.csv'},
                                  'current_file':{'primary_key_file':'table_primary_key.csv','base_file':'TEST_TABLE_0',\
                                                  'dir':'/replication/'}},body=[['INDEX']])
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

