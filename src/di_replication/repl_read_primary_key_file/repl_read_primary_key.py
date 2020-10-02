import io
import subprocess
import os
import pandas as pd
import io

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

try:
    api
except NameError:
    class api:
        class Message:
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes

        def send(port,msg) :
            if port == outports[1]['name'] :
                print('ATTRIBUTES: ')
                print(msg.attributes)#
                print('CSV-String: ')
                print(msg.body)

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils':''}
            version = "0.0.1"
            operator_name = 'repl_read_primary_key_file'
            operator_description = "Read Primary Key"
            operator_description_long = "Read primary key file."
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}


def process(msg):

    att = dict(msg.attributes)
    att['operator'] = 'read_primary_key_file'
    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)

    ## read csv
    csv_io = io.BytesIO(msg.body)
    key_df = pd.read_csv(csv_io)

    att['current_primary_keys'] = key_df['COLUMN_NAME'].values.tolist()
    logger.info('Primary key: {}  ({})'.format(att['current_primary_keys'],att['current_file']['table_name']))
    att['file']['path'] = os.path.join(att['current_file']['dir'], att['current_file']['base_file'])

    msg = api.Message(attributes=att, body=att['current_file']['table_name'])
    api.send(outports[1]['name'], msg)

    log = log_stream.getvalue()
    if len(log)>0 :
        api.send(outports[0]['name'], log_stream.getvalue())

inports = [{'name': 'input', 'type': 'message.file', "description": "Input"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'base', 'type': 'message.file', "description": "Output"}]


# api.set_port_callback(inports[0]['name'], process)

def test_operator() :

    att =  {'current_file':{
            "dir": "/replication/REPLICATION/TEST_TABLE_17",
            "update_files": [
                "12345_TEST_TABLE_17.csv"
            ],
            "base_file": "TEST_TABLE_17.csv",
            "schema_name": "REPLICATION",
            "table_name": "TEST_TABLE_17",
            "primary_key_file": "TEST_TABLE_17_primary_keys.csv",
            "consistency_file": "",
            "misc": []},
        'file':{'path':'ysdf/asdf.csv'}
    }

    csv = b'COLUMN_NAME,TABLE_NAME,SCHEMA_NAME\nINDEX,TABLE,SCHEMA'
    msg = api.Message(attributes=att,body=csv)
    process(msg)



if __name__ == '__main__':
    test_operator()
    if True :
        subprocess.run(["rm", '-r','../../../solution/operators/sdi_replication_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name + '_' + api.config.version
        subprocess.run(["vctl", "solution", "bundle",'../../../solution/operators/sdi_replication_' + api.config.version, \
                        "-t", solution_name])
        subprocess.run(["mv", solution_name + '.zip', '../../../solution/operators'])
