import sdi_utils.gensolution as gs
import subprocess
import os

import io
import logging
import pandas as pd

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
            tags = {'pandas':''}
            version = "0.0.1"
            operator_name = 'repl_read_primary_key_file'
            operator_description = "Read Primary Key"
            operator_description_long = "Read primary key file."
            add_readme = dict()

        format = '%(asctime)s |  %(levelname)s | %(name)s | %(message)s'
        logging.basicConfig(level=logging.DEBUG, format=format, datefmt='%H:%M:%S')
        logger = logging.getLogger(name=config.operator_name)


# catching logger messages for separate output
log_stream = io.StringIO()
sh = logging.StreamHandler(stream=log_stream)
sh.setFormatter(logging.Formatter('%(asctime)s |  %(levelname)s | %(name)s | %(message)s', datefmt='%H:%M:%S'))
api.logger.addHandler(sh)


def process(msg):

    att = dict(msg.attributes)
    att['operator'] = 'read_primary_key_file'

    ## read csv
    csv_io = io.BytesIO(msg.body)
    key_df = pd.read_csv(csv_io)

    att['current_primary_keys'] = key_df['COLUMN_NAME'].values.tolist()
    api.logger.info('Primary key: {}  ({})'.format(att['current_primary_keys'],att['current_file']['table_name']))
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
        basename = os.path.basename(__file__[:-3])
        package_name = os.path.basename(os.path.dirname(os.path.dirname(__file__)))
        project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        solution_name = '{}_{}.zip'.format(basename, api.config.version)
        package_name_ver = '{}_{}'.format(package_name, api.config.version)

        solution_dir = os.path.join(project_dir, 'solution/operators', package_name_ver)
        solution_file = os.path.join(project_dir, 'solution/operators', solution_name)

        # rm solution directory
        subprocess.run(["rm", '-r', solution_dir])

        # create solution directory with generated operator files
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)

        # Bundle solution directory with generated operator files
        subprocess.run(["vctl", "solution", "bundle", solution_dir, "-t", solution_file])
