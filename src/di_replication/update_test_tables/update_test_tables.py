import sdi_utils.gensolution as gs
import subprocess
import os

import logging
import io
import random
import pandas as pd
import numpy as np
from datetime import datetime, timezone

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
            version = '0.1.0'
            tags = {}
            operator_name = 'update_test_tables'
            operator_description = "Update Test Tables"

            operator_description_long = "Update test tables."
            add_readme = dict()
            add_readme["References"] = ""

            num_updates = 10
            config_params['num_updates'] = {'title': 'Number of updates',
                                           'description': 'Number of updates.',
                                           'type': 'integer'}


        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
        logger = logging.getLogger(name=config.operator_name)

# catching logger messages for separate output
log_stream = io.StringIO()
sh = logging.StreamHandler(stream=log_stream)
sh.setFormatter(logging.Formatter('%(asctime)s |  %(levelname)s | %(name)s | %(message)s', datefmt='%H:%M:%S'))
api.logger.addHandler(sh)

def process(msg):

    att = dict(msg.attributes)
    operator_name = 'insert_test_tables'

    if 'max_index' in att:
        max_index = att['max_index']
    else :
        max_index = api.config.num_updates

    maxn = 10000

    indices_update = random.sample(list(range(max_index)),k=api.config.num_updates)
    df = pd.DataFrame(indices_update, columns=['INDEX']).reset_index()
    df['NUMBER'] = np.random.randint(0, maxn, api.config.num_updates)
    df['DIREPL_UPDATED'] = datetime.now(timezone.utc).isoformat()
    df['DIREPL_PID'] = 0
    df['DIREPL_STATUS'] = 'W'
    df['DIREPL_TYPE'] = 'U'
    df['DATETIME'] =  datetime.now(timezone.utc) - pd.to_timedelta(1,unit='d')
    df['DATETIME'] = df['DATETIME'].apply(datetime.isoformat)

    table_name = att['replication_table']
    att['table'] = {
        "columns": [{"class": "integer", "name": "INDEX", "nullable": False, "type": {"hana": "BIGINT"}}, \
                    {"class": "integer", "name": "NUMBER", "nullable": True, "type": {"hana": "BIGINT"}}, \
                    {"class": "datetime", "name": "DATETIME", "nullable": True, "type": {"hana": "TIMESTAMP"}}, \
                    {"class": "integer", "name": "DIREPL_PID", "nullable": True, "type": {"hana": "BIGINT"}}, \
                    {"class": "datetime", "name": "DIREPL_UPDATED", "nullable": True,"type": {"hana": "TIMESTAMP"}}, \
                    {"class": "string", "name": "DIREPL_STATUS", "nullable": True, "size": 1,"type": {"hana": "NVARCHAR"}}, \
                    {"class": "string", "name": "DIREPL_TYPE", "nullable": True, "size": 1,
                     "type": {"hana": "NVARCHAR"}}],"version": 1, "name": att['replication_table']}
    df = df[['INDEX', 'NUMBER', 'DATETIME','DIREPL_PID', 'DIREPL_UPDATED', 'DIREPL_STATUS','DIREPL_TYPE']]

    table_data = df.values.tolist()

    api.send(outports[1]['name'], api.Message(attributes=att, body=table_data))
    api.send(outports[0]['name'], log_stream.getvalue())
    log_stream.seek(0)


inports = [{'name': 'data', 'type': 'message.table', "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'data', 'type': 'message.table', "description": "msg with sql"}]

#api.set_port_callback(inports[0]['name'], process)

def test_operator():
    api.config.off_set = 2
    api.config.num_rows = 10
    msg = api.Message(attributes={'max_index':200,'replication_table':'REPLICATION.TEST_TABLE_0'},body=[[50]])
    process(msg)


    for st in api.sql_queue :
        print(st.attributes)
        print(st.body)

    for st in api.csv_queue :
        print(st.body)


if __name__ == '__main__':
    test_operator()
    if True:
        basename = os.path.basename(__file__[:-3])
        package_name = os.path.basename(os.path.dirname(os.path.dirname(__file__)))
        project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        solution_name = '{}_{}.zip'.format(basename,api.config.version)
        package_name_ver = '{}_{}'.format(package_name,api.config.version)

        solution_dir = os.path.join(project_dir,'solution/operators',package_name_ver)
        solution_file = os.path.join(project_dir,'solution/operators',solution_name)

        # rm solution directory
        subprocess.run(["rm", '-r',solution_dir])

        # create solution directory with generated operator files
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)

        # Bundle solution directory with generated operator files
        subprocess.run(["vctl", "solution", "bundle", solution_dir, "-t",solution_file])

