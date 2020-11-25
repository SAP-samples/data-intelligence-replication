import sdi_utils.gensolution as gs
import os
import subprocess

import logging
import io

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
            tags = {}
            operator_name = 'repl_insert_test_tables'
            operator_description = "Insert Test Tables"

            operator_description_long = "Update test tables with incremental value."
            add_readme = dict()
            add_readme["References"] = ""

            num_inserts = 10
            config_params['num_inserts'] = {'title': 'Number of inserts',
                                           'description': 'Number of inserts.',
                                           'type': 'integer'}


            max_random_num = 10000
            config_params['max_random_num'] = {'title': 'Maximum Random Number',
                                           'description': 'Maximum random number.',
                                           'type': 'integer'}

        logger = logging.getLogger(name=config.operator_name)

# catching logger messages for separate output
log_stream = io.StringIO()
sh = logging.StreamHandler(stream=log_stream)
sh.setFormatter(logging.Formatter('%(asctime)s |  %(levelname)s | %(name)s | %(message)s', datefmt='%H:%M:%S'))
api.logger.addHandler(sh)

def process(msg):

    att = dict(msg.attributes)
    operator_name = 'repl_insert_test_tables'

    max_index = msg.body[0][0]
    if max_index == None :
        max_index = 0
    num_inserts = api.config.num_inserts
    maxn = api.config.max_random_num

    col1 = np.arange(max_index+1, max_index + num_inserts+1)
    df = pd.DataFrame(col1, columns=['INDEX']).reset_index()
    df['NUMBER'] = np.random.randint(0, maxn, num_inserts)
    df['DIREPL_UPDATED'] = datetime.now(timezone.utc).isoformat()
    df['DIREPL_PID'] = 0
    df['DIREPL_STATUS'] = 'W'
    #df['DIREPL_PACKAGEID'] = 0
    df['DIREPL_TYPE'] = 'I'
    df['DATETIME'] =  datetime.now(timezone.utc) - pd.to_timedelta(1,unit='d')
    df['DATETIME'] = df['DATETIME'].apply(datetime.isoformat)
    #df['DATE'] = df['DATE'].dt.strftime("%Y-%m-%d")

    table_name = att['replication_table']
    att['table'] = {
        "columns": [{"class": "integer", "name": "INDEX", "nullable": False, "type": {"hana": "BIGINT"}}, \
                    {"class": "integer", "name": "NUMBER", "nullable": True, "type": {"hana": "BIGINT"}}, \
                    {"class": "datetime", "name": "DATETIME", "nullable": True, "type": {"hana": "TIMESTAMP"}}, \
                    #{"class": "integer", "name": "DIREPL_PACKAGEID", "nullable": False, "type": {"hana": "BIGINT"}}, \
                    {"class": "integer", "name": "DIREPL_PID", "nullable": True, "type": {"hana": "BIGINT"}}, \
                    {"class": "datetime", "name": "DIREPL_UPDATED", "nullable": True,"type": {"hana": "TIMESTAMP"}}, \
                    {"class": "string", "name": "DIREPL_STATUS", "nullable": True, "size": 1,"type": {"hana": "NVARCHAR"}}, \
                    {"class": "string", "name": "DIREPL_TYPE", "nullable": True, "size": 1,
                     "type": {"hana": "NVARCHAR"}}],"version": 1, "name": att['replication_table']}
    #df = df[['INDEX','NUMBER','DATETIME','DIREPL_PACKAGEID','DIREPL_PID','DIREPL_UPDATED','DIREPL_STATUS','DIREPL_TYPE']]
    df = df[['INDEX', 'NUMBER', 'DATETIME','DIREPL_PID', 'DIREPL_UPDATED', 'DIREPL_STATUS','DIREPL_TYPE']]

    table_data = df.values.tolist()

    api.send(outports[1]['name'], api.Message(attributes=att, body=table_data))
    api.send(outports[0]['name'], log_stream.getvalue())


inports = [{'name': 'data', 'type': 'message.table', "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'data', 'type': 'message.table', "description": "msg with sql"}]

#api.set_port_callback(inports[0]['name'], process)

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

