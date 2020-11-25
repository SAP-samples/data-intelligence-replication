import sdi_utils.gensolution as gs
#import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp

import subprocess
import logging
import os
import io

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
            operator_name = 'repl_populate_test_tables'
            operator_description = "Populate Test Tables"

            operator_description_long = "Create Test Tables."
            add_readme = dict()
            add_readme["References"] = ""

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}


            num_rows = 100
            config_params['num_rows'] = {'title': 'Number of table rows',
                                           'description': 'Number of table rows',
                                           'type': 'integer'}

            package_size = 5
            config_params['package_size'] = {'title': 'Package size',
                                           'description': 'Package size',
                                           'type': 'integer'}
        logger = logging.getLogger(name=config.operator_name)


# catching logger messages for separate output
log_stream = io.StringIO()
sh = logging.StreamHandler(stream=log_stream)
sh.setFormatter(logging.Formatter('%(asctime)s ;  %(levelname)s ; %(name)s ; %(message)s', datefmt='%H:%M:%S'))
api.logger.addHandler(sh)

def process(msg):
    att = dict(msg.attributes)
    att['operator'] = 'repl_populate_test_tables'
    #api.logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)

    api.logger.info("Process started. Logging level: {}".format(api.logger.level))
    api.logger.debug('Attributes: {}'.format(str(att)))

    # No further processing
    if att['sql'] == 'DROP':
        api.logger.info('Drop message - return 0')
        return 0

    offset = att['message.batchIndex']
    col1 = np.arange(offset, api.config.num_rows+offset)
    df = pd.DataFrame(col1, columns=['NUMBER']).reset_index()
    df.rename(columns={'index': 'INDEX'}, inplace=True)
    df['DIREPL_UPDATED'] = datetime.now(timezone.utc).isoformat()
    df['DIREPL_PID'] = 0
    df['DIREPL_STATUS'] = 'W'
    #df['DIREPL_PACKAGEID'] = 0
    df['DIREPL_TYPE'] = 'I'
    df['NUM_MOD'] = df['NUMBER']%100
    df['DATETIME'] = datetime.now(timezone.utc) - pd.to_timedelta(df['NUM_MOD'],unit='d')
    df['DATETIME'] = df['DATETIME'].apply(datetime.isoformat)


    #days_into_past = api.config.num_rows if api.config.num_rows < 3650 else 3650
    #days_step =  1 if int(api.config.num_rows/days_into_past) == 0 else int(api.config.num_rows/days_into_past)
    #first_date  = datetime.now(timezone.utc) - timedelta(days = days_into_past)
    #df['DATE'] = df['INDEX'].apply(lambda x : (first_date + timedelta(days = int(x/days_step))).strftime('%Y-%m-%d'))
    #df['DATE'] = df['INDEX'].apply(lambda x: (first_date + timedelta(days=int(x / days_step))).isoformat())

    #packageid_start = 0
    #for i, start in enumerate(range(0, df.shape[0], api.config.package_size)):
    #    df.DIREPL_PACKAGEID.iloc[start:start + api.config.package_size] = packageid_start + i

    # ensure the sequence of the table corresponds to attribute table:columns
    att['table'] = {
        "columns": [{"class": "integer", "name": "INDEX", "nullable": False, "type": {"hana": "BIGINT"}}, \
                    {"class": "integer", "name": "NUMBER", "nullable": True, "type": {"hana": "BIGINT"}},
                    {"class": "timestamp", "name": "DATETIME", "nullable": False, "type": {"hana": "TIMESTAMP"}}, \
                    #{"class": "integer", "name": "DIREPL_PACKAGEID", "nullable": False, "type": {"hana": "BIGINT"}}, \
                    {"class": "integer", "name": "DIREPL_PID", "nullable": True, "type": {"hana": "BIGINT"}}, \
                    {"class": "timestamp", "name": "DIREPL_UPDATED", "nullable": True, "type": {"hana": "TIMESTAMP"}}, \
                    {"class": "string", "name": "DIREPL_STATUS", "nullable": True, "size": 1, "type": {"hana": "NVARCHAR"}}, \
                    {"class": "string", "name": "DIREPL_TYPE", "nullable": True, "size": 1,"type": {"hana": "NVARCHAR"}}], \
                    "version": 1, "name": att['table_name']}


    #df = df[['INDEX','NUMBER','DATETIME','DIREPL_PACKAGEID','DIREPL_PID','DIREPL_UPDATED','DIREPL_STATUS','DIREPL_TYPE']]
    df = df[['INDEX', 'NUMBER', 'DATETIME','DIREPL_PID', 'DIREPL_UPDATED', 'DIREPL_STATUS','DIREPL_TYPE']]
    table_data = df.values.tolist()

    api.send(outports[1]['name'], api.Message(attributes=att, body=table_data))

    api.logger.debug('Process ended: {}')
    api.send(outports[0]['name'], log_stream.getvalue())


inports = [{'name': 'data', 'type': 'message.table', "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'table', 'type': 'message.table', "description": "msg with table"}]

#api.set_port_callback(inports[0]['name'], process)

def test_operator():
    att_dict = {'sql':'CREATE','message.batchIndex':1,'message.lastBatch':False,'sql':'CREATE','table_name':'REPLICATION.TEST_TABLE_0'}
    att_dict['repl_table'] = {
        "columns": [{"class": "integer", "name": "INDEX", "nullable": False, "type": {"hana": "BIGINT"}}, \
                    {"class": "integer", "name": "NUMBER", "nullable": True, "type": {"hana": "BIGINT"}}, \
                    {"class": "integer", "name": "DATE", "nullable": True, "type": {"hana": "DATE"}}, \
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
        basename = os.path.basename(__file__[:-3])
        package_name = os.path.basename(os.path.dirname(os.path.dirname(__file__)))
        project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        solution_name = '{}_{}'.format(basename,api.config.version)
        package_name_ver = '{}_{}'.format(package_name,api.config.version)
        solution_dir = os.path.join(project_dir,'solution/operators',package_name_ver)
        solution_file = os.path.join(solution_dir,solution_name+'.zip')

        subprocess.run(["rm", '-r',solution_file])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)

        subprocess.run(["vctl", "solution", "bundle", solution_dir, "-t", solution_file])
        subprocess.run(["mv", solution_file, os.path.join(project_dir,'solution/operators')])
        logging.info(f"Solution created: {solution_file}")

