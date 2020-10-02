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
            operator_name = 'repl_file_profile'
            operator_description = "File Profiles"
            operator_description_long = "Gets the file profiles."
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}


def process(msg):
    # merge attributes
    att = dict(msg.attributes)
    att['operator'] = 'repl_file_profile'
    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)

    header = [c["name"] for c in att['table']['columns']]
    ## read csv
    csv_io = io.BytesIO(msg.body)
    df = pd.read_csv(csv_io)
    #df = msg_data.body

    num_rows = df.shape[0]
    max_date = df['DIREPL_UPDATED'].max()

    if len(att['checksum_col']) == 0:
        logger.warning('No checksum column for table: {}'.format(att['table_name']))
        checksum = 0
    else:
        logger.info('Checksum on: {} ({})'.format(att['checksum_col'], df[att['checksum_col']].dtype))
        checksum = df[att['checksum_col']].sum()

    table_repos = att['table_repository']
    table = att['schema_name'] + '.' + att['table_name']

    sql = 'UPDATE {table_repos} SET \"FILE_CHECKSUM\" = \'{csc}\', '\
          ' \"FILE_ROWS\" = \'{nrows}\', \"TABLE_UPDATED\" = CURRENT_UTCTIMESTAMP ' \
          ' WHERE \"TABLE\" = \'{table}\' ' .format(table=table, table_repos = table_repos,csc = checksum,nrows = num_rows)
    att['sql'] = sql
    msg = api.Message(attributes=att, body=sql)
    api.send(outports[1]['name'], msg)
    api.send(outports[0]['name'], log_stream.getvalue())

inports = [{'name': 'data', 'type': 'message.file', "description": "Input message"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'sql', 'type': 'message', "description": "sql"}]


# api.set_port_callback(inports[0]['name'], process)

def test_operator() :

    att = {"file":{"connection":{"configurationType":"Connection Management","connectionID":"ADL_THH"},\
                         "isDir":False,"modTime":"2020-07-22T04:59:35Z","path":"/replication/REPLICATION/TEST_TABLE_0/TEST_TABLE_0.csv","size":4863},\
                 "message.batchIndex":0,"message.batchSize":1,"message.lastBatch":False,\
                 "operator":"repl_get_check_profile","schema_name":"REPLICATION","select_sql":"SELECT COUNT(*) AS NUM_ROWS, SUM(\"INDEX\") AS CHECKSUM, MAX(\"DIREPL_UPDATED\") AS LATEST FROM REPLICATION.TEST_TABLE_0",\
                 "table":{"columns":[{"class":"integer","name":"NUM_ROWS","nullable":False,"type":{"hana":"BIGINT"}},\
                                     {"class":"integer","name":"CHECKSUM","nullable":True,"type":{"hana":"BIGINT"}},\
                                     {"class":"timestamp","name":"LATEST","nullable":True,"type":{"hana":"TIMESTAMP"}}],"version":1},\
                 "table_name":"TEST_TABLE_0","checksum_col": 'INT_NUM','table_repository':'TEST_TABLES_REPOS'}

    data = b"INDEX,INT_NUM,DIREPL_UPDATED\n0,1,'2020-07-14\n1,2,'2020-07-15'\n0,3,'2020-07-16"
    msg = api.Message(attributes=att,body = data)
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
