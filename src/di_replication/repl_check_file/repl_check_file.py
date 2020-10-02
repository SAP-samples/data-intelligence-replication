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
            operator_name = 'repl_check_file'
            operator_description = "check_file"
            operator_description_long = "Checks file on consistency with original table."
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}


def process(msg_check, msg_data):
    # merge attributes
    att = dict(msg_check.attributes)
    att.update(msg_data.attributes)

    att['operator'] = 'repl_check_file'
    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)

    logger.info("Process started")
    time_monitor = tp.progress()

    header = [c["name"] for c in att['table']['columns']]
    header.insert(0, 'SOURCE')
    profile_data = msg_check.body[0]
    profile_data.insert(0, 'table')

    ## read csv
    csv_io = io.BytesIO(msg_data.body)
    df = pd.read_csv(csv_io)
    #df = msg_data.body

    df_check = pd.DataFrame([profile_data], columns=header)
    num_rows = df.shape[0]
    max_date = df['DIREPL_UPDATED'].max()

    if len(att['checksum_col']) == 0:
        logger.warning('No checksum column for table: {}'.format(att['table_name']))
        checksum = 0
    else:
        logger.info('Checksum on: {} ({})'.format(att['checksum_col'], df[att['checksum_col']].dtype))
        checksum = df[att['checksum_col']].sum()

    df_check = df_check.append({'SOURCE': 'file', 'NUM_ROWS': num_rows, 'CHECKSUM': checksum, 'LATEST': max_date},ignore_index=True)

    logger.debug('Consistency Rows: {}'.format(df_check['NUM_ROWS'].values))
    logger.debug('Consistency Rows: {}'.format(df_check['CHECKSUM'].values))
    logger.debug('Consistency Rows: {}'.format(df_check['LATEST'].values))

    ## Consistency Check
    att['consistency_code'] = 0
    if df_check.iloc[0]['NUM_ROWS'] != df_check.iloc[1]['NUM_ROWS']:
        logger.warning(('Inconsistent number of rows!'))
        att['consistency_code'] = 1
    if df_check.iloc[0]['CHECKSUM'] != df_check.iloc[1]['CHECKSUM']:
        logger.warning(('Inconsistent checksum!'))
        att['consistency_code'] += 2


    ## New log file for consistency
    att['file']['path'] = os.path.join(os.path.dirname(att['file']['path']), att['table_name'] + '_ccheck.csv')

    msg = api.Message(attributes=att, body=df_check.to_csv(index=False))
    api.send(outports[1]['name'], msg)
    api.send(outports[0]['name'], log_stream.getvalue())

inports = [{'name': 'check', 'type': 'message.table', "description": "Input Check"}, \
           {'name': 'data', 'type': 'message.file', "description": "Input message"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'csv', 'type': 'message.file', "description": "Output data as csv"}]


# api.set_port_callback([inports[0]['name'],inports[1]['name']], process)

def test_operator() :

    att_check = {"file":{"connection":{"configurationType":"Connection Management","connectionID":"ADL_THH"},\
                         "isDir":False,"modTime":"2020-07-22T04:59:35Z","path":"/replication/REPLICATION/TEST_TABLE_0/TEST_TABLE_0.csv","size":4863},\
                 "message.batchIndex":0,"message.batchSize":1,"message.lastBatch":False,\
                 "operator":"repl_get_check_profile","schema_name":"REPLICATION","select_sql":"SELECT COUNT(*) AS NUM_ROWS, SUM(\"INDEX\") AS CHECKSUM, MAX(\"DIREPL_UPDATED\") AS LATEST FROM REPLICATION.TEST_TABLE_0",\
                 "table":{"columns":[{"class":"integer","name":"NUM_ROWS","nullable":False,"type":{"hana":"BIGINT"}},\
                                     {"class":"integer","name":"CHECKSUM","nullable":True,"type":{"hana":"BIGINT"}},\
                                     {"class":"timestamp","name":"LATEST","nullable":True,"type":{"hana":"TIMESTAMP"}}],"version":1},\
                 "table_name":"TEST_TABLE_0","checksum_col": 'INT_NUM'}
    body_check = [[123,234,345]]
    msg_check = api.Message(attributes=att_check,body=body_check)

    att_data = {'format':'DataFrame'}

    body_data = b"INDEX,INT_NUM,DIREPL_UPDATED\n0,1,'2020-07-14\n1,2,'2020-07-15'\n0,3,'2020-07-16"
    #body_data = pd.DataFrame([[0,1,'2020-07-14'],[1,2,'2020-07-15'],[0,3,'2020-07-16']], columns = ['INDEX','INT_NUM','DIREPL_UPDATED'] )
    msg_body = api.Message(attributes=att_data,body = body_data)

    process(msg_check,msg_body)



if __name__ == '__main__':
    test_operator()
    if True :
        subprocess.run(["rm", '-r','../../../solution/operators/sdi_replication_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name + '_' + api.config.version
        subprocess.run(["vctl", "solution", "bundle",'../../../solution/operators/sdi_replication_' + api.config.version, \
                        "-t", solution_name])
        subprocess.run(["mv", solution_name + '.zip', '../../../solution/operators'])
