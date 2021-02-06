import sdi_utils.gensolution as gs
import subprocess
import os

import io
import pandas as pd
import logging
import json

pd.set_option('expand_frame_repr', True)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 200)
try:
    api
except NameError:
    class api:

        queue = list()

        class Message:
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes

        def send(port,msg) :
            if port == outports[1]['name'] :
                api.queue.append(msg)

        class config:
            ## Meta data
            config_params = dict()
            tags = {}
            version = "0.1.0"
            operator_name = 'update_file'
            operator_description = "Update File"
            operator_description_long = "Update file according to updates and deletes."
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
    att['operator'] = 'update_file'

    df = msg.body

    api.logger.debug('Update file: {} ({})'.format(df.shape[0],att['file']['path']))

    api.logger.debug('Last Update file - merge')
    #  cases:
    #  I,U: normal, U,I : should not happen, sth went wrong with original table (no test)
    #  I,D: normal, D,I : either sth went wrong in the repl. table or new record (no test)
    #  U,D: normal, D,U : should not happen, sth went wrong with original table (no test)

    # keep only the most updated records irrespective of change type I,U,D
    if not 'DIREPL_UPDATED' in att['primary_keys'] :
        api.logger.warning('DIREPL_UPDATED not part of primar key!')
    prim_keys = [p for p in att['primary_keys']  if not p == 'DIREPL_UPDATED']
    gdf = df.groupby(by = prim_keys)['DIREPL_UPDATED'].max().reset_index()
    df = pd.merge(gdf, df, on=att['primary_keys'], how='inner')

    # remove D-type records
    df = df.loc[~(df['DIREPL_TYPE']=='D')]

    # prepare for saving
    df = df[sorted(df.columns)]
    if df.empty :
        raise ValueError('DataFrame is empty - Avoiding to create empty file!')

    csv = df.to_csv(index=False)
    api.send(outports[1]['name'],api.Message(attributes=att, body=csv))


    log = log_stream.getvalue()
    if len(log)>0 :
        api.send(outports[0]['name'], log_stream.getvalue())

inports = [{'name': 'data', 'type': 'message.DataFrame', "description": "Input DataFrame"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'data', 'type': 'message.file', "description": "Output data"}]


api.set_port_callback(inports[0]['name'], process)


def test_operator() :
    att = {'operator': 'collect_files', 'file': {'path': '/adbd/abd.csv'},'primary_keys':['INDEX','DIREPL_UPDATED'],\
           'message.last_update_file':False,'checksum_col':'INDEX','table_repository':'REPLICATION.TEST_TABLES_REPOS', 'schema_name':'REPLICATION',\
           'table_name':'TEST_TABLE_0','target_file':True}
    att['current_file'] = {
            "dir": "/replication/REPLICATION/TEST_TABLE_17",
            "update_files": [
                "12345_TEST_TABLE_17.csv"
            ],
            "base_file": "TEST_TABLE_17.csv",
            "schema_name": "REPLICATION",
            "table_name": "TEST_TABLE_17",
            "primary_key_file": "TEST_TABLE_17_primary_keys.csv",
            "consistency_file": "",
            "misc": []
        }

    file = './datasets/TEST_TABLE_0_20210204_09.json'
    jsonstream = open(file, mode='rb').read()
    json_io = io.BytesIO(jsonstream)
    jdict = [json.loads(line) for line in json_io]
    df = pd.DataFrame(jdict)

    msg = api.Message(attributes=att,body=df)
    process(msg)

    file = './datasets/TEST_TABLE_0_20210204_09.csv'
    for q in api.queue :
        print(q.body)
        open(file, mode='w').writelines(q.body)







