import io

import os
import time
import pandas as pd


import subprocess

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

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
            tags = {'sdi_utils':''}
            version = "0.1.0"

            operator_description = "Get primary keys"
            operator_name = 'repl_get_primary_keys'
            operator_description_long = "Get primary keys of all tables of table repository"
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}





def process(msg) :

    att = dict(msg.attributes)
    att['operator'] = 'repl_get_primary_keys'

    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)

    header = [c["name"] for c in msg.attributes['table']['columns']]
    df = pd.DataFrame(msg.body,columns=header)
    repl_tables = df['TABLE'].values

    # case no repl tables provided
    if len(repl_tables) == 0 :
        logger.warning('No replication tables provided!')
        api.send(outports[0]['name'], log_stream.getvalue())
        raise ValueError('No replication tables provided!')

    for i,t in enumerate(repl_tables) :

        att_table = dict(att)

        lastbatch = False if not i == len(repl_tables) - 1 else True
        att_table['message.batchIndex'] = i
        att_table['message.lastBatch'] = lastbatch

        # split table from schema
        if '.' in t:
            att_table['table_name'] = t.split('.')[1]
            att_table['schema_name'] = t.split('.')[0]
        else:
            statment = 'No \"SCHEMA\" detected in table name!'
            logger.error(statment)
            raise ValueError(statment)

        sql = 'SELECT \"SCHEMA_NAME\", \"TABLE_NAME", \"COLUMN_NAME\" from SYS.\"CONSTRAINTS\" WHERE '\
              '\"SCHEMA_NAME\" = \'{schema}\' AND \"TABLE_NAME\" = \'{table}\' AND \"IS_PRIMARY_KEY\" = \'TRUE\''\
            .format(schema = att_table['schema_name'],table = att_table['table_name'])

        logger.info("Send msg: {}".format(sql))
        api.send(outports[1]['name'],api.Message(attributes=att_table,body=sql))
        api.send(outports[0]['name'], log_stream.getvalue())
        log_stream.seek(0)
        log_stream.truncate()

    api.send(outports[0]['name'], log_stream.getvalue())

inports = [{'name': 'tables', 'type': 'message.table',"description":"List of tables"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'sqlkeys', 'type': 'message',"description":"sql keys"}]


#api.set_port_callback(inports[0]['name'], process)

def test_operator() :

    api.config.debug_mode = True

    att = {"table":{"columns":[{"class":"string","name":"TABLE","nullable":False,"size":100,"type":{"hana":"NVARCHAR"}},\
                               {"class":"integer","name":"LATENCY","nullable":True,"type":{"hana":"INTEGER"}}],"version":1}}

    data = [["REPLICATION.DOC_METADATA_REPL",0],["REPLICATION.TEXT_WORDS_REPL",2],["REPLICATION.WORD_INDEX_REPL",1],["REPLICATION.WORD_SENTIMENT_REPL",5]]

    #data = [{'TABLE':'repl_TABLE1', 'LATENCY':0},{'TABLE':'repl_TABLE2', 'LATENCY':0},{'TABLE':'repl_TABLE3', 'LATENCY':0},
    #        {'TABLE':'repl_TABLE4', 'LATENCY':0},{'TABLE':'repl_TABLE5', 'LATENCY':0},{'TABLE':'repl_TABLE6', 'LATENCY':0}]

    msg = api.Message(attributes=att, body=data)
    process(msg)

    for m in api.queue :    header = [c["name"] for c in msg.attributes['table']['columns']]
    df = pd.DataFrame(msg.body,columns=header)
    print(m.attributes)
    print(m.body)

if __name__ == '__main__':
    test_operator()
    if True:
        subprocess.run(["rm", '-r','../../../solution/operators/sdi_replication_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name + '_' + api.config.version
        subprocess.run(["vctl", "solution", "bundle",'../../../solution/operators/sdi_replication_' + api.config.version, \
                        "-t", solution_name])
        subprocess.run(["mv", solution_name + '.zip', '../../../solution/operators'])



