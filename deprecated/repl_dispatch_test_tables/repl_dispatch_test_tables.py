import io

import os
import time



import subprocess

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
                print(msg.body)

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils':''}
            version = "0.1.0"

            operator_description = "Repl. Dispatch Test Tables"
            operator_name = 'repl_dispatch_test_tables'
            operator_description_long = "Dispatch test tables for incremental updates."
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}


def process(msg) :


    att = dict(msg.attributes)
    att['operator'] = 'repl_dispatch_test_tables'

    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)
    logger.debug('Attributes: {} - {}'.format(str(msg.attributes),str(att)))

    repl_tables = [ r[0] for r in msg.body]

    att['table_repository'] = msg.attributes['table']['name']
    att['num_tables'] = len(repl_tables)

    # case no repl tables provided
    if len(repl_tables) == 0 :
        logger.warning('No replication tables yet provided!')
        api.send(outports[0]['name'], log_stream.getvalue())
        return 0

    for i,t in enumerate(repl_tables) :
        tab_att = dict(msg.attributes)
        tab_att['replication_table'] = t
        tab_att['message.lastBatch'] = True if i == len(repl_tables) - 1 else False

        table_msg = api.Message(attributes= tab_att,body = t)
        api.send(outports[1]['name'], table_msg)

        logger.info('Dispatch table: {} ({}/{})'.format(tab_att['replication_table'],i,len(repl_tables)))
        api.send(outports[0]['name'], log_stream.getvalue())
        log_stream.seek(0)
        log_stream.truncate()


inports = [{'name': 'tables', 'type': 'message.table',"description":"List of tables"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'trigger', 'type': 'message',"description":"trigger"}]


#api.set_port_callback(inports[0]['name'], process)


def test_operator() :

    att = {"table":{"columns":[{"class":"string","name":"TABLE","nullable":False,"size":100,"type":{"hana":"NVARCHAR"}},\
                               {"class":"integer","name":"LATENCY","nullable":True,"type":{"hana":"INTEGER"}}],\
                    "name":"repository","version":1}}

    data = [["REPLICATION.DOC_METADATA_REPL",0],["REPLICATION.TEXT_WORDS_REPL",2],["REPLICATION.WORD_INDEX_REPL",1],["REPLICATION.WORD_SENTIMENT_REPL",5]]

    msg = api.Message(attributes=att, body=data)
    process(msg)

if __name__ == '__main__':
    test_operator()
    if True:
        subprocess.run(["rm", '-r','../../../solution/operators/sdi_replication_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name + '_' + api.config.version
        subprocess.run(["vctl", "solution", "bundle",'../../../solution/operators/sdi_replication_' + api.config.version, \
                        "-t", solution_name])
        subprocess.run(["mv", solution_name + '.zip', '../../../solution/operators'])



