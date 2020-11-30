
import sdi_utils.gensolution as gs
import subprocess
import os

import pandas as pd
import logging
import io





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

            operator_description = "Get primary keys"
            operator_name = 'repl_get_primary_keys'
            operator_description_long = "Get primary keys of all tables of table repository"
            add_readme = dict()

        logger = logging.getLogger(name=config.operator_name)


# catching logger messages for separate output
log_stream = io.StringIO()
sh = logging.StreamHandler(stream=log_stream)
sh.setFormatter(logging.Formatter('%(asctime)s |  %(levelname)s | %(name)s | %(message)s', datefmt='%H:%M:%S'))
api.logger.addHandler(sh)


# send log-stream
def send_log_stream():
    api.send(outports[0]['name'], log_stream.getvalue())
    log_stream.seek(0)
    log_stream.truncate()


def send_adhoc_debug_log(str):
    api.logger.debug(str)
    send_log_stream()

def process(msg) :
    att = dict(msg.attributes)
    att['operator'] = 'repl_get_primary_keys'

    send_adhoc_debug_log('Process started')

    header = [c["name"] for c in msg.attributes['table']['columns']]
    df = pd.DataFrame(msg.body, columns=header)
    repl_tables = df['TABLE_NAME'].values

    send_adhoc_debug_log('Tables: {}'.format(repl_tables))

    # case no repl tables provided
    if len(repl_tables) == 0:
        api.logger.warning('No replication tables provided!')
        api.send(outports[0]['name'], log_stream.getvalue())
        raise ValueError('No replication tables provided!')

    send_adhoc_debug_log('Entering loop')
    for i, t in enumerate(repl_tables):

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
            api.logger.error(statment)
            raise ValueError(statment)

        sql = 'SELECT \"SCHEMA_NAME\", \"TABLE_NAME", \"COLUMN_NAME\" from SYS.\"CONSTRAINTS\" WHERE ' \
              '\"SCHEMA_NAME\" = \'{schema}\' AND \"TABLE_NAME\" = \'{table}\' AND \"IS_PRIMARY_KEY\" = \'TRUE\'' \
            .format(schema=att_table['schema_name'], table=att_table['table_name'])

        api.logger.info("Send msg: {}".format(sql))
        api.send(outports[1]['name'], api.Message(attributes=att_table, body=sql))
        api.send(outports[0]['name'], log_stream.getvalue())

        log_stream.seek(0)
        log_stream.truncate()

    api.logger.info('Process started')

    api.send(outports[0]['name'], log_stream.getvalue())
    api.send(outports[0]['name'], log_stream.getvalue())

inports = [{'name': 'tables', 'type': 'message.table',"description":"List of tables"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'sqlkeys', 'type': 'message',"description":"sql keys"}]


#api.set_port_callback(inports[0]['name'], process)

def test_operator() :

    api.config.debug_mode = True

    att = {"table":{"columns":[{"class":"string","name":"TABLE_NAME","nullable":False,"size":100,"type":{"hana":"NVARCHAR"}},\
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




