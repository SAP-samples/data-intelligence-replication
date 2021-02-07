
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

            operator_description = "Primary key SQL"
            operator_name = 'primarykey_sql'
            operator_description_long = "Primary key sql for table"
            add_readme = dict()

            schema = 'SCHEMA'
            config_params['schema'] = {'title': 'DB Schema',
                                           'description': 'Database schema',
                                           'type': 'string'}

        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
        logger = logging.getLogger(name=config.operator_name)


# catching logger messages for separate output
log_stream = io.StringIO()
sh = logging.StreamHandler(stream=log_stream)
sh.setFormatter(logging.Formatter('%(asctime)s |  %(levelname)s | %(name)s | %(message)s', datefmt='%H:%M:%S'))
api.logger.addHandler(sh)


def process(msg) :
    att = dict(msg.attributes)
    att['operator'] = 'primarykey_sql'

    att['table_name'] = os.path.basename(os.path.dirname(att['file']['path']))

    sql = 'SELECT \"SCHEMA_NAME\", \"TABLE_NAME", \"COLUMN_NAME\" from SYS.\"CONSTRAINTS\" WHERE ' \
          '\"SCHEMA_NAME\" = \'{schema}\' AND \"TABLE_NAME\" = \'{table}\' AND \"IS_PRIMARY_KEY\" = \'TRUE\'' \
        .format(schema=api.config.schema, table=att['table_name'])

    api.logger.info("Send msg: {}".format(sql))
    api.send(outports[1]['name'], api.Message(attributes=att, body=sql))
    api.send(outports[0]['name'], log_stream.getvalue())

    log_stream.seek(0)

inports = [{'name': 'filename', 'type': 'message.file',"description":"filename"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'sqlkeys', 'type': 'message',"description":"sql keys"}]


#api.set_port_callback(inports[0]['name'], process)

def test_operator() :

    api.config.debug_mode = True

    att = {"file":{"connection":{"configurationType":"Connection Management","connectionID":"S3_THH"},"isDir":False,
                   "modTime":"2021-02-03T10:20:49Z","path":"/replication3/TEST_TABLE_0/TEST_TABLE_0_20210203_11.json",
                   "size":13090},"message.batchIndex":0,"message.batchSize":1,"message.lastBatch":True}

    data = [["REPLICATION.DOC_METADATA_REPL",0],["REPLICATION.TEXT_WORDS_REPL",2],["REPLICATION.WORD_INDEX_REPL",1],["REPLICATION.WORD_SENTIMENT_REPL",5]]

    msg = api.Message(attributes=att, body=data)
    process(msg)

    print(msg.attributes)
    print(msg.body)

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




