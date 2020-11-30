import sdi_utils.gensolution as gs
import subprocess
import os

import io
import logging


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
            tags = {}
            operator_name = 'repl_get_checksum_col'
            operator_description = "Get Checksum Column"

            operator_description_long = "Get checksum column."
            add_readme = dict()
            add_readme["References"] = ""

            replication_repos = 'REPLICATION.TABLE_REPOSITORY'
            config_params['replication_repos'] = {'title': 'Replication Repository',
                                           'description': 'Replication Repository',
                                           'type': 'string'}

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
    att['operator'] = 'repl_get_checksum_col'

    api.logger.info("Process started")

    table_repos = api.config.replication_repos
    att['table_repository'] = table_repos
    if len(table_repos) == 0 :
        err_stat = 'Table Repository has been set in configuration!'
        api.logger.error(err_stat)
        raise ValueError(err_stat)

    table = att['schema_name'] + '.' + att['table_name']
    select_sql = 'SELECT \"CHECKSUM_COL\" FROM {repos}  WHERE \"TABLE_NAME\" = \'{table}\''.format(repos = table_repos, table = table)

    api.logger.info('Select statement: {}'.format(select_sql))
    att['select_sql'] = select_sql

    api.logger.debug("Process ended")

    #api.send(outports[1]['name'], update_sql)
    api.send(outports[1]['name'], api.Message(attributes=att,body=select_sql))

    log = log_stream.getvalue()
    if len(log) > 0 :
        api.send(outports[0]['name'], log )


inports = [{'name': 'data', 'type': 'message.file', "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'msg', 'type': 'message', "description": "msg with sql statement"}]

api.set_port_callback(inports[0]['name'], process)

def test_operator():
    api.config.use_package_id = False
    api.config.package_size = 1

    msg = api.Message(attributes={'packageid':4711,'table_name':'repl_table','base_table':'repl_table','latency':30,\
                                  'append_mode' : 'I', 'data_outcome':True, 'schema_name':'REPLICATION'},body='')
    process(msg)

    for msg in api.queue :
        print(msg.attributes)
        print(msg.body)


