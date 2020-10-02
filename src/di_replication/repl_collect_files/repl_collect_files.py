import io

import os
import re
import json

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
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if port == outports[1]['name']:
                api.queue.append(msg)

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils': ''}
            version = "0.1.0"

            operator_description = "Repl. Collect Files"
            operator_name = 'repl_collect_files'
            operator_description_long = "Collect files and send a dict."
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

table_collector = dict()
num_tables = 0
num_files = 0


def process(msg):
    global table_collector

    att = dict(msg.attributes)
    att['operator'] = 'repl_collect_files'

    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)
    logger.info("Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()

    dir_name = os.path.dirname(att['file']['path'])
    filename = os.path.basename(att['file']['path'])
    schema_name = os.path.basename(os.path.dirname(dir_name))
    table_name = os.path.basename(dir_name)

    ## constructor collector structure
    if not schema_name in table_collector :
        table_collector[schema_name] = dict()
    if not table_name in table_collector[schema_name] :
        table_collector[schema_name][table_name] = {'dir':dir_name,'update_files':[],'base_file':'', 'schema_name':schema_name,\
                                                    'table_name':table_name,'primary_key_file':'','consistency_file':'','misc':[]}


    if filename == (table_name + '.csv') :
        table_collector[schema_name][table_name]['base_file'] = filename
    elif '_primary_keys.csv' in filename :
        table_collector[schema_name][table_name]['primary_key_file'] = filename
    elif '_ccheck.csv' in filename :
        table_collector[schema_name][table_name]['consistency_file'] = filename
    elif re.match('\d+_.*\.csv$',filename) :
        table_collector[schema_name][table_name]['update_files'].append(filename)
    else :
        table_collector[schema_name][table_name]['misc'].append(filename)


    logger.info('File collected: {}'.format(table_collector[schema_name][table_name]))

    if att['message.lastBatch'] == True:

        # flat structure
        files = [ b for a in list(table_collector.values()) for b in list(a.values())]

        logger.info('Send table collector. #files: {}'.format(len(table_collector)))
        logger.info('Process ended - {}'.format(time_monitor.elapsed_time()))
        msg = api.Message(attributes=att, body=files)
        api.send(outports[1]['name'], msg )

    log = log_stream.getvalue()
    if len(log)>0 :
        api.send(outports[0]['name'], log_stream.getvalue())

inports = [{'name': 'files', 'type': 'message.file', "description": "List of files"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'file', 'type': 'message.file', "description": "file"}]


# api.set_port_callback(inports[0]['name'], process)


def test_operator():
    files = list()
    files.append({"file": {"connection": {"configurationType": "Connection Management", "connectionID": "ADL_THH"}, \
                      "isDir": False, "modTime": "2020-07-21T10:13:01Z",
                      "path": "/replication/REPLICATION/TEST_TABLE_17/TEST_TABLE_17_primary_keys.csv", "size": 67}, \
             "message.batchIndex": 39, "message.batchSize": 1, "message.lastBatch": False})
    files.append({"file": {"connection": {"configurationType": "Connection Management", "connectionID": "ADL_THH"}, \
                      "isDir": False, "modTime": "2020-07-21T10:10:04Z",
                      "path": "/replication/REPLICATION/TEST_TABLE_17/TEST_TABLE_17.csv", "size": 82}, \
             "message.batchIndex": 40, "message.batchSize": 1, "message.lastBatch": False})
    files.append({"file": {"connection": {"configurationType": "Connection Management", "connectionID": "ADL_THH"}, \
                      "isDir": False, "modTime": "2020-07-21T10:10:04Z",
                      "path": "/replication/REPLICATION/TEST_TABLE_17/12345_TEST_TABLE_17.csv", "size": 82}, \
             "message.batchIndex": 40, "message.batchSize": 1, "message.lastBatch": False})
    files.append({"file": {"connection": {"configurationType": "Connection Management", "connectionID": "ADL_THH"}, \
                      "isDir": False, "modTime": "2020-07-21T10:05:56Z",
                      "path": "/replication/REPLICATION/TEST_TABLE_18/TEST_TABLE_18.csv", "size": 4891}, \
             "message.batchIndex": 41, "message.batchSize": 1, "message.lastBatch": True})

    files.append({"file": {"connection": {"configurationType": "Connection Management", "connectionID": "ADL_THH"}, \
                      "isDir": False, "modTime": "2020-07-21T10:13:01Z",
                      "path": "/replication/REPLICATION2/TEST_TABLE_17/TEST_TABLE_17_primary_keys.csv", "size": 67}, \
             "message.batchIndex": 39, "message.batchSize": 1, "message.lastBatch": False})
    files.append({"file": {"connection": {"configurationType": "Connection Management", "connectionID": "ADL_THH"}, \
                      "isDir": False, "modTime": "2020-07-21T10:10:04Z",
                      "path": "/replication/REPLICATION2/TEST_TABLE_17/TEST_TABLE_17.csv", "size": 82}, \
             "message.batchIndex": 40, "message.batchSize": 1, "message.lastBatch": False})
    files.append({"file": {"connection": {"configurationType": "Connection Management", "connectionID": "ADL_THH"}, \
                      "isDir": False, "modTime": "2020-07-21T10:10:04Z",
                      "path": "/replication/REPLICATION2/TEST_TABLE_17/12345_TEST_TABLE_17.csv", "size": 82}, \
             "message.batchIndex": 40, "message.batchSize": 1, "message.lastBatch": False})
    files.append({"file": {"connection": {"configurationType": "Connection Management", "connectionID": "ADL_THH"}, \
                      "isDir": False, "modTime": "2020-07-21T10:05:56Z",
                      "path": "/replication/REPLICATION2/TEST_TABLE_18/TEST_TABLE_18.csv", "size": 4891}, \
             "message.batchIndex": 41, "message.batchSize": 1, "message.lastBatch": True})

    for f in files :
        process(api.Message(attributes=f, body=''))


    for m in api.queue:
        print(m.attributes)
        print(json.dumps(m.body,indent=4))


if __name__ == '__main__':
    test_operator()
    if True:
        subprocess.run(["rm", '-r', '../../../solution/operators/sdi_replication_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name + '_' + api.config.version
        subprocess.run(
            ["vctl", "solution", "bundle", '../../../solution/operators/sdi_replication_' + api.config.version, \
             "-t", solution_name])
        subprocess.run(["mv", solution_name + '.zip', '../../../solution/operators'])



