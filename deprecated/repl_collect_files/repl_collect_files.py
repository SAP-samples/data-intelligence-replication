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
            operator_description_long = "Collect files and send a list."
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

files_collector = list()


def process(msg):
    global files_collector

    att = dict(msg.attributes)
    att['operator'] = 'repl_collect_files'

    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)
    logger.info("Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()

    att['base_name'] = os.path.basename(att['file']['path'])

    if 'primary_keys.csv' in att['base_name'] or 'ccheck.csv' in att['base_name']  :
        logger.info('Filename contains \"primary_keys.csv\" or \"ccheck.csv\"- skipped ({})'.format(att['base_name']))
    else:
        files_collector.append(msg)
        logger.info('File collected: {}'.format(att['file']['path']))

    if att['message.lastBatch'] == True:
        logger.info('Send files collection. #Files: {}'.format(len(files_collector)))
        logger.info('Process ended - {}'.format(time_monitor.elapsed_time()))
        msg = api.Message(attributes=att, body=files_collector)
        api.send(outports[1]['name'], msg )

    api.send(outports[0]['name'], log_stream.getvalue())


inports = [{'name': 'files', 'type': 'message.file', "description": "List of files"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'file', 'type': 'message.file', "description": "file"}]


# api.set_port_callback(inports[0]['name'], process)


def test_operator():
    file1 = {"file": {"connection": {"configurationType": "Connection Management", "connectionID": "ADL_THH"}, \
                      "isDir": False, "modTime": "2020-07-21T10:13:01Z",
                      "path": "/replication/REPLICATION/TEST_TABLE_17/TEST_TABLE_17_primary_keys.csv", "size": 67}, \
             "message.batchIndex": 39, "message.batchSize": 1, "message.lastBatch": False}
    file2 = {"file": {"connection": {"configurationType": "Connection Management", "connectionID": "ADL_THH"}, \
                      "isDir": False, "modTime": "2020-07-21T10:10:04Z",
                      "path": "/replication/REPLICATION/TEST_TABLE_17/TEST_TABLE_17_primary_keys.json", "size": 82}, \
             "message.batchIndex": 40, "message.batchSize": 1, "message.lastBatch": False}
    file3 = {"file": {"connection": {"configurationType": "Connection Management", "connectionID": "ADL_THH"}, \
                      "isDir": False, "modTime": "2020-07-21T10:05:56Z",
                      "path": "/replication/REPLICATION/TEST_TABLE_18/TEST_TABLE_18.csv", "size": 4891}, \
             "message.batchIndex": 41, "message.batchSize": 1, "message.lastBatch": True}

    process(api.Message(attributes=file1, body=''))
    process(api.Message(attributes=file2, body=''))
    process(api.Message(attributes=file3, body=''))

    for m in api.queue:
        print(m.attributes)
        for mi in m.body :
            print(mi.body)


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



