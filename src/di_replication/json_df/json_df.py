
import sdi_utils.gensolution as gs
import subprocess

import io
import json
import os
import re

import pandas as pd
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
            if port == outports[1]["name"]:
                api.queue.append(msg)

        def set_config(config):
            api.config = config

        class config:
            ## Meta data
            config_params = dict()
            tags = {}
            version = "0.1.0"
            operator_description = "json to df"
            operator_name = 'json_df'
            operator_description_long = "Converts json stream to DataFrame"
            add_readme = dict()

        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
        logger = logging.getLogger(name=config.operator_name)

# catching logger messages for separate output
log_stream = io.StringIO()
sh = logging.StreamHandler(stream=log_stream)
sh.setFormatter(logging.Formatter('%(asctime)s |  %(levelname)s | %(name)s | %(message)s', datefmt='%H:%M:%S'))
api.logger.addHandler(sh)

def process(msg):

    att = dict(msg.attributes)

    att['operator'] = 'json_df'

    try :
        jdict = json.loads(msg.body)
        if not isinstance(jdict, list):
            jdict = [jdict]
    except json.decoder.JSONDecodeError :
        api.logger.warning('File not a valid JSON-file. Try to read it as JSON records line by line.')
        json_io = io.BytesIO(msg.body)
        jdict = [json.loads(line) for line in json_io]

    df = pd.DataFrame(jdict)

    msg = api.Message(attributes=att, body=df)
    api.send(outports[1]['name'], msg)
    api.send(outports[0]['name'], log_stream.getvalue())



inports = [{'name': 'stream', 'type': 'message.file', "description": "Input json byte or string"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'data', 'type': 'message.DataFrame', "description": "Output DataFrame"}]


#api.set_port_callback(inports[0]['name'], process)

def test_operator() :

    api.config.debug_mode = True
    api.config.collect = False

    file = './datasets/TEST_TABLE_0_20210203_11.json'


    attributes = {"file":{"connection":{"configurationType":"Connection Management","connectionID":"S3_THH"},"isDir":False,
                          "modTime":"2021-02-03T10:20:49Z","path":"/replication3/TEST_TABLE_0/TEST_TABLE_0_20210203_11.json",
                          "size":13090},"message.batchIndex":0,"message.batchSize":1,"message.lastBatch":True}
    jsonstream = open(file, mode='rb').read()
    msg = api.Message(attributes=attributes, body=jsonstream)
    process(msg)

    print(api.queue[0].body)


if __name__ == '__main__':

    test_operator()
    if True :
        subprocess.run(["rm", '-r', './solution/operators/di_replication_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name + '_' + api.config.version
        subprocess.run(
            ["vctl", "solution", "bundle", './solution/operators/di_replication_' + api.config.version, "-t",
             solution_name])
        subprocess.run(["mv", solution_name + '.zip', './solution/operators'])

