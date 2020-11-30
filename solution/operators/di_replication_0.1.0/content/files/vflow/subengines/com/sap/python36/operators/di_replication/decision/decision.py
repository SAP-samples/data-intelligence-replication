import sdi_utils.gensolution as gs
import subprocess
import os

import io
import logging

try:
    api
except NameError:
    class api:
        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if port == outports[1]['name']:
                print('Message passed: {} - {}'.format(msg.attributes,msg.body))
            elif port == outports[2]['name']:
                print('Message did not pass: {} - {}'.format(msg.attributes,msg.body))

        class config:
            ## Meta data
            config_params = dict()
            tags = {}
            version = "0.1.0"
            operator_name = 'decision'
            operator_description = "Decision"
            operator_description_long = "Decision gate that channels messages."

            decision_attribute = 'message.lastBatch'
            config_params['decision_attribute'] = {'title': 'Descision Attribute',
                                           'description': 'Decision Attribute',
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

    if api.config.decision_attribute in msg.attributes and msg.attributes[api.config.decision_attribute] == True:
        api.send(outports[1]['name'], msg)
        api.logger.info('Msg passed: {}'.format(msg.attributes))
        api.send(outports[0]['name'], log_stream.getvalue())
    else :
        api.send(outports[2]['name'], msg)
        api.logger.info('Msg did not pass: {}'.format(msg.attributes))
        api.send(outports[0]['name'], log_stream.getvalue())


inports = [{"name": "input", "type": "message.*", "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'True', 'type': 'message.*', "description": "True message"},
            {"name": "False", "type": "message.*", "description": "False message"}]

api.set_port_callback(inports[0]['name'], process)


def test_operator():
    #api.config.last_attribute = 'message.last_update'
    process(api.Message(attributes={'message.lastBatch':False},body='0'))
    process(api.Message(attributes={'message.lastBatch':True},body='1'))
    process(api.Message(attributes={'message.last_update': True}, body='2'))


