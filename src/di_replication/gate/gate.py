
import subprocess
import os
import logging
import io

import sdi_utils.gensolution as gs


try:
    api
except NameError:
    class api:
        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if isinstance(msg, api.Message):
                print('{}: {}'.format(port, msg.body))
            else:
                print('{}: {}'.format(port, msg))

        class config:
            ## Meta data
            config_params = dict()
            tags = {}
            version = "0.1.0"
            operator_name = 'gate'
            operator_description = "Gate"
            operator_description_long = "Gate sends out message when lastBatch in attribute."
            add_readme = dict()

        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
        logger = logging.getLogger(name=config.operator_name)


# catching logger messages for separate output
log_stream = io.StringIO()
sh = logging.StreamHandler(stream=log_stream)
sh.setFormatter(logging.Formatter('%(asctime)s -  %(levelname)s - %(name)s - %(message)s', datefmt='%H:%M:%S'))
api.logger.addHandler(sh)


inports = [ {"name": "data", "type": "message.*", "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {"name": "data", "type": "message", "description": "Output of unchanged last input data"}]

def call_on_message(msg):

    if 'message.lastBatch' in msg.attributes and msg.attributes['message.lastBatch'] == True:
        api.send(outports[1]['name'], msg)
        api.logger.info('Msg send to outports: Last batch in attributes')
    else:
        api.logger.info('No message send')

    api.logger.debug('Message Attributes: {}'.format(msg.attributes))
    api.send(outports[0]['name'], log_stream.getvalue())


#api.set_port_callback(inports[0]['name'], call_on_message)


def test_operator():
    msg = api.Message(attributes={'message.lastBatch':False},body='Nothing')
    call_on_message(msg)
    msg = api.Message(attributes={'message.lastBatch': True}, body='Nothing')
    call_on_message(msg)


if __name__ == '__main__':
    test_operator()
    if True :
        subprocess.run(["rm", '-r','./solution/operators/di_replication_' + api.config.version])
        api.logger.info('create solution')
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name + '_' + api.config.version
        subprocess.run(["vctl", "solution", "bundle",'./solution/operators/di_replication_' + api.config.version, "-t", solution_name])
        subprocess.run(["mv", solution_name + '.zip', './solution/operators'])