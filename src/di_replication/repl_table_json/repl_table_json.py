import io
import subprocess
import os
import pandas as pd

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
                print('ATTRIBUTES: ')
                print(msg.attributes)#
                print('CSV-String: ')
                print(msg.body)

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils':''}
            version = "0.0.1"
            operator_name = 'repl_table_json'
            operator_description = "table to json"
            operator_description_long = "Converts table to json."
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}



def process(msg):

    att = dict(msg.attributes)
    att['operator'] = 'repl_table_json'
    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)

    logger.info("Process started")
    time_monitor = tp.progress()

    header = [c["name"] for c in msg.attributes['table']['columns']]
    df = pd.DataFrame(msg.body,columns=header)

    if df.shape[0] == 0  :
        att['data_outcome'] = False
        api.send(outports[2]['name'],api.Message(attributes=att,body = att['data_outcome']))
        logger.info('No data received, msg to port error_status sent.')
        logger.info('Process ended: {}'.format(time_monitor.elapsed_time()))
        api.send(outports[0]['name'], log_stream.getvalue())
        return 0

    json_data = df.to_json(orient='records')
    att["file"] =  {"connection": {"configurationType": "Connection Management", "connectionID": "unspecified"}, \
                    "path": "open", "size": 0}

    msg = api.Message(attributes=att,body = json_data)
    api.send(outports[1]['name'],msg)

    api.send(outports[0]['name'], log_stream.getvalue())


inports = [{'name': 'data', 'type': 'message.table',"description":"Input message with table"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'csv', 'type': 'message.file',"description":"Output data as csv"},\
            {'name': 'error', 'type': 'message',"description":"Error status"}]


#api.set_port_callback(inports[0]['name'], process)

def test_operator() :

    api.config.debug_mode = True

    attributes = {"table":{"columns":[{"class":"string","name":"header1","nullable":True,"size":80,"type":{"hana":"NVARCHAR"}},
                                      {"class":"string","name":"header2","nullable":True,"size":3,"type":{"hana":"NVARCHAR"}},
                                      {"class":"string","name":"header3","nullable":True,"size":10,"type":{"hana":"NVARCHAR"}}],
                           "name":"test.table","version":1}}
    table = [ [(j*3 + i) for i in range(0,3)] for j in range (0,5)]
    msg = api.Message(attributes=attributes, body=table)
    print(table)
    process(msg)


if __name__ == '__main__':
    test_operator()
    if True :
        subprocess.run(["rm", '-r','../../../solution/operators/sdi_replication_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name + '_' + api.config.version
        subprocess.run(["vctl", "solution", "bundle",'../../../solution/operators/sdi_replication_' + api.config.version, \
                        "-t", solution_name])
        subprocess.run(["mv", solution_name + '.zip', '../../../solution/operators'])
