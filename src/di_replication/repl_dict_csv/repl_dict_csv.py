import io

import os
import pandas as pd
import numpy as np

import subprocess

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

import random

try:
    api
except NameError:
    class api:
        class Message:
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes
                
        def send(port,msg) :
            if port == outports[1]['name'] or port == outports[2]['name'] :
                print(msg.attributes)
                print(msg.body)

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils':''}
            version = "0.1.0"

            operator_description = "dict to csv"
            operator_name = 'repl_dict_csv'
            operator_description_long = "Converts dictionary outcome of HANA Client to csv to store via File operator."
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            separator = ','
            config_params['separator'] = {'title': 'Separator', 'description': 'Separator', 'type': 'string'}

            bool_to_int = True
            config_params['bool_to_int'] = {'title': 'Convert boolean to int',
                                            'description': 'Converting boolean value to integer.',
                                            'type': 'boolean'}

            drop_replication_cols = True
            config_params['drop_replication_cols'] = {'title': 'Drop Replication columns',
                                           'description': 'Drop Replication columns.',
                                           'type': 'boolean'}

            drop_header = True
            config_params['drop_header'] = {'title': 'Drop header',
                                           'description': 'Drop header (not only for the first run).',
                                           'type': 'boolean'}


list_dicts = set()

def process(msg) :
    global list_dicts

    att = dict()
    att['operator'] = 'repl_dict_csv'
    att['table'] = msg.attributes['table']
    att['base_table'] = msg.attributes['base_table']
    att['latency'] = msg.attributes['latency']
    att['data_outcome'] = msg.attributes['data_outcome']
    att['pid'] = msg.attributes['pid']

    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)
    logger.info("Process started. Logging level: {}".format(logger.level))
    logger.debug('Attributes: {}  - {}'.format(str(msg.attributes),str(msg.attributes)))
    time_monitor = tp.progress()

    df = pd.DataFrame(msg.body)
    logger.debug('DataFrame Shape: {} - {}'.format(df.shape[0],df.shape[1]))

    # for resilience test
    #if random.random() < 0.05 :
    #    logger.debug('Planned crash: {}')
    #    raise SystemExit('Planned Crash')

    # For empty data records no csv send but to error status
    if df.shape[0] == 0  :
        att['data_outcome'] = False
        api.send(outports[2]['name'],api.Message(attributes=att,body = att['data_outcome']))
        logger.info('No data received, msg to port error_status sent.')
        logger.info('Process ended: {}'.format(time_monitor.elapsed_time()))
        api.send(outports[0]['name'], log_stream.getvalue())
        return 0
    att['data_outcome'] = True

    package_id_list = df['DIREPL_PACKAGEID'].unique().tolist()
    if len(package_id_list) > 1:
        logger.error('More than \'1\' package id.')
        raise ValueError('More than \'1\' package id: {}'.format(package_id_list))
    att['packageid'] = package_id_list[0]

    if api.config.drop_replication_cols:
        df = df.drop(columns=['DIREPL_PACKAGEID', 'DIREPL_STATUS', 'DIREPL_UPDATED','DIREPL_PID'])

    if api.config.bool_to_int :
        bool_cols = df.select_dtypes(include='bool').columns
        logger.debug('Boolean columns: {}'.format(bool_cols))
        for bc in bool_cols:
            logger.debug('Boolean column conversion to int: {}'.format(bc))
            df[bc] = df[bc].astype(int)

    sep = api.config.separator
    if not sep:
        sep = ','
    logger.debug('to_csv - delimiter: {}'.format(sep))

    # always sort the columns alphabetically because DB columns do not have an order
    df = df.reindex(sorted(df.columns), axis=1)

    # drop headers if it is part of multiple calls (key: table name and cols)
    if api.config.drop_header :
        data_str = df.to_csv(sep=sep, index=False,header=False)
    else :
        col_str = att['base_table'] + '-' + ' '.join(df.columns.tolist())
        if col_str in list_dicts :
            data_str = df.to_csv(sep=sep, index=False,header=False)
        else :
            data_str = df.to_csv(sep=sep, index=False)
            list_dicts.add(col_str)

    att['file'] = {'connection':{"configurationType": "Connection Management", "connectionID": ""},"path": "", "size": 0}
    api.send(outports[1]['name'], api.Message(attributes=att,body=data_str))

    logger.debug('Process ended: {}'.format(time_monitor.elapsed_time()))
    log = log_stream.getvalue()
    if len(log) > 0 :
        api.send(outports[0]['name'], log )


inports = [{'name': 'dict', 'type': 'message',"description":"Input dict"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'csv', 'type': 'message.file',"description":"Output csv string"},
            {'name': 'error', 'type': 'message',"description":"Error status"}]


#api.set_port_callback(inports[0]['name'], process)

def test_operator() :

    api.config.debug_mode = True
    api.config.drop_replication_cols = True


    data = [{'name': 'Anna', 'age': 25, 'born': '1995-03-14', 'DIREPL_PACKAGEID':1, 'DIREPL_UPDATED':'2020-06-27','DIREPL_STATUS':'B','DIREPL_PID':'123'},\
            {'name': 'Berta', 'age': 31, 'born': '1989-08-14', 'DIREPL_PACKAGEID':1, 'DIREPL_UPDATED':'2020-06-27','DIREPL_STATUS':'B','DIREPL_PID':'123'},
            {'name': 'Cecilia', 'age': 41, 'born': '1979-08-14', 'DIREPL_PACKAGEID':1, 'DIREPL_UPDATED':'2020-06-27','DIREPL_STATUS':'B','DIREPL_PID':'123'}]
    msg = api.Message(attributes={'pid': 123123213, 'table':'REPL_TABLE','base_table':'REPL_TABLE','latency':30,'data_outcome':True}, body=data)
    process(msg)

    msg = api.Message(attributes={'pid': 123123213, 'table':'REPL_TABLE','base_table':'REPL_TABLE','latency':30,'data_outcome':True}, body={})
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



