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
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes
                
        def send(port,msg) :
            if port == outports[1]['name'] :
                api.queue.append(msg)

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils':''}
            version = "0.1.0"

            operator_description = "Repl. Dispatch Files"
            operator_name = 'repl_dispatch_files'
            operator_description_long = "Dispatch files."
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            file_path_att = 'P'
            config_params['file_path_att'] = {'title': 'File Path Attribute (B/P/C)',
                                           'description': 'Set File Path Attribute (B-ase/P-rimary Key/C-onsitency)',
                                           'type': 'string'}

            update_files_mandatory = True
            config_params['update_files_mandatory'] = {'title': 'Update Files mandatory',
                                           'description': 'Update files mandatory',
                                           'type': 'boolean'}

            base_file_mandatory = True
            config_params['base_file_mandatory'] = {'title': 'Base File mandatory',
                                           'description': 'Base file mandatory',
                                           'type': 'boolean'}

            primary_key_file_mandatory = True
            config_params['primary_key_file_mandatory'] = {'title': 'Primary Key File mandatory',
                                           'description': 'Primary key file mandatory',
                                           'type': 'boolean'}



files_list = list()
file_index = 0

def on_files_process(msg) :
    global files_list
    files_list = msg.body
    process(msg)

def process(msg) :
    global files_list
    global file_index

    att = dict(msg.attributes)
    att['operator'] = 'repl_dispatch_files'
    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)

    if len(files_list) == 0:
        err_statement = 'No files to process!'
        logger.error(err_statement)
        raise ValueError(err_statement)

    file_path_att = api.config.file_path_att
    if not file_path_att in 'CPB':
        err_statement = "File path attribute wrongly set (C or P or B) not  {}!".format(api.config.file_path_att)
        logger.error(err_statement)
        raise ValueError(err_statement)

    criteria_match = False
    while (not criteria_match):
        if file_index == len(files_list):
            logger.info('No files to process - ending pipeline')
            api.send(outports[0]['name'], log_stream.getvalue())
            api.send(outports[2]['name'], api.Message(attributes=att, body=None))
            return 0

        fpa_criteria = True
        if file_path_att == 'C' and len(files_list[file_index]['consistency_file']) > 0:
            att['file']['path'] = os.path.join(files_list[file_index]['dir'], files_list[file_index]['consistency_file'])
        elif file_path_att == 'P' and len(files_list[file_index]['primary_key_file']) > 0:
            att['file']['path'] = os.path.join(files_list[file_index]['dir'], files_list[file_index]['primary_key_file'])
        elif file_path_att == 'B' and len(files_list[file_index]['base_file']) > 0:
            att['file']['path'] = os.path.join(files_list[file_index]['dir'], files_list[file_index]['base_file'])
        else:
            fpa_criteria = False

        u_criteria = True
        if len(files_list[file_index]['update_files']) == 0 and api.config.update_files_mandatory == True:
            logger.warning('Update files mandatory, but not found: {}'.format(files_list[file_index]['dir']))
            u_criteria = False

        b_criteria = True
        if len(files_list[file_index]['base_file']) == 0 and api.config.base_file_mandatory == True:
            logger.warning('Base file mandatory, but not found: {}'.format(files_list[file_index]['dir']))
            b_criteria = False

        pk_criteria = True
        if len(files_list[file_index]['base_file']) == 0 and api.config.base_file_mandatory == True:
            logger.warning('Primary key file mandatory, but not found: {}'.format(files_list[file_index]['dir']))
            pk_criteria = False

        criteria_match = fpa_criteria and u_criteria and pk_criteria and b_criteria
        logger.debug('Criteria: {}'.format(criteria_match))
        if criteria_match == False :
            logger.debug('File does not comply to requirements: file path:{} -> next '.format(files_list[file_index]['dir']))
            file_index += 1

    att['message.batchIndex'] = file_index
    att['message.lastBatch'] = False

    # sort update files
    files_list[file_index]['update_files'] = sorted(files_list[file_index]['update_files'])

    # get table and schema from folder structure
    att['table_name'] = files_list[file_index]['table_name']
    att['schema_name'] = files_list[file_index]['schema_name']
    att['current_file'] = files_list[file_index]

    logger.info('Send File: {} ({}/{})'.format(files_list[file_index]['schema_name'],files_list[file_index]['table_name'],\
                                               file_index, len(files_list)))
    api.send(outports[1]['name'], api.Message(attributes=att,body=files_list[file_index]))
    file_index += 1

    log = log_stream.getvalue()
    if len(log)>0 :
        api.send(outports[0]['name'], log_stream.getvalue())


inports = [{'name': 'files', 'type': 'message.file',"description":"List of files"},
           {'name': 'trigger', 'type': 'message.*',"description":"Trigger"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'file', 'type': 'message.file',"description":"file"},
            {'name': 'limit', 'type': 'message',"description":"Limit"}]


#api.set_port_callback(inports[0]['name'], on_files_process)
#api.set_port_callback(inports[1]['name'], process)


def test_operator() :

    att = {'operator':'collect_files','file':{'path':'/adbd/abd.csv'}}

    files = [
        {
            "dir": "/replication/REPLICATION/TEST_TABLE_17",
            "update_files": [
                "12345_TEST_TABLE_17.csv"
            ],
            "base_file": "TEST_TABLE_17.csv",
            "schema_name": "REPLICATION",
            "table_name": "TEST_TABLE_17",
            "primary_key_file": "TEST_TABLE_17_primary_keys.csv",
            "consistency_file": "",
            "misc": []
        },
        {
            "dir": "/replication/REPLICATION/TEST_TABLE_18",
            "update_files": [],
            "base_file": "TEST_TABLE_18.csv",
            "schema_name": "REPLICATION",
            "table_name": "TEST_TABLE_18",
            "primary_key_file": "",
            "consistency_file": "",
            "misc": []
        },
        {
            "dir": "/replication/REPLICATION2/TEST_TABLE_17",
            "update_files": [
                "12345_TEST_TABLE_17.csv"
            ],
            "base_file": "TEST_TABLE_17.csv",
            "schema_name": "REPLICATION2",
            "table_name": "TEST_TABLE_17",
            "primary_key_file": "TEST_TABLE_17_primary_keys.csv",
            "consistency_file": "",
            "misc": []
        },
        {
            "dir": "/replication/REPLICATION2/TEST_TABLE_18",
            "update_files": [],
            "base_file": "TEST_TABLE_18.csv",
            "schema_name": "REPLICATION2",
            "table_name": "TEST_TABLE_18",
            "primary_key_file": "",
            "consistency_file": "",
            "misc": []
        }
    ]


    on_files_process(api.Message(attributes=att, body=files))
    for i in range(1,len(files)) :
        process(api.Message(attributes=att,body=''))

    for m in api.queue :
        print(m.attributes)
        print(m.body)

if __name__ == '__main__':
    test_operator()
    if True:
        subprocess.run(["rm", '-r','../../../solution/operators/sdi_replication_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name + '_' + api.config.version
        subprocess.run(["vctl", "solution", "bundle",'../../../solution/operators/sdi_replication_' + api.config.version, \
                        "-t", solution_name])
        subprocess.run(["mv", solution_name + '.zip', '../../../solution/operators'])



