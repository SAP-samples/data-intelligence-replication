import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

import subprocess
import logging
import os
import random
from datetime import datetime, timezone
import pandas as pd

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
            tags = {'sdi_utils': ''}
            operator_name = 'repl_block'
            operator_description = "Repl. Block"

            operator_description_long = "Update replication table status to done."
            add_readme = dict()
            add_readme["References"] = ""

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            package_size = False
            config_params['package_size'] = {'title': 'Package size',
                                           'description': 'Defining the package size that should be picked for replication. '
                                            'This is not used together with \'Pacakge ID\'',
                                           'type': 'integer'}

            use_package_id = False
            config_params['use_package_id'] = {'title': 'Using Package ID',
                                           'description': 'Using Package ID rather than generated packages by package size',
                                           'type': 'boolean'}

            change_types = 'UD'
            config_params['change_types'] = {'title': 'Insert (\'I\'), update (\'U\'), delete (\'D\')' ,
                                       'description': 'Insert (\'I\'), update (\'U\'), delete (\'D\')',
                                       'type': 'string'}



def process(msg):

    att = dict(msg.attributes)
    att['operator'] = 'repl_block'

    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)

    replication_types = api.config.change_types
    att['insert_type'] = True if 'I' in replication_types else False
    att['update_type'] = True if 'U' in replication_types else False
    att['delete_type'] = True if 'D' in replication_types else False
    logger.info('Replication types set: Insert: {}  - Update: {}  - Delete {}'.\
                format(att['insert_type'],att['update_type'],att['delete_type']))

    if not (att['insert_type'] or att['update_type'] or att['delete_type'] ):
        err_stat = 'Replication not set properly: {} (Valid: I,U,D)'.format(replication_types)
        logger.error(err_stat)
        api.send(outports[0]['name'], log_stream.getvalue())
        raise ValueError(err_stat)

    logger.info('Replication table from attributes: {} {}'.format(att['schema_name'],att['table_name']))

    att['pid'] = int(datetime.utcnow().timestamp()) * 1000 + random.randint(0,1000)

    wheresnippet = " \"DIREPL_STATUS\" = \'W\' AND ("
    if att['insert_type'] :
        wheresnippet += " \"DIREPL_TYPE\" = \'I\' OR "
    if att['update_type']:
        wheresnippet += " \"DIREPL_TYPE\" = \'U\' OR "
    if att['delete_type']:
        wheresnippet += " \"DIREPL_TYPE\" = \'D\' OR "
    wheresnippet = wheresnippet[:-3] + ') '

    table = att['schema_name'] + '.' + att['table_name']
    if  api.config.use_package_id :
        sql = 'UPDATE {table} SET \"DIREPL_STATUS\" = \'B\', \"DIREPL_PID\" = \'{pid}\' WHERE ' \
                     '\"DIREPL_PACKAGEID\" = (SELECT min(\"DIREPL_PACKAGEID\") ' \
                     'FROM {table} WHERE  {ws}) AND {ws}' \
            .format(table=table, pid = att['pid'],ws = wheresnippet)
    elif api.config.package_size > 0 :
        sql = 'UPDATE TOP {packagesize} {table} SET \"DIREPL_STATUS\" = \'B\', \"DIREPL_PID\" = \'{pid}\' WHERE  {ws}' \
            .format(packagesize=api.config.package_size,table=table, pid=att['pid'], ws=wheresnippet)
    else :
        sql = 'UPDATE {table} SET \"DIREPL_STATUS\" = \'B\', \"DIREPL_PID\" = \'{pid}\' WHERE  {ws}' \
            .format(table=table, pid = att['pid'],ws = wheresnippet)

    logger.info('Update statement: {}'.format(sql))
    att['sql'] = sql


    api.send(outports[1]['name'], api.Message(attributes=att,body=sql))

    log = log_stream.getvalue()
    if len(log) > 0 :
        api.send(outports[0]['name'], log )


inports = [{'name': 'data', 'type': 'message', "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'msg', 'type': 'message', "description": "msg with sql statement"}]

#api.set_port_callback(inports[0]['name'], process)

def test_operator():
    #api.config.package_size = 100
    msg = api.Message(attributes={'packageid':4711,'table_name':'repl_table','schema_name':'schema',\
                                  'data_outcome':True},body='')
    process(msg)

    for msg in api.queue :
        print(msg.attributes)
        print(msg.body)


if __name__ == '__main__':
    test_operator()
    if True:
        basename = os.path.basename(__file__[:-3])
        package_name = os.path.basename(os.path.dirname(os.path.dirname(__file__)))
        project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        solution_name = '{}_{}'.format(basename,api.config.version)
        package_name_ver = '{}_{}'.format(package_name,api.config.version)
        solution_dir = os.path.join(project_dir,'solution/operators',package_name_ver)
        solution_file = os.path.join(solution_dir,solution_name+'.zip')

        subprocess.run(["rm", '-r',solution_file])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)

        subprocess.run(["vctl", "solution", "bundle", solution_dir, "-t", solution_file])
        subprocess.run(["mv", solution_file, os.path.join(project_dir,'solution/operators')])
        logging.info(f"Solution created: {solution_file}")

