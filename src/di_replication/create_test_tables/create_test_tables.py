import sdi_utils.gensolution as gs


import subprocess
import logging
import os
import io

import pandas as pd


pd.set_option('mode.chained_assignment',None)

try:
    api
except NameError:
    class api:

        sql_queue = list()

        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if port == outports[1]['name']:
                api.sql_queue.append(msg)


        class config:
            ## Meta data
            config_params = dict()
            version = '0.1.0'
            tags = {}
            operator_name = 'create_test_tables'
            operator_description = "Create Test Tables"

            operator_description_long = "Create Test Tables."
            add_readme = dict()
            add_readme["References"] = ""


            base_tablename = 'REPLICATION.TEST_TABLE_'
            config_params['base_tablename'] = {'title': 'Base Tablename',
                                           'description': 'Base tablename.',
                                           'type': 'string'}
            num_new_tables = 5
            config_params['num_new_tables'] = {'title': 'Number of new tables',
                                           'description': 'Number of new tables.',
                                           'type': 'integer'}

            num_drop_tables = 5
            config_params['num_drop_tables'] = {'title': 'Number of existing tables to drop',
                                           'description': 'Number of existing tables to drop.',
                                           'type': 'integer'}

            table_repos = 'REPLICATION.TABLE_REPOS_BASIC'
            config_params['table_repos'] = {'title': 'Table Repository',
                                           'description': 'Table Repository',
                                           'type': 'string'}


        logger = logging.getLogger(name=config.operator_name)

# catching logger messages for separate output
log_stream = io.StringIO()
sh = logging.StreamHandler(stream=log_stream)
sh.setFormatter(logging.Formatter('%(asctime)s ;  %(levelname)s ; %(name)s ; %(message)s', datefmt='%H:%M:%S'))
api.logger.addHandler(sh)

def process():

    operator_name = 'create_test_tables'

    # DROP TABLES
    for i in range(0, api.config.num_drop_tables):

        table_name = api.config.base_tablename + '_' + str(i)
        sql = "DROP TABLE {table}".format(table = table_name)
        att = {"table_name": table_name,
               'message.batchIndex': i,
               'message.lastBatch': False,
               'sql': sql,
               'table_basename': api.config.base_tablename,
               'num_new_tables': api.config.num_new_tables}
        api.logger.info("Drop table: {}".format(sql))

        api.send(outports[1]['name'], api.Message(attributes=att, body=sql))
        api.send(outports[0]['name'], log_stream.getvalue())
        log_stream.seek(0)
        log_stream.truncate()

    # CREATE TABLES and add to table repos
    for i in range (0,api.config.num_new_tables) :

        table_name = api.config.base_tablename + '_' + str(i)
        sql = "CREATE COLUMN TABLE {table} (\"INDEX\" BIGINT , \"NUMBER\" BIGINT,  \"DATETIME\" TIMESTAMP, "\
              "\"DIREPL_PID\" BIGINT , \"DIREPL_UPDATED\" LONGDATE, " \
              "\"DIREPL_STATUS\" NVARCHAR(1), \"DIREPL_TYPE\" NVARCHAR(1), " \
              "PRIMARY KEY (\"INDEX\",\"DIREPL_UPDATED\"));".format(table = table_name )
        api.logger.info('Create Table: {}'.format(sql))
        att = {"table_name": table_name,
               'message.batchIndex': i,
               'message.lastBatch': False,
               'sql': sql,
               'table_basename': api.config.base_tablename,
               'num_new_tables': api.config.num_new_tables}

        api.send(outports[1]['name'], api.Message(attributes=att, body=sql))
        api.send(outports[0]['name'], log_stream.getvalue())
        log_stream.seek(0)
        log_stream.truncate()


    #TRUNCATE TABLE REPOS
    sql = 'TRUNCATE TABLE {}'.format(api.config.table_repos)
    api.logger.info('TRUNCATE Table REPOSITORY: {}'.format(sql))
    att = {"table_name": table_name,
           'message.batchIndex': i,
           'message.lastBatch': False,
           'sql': sql,
           'table_basename': api.config.base_tablename,
           'num_new_tables': api.config.num_new_tables}
    api.send(outports[1]['name'], api.Message(attributes=att, body=sql))


    # ADD TABLES to Table Repository
    for i in range (0,api.config.num_new_tables) :
        lastbatch = False if not i == api.config.num_new_tables - 1 else True

        table_name = api.config.base_tablename + '_' + str(i)
        sql = "INSERT INTO {} VALUES(\'{}\',\'H1\')".format(api.config.table_repos,table_name)
        api.logger.info('INSERT Table into Table Repository: {}'.format(sql))
        att = {"table_name": table_name,
               'message.batchIndex': i,
               'message.lastBatch': lastbatch,
               'sql': sql,
               'table_basename': api.config.base_tablename,
               'num_new_tables': api.config.num_new_tables}

        api.send(outports[1]['name'], api.Message(attributes=att, body=sql))
        api.send(outports[0]['name'], log_stream.getvalue())
        log_stream.seek(0)
        log_stream.truncate()



inports = [{'name': 'input', 'type': 'message', "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'sql', 'type': 'message', "description": "msg with sql"}]

#api.add_generator( process)

def test_operator():
    api.config.off_set = 2
    api.config.num_rows = 10
    msg = api.Message(attributes={'table_name':'repl_table'},body='')
    process()

    for st in api.sql_queue :
        print(st.attributes)
        print(st.body)



if __name__ == '__main__':
    test_operator()
    if True:
        basename = os.path.basename(__file__[:-3])
        package_name = os.path.basename(os.path.dirname(os.path.dirname(__file__)))
        project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        solution_name = '{}_{}.zip'.format(basename, api.config.version)
        package_name_ver = '{}_{}'.format(package_name, api.config.version)

        solution_dir = os.path.join(project_dir, 'solution/operators', package_name_ver)
        solution_file = os.path.join(project_dir, 'solution/operators', solution_name)

        # rm solution directory
        subprocess.run(["rm", '-r', solution_dir])

        # create solution directory with generated operator files
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)

        # Bundle solution directory with generated operator files
        subprocess.run(["vctl", "solution", "bundle", solution_dir, "-t", solution_file])

