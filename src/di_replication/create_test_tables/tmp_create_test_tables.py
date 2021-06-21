#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#


import copy
from datetime import datetime

operator_name = 'create_test_tables'

def log(log_str,level='info') :
    if level == 'debug' :
        api.logger.debug(log_str)
    elif level == 'warning':
        api.logger.warning(log_str)
    elif level == 'error':
        api.logger.error(log_str)
    else :
        api.logger.info(log_str)

    now = datetime.now().strftime('%H:%M:%S')
    api.send('log','{} | {} | {} | {}'.format(now,level,operator_name,log_str))


import pandas as pd

pd.set_option('mode.chained_assignment',None)


def on_gen():

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
        log("Drop table: {}".format(sql))

        api.send('output', api.Message(attributes=att, body=sql))

    # CREATE TABLES and add to table repos
    for i in range (0,api.config.num_new_tables) :

        table_name = api.config.base_tablename + '_' + str(i)
        sql = "CREATE COLUMN TABLE {table} (\"INDEX\" BIGINT , \"NUMBER\" BIGINT,  \"DATETIME\" TIMESTAMP, "\
              "\"DIREPL_PID\" BIGINT , \"DIREPL_UPDATED\" LONGDATE, " \
              "\"DIREPL_STATUS\" NVARCHAR(1), \"DIREPL_TYPE\" NVARCHAR(1), " \
              "PRIMARY KEY (\"INDEX\",\"DIREPL_UPDATED\"));".format(table = table_name )
        log('Create Table: {}'.format(sql))
        att = {"table_name": table_name,
               'message.batchIndex': i,
               'message.lastBatch': False,
               'sql': sql,
               'table_basename': api.config.base_tablename,
               'num_new_tables': api.config.num_new_tables}

        api.send('output', api.Message(attributes=att, body=sql))



    #TRUNCATE TABLE REPOS
    sql = 'TRUNCATE TABLE {}'.format(api.config.table_repos)
    log('TRUNCATE Table REPOSITORY: {}'.format(sql))
    att = {"table_name": table_name,
           'message.batchIndex': i,
           'message.lastBatch': False,
           'sql': sql,
           'table_basename': api.config.base_tablename,
           'num_new_tables': api.config.num_new_tables}
    api.send('output', api.Message(attributes=att, body=sql))


    # ADD TABLES to Table Repository
    for i in range (0,api.config.num_new_tables) :
        lastbatch = False if not i == api.config.num_new_tables - 1 else True

        table_name = api.config.base_tablename + '_' + str(i)
        sql = "INSERT INTO {} VALUES(\'{}\')".format(api.config.table_repos,table_name)
        log('INSERT Table into Table Repository: {}'.format(sql))
        att = {"table_name": table_name,
               'message.batchIndex': i,
               'message.lastBatch': lastbatch,
               'sql': sql,
               'table_basename': api.config.base_tablename,
               'num_new_tables': api.config.num_new_tables}

        api.send('output', api.Message(attributes=att, body=sql))


api.add_generator(on_gen)
