#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#


import copy
from datetime import datetime

operator_name = 'complete'

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

def on_input(msg):

    att = copy.deepcopy(msg.attributes)

    table = att['schema_name'] + '.' + att['table_name']

    # Create SQL-statement
    sql = 'UPDATE {table} SET \"DIREPL_STATUS\" = \'C\' WHERE  \"DIREPL_PID\" = {pid}'.\
        format(table=table, pid = att['pid'])

    log(sql)

    # Send to data-outport
    api.send('output', api.Message(attributes=att,body=sql))


api.set_port_callback('input', on_input)
