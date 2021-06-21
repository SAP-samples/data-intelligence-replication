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

    # Create transaction id
    att['pid'] = int(datetime.utcnow().timestamp() * 1000000)

    package_size = int(api.config.package_size)

    # SQL Statement
    table = att['schema_name'] + '.' + att['table_name']
    if package_size > 0:
        sql = 'UPDATE TOP {packagesize} {table} SET \"DIREPL_STATUS\" = \'B\', \"DIREPL_PID\" = {pid} ' \
              'WHERE  \"DIREPL_STATUS\" = \'W\' OR \"DIREPL_STATUS\" IS NULL '.format(packagesize=package_size, table=table, pid=att['pid'])
    else:
        sql = 'UPDATE {table} SET \"DIREPL_STATUS\" = \'B\', \"DIREPL_PID\" = {pid} ' \
              'WHERE  \"DIREPL_STATUS\" = \'W\' OR \"DIREPL_STATUS\" IS NULL '.format(table=table, pid = att['pid'])

    log(sql)

    # Send sql to data
    api.send('output', api.Message(attributes=att, body=sql))


api.set_port_callback('input', on_input)
