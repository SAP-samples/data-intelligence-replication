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



def on_input(msg):

    if 'message.lastBatch' in msg.attributes and msg.attributes['message.lastBatch'] == True:
        api.send("output", msg)
        log('Msg send to outport: Last batch in attributes')
    else:
        log('No message send')

    log('Message Attributes: {}'.format(msg.attributes),level='debug')


api.set_port_callback('input', on_input)

