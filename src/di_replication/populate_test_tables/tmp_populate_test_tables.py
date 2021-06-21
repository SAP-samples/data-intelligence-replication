#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#


import copy
from datetime import datetime, timezone

import numpy as np
import pandas as pd

operator_name = 'populate_test_tables'

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
    

    for i in range(0, msg.attributes['num_new_tables']):

        att = copy.deepcopy(msg.attributes)
        att['table_name'] = att['table_basename'] + '_' + str(i)

        col1 = np.arange(i, api.config.num_rows+i)
        df = pd.DataFrame(col1, columns=['NUMBER']).reset_index()
        df.rename(columns={'index': 'INDEX'}, inplace=True)
        df['DIREPL_UPDATED'] = datetime.now(timezone.utc).isoformat()
        df['DIREPL_TYPE'] = 'I'
        df['NUM_MOD'] = df['NUMBER']%100
        df['DATETIME'] = datetime.now(timezone.utc) - pd.to_timedelta(df['NUM_MOD'],unit='d')
        df['DATETIME'] = df['DATETIME'].apply(datetime.isoformat)

        # ensure the sequence of the table corresponds to attribute table:columns
        att['table'] = {
            "columns": [{"class": "integer", "name": "INDEX", "nullable": False, "type": {"hana": "BIGINT"}}, \
                        {"class": "integer", "name": "NUMBER", "nullable": True, "type": {"hana": "BIGINT"}},
                        {"class": "timestamp", "name": "DATETIME", "nullable": False, "type": {"hana": "TIMESTAMP"}}, \
                        #{"class": "integer", "name": "DIREPL_PID", "nullable": True, "type": {"hana": "BIGINT"}}, \
                        {"class": "timestamp", "name": "DIREPL_UPDATED", "nullable": True, "type": {"hana": "TIMESTAMP"}}, \
                        #{"class": "string", "name": "DIREPL_STATUS", "nullable": True, "size": 1, "type": {"hana": "NVARCHAR"}}, \
                        {"class": "string", "name": "DIREPL_TYPE", "nullable": True, "size": 1,"type": {"hana": "NVARCHAR"}}], \
                        "version": 1, "name": att['table_name']}
        att['message.batchIndex'] = i
        att['message.lastBatch'] = True if  i == msg.attributes['num_new_tables'] - 1 else False
        log('Attributes: {}'.format(att))

        #df = df[['INDEX', 'NUMBER', 'DATETIME','DIREPL_PID', 'DIREPL_UPDATED', 'DIREPL_STATUS','DIREPL_TYPE']]
        df = df[['INDEX', 'NUMBER', 'DATETIME', 'DIREPL_UPDATED',  'DIREPL_TYPE']]
        table_data = df.values.tolist()
        log('Table inserts sent to: {}'.format(att['table_name']))
        

        api.send("output", api.Message(attributes=att, body=table_data))



api.set_port_callback("input", on_input)
