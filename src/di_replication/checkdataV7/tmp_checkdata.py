

import io
import logging
import binascii
from datetime import datetime
import json
import pandas as pd

operator_name = 'checkdata'

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
    att = dict(msg.attributes)

    ### IF SELECT provided data
    if not msg.body == None:

        # Remove columns 'DIREPL_PID', 'DIREPL_STATUS' and create JSON
        header = [c["name"] for c in msg.attributes['table']['columns']]
        df = pd.DataFrame(msg.body, columns=header).drop(columns=['DIREPL_PID', 'DIREPL_STATUS'])

        varbinary_cols = [c["name"] for c in msg.attributes['table']['columns'] if c['type']['hana'] == 'VARBINARY' ]

        decode_successful = dict()
        for vb in varbinary_cols :
            decode_successful[vb] = False
            for codec in api.config.codecs:
                try:
                    df[vb] = df[vb].str.decode(codec)
                except :
                    log('Decode failed: \'{}\''.format(codec), 'warning')
                    continue
                log('Decode successful: \'{}\''.format(codec))
                decode_successful[vb]  = True
                break
            if not decode_successful[vb] :
                log('Decode with utf-8 and ignore errors: \'{}\''.format(vb))
                try :
                    df[vb] = df[vb].str.decode('utf-8','ignore')
                    decode_successful[vb] = True
                except :
                    log('Decode failed completly: \'{}\''.format(vb), 'error')

        if len(decode_successful) == 0 or all(v for v in decode_successful.values()) :
            msg.body = df.to_json(orient='records', date_format='%Y%m%d %H:%M:%S')
        else :
            log('Due to failed decoding no transformation!')
            msg.body = 'NODATA due to DATA ERROR'
            # Send to Nodata-port
            api.send("nodata", msg)
            # Send to log-outport
            log("No Data send!")
            return

        # Send to data-outport
        api.send("output", api.Message(attributes=att, body=msg.body))

        # Send to log-outport
        log("Data send to file. Records: {}, Data: {}".format(df.shape[0], len(msg.body)))


    ### No data from SELECT
    else:
        msg.body = 'NODATA'
        # Send to Nodata-port
        api.send("nodata", msg)
        # Send to log-outport
        log("No Data send!")



inports = [{'name': 'input', 'type': 'message', "description": "data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'output', 'type': 'message.file', "description": "data"},
            {'name': 'nodata', 'type': 'message.file', "description": "no data"}]

api.set_port_callback("input", on_input)