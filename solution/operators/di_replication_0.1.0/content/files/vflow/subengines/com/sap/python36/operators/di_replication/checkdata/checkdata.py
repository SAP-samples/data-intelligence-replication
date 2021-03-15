

import io
import logging
import os

import pandas as pd



log_stream = io.StringIO()
sh = logging.StreamHandler(stream=log_stream)
sh.setFormatter(logging.Formatter('%(asctime)s , %(name)s , %(message)s', datefmt='%H,%M,%S'))
api.logger.addHandler(sh)


def on_input(msg):
    att = dict(msg.attributes)

    ### IF SELECT provided data
    if not msg.body == None:

        # Remove columns 'DIREPL_PID', 'DIREPL_STATUS' and create JSON
        header = [c["name"] for c in msg.attributes['table']['columns']]
        df = pd.DataFrame(msg.body, columns=header).drop(columns=['DIREPL_PID', 'DIREPL_STATUS'])

        num_records = df.shape[0]
        msg.body = df.to_json(orient='records', date_format='%Y%m%d %H:%M:%S')

        # Send to data-outport
        api.send("output", api.Message(attributes=att, body=msg.body))

        # Send to log-outport
        api.logger.info("Data send to file. Records: {}, Data: {}".format(num_records, len(msg.body)))
        api.send('log', log_stream.getvalue())
        log_stream.seek(0)
        log_stream.truncate()

    ### No data from SELECT
    else:
        msg.body = 'NODATA'

        # Send to Nodata-port
        api.send("nodata", msg)

        # Send to log-outport
        api.logger.info("No Data send!")
        api.send('log', log_stream.getvalue())
        log_stream.seek(0)
        log_stream.truncate()


inports = [{'name': 'input', 'type': 'message', "description": "data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'output', 'type': 'message.file', "description": "data"},
            {'name': 'nodata', 'type': 'message.file', "description": "no data"}]

api.set_port_callback("input", on_input)


