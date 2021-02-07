
import os

import pandas as pd
import logging
import io


# catching logger messages for separate output
log_stream = io.StringIO()
sh = logging.StreamHandler(stream=log_stream)
sh.setFormatter(logging.Formatter('%(asctime)s |  %(levelname)s | %(name)s | %(message)s', datefmt='%H:%M:%S'))
api.logger.addHandler(sh)


def process(msg) :
    att = dict(msg.attributes)
    att['operator'] = 'primarykey_sql'

    att['table_name'] = os.path.basename(os.path.dirname(att['file']['path']))

    sql = 'SELECT \"SCHEMA_NAME\", \"TABLE_NAME", \"COLUMN_NAME\" from SYS.\"CONSTRAINTS\" WHERE ' \
          '\"SCHEMA_NAME\" = \'{schema}\' AND \"TABLE_NAME\" = \'{table}\' AND \"IS_PRIMARY_KEY\" = \'TRUE\'' \
        .format(schema=api.config.schema, table=att['table_name'])

    api.logger.info("Send msg: {}".format(sql))
    api.send(outports[1]['name'], api.Message(attributes=att, body=sql))
    api.send(outports[0]['name'], log_stream.getvalue())

    log_stream.seek(0)

inports = [{'name': 'filename', 'type': 'message.file',"description":"filename"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'sqlkeys', 'type': 'message',"description":"sql keys"}]


api.set_port_callback(inports[0]['name'], process)

