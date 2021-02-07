
import os
import logging
import io


# catching logger messages for separate output
log_stream = io.StringIO()
sh = logging.StreamHandler(stream=log_stream)
sh.setFormatter(logging.Formatter('%(asctime)s |  %(levelname)s | %(name)s | %(message)s', datefmt='%H:%M:%S'))
api.logger.addHandler(sh)

api.logger.info('Logger setup')

def process(msg):

    att = dict(msg.attributes)
    att['operator'] = 'complete'

    api.logger.info("Process started")

    api.logger.debug('Attributes: {} - {}'.format(str(msg.attributes),str(att)))

    # The constraint of STATUS = 'B' due the case the record was updated in the meanwhile
    sql = 'UPDATE {table} SET \"DIREPL_STATUS\" = \'C\' WHERE  \"DIREPL_PID\" = {pid} AND \"DIREPL_STATUS\" = \'B\''.\
        format(table=att['replication_table'], pid = att['pid'])

    api.logger.info('Update statement: {}'.format(sql))
    att['sql'] = sql

    api.logger.info('Process ended')
    #api.send(outports[1]['name'], update_sql)
    api.send(outports[1]['name'], api.Message(attributes=att,body=sql))

    log = log_stream.getvalue()
    api.send(outports[0]['name'], log )


inports = [{'name': 'data', 'type': 'message.file', "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'msg', 'type': 'message', "description": "msg with sql statement"}]

api.set_port_callback(inports[0]['name'], process)

