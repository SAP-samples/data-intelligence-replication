import sdi_utils.gensolution as gs
import subprocess

import os
import logging
import io


try:
    api
except NameError:
    class api:

        queue = list()

        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if port == outports[1]['name']:
                api.queue.append(msg)

        class config:
            ## Meta data
            config_params = dict()
            version = '0.0.1'
            tags = {}
            operator_name = 'complete'
            operator_description = "Complete"

            operator_description_long = "Update replication table status to complete."
            add_readme = dict()
            add_readme["References"] = ""

        format = '%(asctime)s |  %(levelname)s | %(name)s | %(message)s'
        logging.basicConfig(level=logging.DEBUG, format=format, datefmt='%H:%M:%S')
        logger = logging.getLogger(name=config.operator_name)



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

#api.set_port_callback(inports[0]['name'], process)

def test_operator():

    msg = api.Message(attributes={'pid': 123123213, 'table':'REPL_TABLE','base_table':'REPL_TABLE','latency':30,\
                                  'replication_table':'repl_table', 'data_outcome':True,'packageid':1},body='')
    process(msg)

    for st in api.queue :
        print(st)


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

