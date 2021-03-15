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
            operator_name = 'resetPID'
            operator_description = "Reset PID"

            operator_description_long = "Resets all DIREPL_STATUS = \'W\' for given PIDs."
            add_readme = dict()
            add_readme["References"] = ""

            reset_all = False
            config_params['reset_all'] = {'title': 'Reset All', \
                                          'description':'Resets all records to DIREPL_STATUS = \'W\'',\
                                          'type': 'boolean'}

            pid_list = ''
            config_params['pid_list'] = {'title': 'PID List', \
                                          'description':'List of PIDs to reset.',\
                                          'type': 'string'}


        format = '%(asctime)s |  %(levelname)s | %(name)s | %(message)s'
        logging.basicConfig(level=logging.DEBUG, format=format, datefmt='%H:%M:%S')
        logger = logging.getLogger(name=config.operator_name)


def read_list(text):

    result_list = list()
    #elem_list = text.split(sep)
    elem_list = text.split(',')
    for x in  elem_list:
        elem = int(x.strip())
        result_list.append(elem)

    return result_list

# catching logger messages for separate output
log_stream = io.StringIO()
sh = logging.StreamHandler(stream=log_stream)
sh.setFormatter(logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s', datefmt='%H:%M:%S'))
api.logger.addHandler(sh)


def process(msg):

    att = dict(msg.attributes)
    att['operator'] = 'resetPID'

    if api.config.reset_all :
        sql = 'UPDATE {table} SET \"DIREPL_STATUS\" = \'W\'; '

    pid_list = read_list(api.config.pid_list)
    pid_where_list  = ' OR '.join(['DIREPL_PID = \'{}\''.format(i) for i in pid_list])
    sql = 'UPDATE {table} SET \"DIREPL_STATUS\" = \'W\' WHERE  {pid} ;'. \
        format(table=att['replication_table'], pid=pid_where_list)

    api.logger.info('Update statement: {}'.format(sql))
    att['sql'] = sql

    #api.send(outports[1]['name'], update_sql)
    api.send(outports[1]['name'], api.Message(attributes=att,body=sql))

    api.send(outports[0]['name'], log_stream.getvalue() )
    log_stream.seek(0)


inports = [{'name': 'data', 'type': 'message.file', "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'msg', 'type': 'message', "description": "msg with sql statement"}]

#api.set_port_callback(inports[0]['name'], process)

def test_operator():

    msg = api.Message(attributes={'pid': 123123213, 'table':'REPL_TABLE','base_table':'REPL_TABLE','latency':30,\
                                  'replication_table':'repl_table', 'data_outcome':True,'packageid':1},body='')

    api.config.pid_list = '123, 234, 642, 984'
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

