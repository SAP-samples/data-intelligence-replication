import sdi_utils.gensolution as gs
import subprocess
import os

import logging
import io

try:
    api
except NameError:
    class api:

        sql_queue = list()
        csv_queue = list()

        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if port == outports[1]['name']:
                api.sql_queue.append(msg)


        class config:
            ## Meta data
            config_params = dict()
            version = '0.0.1'
            tags = {'sdi_utils': ''}
            operator_name = 'repl_max_index_test_tables'
            operator_description = "Max Index Test Tables"

            operator_description_long = "SQL for maximum index."
            add_readme = dict()
            add_readme["References"] = ""

        logger = logging.getLogger(name=config.operator_name)

# catching logger messages for separate output
log_stream = io.StringIO()
sh = logging.StreamHandler(stream=log_stream)
sh.setFormatter(logging.Formatter('%(asctime)s |  %(levelname)s | %(name)s | %(message)s', datefmt='%H:%M:%S'))
api.logger.addHandler(sh)

def process(msg):

    att = dict(msg.attributes)
    operator_name = 'repl_max_index_test_tables'

    sql = 'SELECT MAX("INDEX") FROM {table}'.format(table = att['replication_table'])

    api.logger.info('SQL statement: {}'.format(sql))
    att['sql'] = sql

    api.send(outports[1]['name'], api.Message(attributes=att,body=sql))
    api.send(outports[0]['name'], log_stream.getvalue())


inports = [{'name': 'data', 'type': 'message', "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'sql', 'type': 'message', "description": "msg with sql"}]

#api.set_port_callback(inports[0]['name'], process)

def test_operator():
    api.config.off_set = 2
    api.config.num_rows = 10
    msg = api.Message(attributes={'packageid':4711,'replication_table':'repl_table'},body='')
    process(msg)


    for st in api.sql_queue :
        print(st.attributes)
        print(st.body)

    for st in api.csv_queue :
        print(st.body)


if __name__ == '__main__':
    test_operator()
    if True:
        basename = os.path.basename(__file__[:-3])
        package_name = os.path.basename(os.path.dirname(os.path.dirname(__file__)))
        project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        solution_name = '{}_{}.zip'.format(basename,api.config.version)
        package_name_ver = '{}_{}'.format(package_name,api.config.version)

        solution_dir = os.path.join(project_dir,'solution/operators',package_name_ver)
        solution_file = os.path.join(project_dir,'solution/operators',solution_name)

        # rm solution directory
        subprocess.run(["rm", '-r',solution_dir])

        # create solution directory with generated operator files
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)

        # Bundle solution directory with generated operator files
        subprocess.run(["vctl", "solution", "bundle", solution_dir, "-t",solution_file])

