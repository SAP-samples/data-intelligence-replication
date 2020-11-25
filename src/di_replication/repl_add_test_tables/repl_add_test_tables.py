import sdi_utils.gensolution as gs

import subprocess
import logging
import os
import io
from datetime import datetime, timezone, timedelta
import pandas as pd

pd.set_option('mode.chained_assignment',None)

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
            tags = {'sdi_utils': ''}
            operator_name = 'repl_add_test_tables'
            operator_description = "Add Test Tables to Repository Table"

            operator_description_long = "Add Test Tables to Repository Table (SQL Statement)"
            add_readme = dict()
            add_readme["References"] = ""

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

        logger = logging.getLogger(name=config.operator_name)

# catching logger messages for separate output
log_stream = io.StringIO()
sh = logging.StreamHandler(stream=log_stream)
sh.setFormatter(logging.Formatter('%(asctime)s ;  %(levelname)s ; %(name)s ; %(message)s', datefmt='%H:%M:%S'))
api.logger.addHandler(sh)

def process(msg):

    att = dict(msg.attributes)
    att['operator'] = 'repl_add_test_tables'

    api.logger.info("Process started. Logging level: {}".format(api.logger.level))

    now_str = datetime.now(timezone.utc).isoformat()
    rec = [[att['table']['name'],'NUMBER',now_str]]

    att['table'] =  {"columns": [
        {"class": "string", "name": "TABLE_NAME", "nullable": False, "size": 100, "type": {"hana": "NVARCHAR"}},
        {"class": "string", "name": "CHECKSUM_COL", "nullable": True, "size": 100, "type": {"hana": "NVARCHAR"}},
        {"name": "TABLE_UPDATED", "nullable": True, "type": {"hana": "TIMESTAMP"}}],
               "name": "REPLICATION.TABLE_REPOSITORY", "version": 1}

    api.send(outports[1]['name'], api.Message(attributes=att, body=rec))
    api.send(outports[0]['name'], log_stream.getvalue())
    log_stream.seek(0)
    log_stream.truncate()

    api.logger.debug('Process ended')
    api.send(outports[0]['name'], log_stream.getvalue())


inports = [{'name': 'data', 'type': 'message.table', "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'data', 'type': 'message.table', "description": "data"}]

#api.set_port_callback(inports[0]['name'], process)

def test_operator():
    msg = api.Message(attributes={'packageid':4711,'table':{'name':'repl_table'}},body='')
    process(msg)

    for st in api.queue :
        print(st.attributes)
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

