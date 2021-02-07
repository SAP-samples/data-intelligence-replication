import sdi_utils.gensolution as gs
import os
import subprocess


import logging
import io
import random
from datetime import datetime, timezone
import re

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
            version = '0.1.0'
            tags = {}
            operator_name = 'block'
            operator_description = "Block"

            operator_description_long = "Update replication table status to done."
            add_readme = dict()
            add_readme["References"] = ""

            package_size = 10
            config_params['package_size'] = {'title': 'Package size',
                                           'description': 'Defining the package size that should be picked for replication. '
                                            'This is not used together with \'Pacakge ID\'',
                                           'type': 'integer'}

            change_types = 'UDI'
            config_params['change_types'] = {'title': 'Insert (\'I\'), update (\'U\'), delete (\'D\')' ,
                                       'description': 'Insert (\'I\'), update (\'U\'), delete (\'D\')',
                                       'type': 'string'}

        format = '%(asctime)s |  %(levelname)s | %(name)s | %(message)s'
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
        logger = logging.getLogger(name=config.operator_name)

def get_timestampsuffix(frmt) :
    dt = datetime.utcnow()
    r = re.match('([YMDH])(\d{1})',frmt)
    slice = int(r.group(2))
    if not r :
        return ''
    if r.group(1) == 'H' :
        hour = (int(dt.hour//slice) + 1)*slice
        return datetime(dt.year,dt.month,dt.day,hour).strftime('%Y%m%d_%H')
    if r.group(1) == 'D' :
        day = (int(dt.day//slice) + 1)*slice
        return datetime(dt.year,dt.month,day).strftime('%Y%m%d')
    if r.group(1) == 'M' :
        month = (int(dt.month//slice) + 1)*slice
        return datetime(dt.year,month,dt.day).strftime('%Y%m')
    if r.group(1) == 'Y' :
        year = (int(dt.year//slice) + 1)*slice
        return datetime(year,dt.month,dt.day).strftime('%Y')


# catching logger messages for separate output
log_stream = io.StringIO()
sh = logging.StreamHandler(stream=log_stream)
sh.setFormatter(logging.Formatter('%(asctime)s |  %(levelname)s | %(name)s | %(message)s', datefmt='%H:%M:%S'))
api.logger.addHandler(sh)


def process(msg):

    att = dict(msg.attributes)
    att['operator'] = 'block'

    replication_types = api.config.change_types
    att['insert_type'] = True if 'I' in replication_types else False
    att['update_type'] = True if 'U' in replication_types else False
    att['delete_type'] = True if 'D' in replication_types else False
    api.logger.info('Replication types set: Insert: {}  - Update: {}  - Delete {}'.\
                format(att['insert_type'],att['update_type'],att['delete_type']))

    if not (att['insert_type'] or att['update_type'] or att['delete_type'] ):
        err_stat = 'Replication not set properly: {} (Valid: I,U,D)'.format(replication_types)
        api.logger.error(err_stat)
        api.send(outports[0]['name'], log_stream.getvalue())
        raise ValueError(err_stat)

    api.logger.info('Replication table from attributes: {} {}'.format(att['schema_name'],att['table_name']))

    att['pid'] = int(datetime.utcnow().timestamp()) * 1000 + random.randint(0,1000)

    if 'slice_period' in att :
        att['timestamp_suffix'] = get_timestampsuffix(att['slice_period'])
        api.logger.debug('slice_period -> {}'.format(att['timestamp_suffix']))

    wheresnippet = " \"DIREPL_STATUS\" = \'W\' AND ("
    if att['insert_type'] :
        wheresnippet += " \"DIREPL_TYPE\" = \'I\' OR "
    if att['update_type']:
        wheresnippet += " \"DIREPL_TYPE\" = \'U\' OR "
    if att['delete_type']:
        wheresnippet += " \"DIREPL_TYPE\" = \'D\' OR "
    wheresnippet = wheresnippet[:-3] + ') '

    table = att['schema_name'] + '.' + att['table_name']
    if api.config.package_size > 0 :
        sql = 'UPDATE TOP {packagesize} {table} SET \"DIREPL_STATUS\" = \'B\', \"DIREPL_PID\" = \'{pid}\' WHERE  {ws}' \
            .format(packagesize=api.config.package_size,table=table, pid=att['pid'], ws=wheresnippet)
    else :
        sql = 'UPDATE {table} SET \"DIREPL_STATUS\" = \'B\', \"DIREPL_PID\" = \'{pid}\' WHERE  {ws}' \
            .format(table=table, pid = att['pid'],ws = wheresnippet)

    api.logger.info('Update statement: {}'.format(sql))
    att['sql'] = sql


    api.send(outports[1]['name'], api.Message(attributes=att,body=sql))

    log = log_stream.getvalue()
    log_stream.seek(0)
    if len(log) > 0 :
        api.send(outports[0]['name'], log )


inports = [{'name': 'data', 'type': 'message', "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'msg', 'type': 'message', "description": "msg with sql statement"}]

#api.set_port_callback(inports[0]['name'], process)

def test_operator():
    #api.config.package_size = 100
    msg = api.Message(attributes={'packageid':4711,'table_name':'repl_table','schema_name':'schema',\
                                  'data_outcome':True},body='')
    process(msg)

    for msg in api.queue :
        print(msg.attributes)
        print(msg.body)


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

