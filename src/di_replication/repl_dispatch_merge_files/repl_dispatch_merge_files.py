import sdi_utils.gensolution as gs
import subprocess
import os

import io
import logging

try:
    api
except NameError:
    class api:

        queue = list()

        class Message:
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes
                
        def send(port,msg) :
            if port == outports[1]['name'] :
                api.queue.append(msg)

        class config:
            ## Meta data
            config_params = dict()
            tags = {}
            version = "0.1.0"

            operator_description = "Dispatch Merge Files"
            operator_name = 'repl_dispatch_merge_files'
            operator_description_long = "Dispatch merge files."
            add_readme = dict()


        format = '%(asctime)s |  %(levelname)s | %(name)s | %(message)s'
        logging.basicConfig(level=logging.DEBUG, format=format, datefmt='%H:%M:%S')
        logger = logging.getLogger(name=config.operator_name)



# catching logger messages for separate output
log_stream = io.StringIO()
sh = logging.StreamHandler(stream=log_stream)
sh.setFormatter(logging.Formatter('%(asctime)s |  %(levelname)s | %(name)s | %(message)s', datefmt='%H:%M:%S'))
api.logger.addHandler(sh)


file_index = -1

def on_target(msg) :
    global file_index
    att = dict(msg.attributes)
    att['operator'] = 'repl_dispatch_merge_files_on_target'
    att['message.last_update_file'] = False
    att['target_file'] = True
    api.send(outports[1]['name'], api.Message(attributes=att, body=msg.body))

    # reset when base table received
    file_index = -1

def on_init(msg) :
    global file_index
    # reset when base table received
    file_index = -1
    on_next(msg)

def on_next(msg) :

    global file_index

    att = dict(msg.attributes)
    att['operator'] = 'repl_dispatch_merge_files_on_trigger'

    files_list = msg.attributes['current_file']['update_files']
    file_index += 1
    att['message.index_update'] = file_index
    att['message.index_num'] = len(files_list)
    att['message.last_update_file'] = False
    att['target_file'] = False
    if file_index == len(files_list) - 1 :
        att['message.last_update_file'] = True
    if file_index >= len(files_list) :
        raise ValueError('File index out of bound: {}'.format(att))

    att['file']['path'] = os.path.join(msg.attributes['current_file']['dir'],files_list[file_index])

    api.logger.info('Send File: {} ({}/{})'.format(files_list[file_index],file_index, len(files_list)))
    api.send(outports[1]['name'], api.Message(attributes=att,body=files_list[file_index]))


    log = log_stream.getvalue()
    if len(log)>0 :
        api.send(outports[0]['name'], log_stream.getvalue())

inports = [{'name': 'target', 'type': 'message.file',"description":"Target file"},\
           {'name': 'init', 'type': 'message.file',"description":"Init"},\
           {'name': 'next', 'type': 'message.*',"description":"Next"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'file', 'type': 'message.file',"description":"file"}]


#api.set_port_callback(inports[0]['name'], on_target)
#api.set_port_callback(inports[1]['name'], on_init)
#api.set_port_callback(inports[2]['name'], on_next)


def test_operator() :

    att = {'operator':'collect_files','file':{'path':'/adfg/asdf.cfg'}}
    att['current_file'] = {
            "dir": "/replication/REPLICATION/TEST_TABLE_17",
            "update_files": ["22222_TEST_TABLE_17.csv", "11111_TEST_TABLE_17.csv", "33333_TEST_TABLE_17.csv"],
            "base_file": "TEST_TABLE_17.csv",
            "schema_name": "REPLICATION",
            "table_name": "TEST_TABLE_17",
            "key": "TEST_TABLE_17_primary_keys.csv",
            "consistency": "",
            "misc": []
        }
    att['current_file']['update_files'] = sorted(att['current_file']['update_files'])

    csv = r'''DIREPL_PACKAGEID,DIREPL_PID,DIREPL_TYPE,DIREPL_UPDATED,INDEX,INT_NUM
0,1595842726816,U,2020-07-27T09:38:46.038Z,0,1
0,1595842726816,U,2020-07-27T09:38:46.038Z,3,4
1,1595842726816,U,2020-07-27T09:38:46.038Z,6,7
1,1595842726817,U,2020-07-27T09:38:46.038Z,9,10
2,1595842726817,U,2020-07-27T09:38:46.038Z,12,13
3,1595842726817,U,2020-07-27T09:38:46.038Z,15,16
3,1595842726818,U,2020-07-27T09:38:46.038Z,18,19
4,1595842726818,U,2020-07-27T09:38:46.038Z,21,22
4,1595842726818,U,2020-07-27T09:38:46.038Z,24,25'''

    on_target(api.Message(attributes=att, body=csv))
    for i in range(0,len(att['current_file']['update_files'])) :
        on_next(api.Message(attributes=att,body=''))

    for m in api.queue :
        print(m.attributes)
        print(m.body)

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


