import io
import subprocess
import os
import pandas as pd
import io

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

try:
    api
except NameError:
    class api:
        class Message:
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes

        def send(port,msg) :
            if port == outports[1]['name'] :
                print('ATTRIBUTES: ')
                print(msg.attributes)#
                print('CSV-String: ')
                print(msg.body)

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils':''}
            version = "0.0.1"
            operator_name = 'repl_update_file'
            operator_description = "Update Base File"
            operator_description_long = "Checks file on consistency with original table."
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}


att = dict()
df = pd.DataFrame()


def process(msg):
    global df
    global df_list
    global att

    att['operator'] = 'repl_update_file'
    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)

    csv_io = io.BytesIO(msg.body)
    att = dict(msg.attributes)
    udf = pd.read_csv(csv_io)

    dflist = [df] + df_list
    df = pd.concat(dflist,axis=0)
    print(df)


    gdf = df.groupby(by=att['current_primary_keys'])['DIREPL_PID'].max().reset_index()
    df = gdf.join(df, on=att['current_primary_keys'], lsuffix='', rsuffix='_RSFX')
    drop_cols = [cp + '_RSFX' for cp in att['current_primary_keys']]
    drop_cols = drop_cols + ['DIREPL_PID_RSFX']
    df = df.drop(columns=drop_cols)

    if att['message.last_update'] == True :
        df = df[sorted(df.columns)]
        csv = df.to_csv(index=False)
        att['file']['path'] = os.path.join(att['current_file']['dir'], att['current_file']['base_file'])
        api.send(outports[1]['name'],api.Message(attributes=att,body = csv))
    else :
        api.send(outports[2]['name'],api.Message(attributes=att, body=None))

    api.send(outports[0]['name'], log_stream.getvalue())

inports = [{'name': 'data', 'type': 'message.file', "description": "Data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'csv', 'type': 'message.file', "description": "Output data as csv"},
            {'name': 'trigger', 'type': 'message', "description": "Trigger"}]


# api.set_port_callback(inports[0]['name'], on_update)
# api.set_port_callback(inports[1]['name'], on_base)

def test_operator() :
    att = {'operator': 'collect_files', 'file': {'path': '/adbd/abd.csv'},'current_primary_keys':['INDEX'],\
           'message.last_update':False}
    att['current_file'] = {
            "dir": "/replication/REPLICATION/TEST_TABLE_17",
            "update_files": [
                "12345_TEST_TABLE_17.csv"
            ],
            "base_file": "TEST_TABLE_17.csv",
            "schema_name": "REPLICATION",
            "table_name": "TEST_TABLE_17",
            "primary_key_file": "TEST_TABLE_17_primary_keys.csv",
            "consistency_file": "",
            "misc": []
        }
    csv_update1 = r'''DIREPL_PACKAGEID,DIREPL_PID,DIREPL_TYPE,DIREPL_UPDATED,INDEX,INT_NUM
0,1595842726816,U,2020-07-27T09:38:46.038Z,0,1
0,1595842726816,U,2020-07-27T09:38:46.038Z,3,4
1,1595842726816,U,2020-07-27T09:38:46.038Z,6,7'''
    csv_update1 = str.encode((csv_update1))
    csv_update2 = r'''DIREPL_PACKAGEID,DIREPL_PID,DIREPL_TYPE,DIREPL_UPDATED,INDEX,INT_NUM
1,1595842726817,U,2020-07-27T09:38:46.038Z,9,10
2,1595842726817,U,2020-07-27T09:38:46.038Z,12,13
3,1595842726817,U,2020-07-27T09:38:46.038Z,15,16'''
    csv_update2 = str.encode((csv_update2))
    csv_update3 = r'''DIREPL_PACKAGEID,DIREPL_PID,DIREPL_TYPE,DIREPL_UPDATED,INDEX,INT_NUM
3,1595842726818,U,2020-07-27T09:38:46.038Z,18,19
4,1595842726818,U,2020-07-27T09:38:46.038Z,21,22
4,1595842726818,U,2020-07-27T09:38:46.038Z,24,25'''
    csv_update3 = str.encode((csv_update3))

    csv_base = r'''DIREPL_PACKAGEID,DIREPL_PID,DIREPL_TYPE,DIREPL_UPDATED,INDEX,INT_NUM
0,1595842686470,I,2020-07-27T09:38:06.657Z,1,1
0,1595842686470,I,2020-07-27T09:38:06.657Z,2,2
0,1595842686470,I,2020-07-27T09:38:06.657Z,2,3
0,1595842686470,I,2020-07-27T09:38:06.657Z,4,4
1,1595842686470,I,2020-07-27T09:38:06.657Z,5,5
0,1595842686470,I,2020-07-27T09:38:06.657Z,6,6
1,1595842686470,I,2020-07-27T09:38:06.657Z,7,7
1,1595842686470,I,2020-07-27T09:38:06.657Z,8,8
0,1595842686470,I,2020-07-27T09:38:06.657Z,9,9
2,1595842686470,I,2020-07-27T09:38:06.657Z,10,10
2,1595842686470,I,2020-07-27T09:38:06.657Z,11,11
0,1595842686470,I,2020-07-27T09:38:06.657Z,12,12
2,1595842686470,I,2020-07-27T09:38:06.657Z,13,13
2,1595842686470,I,2020-07-27T09:38:06.657Z,14,14
0,1595842686470,I,2020-07-27T09:38:06.657Z,15,15
3,1595842686470,I,2020-07-27T09:38:06.657Z,16,16
3,1595842686470,I,2020-07-27T09:38:06.657Z,17,17
0,1595842686470,I,2020-07-27T09:38:06.657Z,18,18
3,1595842686470,I,2020-07-27T09:38:06.657Z,19,19
4,1595842686470,I,2020-07-27T09:38:06.657Z,20,20
4,1595842686470,I,2020-07-27T09:38:06.657Z,22,22
4,1595842686470,I,2020-07-27T09:38:06.657Z,23,23'''

    csv_base = str.encode(csv_base)

    #csv_base = b"INDEX,NUM_INT,PID\n0,1,1234\n1,2,1234\n2,3,1234\n3,4,1234\n4,5,1234"
    msg_base = api.Message(attributes=att,body=csv_base)
    on_base(msg_base)
    #csv_update = b"INDEX,NUM_INT,PID\n0,5,1111"
    #on_update(api.Message(attributes=att,body=csv_update))
    #csv_update = b"INDEX,NUM_INT,PID\n2,5,2222\n3,6,2222"
    #on_update(api.Message(attributes=att,body=csv_update))
    #csv_update = b"INDEX,NUM_INT,PID\n4,7,2222\n5,8,3333"
    on_update(api.Message(attributes=att,body=csv_update1))
    on_update(api.Message(attributes=att,body=csv_update2))
    att['message.last_update'] = True
    on_update(api.Message(attributes=att,body=csv_update3))


if __name__ == '__main__':
    test_operator()
    if True :
        subprocess.run(["rm", '-r','../../../solution/operators/sdi_replication_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name + '_' + api.config.version
        subprocess.run(["vctl", "solution", "bundle",'../../../solution/operators/sdi_replication_' + api.config.version, \
                        "-t", solution_name])
        subprocess.run(["mv", solution_name + '.zip', '../../../solution/operators'])
