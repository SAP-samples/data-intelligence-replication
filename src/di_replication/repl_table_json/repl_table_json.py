
import sdi_utils.gensolution as gs
import subprocess
import os

import io
import logging
import pandas as pd
import json
import re



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
            operator_name = 'repl_table_json'
            operator_description = "table to json"
            operator_description_long = "Converts table to json stream."
            add_readme = dict()
            debug_mode = True

            drop_columns = 'None'
            config_params['drop_columns'] = {'title': 'Drop Columns',
                                           'description': 'List of columns to drop.',
                                           'type': 'string'}

        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
        logger = logging.getLogger(name=config.operator_name)

def number_test(str) :
    if str :
        if re.match('True',str) :
            return True
        elif re.match('False', str):
            return False
        elif re.match('None',str) :
            return None
        elif str.isdigit() :
            return int(str)
        else :
            try :
                f = float(str)
                return f
            except :
                return str
    else :
        return str

#### split text with quotes
# 1. find quote indices and separator indices
# 2. ignore all separator indices within quotes (only even number of quote indices less than sep index)
def split_text(text,sep) :
    sep_indices = [m.start() for m in re.finditer(sep, text)]
    quote_indices = [m.start() for m in re.finditer("'", text) if m.start() == 0 or text[m.start()-1] != '\\' ]

    list_seps = list()
    for s_index in sep_indices :
        if not len([ mi for mi in quote_indices if mi < s_index]) % 2 :
            list_seps.append(s_index)

    list_texts = list()
    last_index = 0
    for i,s_index in enumerate(list_seps) :
        if last_index == 0 :
            list_texts.append(text[:s_index].strip())
        else :
            list_texts.append(text[last_index+1:s_index].strip())
        last_index = s_index
    if last_index == 0 :
        list_texts.append(text[:].strip())
    else:
        list_texts.append(text[last_index + 1:].strip())

    return list_texts

#### READ LIST
def read_list(text,value_list=None,sep = ',',modifier_list_not=None,test_number = True, ignore_quotes = False):

    if not text or text.upper() == 'NONE':
        return None

    if not modifier_list_not:
        modifier_list_not = ['!', '~', 'not', 'Not', 'NOT']

    #text = text.strip()

    # test for all
    if len(text) < 4 and ('all' in text or 'All' in text or 'ALL' in text) :
        return value_list

    negation = False
    # Test for Not
    if len(text) > 1 and text[0] in modifier_list_not :
        text = text[1:]
        negation = True
    elif len(text) > 3 and text[:3] in modifier_list_not :
        text = text[4:].strip()
        negation = True

    result_list = list()
    #elem_list = text.split(sep)
    elem_list = split_text(text, sep)
    for x in  elem_list:
        if ignore_quotes :
            elem = x.strip()
        else:
            elem = x.strip().strip("'\"")
        if test_number :
            elem = number_test(elem)
        result_list.append(elem)

    if negation :
        if not isinstance(value_list,list) :
            value_list = list(value_list)
        if not value_list  :
            raise ValueError("Negation needs a value list to exclude items")
        result_list = [x for x in value_list if x not in result_list]

    return result_list

# catching logger messages for separate output
log_stream = io.StringIO()
sh = logging.StreamHandler(stream=log_stream)
sh.setFormatter(logging.Formatter('%(asctime)s |  %(levelname)s | %(name)s | %(message)s', datefmt='%H:%M:%S'))
api.logger.addHandler(sh)


def process(msg):

    att = dict(msg.attributes)
    att['operator'] = 'repl_table_json'

    header = [c["name"] for c in msg.attributes['table']['columns']]
    df = pd.DataFrame(msg.body, columns=header)

    att['data_outcome'] = True

    drop_columns = read_list(api.config.drop_columns)
    if drop_columns:
        api.logger.info('Drop columns: {}'.format(drop_columns))
        df = df.drop(columns=drop_columns)

    if df.shape[0] == 0 :
        att['data_outcome'] = False
        api.send(outports[2]['name'], api.Message(attributes=att, body=att['data_outcome']))
        api.logger.info('No data received, msg to port error_status sent.')
        api.send(outports[0]['name'], log_stream.getvalue())
        return 0

    # always sort the columns alphabetically because DB columns do not have an order
    df = df[sorted(df.columns)]

    data_str = df.to_json(orient='records',date_format='%Y%m%d %H:%M:%S')


    att["file"] = {"connection": {"configurationType": "Connection Management", "connectionID": "unspecified"}, \
                   "path": "open", "size": 0}

    api.logger.info('CSV-table: {}.{} ({} - {})'.format(att['schema_name'], att['table_name'], df.shape[0], df.shape[1]))
    api.logger.debug('First to rows: {}'.format(df.head(2)))

    msg = api.Message(attributes=att, body=data_str)
    api.send(outports[1]['name'], msg)

    log = log_stream.getvalue()
    if len(log) > 0:
        api.send(outports[0]['name'], log_stream.getvalue())


inports = [{'name': 'data', 'type': 'message.table',"description":"Input message with table"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'json', 'type': 'message.file',"description":"Output data as csv"},\
            {'name': 'error', 'type': 'message',"description":"Error status"}]


#api.set_port_callback(inports[0]['name'], process)

def test_operator() :
    #api.config.drop_header = False
    #api.config.only_header = True

    attributes = {"table":{"columns":[{"class":"string","name":"header1","nullable":True,"size":80,"type":{"hana":"NVARCHAR"}},
                                      {"class":"string","name":"header2","nullable":True,"size":3,"type":{"hana":"NVARCHAR"}},
                                      {"class":"string","name":"header3","nullable":True,"size":10,"type":{"hana":"NVARCHAR"}}],
                           "name":"test.table","version":1},
                  'base_table':'TABLE','schema_name':'schema','table_name':'table'}
    table = [ [(j*3 + i) for i in range(0,3)] for j in range (0,5)]
    msg = api.Message(attributes=attributes, body=table)
    print(table)
    process(msg)
    process(msg)
    process(msg)

    for m in api.queue :
        print(m.body)



if __name__ == '__main__':
    test_operator()
    if True :
        subprocess.run(["rm", '-r', './solution/operators/di_replication_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name + '_' + api.config.version
        subprocess.run(
            ["vctl", "solution", "bundle", './solution/operators/di_replication_' + api.config.version, "-t",
             solution_name])
        subprocess.run(["mv", solution_name + '.zip', './solution/operators'])
