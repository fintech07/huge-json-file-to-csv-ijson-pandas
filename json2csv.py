import sys
import pandas as pd
import ijson
import csv

def check_header_key(headers, key):
    if (len(key) < 1):
        return False
    if (get_index_of_list(headers, key) > 0):
        return False
    return True

def get_header_list_of_list(y):
    out = []

    def flatten(x, name=''):
        if type(x) is dict:
            if (check_header_key(out, name[5:-1])):
                out.extend([name[5:-1]])
            for a in x:
                flatten(x[a], name + a + '.')
        elif type(x) is list:
            for a in x:
                flatten(a, name + 'item.')
        else:
            if (check_header_key(out, name[5:-1])):
                out.extend([name[5:-1]])

    flatten(y)
    return out

def release_list(a):
    del a[:]
    del a

def get_header_keys_from_file(infile):
    with open(infile, 'r') as f:
        columns = list(ijson.items(f, ''))
        headers = get_header_list_of_list(columns)
        release_list(columns)
        return headers

def get_index_of_list(_list, _key):
    try:
        return _list.index(_key)
    except ValueError:
        return -1

def initial_record(infile, headers):
    record = dict.fromkeys(headers)
    f = open(infile, 'r')
    json_obj = next(ijson.items(f, ''))
    for item in json_obj:
        if type(json_obj[item]) is int:
            record[item] = json_obj[item]
        elif type(json_obj[item]) is str:
            record[item] = json_obj[item]
    return record

def write_csv(df, headers, infile):
    record = dict.fromkeys(headers)
    for item in record:
        record[item] = item.replace('.item', '')
    df.rename(columns=record, inplace=True)
    df.to_csv(infile.replace('json', 'csv'))

def convert(infile):
    csv_headers = get_header_keys_from_file(infile)
    irecord = initial_record(infile, csv_headers)
    
    df = pd.DataFrame([irecord], columns=csv_headers)

    record = irecord
    with open(infile, 'r') as f:
        parser = ijson.parse(f)
        for prefix, event, value in parser:
            if prefix == 'measurements.item':
                if event == 'start_map':
                    record = irecord
                elif event == 'end_map':
                    df_ = pd.DataFrame([record], columns=csv_headers)
                    df = pd.concat([df, df_]).reset_index(drop=True)


            if (value is not None and event is not 'map_key'):
                if get_index_of_list(csv_headers, prefix) > 0:
                    record[prefix] = value
    write_csv(df, csv_headers, infile)
          

if __name__=='__main__':
    if len(sys.argv) < 2:
        print('please type the json file name or path')
        exit(0)
    else:
        convert(sys.argv[1])