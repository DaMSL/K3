#!/usr/bin/env python3
#
# Change json output to be human-readable
import argparse
import csv
import json
import re

def convert_dict(d):
    # for addresses, options, records, etc, just dereference
    if "type" in d and d["type"] in ["address", "option_or_ind", "record", "tuple", "Collection", "Map", "Seq", "Set"]:
        return convert_any(d["value"])
    # change record mapping back to tuple
    elif "r1" in d:
        res = []
        max = 0
        for key in d:
            k = int(key[1:])
            if k > max:
                max = k
        for i in range(max): # +1 for record fields
            res.append(convert_any(d["r" + str(i+1)]))
        return res
    elif "key" in d and "value" in d:
        res = []
        res.append(convert_any(d["key"]))
        res.append(convert_any(d["value"]))
        return res
    elif "i" in d:
        res = []
        res.append(convert_any(d["i"]))
        return res
    elif "addr" in d:
        res = []
        res.append(convert_any(d["addr"]))
        return res
    # standard record
    else:
        res = {}
        for key in d:
            res[key] = convert_any(d[key])
        return res

def convert_list(xs):
    res = []
    for x in xs:
        res.append(convert_any(x))
    return res

def convert_any(x):
    if type(x) is dict:
        return convert_dict(x)
    elif type(x) is list:
        return convert_list(x)
    else:
        return x

def convert_file(file_nm, writer):
    with open(file_nm, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter='|', quotechar="'")
        for row in reader:
            res = []
            for x in row:
                #print(x)
                obj = json.loads(x)
                # print("obj: " + str(obj))
                new_obj = convert_any(obj)
                #print(new_obj)
                res += [json.dumps(new_obj)]
            writer.writerow(res)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("json_files", type=str, nargs='+', help="Specify path")
    args = parser.parse_args()
    with open("messages.dsv", "w", newline='') as msg_file:
        with open("globals.dsv", "w", newline='') as glb_file:
            msg_writer = csv.writer(msg_file, delimiter='|', quotechar="'")
            glb_writer = csv.writer(glb_file, delimiter='|', quotechar="'")
            for f in args.json_files:
                # is it a messages or a globals?
                if re.search("Globals", f):
                    convert_file(f, glb_writer)
                else:
                    convert_file(f, msg_writer)

if __name__=='__main__':
    main ()
