#!/usr/bin/env python3
#
# Change json output to be human-readable
import argparse
import csv
import json

def convert_dict(d):
    # for addresses, options, records, etc, just dereference
    if "type" in d and d["type"] in ["address", "option_or_ind", "record", "Collection", "Map", "Seq"]:
        return convert_any(d["value"])
    else:
        res = {}
        for key in d:
            res[key] = convert_any(d[key])
        return res;

def convert_list(xs):
    res = []
    for x in xs:
        res.append([convert_any(x)])
    return res

def convert_any(x):
    if type(x) is dict:
        return convert_dict(x)
    elif type(x) is list:
        return convert_list(x)
    else:
        return x

def convert_file(file_nm):
    out_file = file_nm + "_out"
    with open(file_nm, 'r', newline='') as csvfile:
        with open(out_file, 'w', newline='') as out_csvfile:
            reader = csv.reader(csvfile, delimiter='|', quotechar="'")
            writer = csv.writer(out_csvfile, delimiter='|', quotechar="'")
            for row in reader:
                res = []
                for x in row:
                    # print(x)
                    obj = json.loads(x)
                    # print("obj: " + str(obj))
                    new_obj = convert_any(obj)
                    print(new_obj)
                    res += [json.dumps(new_obj)]
                writer.writerow(res)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("json_file", type=str, help="Specify path")
    args = parser.parse_args()
    convert_file(args.json_file)

if __name__=='__main__':
    main ()
