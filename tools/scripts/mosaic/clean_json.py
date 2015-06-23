#!/usr/bin/env python3
#
# Change json output to be human-readable
import argparse
import csv
import json
import re

def int_of_label(s):
    total = 0
    for c in s:
        total += ord(c) - ord('a') + 1
    return total

def label_of_int(i):
    s = ""
    maxint = ord('z') - ord('a') + 1
    while i > maxint:
        s += 'z'
        i -= maxint
    s += chr(i + ord('a') - 1)


def convert_dict(d):
    # for addresses, options, records, etc, just dereference
    if "type" in d and d["type"] in ["address", "option_or_ind", "record", "tuple", "Collection", "Map", "Seq", "Set"]:
        return convert_any(d["value"])
    # change record mapping back to tuple
    elif "ra" in d:
        res = []
        max = 0
        for key in d:
            k = int_of_label(key[1:])
            if k > max:
                max = k
        for i in range(max): # +1 for record fields
            res.append(convert_any(d["r" + label_of_int(i+1)]))
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

# Special conversion for first level of object
# So we have all array values
def convert_fst_level(x):
    if type(x) is dict:
        return convert_dict(x)
    elif type(x) is list:
        return convert_list(x)
    elif x == "()":
        return []
    else:
        return [x]

def convert_file(file_nm, writer, treat_as_ints):
    with open(file_nm, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter='|', quotechar="'")
        i = 0
        for row in reader:
            res = []
            j = 0
            for x in row:
                #print(x)
                try:
                    obj = json.loads(x)
                    # print("obj: " + str(obj))
                    # keep certain fields as raw integers rather than wrapping in array
                    if j in treat_as_ints:
                        new_obj = convert_any(obj)
                    else:
                        new_obj = convert_fst_level(obj)
                    #print(new_obj)
                    res += [json.dumps(new_obj)]
                except:
                    # if we can't convert, just add it as a string
                    res += [x]
                j += 1
            writer.writerow(res)
            i += 1

def process_files(files, prefix_path):
    with open(os.path.join(prefix_path, "messages.dsv"), "w", newline='') as msg_file:
        with open(os.path.join(prefix_path, "globals.dsv"), "w", newline='') as glb_file:
            msg_writer = csv.writer(msg_file, delimiter='|', quotechar="'")
            glb_writer = csv.writer(glb_file, delimiter='|', quotechar="'")
            for f in files:
                # is it a messages or a globals?
                if re.search("Globals", f):
                    convert_file(f, glb_writer, [0])
                else:
                    convert_file(f, msg_writer, [0, 5])

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("json_files", type=str, nargs='+', help="Specify path")
    parser.add_argument("prefix_path", type=str, help="Specify prefix to output", default="")
    args = parser.parse_args()
    process_files(args.json_files, args.prefix_path)

if __name__=='__main__':
    main ()
