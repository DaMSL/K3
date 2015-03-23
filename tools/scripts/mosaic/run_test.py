#!/usr/bin/env python3
#
# Change json output to be human-readable
import argparse
import os
import sys
import subprocess
script_path = "tools/scripts/mosaic"
sys.path.append(os.path.abspath(script_path))
import clean_json
import create_schema

schema_name = "temp.schema"
json_dir = "out"

def run_test(k3_file):
    # generate yaml path
    (path, prog_nm) = os.path.split(k3_file)
    (prog_nm, ext) = os.path.splitext(prog_nm)
    yaml_file = os.path.join(script_path, prog_nm) + ".yaml"
    # create the schema for the k3 file
    create_schema.do_schema(k3_file, out_file=schema_name)

    # create path for json
    if not os.path.isdir(json_dir):
        os.makedirs(json_dir)
    # run and create jsons
    subprocess.call('__build/A -p {0} -j {1}'.format(yaml_file, json_dir), shell=True)

    # create new jsons
    for f in os.listdir(json_dir):
        path = os.path.join(json_dir, f)
        if not isfile(path):
            continue
        (_, ext) = os.path.splitext(f)
        if not ext == "dsv":
            continue
        clean_json.convert_file(path)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("k3_file", type=str, help="Specify path")
    args = parser.parse_args()
    run_test(args.k3_file)

if __name__=='__main__':
    main ()
