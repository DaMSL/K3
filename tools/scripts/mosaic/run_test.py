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

    print("Generating schema...")
    create_schema.do_schema(k3_file, out_file=schema_name)

    print("Cleaning json output files...")
    files = os.listdir(json_dir)
    paths = [os.path.join(json_dir, f) for f in files]
    clean_json.process_files(paths)

    # append to schema file
    with open(schema_name, 'a') as f:
        f.write("copy globals from '{0}' delimiter '|' quote '`' csv;\n".format(os.path.abspath('globals.dsv')))
        f.write("copy messages from '{0}' delimiter '|' quote '`' csv;\n".format(os.path.abspath('messages.dsv')))

    print("Loading into postgres...")
    subprocess.call('psql < {0}'.format(schema_name), shell=True)

    print("done")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("k3_file", type=str, help="Specify path")
    args = parser.parse_args()
    run_test(args.k3_file)

if __name__=='__main__':
    main ()
