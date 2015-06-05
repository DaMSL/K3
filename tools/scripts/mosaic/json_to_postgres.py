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

schema_path = "tools/scripts/mosaic/db.schema"
json_dir = "out"

def run_test():
    print("Cleaning json output files...")
    files = os.listdir(json_dir)
    paths = [os.path.join(json_dir, f) for f in files]
    clean_json.process_files(paths)

    print("Loading into postgres...")
    subprocess.call('psql < {0}'.format(schema_path), shell=True)

    print("done")


def main():
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    run_test()

if __name__=='__main__':
    main ()
