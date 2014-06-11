#!/usr/bin/env python3
#
# Regression tests for K3
import sys
import os
import re
import argparse

temp_file = 'temp.out'

test_paths = ['K3-Core/examples',
              'K3-Core/examples/language',
              'K3-Core/examples/language/builtins',
              'K3-Core/examples/language/collections']

script_path = os.path.dirname(__file__)
root_path = os.path.join(script_path, '../../../')

def find_error(file):
    with open(file, 'r') as f:
        s = f.read()
        if re.search(r'(Error|unexpected|No such file)', s):
            return True
        else:
            return False

def test_file(count, file):
    temp = temp_file
    k3_path = os.path.join(root_path, './K3-Driver/dist/build/k3/k3')
    lib_path = os.path.join(root_path, './K3-Core/lib/k3')
    print('[{count}] Testing {file}...'.format(**locals()), end="")
    cmd = r'{k3_path} -I {lib_path} interpret -b -p 127.0.0.1:40000:role=\"s1\" {file} > {temp} 2>&1'.format(**locals())
    # print(cmd)
    os.system(cmd)
    if find_error(temp_file):
        print("[ERROR]")
        return False
    else:
        print("[OK]")
        return True

def find_test_files():
    file_list = []
    counter = 1
    failed = 0
    for dir in test_paths:
        d = os.path.join(root_path, dir)
        for file in os.listdir(dir):
            if file.endswith(".k3"):
                error = not test_file(counter, os.path.join(dir, file))
                counter += 1
                failed += 1 if error else 0
    count = counter - 1
    if failed > 0:
        print('[{failed}/{count}] tests failed.'.format(**locals()))
        return False
    else:
        print('All tests passed')
        return True

def main(options):
    if options.test_file:
        ret = test_file(1, options.test_file)
    else:
        ret = find_test_files()
    if not ret:
        sys.exit(1)

if __name__ == '__main__':
    usage = "Usage: {sys.argv[0]} [options]".format(**locals())
    parser = argparse.ArgumentParser(usage=usage)
    parser.add_argument("-f", "--file", type=str, dest="test_file",
                        default=None, help="Specify path to a specific k3 test file")
    (options) = parser.parse_args()
    main(options)

