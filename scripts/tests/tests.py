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
        if re.search(r'(Error|unexpected|No such file|Invalid)', s):
            return True
        else:
            return False

def test_file(count, file, verbose=False):
    temp = temp_file
    k3_path = os.path.join(root_path, './K3-Driver/dist/build/k3/k3')
    lib_path = os.path.join(root_path, './K3-Core/lib/k3')
    print('[{count}] Testing {file}...'.format(**locals()), end="")
    cmd = r'{k3_path} -I {lib_path} interpret -b -p 127.0.0.1:40000:role=\"s1\" {file} > {temp} 2>&1'.format(**locals())
    if verbose:
        print(cmd)
    os.system(cmd)
    if find_error(temp_file):
        print("[ERROR]") 
        if verbose:
            with open(temp_file, 'r') as f:
                print(f.read())
        return False
    else:
        print("[OK]")
        return True

def find_test_files():
    file_list = []
    for dir in test_paths:
        d = os.path.join(root_path, dir)
        for file in os.listdir(d):
            if file.endswith(".k3"):
                file_list += [os.path.join(root_path, dir, file)]
    return file_list

def run_tests(file_list, verbose=False):
    failed = 0
    for i, f in enumerate(file_list):
        if not test_file(i + 1, f, verbose):
            failed += 1

    if failed > 0:
        print('[{0}/{1}] tests failed.'.format(failed, len(file_list)))
        return False
    else:
        print('All tests passed')
        return True

def main(options):
    file_list = []
    # handle single files
    if options.test_file:
        file_list += [options.test_file]
    # Handle a list in a file
    if options.list_file:
        files = []
        with open(options.list_file, 'r') as f:
            files = f.readlines()
        for x in files:
            file_list += [os.path.join(root_path, x)]

    if len(file_list) == 0:
        file_list = find_test_files()

    if not run_tests(file_list, options.verbose):
        sys.exit(1)

if __name__ == '__main__':
    usage = "Usage: {sys.argv[0]} [options]".format(**locals())
    parser = argparse.ArgumentParser(usage=usage)
    parser.add_argument("-f", "--file", type=str, dest="test_file",
                        default=None, help="Specify path to a specific k3 test file")
    parser.add_argument("-l", "--list", type=str, dest="list_file",
                        default=None, help="Specify path to a file containing a list of test files")
    parser.add_argument("-v", "--verbose", action='store_true', dest="verbose",
                        default=False, help="Verbose printing (show output of test)")
    (options) = parser.parse_args()
    main(options)

