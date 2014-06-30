#!/usr/bin/env python
# remove the huge hash numbers that mess up logs
# this script is built to work on logs meant for the db

from optparse import OptionParser
import re
import sys

def find_hashes(s):
  # find (num, num, bignum)
  matches = re.findall(r'\(\d+,\s\d+,\s(\d{8}\d*)\)', s)
  m = set()
  for s in matches:
    # filter out dates
    if not re.match(r'^(19|20)\d+', s):
      m.add(s)
  return m

def remove_hashes(hashes, s):
  r_str = '|'.join(hashes)
  ss = re.sub(r_str, '#', s)
  return ss

def unhash(s):
  hashes = find_hashes(s)
  ss = remove_hashes(hashes, s)
  return ss

def main(filename):
  with open(filename, "r") as f:
    if f:
      s = f.read()
      ss = unhash(s)
      print(ss)
    else:
      print("File {filename} not found".format(**locals()))

if __name__ == '__main__':
  usage = "Usage: {sys.argv[0]} <log file>".format(**locals())
  parser = OptionParser(usage=usage)
  (options, args) = parser.parse_args()
  if args:
    main(args[0])
  else:
    print usage

