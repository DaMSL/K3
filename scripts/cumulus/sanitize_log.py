#!/usr/bin/env python

import re

def reduce_region(lines):
  out = []
  take = False
  for line in lines:
    if not take:
      if re.match("^TRIGGER .*$", line):
        take = True
        out += [line]
    elif take:
      if re.match("^Mode:.*$", line) or re.match("^EVENT LOOP.*$", line):
        take = False
      else:
        out += [line]
  return out

def group_triggers(lines):
  out = []
  group = []
  for line in lines:
    if re.match("^TRIGGER .*$", line):
      if group != []:
        out += [group]
      group = [line]
    else:
      group += [line]
  if group != []:
    out += [group]
  return out

def main():
  with open("results.txt", "r") as f:
    if f:
      lines = f.readlines()
      lines = list(map(lambda l: l.replace('\n',''),lines))
      lines = reduce_region(lines)
      trigs = group_triggers(lines)
      print(trigs)
      #s = ""
      #for l in lines:
        #s += l
    #print(s)

if __name__ == '__main__':
  main()

