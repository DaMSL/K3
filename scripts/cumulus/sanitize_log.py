#!/usr/bin/env python

import re

def reduce_region(lines):
  out = []
  take = False
  for line in lines:
    if not take:
      if re.match("^ *TRIGGER .*$", line):
        take = True
        out += [line]
    elif take:
      if re.match("^Mode:.*$", line) or re.match("^ *EVENT LOOP.*$", line):
        take = False
      else:
        out += [line]
  return out

def group_triggers(lines):
  out = []
  group = []
  for line in lines:
    if re.match("^ *TRIGGER .*$", line):
      if group != []:
        out += [group]
      group = [line]
    else:
      group += [line]
  if group != []:
    out += [group]
  return out

# Take a group of lines representing a trigger and convert them to proper output
def lines_of_trig(trig):
  mobj =  re.match(r'^TRIGGER (.+) (.+) {', trig[0])
  if mobj:
    name, addr = mobj.group(1), mobj.group(2)
    if name and addr:
      mobj = re.match(r'^Args: (\(.*\))$', trig[1])
      args = mobj.group(1)
      if args:
        pairs = [("args", args)]
        for l in trig[2:]:
          mobj = re.match(r'^(.*) => (.*)$', l)
          if mobj:
            pairs.append((mobj.group(1), mobj.group(2)))
        # convert pairs to lines
        out = ['/'.join([addr, name, pair[0], pair[1]]) for pair in pairs]
        return out

# clean a line by removing initial whitespace, \n, long whitespace
def clean(line):
  line = line.replace('\n','')
  line = re.sub(r'\s+', ' ', line)
  line = re.sub(r'^ ', '', line)
  return line

def main():
  with open("results.txt", "r") as f:
    if f:
      lines = f.readlines()
      lines = [clean(l) for l in lines]
      lines = reduce_region(lines)
      trigs = group_triggers(lines)
      trigs = [lines_of_trig(t) for t in trigs]
      out = []
      for t in trigs:
        out += t
      out = '\n'.join(out)
      print(out)

if __name__ == '__main__':
  main()

