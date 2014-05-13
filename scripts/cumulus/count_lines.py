#!/usr/bin/env python

from optparse import OptionParser
import re
import sys
from os.path import basename

def group_lines(lines):
  out = []
  counter = 1
  current = "pre"

  for line in lines:
    mobj = re.match(r"^((trigger|declare)\s\w+)\s:", line)
    if mobj:
      out.append((current, counter))
      current = mobj.group(1)
      counter = 1
    else:
      # A regular line
      counter += 1
  return out

trig_rs = [
    ("cond_add_delta_to.*", "cond_add_delta"),
    ("add_delta_to_map_.*_buf", "add_delta_to_buf"),
    ("add_delta_to.*", "add_delta_to"),
    ("log_.*", "log"),
    ("route_to_.*", "route"),
    ("shuffle_.*", "shuffle"),
    (".*_send_fetch", "send_fetch"),
    (".*_rcv_fetch", "rcv_fetch"),
    (".*_rcv_put", "rcv_put"),
    (".*_send_push_.*", "send_push"),
    (".*_rcv_push_.*", "rcv_push"),
    (".*_do_complete.*", "do_complete"),
    (".*_rcv_corrective.*", "rcv_corrective"),
    (".*_do_corrective.*", "do_corrective"),
    (".*_send_correctives.*", "send_corrective")
  ]

len_rs = len(trig_rs)

def categorize(data):
  counters = {}
  for (header, line_count) in data:
    for (i, reg) in enumerate(trig_rs):
      mobj = re.search(reg[0], header)
      if mobj:
        (prev_line, prev_count) = counters.get(reg[1], (0, 0))
        counters[reg[1]] = (prev_line + line_count, prev_count + 1)
        break
  return counters

def pretty_print(total, counters):
  print("Total lines: {total}".format(**locals()))
  total_percent = 0.0
  for (_,counter_nm) in trig_rs:
    (lines, instances) = counters.get(counter_nm, (0,0))
    percent = float(lines) / float(total)
    total_percent += percent
    print('{counter_nm} lines: {lines}, instances: {instances}, {percent:.2%}'.format(**locals()))
  print('Total listed: {total_percent:.2%}'.format(**locals()))
        
def main(filename):
  with open(filename, 'r') as f:
    lines = f.readlines()
  total = 0
  for line in lines:
    total += 1
  groups = group_lines(lines)
  counters = categorize(groups)
  pretty_print(total, counters)

if __name__ == '__main__':
  usage = "Usage: {sys.argv[0]} [options] <log file>".format(**locals())
  parser = OptionParser(usage=usage)
  (options, args) = parser.parse_args()
  if args:
    main(args[0])
  else:
    print usage

