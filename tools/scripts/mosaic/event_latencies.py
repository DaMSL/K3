#!/usr/bin/env python
#
# Process switch and node event files to extract Mosaic event latencies.

import argparse
import yaml

from intervaltree import Interval, IntervalTree
from pyhistogram import Hist1D

# Event classes.
events = { 'switch_process' : 0
         , 'rcv_fetch'      : 1
         , 'rcv_push'       : 2
         , 'do_complete'    : 3
         , 'corr_done'      : 4
         }

# An interval tree of vid-segments to update start times.
switchspans = IntervalTree()

# A dictionary of event spans per vid.
nodespans = {}

# A dictionary of event latencies per vid.
latencies = {}

# A dictionary of global latency ranges, across all vids per event class.
latencyspans = {v : (sys.maxint, -sys.maxint -1) for v in events.values()}

# A dictionary of latency histograms per event class.
lhists = {}
nbins = 20

def max_from_start(tg, vid, t):
  intervals = switchspans[vid]
  if len(intervals) > 1:
    print("Invalid point query on switch vid intervals")
  else:
    l = t - intervals[0].data

    if vid not in latencies:
      latencies[vid] = {events[tg] : -sys.maxint - 1}

    latencies[vid][events[tg]] = max(latencies[vid][events[tg]], l)

    (rmin, rmax) = latencyspans[events[tg]]
    latencyspans[events[tg]] = (min(rmin, l), max(rmax, l))

def event_span(tg, vid, t):
  if vid not in nodespans:
    nodespans[vid] = {events[tg] : (sys.maxint, -sys.maxint -1)}

  (rmin, rmax) = nodespans[vid][events[tg]]
  nodespans[vid][events[tg]] = (min(rmin, t), max(rmax, t))

def banner(s):
  return '\n'.join(['-' * 50, s, '-' * 50])

def process_events(switch_files, node_files):
  # Build an interval tree with switch files.
  # Probe and reconstruct latencies from node files.
  for fn in switch_files:
    with open(fn, newline='') as csvfile:
      swreader = csv.reader(csvfile, delimiter=',')
      prev_vid = 0
      prev_v = 0
      for row in swreader:
        [tg, vid, comp, t] = row

        if tg == 0:
          prev_vid = vid
          prev_t = t
        elif tg == 1:
          switchspans[prev_vid:vid] = t
          latencies[vid][events['switch_process']] = t - prev_t
        else:
          print("Unknown switch tag: {tg}".format(**locals()))

  for fn in node_files:
    with open(fn, newline='') as csvfile:
      swreader = csv.reader(csvfile, delimiter=',')
      for row in swreader:
        [tg, vid, comp, t] = row

        # For each class of event, track the last event from the start of the
        # update at this vid, and the timespan of the event class.
        if tg == 2:
          max_from_start('rcv_fetch', vid, t)
          event_span('rcv_fetch', vid, t)

        elif tg == 3:
          print("NYI: buffered pushes")

        elif tg == 4:
          max_from_start('rcv_push', vid, t)
          event_span('rcv_push', vid, t)

        elif tg == 5:
          max_from_start('do_complete', vid, t)
          event_span('do_complete', vid, t)

        elif tg == 6:
          max_from_start('corr_done', vid, t)
          event_span('corr_done', vid, t)

        else:
          print("Unknown switch tag: {tg}".format(**locals()))

  # Build latency histograms
  print(banner("Computing latency histograms"))

  for ecls, span in latencyspans.iteritems():
    lhists[ecls] = Hist(nbins, span[0], span[1])

  for eventl in latencies.values():
    for ecls, l in eventl.iteritems():
      lhists[ecls].fill(l)

  print(banner("Saving latency histograms"))

  for k, v in events.iteritems():
    with open('latency_{}.yml'.format(k), 'w') as outfile:
      data = {b.x.center : b.value for b in lhists[v].bins()}
      yaml.dump(data, outfile, default_flow_style=True)

  # Finally, save intermediate data for now.
  print(banner("Saving intermediate latency data"))

  with open('latencies.yml', 'w') as outfile:
    yaml.dump(latencies, outfile, default_flow_style=True)

  with open('nodespans.yml', 'w') as outfile:
    yaml.dump(nodespans, outfile, default_flow_style=True)

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--switches', metavar='SWITCH_EVENTS', nargs='+', dest='switch_files', help='switch event data files')
  parser.add_argument('--nodes',    metavar='NODE_EVENTS',   nargs='+', dest='node_files',   help='node event data files')
  args = parser.parse_args()
  if args:
    process_events(args.switch_files, args.node_files)
  else:
    print usage

if __name__ == '__main__':
    main()
