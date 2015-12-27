#!/usr/bin/env python
#
# Process switch and node event files to extract Mosaic event latencies.

import argparse, csv, sys, yaml

from intervaltree import Interval, IntervalTree
from pyhistogram import Hist1D
from ascii_graph import Pyasciigraph

# Event classes.
events = { 'switch_process' : 0
         , 'rcv_fetch'      : 1
         , 'rcv_push'       : 2
         , 'do_complete'    : 3
         , 'corr_done'      : 4
         , 'buffered_push'  : 5
         }

tags = { 'pre_send_fetch'   : 0
       , 'post_send_fetch'  : 1
       , 'rcv_fetch'        : 2
       , 'buffered_push'    : 3
       , 'push_done'        : 4
       , 'do_complete_done' : 5
       , 'corr_done'        : 6
       , 'corr_send'        : 7
       , 'push_cnts'        : 8
       , 'push_decr'        : 9
       , 'fetch_route'      : 10
       , 'send_put'         : 11
       , 'gc_start'         : 12
       , 'gc_done'          : 13
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
    if len(intervals) == 0:
      print("No start interval found for {}".format(vid))
    else:
      l = t - list(intervals)[0].data

      if vid not in latencies:
        latencies[vid] = {v : -sys.maxint - 1 for v in events.values()}

      latencies[vid][events[tg]] = max(latencies[vid][events[tg]], l)

      (rmin, rmax) = latencyspans[events[tg]]
      latencyspans[events[tg]] = (min(rmin, l), max(rmax, l))

def event_span(tg, vid, t):
  if vid not in nodespans:
    nodespans[vid] = {v : (sys.maxint, -sys.maxint -1) for v in events.values()}

  (rmin, rmax) = nodespans[vid][events[tg]]
  nodespans[vid][events[tg]] = (min(rmin, t), max(rmax, t))

def banner(s):
  return '\n'.join(['-' * 50, s, '-' * 50])

def dump_final_latencies():
  print("Dumping Final latencies:")
  with open("latencies.txt", 'w') as f:
    for vid in latencies:
      l = latencies[vid][events['do_complete']]
      if l > 0:
        f.write(str(l) + '\n')


def process_events(switch_files, node_files, save_intermediate):
  # Build an interval tree with switch files.
  # Probe and reconstruct latencies from node files.
  for fn in switch_files:
    with open(fn) as csvfile:
      swreader = csv.reader(csvfile, delimiter=',')
      prev_vid = 0
      prev_v = 0
      prev_t = 0
      for row in swreader:
        [tg, vid, comp, t] = map(lambda x: int(x), row)

        if tg == tags['pre_send_fetch']:
          prev_vid = vid
          prev_t = t
        elif tg == tags['post_send_fetch']:
          switchspans[prev_vid:vid] = t
          l = t - prev_t
          print(vid, l)

          latencies[vid] = {v : -sys.maxint - 1 for v in events.values()}
          latencies[vid][events['switch_process']] = l

          (rmin, rmax) = latencyspans[events['switch_process']]
          latencyspans[events['switch_process']] = (min(rmin, l), max(rmax, l))
        else:
          print("Unknown switch tag: {tg}".format(**locals()))

  for fn in node_files:
    with open(fn) as csvfile:
      swreader = csv.reader(csvfile, delimiter=',')
      for row in swreader:
        [tg, vid, comp, t] = map(lambda x: int(x), row)

        # For each class of event, track the last event from the start of the
        # update at this vid, and the timespan of the event class.
        if tg == tags['rcv_fetch']:
          max_from_start('rcv_fetch', vid, t)
          event_span('rcv_fetch', vid, t)

        elif tg == tags['buffered_push']:
          max_from_start('buffered_push', vid, t)
          event_span('buffered_push', vid, t)

        elif tg == tags['push_done']:
          max_from_start('rcv_push', vid, t)
          event_span('rcv_push', vid, t)

        elif tg == tags['do_complete_done']:
          max_from_start('do_complete', vid, t)
          event_span('do_complete', vid, t)

        elif tg == tags['corr_done']:
          max_from_start('corr_done', vid, t)
          event_span('corr_done', vid, t)

        # else:
        #   print("Unknown node tag: {tg}".format(**locals()))
  dump_final_latencies()

  # Build latency histograms
  #print(banner("Computing latency histograms"))

  #for ecls, span in latencyspans.iteritems():
  #  lhists[ecls] = Hist1D(nbins, span[0], span[1])

  #for eventl in latencies.values():
  #  for ecls, l in eventl.iteritems():
  #    lhists[ecls].fill(l)

  #print(banner("Saving latency histograms"))

  #for k, v in events.iteritems():
  #  if k == "corr_done":
  #    continue
  #  with open('latency_{}.yml'.format(k), 'w') as outfile:
  #    data = {b.x.center: b.value for b in lhists[v].bins()}
  #    yaml.dump(data, outfile, default_flow_style=True)

  #    # Interactive plot
  #    graph = Pyasciigraph()

  #    # Simpler example
  #    try:
  #      for line in graph.graph('{} latency'.format(k), sorted(data.items())):
  #        print(line)
  #    except Exception as e:
  #      print("Exception on {}".format(k))
  #      print(e)

  if save_intermediate:
    # Finally, save intermediate data for now.
    print(banner("Saving intermediate latency data"))

    with open('latencies.yml', 'w') as outfile:
      yaml.dump(latencies, outfile, default_flow_style=True)

    with open('nodespans.yml', 'w') as outfile:
      yaml.dump(nodespans, outfile, default_flow_style=True)

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('-s', '--switches', metavar='SWITCH_EVENTS', nargs='+', required=True, dest='switch_files', help='switch event data files')
  parser.add_argument('-n', '--nodes',    metavar='NODE_EVENTS',   nargs='+', required=True, dest='node_files',   help='node event data files')
  parser.add_argument('--save', default=False, action='store_true', help='save intermediate output')
  args = parser.parse_args()
  if args:
    process_events(args.switch_files, args.node_files, args.save)
  else:
    parser.print_help()

if __name__ == '__main__':
    main()