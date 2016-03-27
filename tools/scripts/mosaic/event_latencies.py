#!/usr/bin/env python
#
# Process switch and node event files to extract Mosaic event latencies.

import argparse
import csv
import sys
import yaml
import re
import math

from intervaltree import Interval, IntervalTree
from pyhistogram import Hist1D
from ascii_graph import Pyasciigraph

# Event classes.
events = {
    'switch_process': 0,
    'node_start_process': 1,
    'rcv_fetch': 2,
    'rcv_push': 3,
    'do_complete': 4,
    'corrective': 5,
    'buffered_push': 6
    }

# detailed tag -> (number, general event for tracking)
tags = {
    'pre_send_fetch': (0, 'switch_process'),
    'post_send_fetch': (1, 'switch_process'),
    'rcv_fetch': (2, 'rcv_fetch'),
    'buffered_push': (3, 'buffered_push'),
    'push_done': (4, 'rcv_push'),
    'do_complete_done': (5, 'do_complete'),
    'corr_done': (6, 'corr_done'),
    'corr_send': (7, 'corr_send'),
    'push_cnts': (8, None),
    'push_decr': (9, None),
    'fetch_route': (10, None),
    'send_put': (11, None),
    'gc_start': (12, None),
    'gc_done': (13, None),
    'node_start_process': (14, 'node_start_process')
    }

tag_to_ev = ['' for x in range(len(tags))]
for _, v in tags.iteritems():
    tag_to_ev[v[0]] = v[1]

# An interval tree of vid-segments to update start times. Uses fetch events
switchspans = IntervalTree()

# A dictionary of event -> vid -> span (min, max). Uses all other events
nodespans = [{} for ev in events]

# A dictionary of event -> vid -> latency
latencies = [{} for ev in events]

# A dictionary of global latency min/max values across all vids per event class.
latencyspans = [(sys.maxint, -sys.maxint - 1) for ev in events]

nd_spans = [{} for ev in events]

def update_latency(tag, vid, t, filename):
    # get the matching interval from the switch interval tree
    intervals = switchspans[vid]
    if len(intervals) > 1:
        print("Invalid point query on switch vid intervals")
    else:
        if len(intervals) == 0:
            print("No start interval found for {}".format(vid))
        else:
            # get the interval's latency (there should be only one match)
            lat = t - list(intervals)[0].data

            ev = events[tag]

            old_lat = latencies[ev][vid] if vid in latencies[ev] else -1
            latencies[ev][vid] = max(old_lat, lat)

            (rmin, rmax) = latencyspans[ev]
            latencyspans[ev] = (min(rmin, lat), max(rmax, lat))

            m = re.search('.*(qp[^/]+)/.*',filename)
            machine = filename if m is None else m.group(1)
            (rmin, rmax) = nd_spans[ev][machine] if machine in nd_spans[ev] else (sys.maxint, -sys.maxint-1)
            nd_spans[ev][machine] = (min(rmin, lat), max(rmax, lat))


def update_nodespan(tag, vid, t):
    ev = events[tag]
    (rmin, rmax) = nodespans[ev][vid] if vid in nodespans[ev] else (sys.maxint, -sys.maxint - 1)
    nodespans[ev][vid] = (min(rmin, t), max(rmax, t))


def banner(s):
    return '\n'.join(['-' * 50, s, '-' * 50])


def dump_final_latencies():
    print(banner('Dumping Final latencies'))
    with open('latencies.txt', 'w') as f:
        f.write("Latency stats (do_complete)\n")
        lats = latencies[events['do_complete']].values()
        total = sum(lats)
        n = len(lats)
        max_val = max(lats)
        min_val = min(lats)
        mean = total / n
        lats.sort()
        median = lats[n/2] if n % 2 == 1 else (lats[n/2] + lats[n/2 - 1] / 2)
        stdev = math.sqrt(sum(map(lambda x: (x - mean)**2, lats)) / n)
        f.write("mean:{}, median:{}, std_dev:{}, n:{}, min:{}, max:{}\n".format(mean, median, stdev, n, min_val, max_val))
        print "mean:{}, median:{}, std_dev:{}, n:{}, min:{}, max:{}\n".format(mean, median, stdev, n, min_val, max_val)

        f.write("\n---- Per machine: ----\n")
        for i,ev in enumerate(nd_spans):
            for file,(min_v,max_v) in ev.iteritems():
                f.write("{}: {}, ({}, {})\n".format(file, i, min_v, max_v))

def process_events(switch_files, node_files, save_intermediate):
    """ Main function """
    # Build an interval tree with switch files.
    # Probe and reconstruct latencies from node files.
    for fn in switch_files:
        with open(fn) as csvfile:
            swreader = csv.reader(csvfile, delimiter=',')
            prev_vid = 0
            prev_v = 0
            prev_t = 0
            count = 0
            for row in swreader:
                count = count + 1
                [tag, vid, comp, t] = map(lambda x: int(x), row)

                if tag == tags['pre_send_fetch'][0]:
                    prev_vid = vid
                    prev_t = t
                elif tag == tags['post_send_fetch'][0]:
                    # add one to post_send_fetch vid to include it also
                    switchspans[prev_vid:vid + 1] = t
                    lat = t - prev_t
                    # print(vid, lat) # debug

                    # initialize latencies for this vid
                    ev = events[tags['post_send_fetch'][1]]
                    latencies[ev][vid] = lat

                    (rmin, rmax) = latencyspans[ev]
                    latencyspans[ev] = (min(rmin, lat), max(rmax, lat))
                else:
                    print("Unknown switch tag: {}".format(tag))
        # print("Processed {} lines in switch file {}".format(count, fn))
    # print("latencies: {}".format(latencies)) # debug

    for fn in node_files:
        with open(fn) as csvfile:
            swreader = csv.reader(csvfile, delimiter=',')
            count = 0
            for row in swreader:
                count = count + 1
                [tag, vid, comp, t] = map(lambda x: int(x), row)

                # For each class of event, track the last event from the start of the
                # update at this vid, and the timespan of the event class.
                if tag < len(tag_to_ev):
                    event = tag_to_ev[tag]
                    if not event is None:
                        update_latency(event, vid, t, fn)
                        update_nodespan(event, vid, t)

                # else:
                #   print("Unknown node tag: {tag}".format(**locals()))
        # print("Processed {} lines in node file {}".format(count, fn))
    # print("latencies: {}".format(latencies)) # debug

    dump_final_latencies()

    build_histograms()

    if save_intermediate:
        # Finally, save intermediate data for now.
        print(banner("Saving intermediate latency data"))

        with open('latencies.yml', 'w') as outfile:
            yaml.dump(latencies, outfile, default_flow_style=True)

        with open('nodespans.yml', 'w') as outfile:
            yaml.dump(nodespans, outfile, default_flow_style=True)

def build_histograms():
    # Build latency histograms
    print(banner("Computing latency histograms"))

    # A dictionary of latency histograms per event class.
    lhists = {}
    nbins = 20

    # Set the spans of the histogram
    for ev, span in enumerate(latencyspans):
        lhists[ev] = Hist1D(nbins, span[0], span[1])

    # Fill out the histogram
    for ev, vid_lat in enumerate(latencies):
       for lat in vid_lat.values():
           lhists[ev].fill(lat)

    print(banner("Saving latency histograms"))

    with open("graph.txt", 'w') as file:
        for k, v in events.iteritems():
            # correctives aren't tracked
            if k == "corrective":
                continue
            # save to file
            with open('latency_{}.yml'.format(k), 'w') as outfile:
                data = {b.x.center: b.value for b in lhists[v].bins()}
                yaml.dump(data, outfile, default_flow_style=True)

            if all(v == 0 for v in data.values()):
                continue

            # plot
            graph = Pyasciigraph(graphsymbol='*')
            try:
              for line in graph.graph('{} latency'.format(k), sorted(data.items())):
                file.write(line + '\n')
            except Exception as e:
              print("Exception on {}".format(k))
              print(e)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--switches', metavar='SWITCH_EVENTS', nargs='+',
                        required=True, dest='switch_files', help='switch event data files')
    parser.add_argument('-n', '--nodes',    metavar='NODE_EVENTS',   nargs='+',
                        required=True, dest='node_files',   help='node event data files')
    parser.add_argument('--save', default=False,
                        action='store_true', help='save intermediate output')
    args = parser.parse_args()
    if args:
        process_events(args.switch_files, args.node_files, args.save)
    else:
        parser.print_help()

if __name__ == '__main__':
    main()
