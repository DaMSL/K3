#!/usr/bin/env python
# Plot the output of process-all-latencies.rb

import numpy as np
import math
import matplotlib.pyplot as plt
import argparse
import yaml
import os
import re

queries = ['1', '3', '4', '6', '11a', '12', '17']
nodes = [1, 2, 4, 8, 16, 31]
scale_factors = [0.1, 1, 10, 100]

tuple_sizes = {0.1: 8 * 10**5, 1: 8 * 10**6, 10: 8 * 10**7, 100: 8*10**8}
batches = [100, 1000, 10000]

def scalability(args, r_file, workdir):
    results = {q:{sf:{nd:[] for nd in nodes} for sf in scale_factors} for q in queries}

    # create the graphs for scalability test
    for l in r_file:
        if not 'time' in l or l[':exp'] != ':scalability':
            continue
        time_sec = float(l['time']) / 1000
        tup_per_sec = tuple_sizes[l[':sf']] / time_sec
        gb_per_sec = l[':sf'] / time_sec
        results[l[':q']][l[':sf']][l[':nd']].append(tup_per_sec)

    results2 = {q:{sf:{nd:None for nd in nodes} for sf in scale_factors} for q in queries}
    # average out the results
    for k1,v1 in results.iteritems():
        for k2,v2 in v1.iteritems():
            for k3,v3 in v2.iteritems():
                if v3 != []:
                    results2[k1][k2][k3] = sum(v3)/len(v3)

    # dump results
    with open(os.path.join(workdir, 'plot_throughput.txt'), 'w') as f:
        yaml.dump(results2, f)

    out_path = os.path.join(workdir, 'plots', 'xput')
    if not os.path.exists(out_path):
        os.mkdir(out_path)

    # create plots per query, per sf
    for q,v1 in results2.iteritems():
        # New Figure per query
        plt.figure()
        f, ax = plt.subplots()

        # A line per scale factor
        for sf, v2 in v1.iteritems():
            plt.plot(nodes, [v2[n] for n in nodes], marker='o', label="SF {}".format(sf))

        # Labels, etc.
        plt.grid(True)
        plt.legend(loc='best')
        plt.title("Query {} Throughput".format(q))
        plt.xlabel("Worker Nodes")
        plt.ylabel("Tuples/sec")

        # Save to file
        plt.savefig(os.path.join(out_path, "q{}_nd_xput.png".format(q)))
        plt.close()

    # create plots per query, per nodes
    for q,v1 in results2.iteritems():
        # New Figure per query
        plt.figure()
        f, ax = plt.subplots()

        # invert the data
        res = {n:{sf:None for sf in scale_factors} for n in nodes}
        for sf, v2 in v1.iteritems():
            for n, val in v2.iteritems():
                res[n][sf] = val

        for n, v2 in res.iteritems():
            plt.plot(scale_factors, [v2[sf] for sf in scale_factors], marker='o', label="{} Nodes".format(n))

        # Labels, etc.
        plt.grid(True)
        plt.legend(loc='best')
        plt.title("Query {} Throughput".format(q))
        plt.xlabel("Scale Factor")
        plt.ylabel("Tuples/sec")
        ax.set_xscale('log')

        # Save to file
        plt.savefig(os.path.join(out_path, "q{}_sf_xput.png".format(q)))
        plt.close()

# This uses a default GC epoch of 5 minutes. Uses scalability data
def memory(args, r_file, workdir, do_queries):
    period = 5
    results = {q:{sf:{nd:None for nd in nodes} for sf in scale_factors} for q in do_queries}
    done = {}

    # Get the data
    for l in r_file:
        q = l[':q']
        sf = l[':sf']
        nd = l[':nd']
        if not ':jobid' in l or l[':exp'] != ':scalability' or q not in do_queries:
            continue
        jobid = l[':jobid']
        # only use 1 job per result
        if (q, sf, nd) in done:
            continue
        jobpath = os.path.join(workdir, 'tpch{}'.format(q), 'job_{}'.format(jobid))
        # Add data from all heap_size documents
        for dirpath, _, files in os.walk(jobpath):
            for fn in files:
                if fn != 'heap_size.txt':
                    continue
                series = []
                with open(os.path.join(dirpath, fn)) as f:
                    for line in f:
                        m = re.match(r'(^\d+),(\d+)$', line)
                        series.append((int(m.group(1)), int(m.group(2))))
                if len(series) == 0:
                    continue
                # collect a series for each machine
                if results[q][sf][nd] is None:
                    results[q][sf][nd] = []
                else:
                    results[q][sf][nd].append(series)
        done[(l[':q'], l[':sf'], l[':nd'])] = True

    # normalize node times
    results2 = {q:{sf:{nd:None for nd in nodes} for sf in scale_factors} for q in queries}
    for q, v1 in results.iteritems():
        for sf, v2 in v1.iteritems():
            for n, data in v2.iteritems():
                if data is None or len(data) == 0:
                    continue
                # Get the minimum: first entry of each list
                minimum = min(map(lambda l: l[0][0], data))
                maximum = max(map(lambda l: l[-1][0], data))
                final_data = np.zeros(1 + (maximum - minimum) / (1000 * period), dtype=np.int64)
                # combine and convert the data
                for list in data:
                    for (t, d) in list:
                        t1 = math.floor((t - minimum) / (1000 * period))
                        final_data[t1] += d
                results2[q][sf][n] = (final_data, maximum - minimum)


    out_path = os.path.join(workdir, 'plots', 'memory')
    if not os.path.exists(out_path):
        os.mkdir(out_path)

    # Graph
    for q, v1 in results2.iteritems():
        for sf, v2 in v1.iteritems():
            if all(map(lambda d: d is None, v2.values())):
                continue

            max_length = max(map(lambda d: 0 if d is None else d[1], v2.values()))

            plt.figure()
            f, ax = plt.subplots()
            x_axis = np.arange(0., float(1 + max_length / 1000), float(period))
            print x_axis

            # A line per node
            for nd, d in v2.iteritems():
                if d is not None:
                    (data, length) = d
                    data = np.append(data, np.zeros(abs(len(x_axis) - len(data)), dtype=np.int64))
                    print data
                    plt.plot(x_axis, data, label="{} Nodes".format(nd))

            # Labels, etc.
            plt.grid(True)
            plt.legend(loc='best')
            plt.title("Query {} SF {} Memory".format(q, sf))
            plt.xlabel("Time (s)")
            plt.ylabel("Bytes")

            # Save to file
            plt.savefig(os.path.join(out_path, "q{}_sf{}_mem.png".format(q, sf)))
            plt.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--results-file', required=True, dest='results_file',
            help='Results file from which to read')
    parser.add_argument('-e', '--experiments', required=True, nargs='+',
            help='Experiments (s, l, m, gc)')
    args = parser.parse_args()
    if args is None:
        parser.print_help()
        exit(1)

    with open(args.results_file) as f:
        results = yaml.load(f)

    workdir = os.path.dirname(args.results_file)
    print("workdir={}".format(workdir))

    tests = results[':tests']
    if 's' in args.experiments:
        scalability(args, tests, workdir)
    if 'm' in args.experiments:
        memory(args, tests, workdir, ['3','4'])


if __name__ == '__main__':
    main()
