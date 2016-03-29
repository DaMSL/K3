#!/usr/bin/env python
# Plot the output of process-all-latencies.rb

import matplotlib.pyplot as plt
import argparse
import yaml
import os

queries = ['1', '3', '4', '6', '11a', '12', '17']
nodes = [1, 4, 8, 16, 31]
scale_factors = [0.1, 1, 10, 100]

tuple_sizes = {0.1: 8 * 10**5, 1: 8 * 10**6, 10: 8 * 10**7, 100: 8*10**8}
batches = [100, 1000, 10000]

def scalability(args, r_file, workdir):
    results = {q:{sf:{nd:[] for nd in nodes} for sf in scale_factors} for q in queries}

    # create the graphs for scalability test
    for l in r_file:
        if not 'time' in l or l[':exp'] != ':scalability':
            continue
        key = (l[':q'], l[':sf'], l[':nd'])
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

    out_path = os.path.join(workdir, 'plots')
    if not os.path.exists(out_path):
        os.mkdir(out_path)

    # create plots per query
    for q,v1 in results2.iteritems():
        # New Figure per query
        plt.figure()
        f, ax = plt.subplots()

        # A line per scale factor
        for sf, v2 in v1.iteritems():
            plt.plot(nodes, [v2[n] for n in nodes], label="Scale Factor {}".format(sf))

        # Labels, etc.
        plt.legend(loc='lower right')
        plt.title("Query {} Throughput".format(q))
        plt.xlabel("Worker Nodes")
        plt.ylabel("Tuples/sec")

        # Save to file
        plt.savefig(os.path.join(out_path, "q{}_throughput.png".format(q)))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--results-file', required=True, dest='results_file',
            help='Results file from which to read')
    parser.add_argument('-e', '--experiment', required=True,
            help='Experiment (s, l, m)')
    args = parser.parse_args()
    if args is None:
        parser.print_help()
        exit(1)

    with open(args.results_file) as f:
        results = yaml.load(f)

    workdir = os.path.dirname(args.results_file)
    print("workdir {}".format(workdir))

    tests = results[':tests']
    if args.experiment == 's':
        scalability(args, results, workdir)


if __name__ == '__main__':
    main()
