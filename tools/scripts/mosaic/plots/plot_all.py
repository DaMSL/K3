#!/usr/bin/env python
# Plot the output of process-all-latencies.rb

import numpy as np
import math
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import argparse
import yaml
import os
import re
import sys

queries = ['1', '3', '4', '6', '11a', '12', '17']
nodes = [1, 2, 4, 8, 16, 31]
scale_factors = [0.1, 1, 10] #, 100]
tuple_sizes = {0.1: 8 * 10**5, 1: 8 * 10**6, 10: 8 * 10**7, 100: 8*10**8}
batches = [100, 1000, 10000]

# for plots
nd_styles = {1:('r','*'), 2:('k','x'), 4:('g','o'), 8:('c','^'), 16:('b','+'), 31:('m','D')}
sf_styles = {0.1:('r','<'), 1:('g','+'), 10:('c','^'), 100:('m','D')}
q_colors = ['b', 'g', 'c', 'b', 'g', 'c', 'b']

# for calculating num of tuples per query
map_sizes = {'cu':2411114, 'li':73646424, 'or':16743122, 'pa':2371090, 'ps':11648193, 'su':138625}
map_total = sum(map_sizes.itervalues())
q_tables = {'1':['li'], '3':['cu','or','li'], '4':['or','li'], '6':['li'], '11a':['ps','su'], '12':['or','li'], '17':['li','pa'] }
q_pct = {k:sum(map(lambda n:map_sizes[n], v))/float(map_total) for k,v in q_tables.iteritems()}

font = {'family':'serif',
        'color':'black',
        'weight':'normal',
        'size':12
        }

def natural_sort_key(s, _nsre=re.compile(r'(\d+)')):
    return [int(text) if text.isdigit() else text.lower() for text in re.split(_nsre, s)]

def add_legend(ax):
    # sort legend
    handles, labels = ax.get_legend_handles_labels()
    # sort both labels and handles by labels
    labels, handles = zip(*sorted(zip(labels, handles), key=lambda x:natural_sort_key(x[0])))
    ax.legend(handles, labels, loc='best')

def scalability(args, r_file, workdir, result_dir):
    results = {q:{sf:{nd:[] for nd in nodes} for sf in scale_factors} for q in queries}

    # create the graphs for scalability test
    for l in r_file:
        if ':jobid' not in l or l[':exp'] != ':scalability':
            continue
        sf = l[':sf']
        q = l[':q']
        time_sec = float(l['time']) / 1000
        tup_per_sec = q_pct[q] * tuple_sizes[sf] / time_sec
        gb_per_sec = q_pct[q] * sf / time_sec
        results[q][sf][l[':nd']].append(tup_per_sec)

    results2 = {q:{sf:{nd:None for nd in nodes} for sf in scale_factors} for q in queries}
    # average out the results
    for q,v1 in results.iteritems():
        for sf,v2 in v1.iteritems():
            for nd,v3 in v2.iteritems():
                if v3 != []:
                    results2[q][sf][nd] = sum(v3)/len(v3)

    # dump results
    with open(os.path.join(result_dir, 'plot_throughput.txt'), 'w') as f:
        yaml.dump(results2, f)

    out_path = os.path.join(result_dir, 'plots')
    if not os.path.exists(out_path):
        os.mkdir(out_path)

    # combined figures
    # create combined plots per query, per sf

    def do_combo_plots(x_max, y_max):
        total = 0
        for q_base_idx in range(0, len(queries), y_max * x_max):
            f, axarr = plt.subplots(y_max, x_max, sharex=True)
            i = 0
            for y, axarr2 in enumerate(axarr):
                for x, ax in enumerate(axarr2):
                    if q_base_idx + i >= len(queries):
                        ax.axis('off')
                        continue
                    q = queries[q_base_idx + i]
                    v1 = results2[q]

                    # A line per scale factor
                    for sf, v2 in v1.iteritems():
                        filtered = filter(lambda x:x[1] is not None, zip(nodes, [v2[n] for n in nodes]))
                        if len(filtered) == 0:
                            continue
                        xvals, yvals = zip(*filtered)
                        ax.plot(xvals, yvals, color=sf_styles[sf][0], marker=sf_styles[sf][1], label="SF {}".format(sf))

                    # Labels, etc.
                    ax.grid(True)
                    ax.set_title("Query {}".format(q), fontdict=font)
                    if y == y_max - 1 or total + x_max >= len(queries):
                        ax.set_xlabel("Worker Nodes", fontdict=font)
                    ax.get_yaxis().set_major_formatter(ticker.FuncFormatter(lambda x,p: "{}".format(int(x/1000))))
                    if x == 0:
                        ax.set_ylabel("KTuples/sec", fontdict=font)
                    if x == x_max - 1 and y == y_max - 1 or total + 1 >= len(queries):
                        add_legend(ax)

                    i += 1
                    total += 1

            # Save to file
            plt.savefig(os.path.join(out_path, "comb_q{}-{}_nd_xput.pdf".format(q_base_idx, q_base_idx+x_max * y_max)))
            plt.close()

    do_combo_plots(2, 2)
    do_combo_plots(3, 3)

    # create plots per query, per sf
    for q,v1 in results2.iteritems():
        # New Figure per query
        f, ax = plt.subplots()

        # A line per scale factor
        for sf, v2 in v1.iteritems():
            filtered = filter(lambda x:x[1] is not None, zip(nodes, [v2[n] for n in nodes]))
            if len(filtered) == 0:
                continue
            x, y = zip(*filtered)
            plt.plot(x, y, color=sf_styles[sf][0], marker=sf_styles[sf][1], label="SF {}".format(sf))

        # Labels, etc.
        plt.grid(True)
        plt.title("Query {} Throughput".format(q), fontdict=font)
        plt.xlabel("Worker Nodes", fontdict=font)
        plt.ylabel("Tuples/sec", fontdict=font)
        add_legend(ax)

        # Save to file
        plt.savefig(os.path.join(out_path, "q{}_nd_xput.pdf".format(q)))
        plt.close()

    # create plots per query, per nodes
    for q,v1 in results2.iteritems():
        # invert the data
        res = {n:{sf:None for sf in scale_factors} for n in nodes}
        for sf, v2 in v1.iteritems():
            for n, val in v2.iteritems():
                res[n][sf] = val

        # New Figure per query
        f, ax = plt.subplots()

        # line per node count
        for n, v2 in res.iteritems():
            filtered = filter(lambda x:x[1] is not None, zip(scale_factors, [v2[sf] for sf in scale_factors]))
            if len(filtered) == 0:
                continue
            x, y = zip(*filtered)
            plt.plot(x, y, color=nd_styles[nd][0], marker=nd_styles[nd][1], label="{} Nodes".format(n))

        # Labels, etc.
        plt.grid(True)
        plt.title("Query {} Throughput".format(q), fontdict=font)
        plt.xlabel("Scale Factor", fontdict=font)
        plt.ylabel("Tuples/sec", fontdict=font)
        ax.set_xscale('log')
        add_legend(ax)

        # Save to file
        plt.savefig(os.path.join(out_path, "q{}_sf_xput.pdf".format(q)))
        plt.close()

    # create bar plots showing differences between queries
    # figure per sf, node count so we can choose
    for sf in scale_factors:
        for nd in nodes:
            data = [results2[q][sf][nd] for q in queries]
            if all(map(lambda x:x is None, data)):
                continue
            data, labels = zip(*filter(lambda x:x[0] is not None, zip(data, queries)))
            x_loc = np.arange(len(labels))
            width = 1

            f, ax = plt.subplots()
            rects = ax.bar(x_loc, data, width, color=q_colors)
            ax.set_ylabel('KTuples/sec', fontdict=font)
            ax.set_xlabel('TPC-H Queries', fontdict=font)
            #ax.set_title('SF {}, {} nodes Throughput'.format(sf, nd), fontdict=font)
            ax.get_yaxis().set_major_formatter(ticker.FuncFormatter(lambda x,p: "{}".format(int(x/1000))))
            ax.set_xticks(x_loc + width / 2.0)
            ax.set_xticklabels(queries)

            # Save to file
            plt.savefig(os.path.join(out_path, "comp_sf{}_{}nd_xput.pdf".format(sf, nd)))
            plt.close()


# This uses a default GC epoch of 5 minutes. Uses scalability data
def memory(args, r_file, workdir, result_dir, do_queries):
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


    out_path = os.path.join(result_dir, 'plots')
    if not os.path.exists(out_path):
        os.mkdir(out_path)

    # Graph
    for q, v1 in results2.iteritems():
        for sf, v2 in v1.iteritems():
            if all(map(lambda d: d is None, v2.values())):
                continue

            max_length = max(map(lambda d: 0 if d is None else d[1], v2.values()))

            f, ax = plt.subplots()
            x_axis = np.arange(0., float(1 + max_length / 1000), float(period))

            # A line per node
            for nd, d in v2.iteritems():
                if d is not None:
                    (data, length) = d
                    data = np.append(data, np.zeros(abs(len(x_axis) - len(data)), dtype=np.int64))
                    plt.plot(x_axis, data, color=nd_styles[nd][0], marker=nd_styles[nd][1], label="{} Nodes".format(nd))

            # Labels, etc.
            plt.grid(True)
            #plt.title("Query {} SF {} Memory".format(q, sf), fontdict=font)
            plt.xlabel("Time (s)", fontdict=font)
            ax.get_yaxis().set_major_formatter(ticker.FuncFormatter(lambda x,p: "{}".format(int(x/1000000000))))
            plt.ylabel("GBytes", fontdict=font)
            add_legend(ax)

            # Save to file
            name = "q{}_sf{}_mem.pdf".format(q, sf)
            print name
            plt.savefig(os.path.join(out_path, name))
            plt.close()

def latency(args, r_file, workdir, result_dir):
    results = {q:{sf:{nd:{sw:[] for sw in nodes if sw <= nd} for nd in nodes if nd <= 16} for sf in scale_factors} for q in ['3','4']}

    # Get the data
    for l in r_file:
        q = l[':q']
        sf = l[':sf']
        nd = l[':nd']
        sw = l[':sw']
        if not ':jobid' in l or l[':exp'] != ':latency':
            continue
        jobid = l[':jobid']
        jobpath = os.path.join(workdir, 'tpch{}'.format(q), 'job_{}'.format(jobid))
        # Add data from all latency files
        buffer = open(os.path.join(jobpath, 'latencies.txt')).read()
        m = re.search(r'mean:(\d+).*std_dev:(\d+)', buffer)
        results[q][sf][nd][sw].append((int(m.group(1)), int(m.group(2))))

    # average out the trials
    results2 = {q:{sf:{nd:{sw:None for sw in nodes if sw <= nd} for nd in nodes if nd <= 16} for sf in scale_factors} for q in ['3','4']}
    for q,v1 in results.iteritems():
        for sf,v2 in v1.iteritems():
            for nd,v3 in v2.iteritems():
                for sw,v4 in v3.iteritems():
                    if v4 != []:
                        avg_avg = sum(map(lambda x: x[0], v4))/len(v4)
                        # average stdev by taking average of variance
                        avg_stdev = math.sqrt(sum(map(lambda x: x[1]**2, v4))/float(len(v4)))
                        results2[q][sf][nd][sw] = (avg_avg, avg_stdev)

    # dump results
    with open(os.path.join(result_dir, 'plot_latency.txt'), 'w') as f:
        yaml.dump(results2, f)

    out_path = os.path.join(result_dir, 'plots')
    if not os.path.exists(out_path):
        os.mkdir(out_path)

    # plot per query, per sf/node, vs switch count
    for q,v1 in results2.iteritems():
        for sf,v2 in v1.iteritems():
            # New Figure per query
            f, ax = plt.subplots()

            # A line per node count
            for nd, v3 in v2.iteritems():
                if v3 is not None:
                    plt.errorbar(nodes, [v3[n][0] if n in v3 and v3[n] is not None else 0 for n in nodes],
                            yerr=[v3[n][1] if n in v3 and v3[n] is not None else 0 for n in nodes],
                            color=nd_styles[nd][0], marker=nd_styles[nd][1], label="{} Nodes".format(nd))

            # Labels, etc.
            plt.grid(True)
            #plt.legend(loc='best')
            plt.title("Query {} Latency".format(q), fontdict=font)
            plt.xlabel("Switches", fontdict=font)
            plt.ylabel("ms", fontdict=font)
            plt.ylim(ymin=0)
            add_legend(ax)

            # Save to file
            plt.savefig(os.path.join(out_path, "sw_q{}_sf{}_lat.pdf".format(q, sf)))
            plt.close()

    # create plots per query, per sf
    for q,v1 in results2.iteritems():
        # New Figure per query
        f, ax = plt.subplots()

        # A line per scale factor
        for sf, v2 in v1.iteritems():
            if v2 is not None:
                # find switch config that gives the min latency
                min_sw = {k: min(v.itervalues(), key=lambda t:sys.maxint if t is None else t[0])
                            for k, v in v2.iteritems()}
                plt.errorbar(nodes, [min_sw[n][0] if n in min_sw and min_sw[n] is not None else 0 for n in nodes],
                        yerr=[min_sw[n][1] if n in min_sw and min_sw[n] is not None else 0 for n in nodes],
                        color=sf_styles[sf][0], marker=sf_styles[sf][1], label="SF {}".format(sf))

        # Labels, etc.
        plt.grid(True)
        plt.title("Query {} Latency".format(q), fontdict=font)
        plt.xlabel("Worker Nodes", fontdict=font)
        plt.ylabel("ms", fontdict=font)
        add_legend(ax)

        # Save to file
        plt.savefig(os.path.join(out_path, "q{}_nd_lat.pdf".format(q)))
        plt.close()

    return

    # create plots per query, per nodes
    for q,v1 in results2.iteritems():
        # New Figure per query
        f, ax = plt.subplots()

        # invert the data
        res = {n:{sf:None for sf in scale_factors} for n in nodes}
        for sf, v2 in v1.iteritems():
            for n, val in v2.iteritems():
                res[n][sf] = val

        for n, v2 in res.iteritems():
            if v2 is not None:
                plt.errorbar(scale_factors,
                        [v2[sf][0] if v2[sf] is not None else 0 for sf in scale_factors],
                        yerr=[v2[sf][1] if v2[sf] is not None else 0 for sf in scale_factors],
                        marker='o', label="{} Nodes".format(n))

        # Labels, etc.
        plt.grid(True)
        plt.legend(loc='best')
        plt.title("Query {} Latency".format(q), fontdict=font)
        plt.xlabel("Scale Factor", fontdict=font)
        plt.ylabel("sec", fontdict=font)
        ax.set_xscale('log')

        # Save to file
        plt.savefig(os.path.join(out_path, "q{}_sf_lat.pdf".format(q)))
        plt.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--result-file', required=True, dest='result_file',
            help='Results file from which to read')
    parser.add_argument('-e', '--experiments', required=True, nargs='+',
            help='Experiments (s, l, m, gc)')
    parser.add_argument('-r', '--result-dir', dest='result_dir',
            help='Direct output to a specific directory')
    args = parser.parse_args()
    if args is None:
        parser.print_help()
        exit(1)

    with open(args.result_file) as f:
        results = yaml.load(f)

    workdir = os.path.dirname(args.result_file)
    print("workdir={}".format(workdir))
    result_dir = args.result_dir if args.result_dir is not None else workdir
    print("result_dir={}".format(result_dir))

    tests = results[':tests']
    if 's' in args.experiments:
        scalability(args, tests, workdir, result_dir)
    if 'm' in args.experiments:
        memory(args, tests, workdir, result_dir, ['3','4'])
    if 'l' in args.experiments:
        latency(args, tests, workdir, result_dir)


if __name__ == '__main__':
    main()
