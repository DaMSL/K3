#!/usr/bin/env python
#
# Create a yaml file for running a mosaic file
# Note: *requires pyyaml*
import os
import argparse
import yaml

def address(port):
    return ['127.0.0.1', port]

def create_peers(peers):
    res = []
    for p in peers:
        res += [{'addr':address(p[1])}]
    return res

def dump_yaml(col):
    print "---"
    for i, p in enumerate(col):
        print yaml.dump(p, default_flow_style=True),
        if i < len(col) - 1:
            print "---"

def create_file(num_switches, num_nodes, file_path):
    peers = []
    peers += [('master', 40000)]
    peers += [('timer',  40001)]
    switch_ports = 50001
    for i in range(num_switches):
        peers += [('switch', switch_ports + i)]
    node_ports = 60001
    for i in range(num_nodes):
        peers += [('node',   node_ports + i)]

    # convert to dictionaries
    peers2 = []
    for (role, port) in peers:
        peers2 += [{'role':role, 'me':address(port), 'switch_path':file_path, 'peers':create_peers(peers)}]

    # dump out
    dump_yaml(peers2)

def create_dist_file(num_switches, perhost, num_nodes, nmask, file_path):
    # for now, each peer is on a different node
    peers = []
    peers += [('Master', 'master', 'qp3', None, None)]
    peers += [('Timer',  'timer',  'qp3', None, None)]
    peers += [('Switch1', 'switch', 'qp3', file_path, None)]
    peers += [('Node' + str(i), 'node', nmask, None, perhost) for i in range(1, num_nodes+1)]
    if num_switches > 1:
        peers += [('Switch' + str(i + 1), 'switch', 'qp' + str((i % 4) + 3), file_path, None) for i in range(1, num_switches)]

    peers2 = []
    for (name, role, addr, path, perh) in peers:
        k3_globals = {'role':role}
        # add path for switch
        if path is not None:
            k3_globals['switch_path'] = path

        newpeer = { 'name':name,
                    'peers':1,
                    'hostmask':addr,
                    'privileged':False,
                    'volumes':[{'host':'/local', 'container':'/local'}],
                    'k3_globals':k3_globals
                   }
        if perh is not None: 
            newpeer['peersperhost'] = perh

        peers2 += [newpeer]

    # dump out
    dump_yaml(peers2)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--switches", type=int, help="number of switches", dest="num_switches", default=1)
    parser.add_argument("-n", "--nodes", type=int, help="number of nodes", dest="num_nodes", default=8)
    parser.add_argument("--nmask", type=str, help="mask for nodes", default="qp-hm.|qp-hd.?")
    parser.add_argument("--perhost", type=int, help="peers per host", default=3)
    parser.add_argument("-d", "--dist", action='store_true', dest="dist_mode", default=False)
    parser.add_argument("-f", "--file", type=str, dest="file_path", help="file path", default="/local/agenda.csv")
    args = parser.parse_args()
    if args.dist_mode:
        create_dist_file(args.num_switches, args.perhost, args.num_nodes, args.nmask, args.file_path)
    else:
        create_file(args.num_switches, args.num_nodes, args.file_path)


if __name__ == '__main__':
    main()
