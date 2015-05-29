#!/usr/bin/env python
#
# Create a yaml file for running a mosaic file
# Note: *requires pyyaml*
import argparse
import yaml

def address(port):
    return ['127.0.0.1', port]

def create_peers(peers):
    res = []
    for p in peers:
        res += [{'addr':address(p[1])}]
    return res

def emit_yaml(col):
    print "---"
    for i, p in enumerate(col):
        print yaml.dump(p, default_flow_style=True),
        if i < len(col) - 1:
            print "---"

def create_file(num_switches, num_nodes):
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
        peers2 += [{'role':role, 'me':address(port), 'peers':create_peers(peers)}]

    # dump out
    emit_yaml(peers2)

def create_nodes():
    hms = [ 'qp-hm' + str(x) for x in range(1,9) ]
    qps = [ 'qp' + str(x) for x in range(1,7) ]
    hds = [ 'qp-hd' + str(x) for x in range(1,17) ]
    return hms + qps + hds

def create_dist_file(num_switches, num_nodes):
    # for now, each peer is on a different node
    nodes = create_nodes()
    peers = []
    peers += [('Master', 'master', nodes.pop(0))]
    peers += [('Timer',  'timer',  nodes.pop(0))]
    peers += [('Switch' + str(i), 'switch', nodes.pop(0)) for i in range(1, num_switches+1)]
    peers += [('Node'   + str(i), 'node',   nodes.pop(0)) for i in range(1, num_nodes+1)]
    peers2 = []
    for (name, role, addr) in peers:
        peers2 += [{'name':name,
                    'peers':1,
                    'hostmask':addr,
                    'privileged':False,
                    'volumes':{'host':'/local', 'container':'/local'},
                    'k3_globals':{'role':role}}]
    # dump out
    emit_yaml(peers2)



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--switches", type=int, help="number of switches", dest="num_switches", default=1)
    parser.add_argument("-n", "--nodes", type=int, help="number of nodes", dest="num_nodes", default=1)
    parser.add_argument("-d", "--dist", action='store_true', dest="dist_mode", default=False)
    args = parser.parse_args()
    if args.dist_mode:
        create_dist_file(args.num_switches, args.num_nodes)
    else:
        create_file(args.num_switches, args.num_nodes)


if __name__ == '__main__':
    main()
