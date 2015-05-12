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

def entity(role, port, peers):
    return {'role':role, 'me':address(port), 'peers':create_peers(peers)}

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
    for p in peers:
        peers2 += [entity(p[0], p[1], peers)]

    # dump out
    print "---"
    for i, p in enumerate(peers2):
        print(yaml.dump(p, default_flow_style=True))
        if i < len(peers2) - 1:
            print "---"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--switches", type=int, help="number of switches", dest="num_switches", default=1)
    parser.add_argument("-n", "--nodes", type=int, help="number of nodes", dest="num_nodes", default=1)
    args = parser.parse_args()
    create_file(args.num_switches, args.num_nodes)

if __name__ == '__main__':
    main()
