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
    master_role = {'role': 'master'}
    timer_role  = {'role': 'timer'}
    switch_role = {'role': 'switch', 'switch_path': file_path}
    node_role   = {'role': 'node'}
    switch1_env = {'peer_globals': [master_role, timer_role, switch_role]}
    switch_env  = {'k3_globals': switch_role}
    node_env    = {'k3_globals': node_role}

    k3_roles = []
    k3_roles += [('Switch1', 'qp3', 3, None, switch1_env)]
    if num_switches > 1:
        k3_roles += [('Switch' + str(i + 1), 'qp' + str((i % 4) + 3), 1, None, switch_env) for i in range(1, num_switches)]

    k3_roles += [('Node' + str(i), nmask, 1, perhost, node_env) for i in range(1, num_nodes+1)]

    launch_roles = []
    for (name, addr, peers, perh, peer_envs) in k3_roles:
        newrole = { 'hostmask'     : addr,
                    'name'         : name,
                    'peers'        : peers,
                    'privileged'   : False,
                    'volumes'      : [{'host':'/local', 'container':'/local'}],
                   }

        if perh is not None:
            newrole['peers_per_host'] = perh

        newrole.update(peer_envs)
        launch_roles += [newrole]

    # dump out
    dump_yaml(launch_roles)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--switches", type=int, help="number of switches", dest="num_switches", default=1)
    parser.add_argument("-n", "--nodes", type=int, help="number of nodes", dest="num_nodes", default=4)
    parser.add_argument("--nmask", type=str, help="mask for nodes", default="qp-hm.|qp-hd.?")
    parser.add_argument("--perhost", type=int, help="peers per host", default=2)
    parser.add_argument("-d", "--dist", action='store_true', dest="dist_mode", default=False)
    parser.add_argument("-f", "--file", type=str, dest="file_path", help="file path", default="/local/agenda.csv")
    args = parser.parse_args()
    if args.dist_mode:
        create_dist_file(args.num_switches, args.perhost, args.num_nodes, args.nmask, args.file_path)
    else:
        create_file(args.num_switches, args.num_nodes, args.file_path)


if __name__ == '__main__':
    main()
