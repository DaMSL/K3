#!/usr/bin/env python
#
# Create a yaml file for running a mosaic file
# Note: *requires pyyaml*
import argparse
import yaml
import os
import copy

def address(port):
    return ['127.0.0.1', port]

def wrap_role(role):
    return [{'i': role}]

def create_peers(peers):
    res = []
    for peer in peers:
        res += [{'addr':address(peer[1])}]
    return res

def dump_yaml(col):
    print "---"
    for i, peer in enumerate(col):
        print yaml.dump(peer, default_flow_style=True),
        if i < len(col) - 1:
            print "---"

def parse_extra_args(args):
    extra_args = {}
    if args is not None:
        args = args.split(",")
        for arg in args:
            val = arg.split("=", 1)
            extra_args[val[0]] = val[1]
    return extra_args

tpch_names = ['sentinel', 'customer', 'lineitem', 'orders', 'part', 'partsupp', 'supplier']
query_tables = {
        1: ['lineitem'],
        2: ['part', 'supplier', 'partsupp'], # nation, region
        3: ['customer', 'orders', 'lineitem'],
        4: ['lineitem', 'orders'],
        5: ['customer', 'orders', 'lineitem', 'supplier'], # nation, region
        6: ['lineitem'],
        7: ['supplier', 'lineitem', 'orders', 'customer'], # nation
        8: ['part', 'supplier', 'lineitem', 'orders', 'cutomer'], # nation, region
        9: ['part', 'supplier', 'lineitem', 'partsupp', 'orders'], # nation
        10: ['customer', 'orders', 'lineitem', 'nation'],
        11: ['partsupp', 'supplier', 'nation'],
        110: ['partsupp', 'supplier'],
        12: ['orders', 'lineitem'],
        13: ['customer', 'orders'],
        14: ['lineitem', 'part'],
        15: ['supplier', 'lineitem'],
        16: ['partsupp', 'part', 'supplier'],
        17: ['lineitem', 'part'],
        170: ['lineitem', 'part'],
        18: ['customer', 'orders', 'lineitem'],
        180: ['customer', 'orders', 'lineitem'],
        19: ['lineitem', 'part'],
        20: ['supplier', 'partsupp', 'part'], # nation
        21: ['supplier', 'lineitem', 'orders'], # nation
        22: ['customer', 'orders'],
        220: ['customer']
        }
        
def tpch_paths_local(path):
    tpch_files = {}
    for nm in tpch_names:
        tpch_files[nm] = []
    for nm in tpch_names:
        full_path = os.path.join(path, nm)
        files = os.listdir(full_path)
        for f in files:
            fname = os.path.join(full_path, f)
            if os.path.isfile(fname):
                tpch_files[nm].append({'path':fname})
    out = []
    for nm in tpch_names:
        out.append({'seq':tpch_files[nm]})

    return out

def tpch_paths_dist(p):
    files = []
    for n in names:
        files.append({'path': os.path.join(p, n)})

    return files

def create_local_file(args):
    extra_args = parse_extra_args(args.extra_args)
    switch_role = "switch_old" if args.csv_path else "switch"

    peers = []
    peers.append(('master', 40000))
    peers.append(('timer', 40001))

    switch_ports = 50001
    for i in range(args.num_switches):
        peers.append((switch_role, switch_ports + i))

    node_ports = 60001
    for i in range(args.num_nodes):
        peers.append(('node', node_ports + i))

    # convert to dictionaries
    peers2 = []
    seqfiles = tpch_paths_local(args.tpch_data_path)
    for (role, port) in peers:
        peer = {'role': wrap_role(role), 'me':address(port), 'peers':create_peers(peers)}

        if role == 'switch' or role == 'switch_old':
            if args.csv_path:
                peer['switch_path'] = args.csv_path
            if args.tpch_data_path:
                peer['seqfiles'] = seqfiles
                peer['inorder'] = args.tpch_inorder_path

        peer.update(extra_args)
        peers2.append(peer)

    # dump out
    dump_yaml(peers2)

def mk_k3_seq_files(num_switches, sw_index, path):
    return [{
        'switch_index': sw_index,
        'num_switches': num_switches,
        'paths': [ os.path.join(path, n) for n in tpch_names ],
        'var': 'seqfiles'
            }]

def create_dist_file(args):
    num_switches = args.num_switches
    num_nodes = args.num_nodes

    extra_args = parse_extra_args(args.extra_args)

    switch_role = "switch_old" if args.csv_path else "switch"

    master_role = {'role': wrap_role('master')}
    master_role.update(extra_args)

    timer_role = {'role': wrap_role('timer')}
    timer_role.update(extra_args)

    switch_role = {'role': wrap_role(switch_role)}
    if args.csv_path:
        switch_role['switch_path'] = args.csv_path
    if args.tpch_inorder_path:
        switch_role['inorder'] = args.tpch_inorder_path
    switch_role.update(extra_args)

    node_role = {'role': wrap_role('node')}
    node_role.update(extra_args)

    switch1_env = {'peer_globals': [master_role, timer_role, switch_role]}
    if args.tpch_data_path:
        switch1_env['k3_seq_files'] = \
          mk_k3_seq_files(num_switches, 0, args.tpch_data_path)

    switch_env  = {'k3_globals': switch_role}
    node_env    = {'k3_globals': node_role}
    master_env  = {'k3_globals': master_role}
    timer_env   = {'k3_globals': timer_role}

    # The amount of cores we have of qps. deduct timer and master on qp3
    switch_res = (['qp3', 'qp4', 'qp5', 'qp6'] * 14) + (['qp4', 'qp5' ,'qp6'] * 2)

    k3_roles = []

    for i in range(num_switches):
        switch_env2 = copy.deepcopy(switch_env)
        if args.tpch_data_path:
            switch_env2['k3_seq_files'] = \
                mk_k3_seq_files(num_switches, i, args.tpch_data_path)
        k3_roles.append(('Switch' + str(i), switch_res.pop(0), 1, None, switch_env2))

    k3_roles.append(('Master', switch_res.pop(0), 1, None, master_env))
    k3_roles.append(('Timer',  switch_res.pop(0), 1, None, timer_env))

    k3_roles.append(('Nodes', args.nmask, num_nodes, args.perhost, node_env))

    launch_roles = []
    for (name, addr, peers, perhost, peer_envs) in k3_roles:
        newrole = {
            'hostmask'  : addr,
            'name'      : name,
            'peers'     : peers,
            'privileged': True,
            'volumes'   : [{'host':'/local', 'container':'/local'},
                           {'host':'/data', 'container':'/data'}]
        }

        if perhost is not None:
            newrole['peers_per_host'] = perhost

        newrole.update(peer_envs)
        launch_roles.append(newrole)

    # dump out
    dump_yaml(launch_roles)

## deprecated?? ##
def create_multicore_file(args):
    if num_switches > 1:
        raise ValueError("Can't create multicore deployment with more than one switch just yet.")

    extra_args = parse_extra_args(args.extra_args)
    switch_role = "switch_old" if args.csv_data else "switch"

    role = {
        "hostmask": "qp3",
        "name": "Everything",
        "peer_globals": [
            {"role": wrap_role("master")}, {"role": wrap_role("timer")}, {"role": wrap_role(switch_role), "switch_path": file_path}
        ] + [{"role": wrap_role("node")} for _ in range(num_nodes)],
        "privileged": False,
        'volumes'   : [{'host':'/local', 'container':'/local'}],
    }

    role["peers"] = len(role["peer_globals"])

    for peer_role in role["peer_globals"]:
        peer_role.update(extra_args)

    dump_yaml([role])

def main():
    parser = argparse.ArgumentParser()
    parser.set_defaults(run_mode = "local")
    parser.add_argument("-d", "--dist", action='store_const', dest="run_mode", const="dist")
    parser.add_argument("-m", "--multicore", action='store_const', dest="run_mode", const="multicore")
    parser.add_argument("-s", "--switches", type=int, help="number of switches",
                        dest="num_switches", default=1)
    parser.add_argument("-n", "--nodes", type=int, help="number of nodes",
                        dest="num_nodes", default=4)
    parser.add_argument("--nmask", type=str, help="mask for nodes", default="qp-hm.|qp-hd.?")
    parser.add_argument("--perhost", type=int, help="peers per host", default=None)

    parser.add_argument("--csv_path", type=str, help="path of csv data source", default=None)
    parser.add_argument("--tpch_data_path", type=str, help="path of tpch flatpolys", default=None)
    parser.add_argument("--tpch_inorder_path", type=str, help="path of tpch inorder file", default=None)
    parser.add_argument("--extra-args", type=str, help="extra arguments in x=y format")
    args = parser.parse_args()
    if args.run_mode == "dist":
        create_dist_file(args)
    elif args.run_mode == "local":
        create_local_file(args)
    elif args.run_mode == "multicore":
        create_multicore_file(args)

if __name__ == '__main__':
    main()
