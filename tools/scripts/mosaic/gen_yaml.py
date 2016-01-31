#!/usr/bin/env python
#
# Create a yaml file for running a mosaic file
# Note: *requires pyyaml*
import argparse
import yaml
import os
import copy
import re

import routing_patterns

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
        '1': ['lineitem'],
        '2': ['part', 'supplier', 'partsupp'], # nation, region
        '3': ['customer', 'orders', 'lineitem'],
        '4': ['lineitem', 'orders'],
        '5': ['customer', 'orders', 'lineitem', 'supplier'], # nation, region
        '6': ['lineitem'],
        '7': ['supplier', 'lineitem', 'orders', 'customer'], # nation
        '8': ['part', 'supplier', 'lineitem', 'orders', 'cutomer'], # nation, region
        '9': ['part', 'supplier', 'lineitem', 'partsupp', 'orders'], # nation
        '10': ['customer', 'orders', 'lineitem', 'nation'],
        '11': ['partsupp', 'supplier', 'nation'],
        '11a': ['partsupp', 'supplier'],
        '12': ['orders', 'lineitem'],
        '13': ['customer', 'orders'],
        '14': ['lineitem', 'part'],
        '15': ['supplier', 'lineitem'],
        '16': ['partsupp', 'part', 'supplier'],
        '17': ['lineitem', 'part'],
        '17a': ['lineitem', 'part'],
        '18': ['customer', 'orders', 'lineitem'],
        '18a': ['customer', 'orders', 'lineitem'],
        '19': ['lineitem', 'part'],
        '20': ['supplier', 'partsupp', 'part'], # nation
        '21': ['supplier', 'lineitem', 'orders'], # nation
        '22': ['customer', 'orders'],
        '22a': ['customer']
        }

def tpch_paths_local(path, switch_index, num_switches):
    tpch_files = {}
    for nm in tpch_names:
        tpch_files[nm] = []
    for nm in tpch_names:
        full_path = os.path.join(path, nm)
        files = sorted(os.listdir(full_path))
        for f in files:
            fname = os.path.join(full_path, f)
            if not os.path.isfile(fname):
                continue
            digit_match = re.search(r'\d+$', f)
            sentinel_match = re.search(r'.*sentinel.*', f)
            if sentinel_match:
                  tpch_files[nm].append({'path':fname})
            elif digit_match:
              file_index = int(digit_match.group())
              if file_index % num_switches == switch_index:
                  tpch_files[nm].append({'path':fname})
            else:
                raise ValueError("File %s does not end with digits. And is not sentintel. Can't partition among switches" % f)

    out = []
    for nm in tpch_names:
        out.append({'seq':tpch_files[nm]})

    return out

def tpch_mux_file_local(path, switch_index, num_switches, tpch_query):
  tables = "_".join(sorted(query_tables[tpch_query]))
  rest = "mux/%d/mux_%d_%d_%s.csv" % (num_switches, switch_index, num_switches, tables)
  return os.path.join(path, rest)

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

    # optimized routing data and corresponding pmap
    # create up front since it's heavy
    opt_route = None
    pmap = None
    if args.tpch_query:
        opt_route = routing_patterns.get_node_data(args.tpch_query)
        pmap = routing_patterns.get_pmap(args.tpch_query)

    # convert to dictionaries
    peers2 = []
    switch_index = 0
    peer_index = 0
    for (role, port) in peers:
        peer = {'role': wrap_role(role),
                'role2': wrap_role(role),
                'me':address(port),
                'peers':create_peers(peers),
                'eventlog': 'events%d.csv' % peer_index}
        peer_index += 1

        if pmap is not None:
            peer.update(pmap)

        if role == 'node':
            if opt_route:
                peer.update(opt_route)
            if args.message_profiling:
                peer['do_profile'] = True
                peer['mosaic_sendupoly_buffer_batch_sz'] = 1
                peer['mosaic_sendupoly_sample_mod'] = 5
                peer['mosaic_sendpoly_buffer_batch_sz'] = 1
                peer['mosaic_sendpoly_sample_mod'] = 5
                peer['mosaic_event_sample_mod'] = 1000000
                peer['mosaic_route_sample_mod'] = 1000000
                peer['mosaic_sendput_sample_mod'] = 1000000
                peer['mosaic_push_sample_mod'] = 1000000

        if role == 'switch' or role == 'switch_old':
            if args.latency_profiling:
                peer['mosaic_event_buffer_batch_sz'] = 1
                peer['mosaic_event_sample_mod'] = 0
                peer['mosaic_route_sample_mod'] = 1000000
                peer['mosaic_sendput_sample_mod'] = 1000000
            if args.message_profiling:
                peer['do_profile'] = True
                peer['mosaic_sendupoly_buffer_batch_sz'] = 1
                peer['mosaic_sendupoly_sample_mod'] = 5
                peer['mosaic_sendpoly_buffer_batch_sz'] = 1
                peer['mosaic_sendpoly_sample_mod'] = 5
                peer['mosaic_event_sample_mod'] = 1000000
                peer['mosaic_route_sample_mod'] = 1000000
                peer['mosaic_sendput_sample_mod'] = 1000000
                peer['mosaic_push_sample_mod'] = 1000000
            if args.csv_path:
                peer['switch_path'] = args.csv_path
            if args.tpch_data_path:
                if not args.tpch_query:
                  raise ValueError("Need tpch_query to when using tpch_data_path")
                peer['seqfiles'] = tpch_paths_local(args.tpch_data_path, switch_index, args.num_switches)
            if args.tpch_inorder_path:
                peer['inorder'] = args.tpch_inorder_path
            elif args.tpch_infer_inorder_path:
                if not args.tpch_query:
                  raise ValueError("Cannot infer mux files without tpch_query number")
                peer['inorder'] = tpch_mux_file_local(args.tpch_data_path, switch_index, args.num_switches, args.tpch_query)
            if opt_route is not None:
                peer.update(opt_route)
            switch_index = switch_index + 1



        peer.update(extra_args)
        peers2.append(peer)

    # dump out
    dump_yaml(peers2)

def mk_k3_seq_files(total_switches, sw_indexes, path, tables):
    return { 'switch_indexes': sw_indexes,
             'num_switches'  : total_switches,
             'data_dir'      : path,
             'tables'        : tables
           }

def create_dist_file(args):
    query = args.tpch_query
    num_switches = args.num_switches
    num_nodes = args.num_nodes

    extra_args = parse_extra_args(args.extra_args)

    switch_role_nm = "switch_old" if args.csv_path else "switch"

    pmap = routing_patterns.get_pmap(query)
    opt_route = routing_patterns.get_node_data(query)

    master_role = {'role': wrap_role('master'),
                   'role2': wrap_role('master')}
    master_role.update(pmap if pmap is not None else {})
    master_role.update(extra_args)

    timer_role = {'role': wrap_role('timer'),
                  'role2': wrap_role('timer')}
    timer_role.update(pmap if pmap is not None else {})
    timer_role.update(extra_args)

    switch_role = {'role': wrap_role(switch_role_nm),
                   'role2': wrap_role(switch_role_nm)}
    if args.csv_path:
        switch_role['switch_path'] = args.csv_path
    if args.tpch_inorder_path:
        switch_role['inorder'] = args.tpch_inorder_path
    switch_role.update(pmap if pmap is not None else {})
    switch_role.update(opt_route if opt_route is not None else {})
    # latency profiling needs full switch logging
    if args.latency_profiling:
        switch_role['mosaic_event_buffer_batch_sz'] = 1
        switch_role['mosaic_event_sample_mod'] = 0

    if args.message_profiling:
      switch_role['do_profile'] = True
      switch_role['mosaic_sendupoly_buffer_batch_sz'] = 1
      switch_role['mosaic_sendupoly_sample_mod'] = 5
      switch_role['mosaic_sendpoly_buffer_batch_sz'] = 1
      switch_role['mosaic_sendpoly_sample_mod'] = 5
      switch_role['mosaic_event_sample_mod'] = 1000000
      switch_role['mosaic_route_sample_mod'] = 1000000
      switch_role['mosaic_sendput_sample_mod'] = 1000000
      switch_role['mosaic_push_sample_mod'] = 1000000

    switch_role.update(extra_args)

    node_role = {'role': wrap_role('node'),
                 'role2': wrap_role('node')}
    node_role.update(pmap if pmap is not None else {})
    node_role.update(opt_route if opt_route is not None else {})
    if args.message_profiling:
      node_role['do_profile'] = True
      node_role['mosaic_sendupoly_buffer_batch_sz'] = 1
      node_role['mosaic_sendupoly_sample_mod'] = 5
      node_role['mosaic_sendpoly_buffer_batch_sz'] = 1
      node_role['mosaic_sendpoly_sample_mod'] = 5
      node_role['mosaic_event_sample_mod'] = 1000000
      node_role['mosaic_route_sample_mod'] = 1000000
      node_role['mosaic_sendput_sample_mod'] = 1000000
      node_role['mosaic_push_sample_mod'] = 1000000
    node_role.update(extra_args)

    # switch1_env = {'peer_globals': [master_role, timer_role, switch_role]}
    #if args.tpch_data_path:
    #     switch1_env['k3_seq_files'] = \
    #     mk_k3_seq_files(num_switches, [0], args.tpch_data_path, )

    switch_env  = {'k3_globals': switch_role, 'mem': 'some'}
    node_env    = {'k3_globals': node_role}
    master_env  = {'k3_globals': master_role, 'mem': 'some'}
    timer_env   = {'k3_globals': timer_role, 'mem': 'some'}

    # The amount of cores we have of qps. # TODO pack multiple switches into a single role
    extra_machine = 'qp-hd1$'
    switch_machines = ['qp3', 'qp4', 'qp5', 'qp6']
    num_switch_machines = len(switch_machines)
    num_cores = 16
    max_switches = num_switch_machines * num_cores

    k3_roles = []

    # Pack multiple switches into each role
    if num_switches > max_switches:
        raise ValueError("Invalid number of switches: {}. Maximum is {}".format(num_switches, max_switches))
    used_machines = num_switches if num_switches < num_switch_machines else num_switch_machines
    # The max switches per machine
    switches_per_machine = max([1, num_switches / num_switch_machines])
    # The point at which we have 1 less switch per machine (0 means there is no such point)
    fewer_point = num_switches % num_switch_machines

    for i in range(used_machines):
        per_machine = switches_per_machine if i < fewer_point else switches_per_machine - 1
        switch_indexes = [(i * switches_per_machine) + j for j in range(per_machine)]
        switch_env2 = copy.deepcopy(switch_env)
        if args.tpch_data_path:
            switch_env2['k3_seq_files'] = \
                mk_k3_seq_files(num_switches, switch_indexes, args.tpch_data_path, sorted(query_tables[query]))
        k3_roles.append(('Switch' + str(i), switch_machines.pop(0), per_machine, None, switch_env2))

    k3_roles.append(('Master', extra_machine, 1, None, master_env))
    k3_roles.append(('Timer',  extra_machine, 1, None, timer_env))

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

def main():
    parser = argparse.ArgumentParser()
    parser.set_defaults(run_mode = "local")
    parser.add_argument("-d", "--dist", action='store_const', dest="run_mode", const="dist")
    parser.add_argument("-s", "--switches", type=int, help="number of switches",
                        dest="num_switches", default=1)
    parser.add_argument("-n", "--nodes", type=int, help="number of nodes",
                        dest="num_nodes", default=4)
    parser.add_argument("--nmask", type=str, help="mask for nodes", default="^.*hd(([2-9])|(1[0-4]))$")
    parser.add_argument("--perhost", type=int, help="peers per host", default=None)

    parser.add_argument("--csv_path", type=str, help="path of csv data source", default=None)
    parser.add_argument("--tpch_data_path", type=str, help="path of tpch flatpolys", default=None)
    parser.add_argument("--tpch_inorder_path", type=str, help="path of tpch inorder file", default=None)
    parser.add_argument("--tpch_infer_inorder_path", action='store_false', default=True, help="automatic inorder file")
    parser.add_argument("--tpch_query", type=str, help="query")
    parser.add_argument("--extra-args", help="extra arguments in x=y format")
    parser.add_argument("--latency-profiling", action="store_true", default=False, dest="latency_profiling", help="activate profiling")
    parser.add_argument("--message-profiling", action="store_true", default=False, dest="message_profiling", help="activate profiling")
    args = parser.parse_args()
    if args.run_mode == "dist":
        create_dist_file(args)
    elif args.run_mode == "local":
        create_local_file(args)

if __name__ == '__main__':
    main()
