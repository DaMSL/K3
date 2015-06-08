#!/usr/bin/env python3
#
# Script to generate command line for running Cumulus
#
# Note: this script assumes it's run from a directory above K3/
# Feel free to change it so it's more portable
import argparse
import os
import re

def print_sys(cmd):
    print(cmd)
    os.system(cmd)

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("k3_file", type=str, help="Specify path of k3 file")
    parser.add_argument("-p", "--peerfile", type=str, dest="peers_file",
                        default=None, help="Specify path to k3 peers file")
    parser.add_argument("-m", "--partmap", type=str, dest="partmap_file",
                        default=None, help="Specify path to k3 partition map file")
    parser.add_argument("-n", "--network", action='store_true', dest="use_network",
                        default=False, help="Use the network")
    parser.add_argument("-i", "--index", type=int, dest="peer_index", default=None,
                        help="Specify which peer (indexed from 1) to run on the local machine")
    parser.add_argument("-t", "--halt_time", type=int, dest="halt_time", default=None,
                        help="Give time for a halting-processing instruction (for distributed)")

    args = parser.parse_args()

    # translate input paths to absolute versions
    k3_file = os.path.abspath(args.k3_file)
    peers_file = os.path.abspath(args.peers_file) if args.peers_file else None
    partmap_file = os.path.abspath(args.partmap_file) if args.partmap_file else None

    # get the script path and k3_prog_path
    # NOTE: we assume a certain location for the script
    script_path = os.path.abspath(os.path.dirname(__file__))
    base_path = os.path.abspath(os.path.join(script_path, '../../..'))
    k3_prog_path = os.path.join(base_path, 'K3/dist/build/k3/k3')

    # get the plain file
    (file_dir, file_name) = os.path.split(k3_file)
    file_no_ext = os.path.splitext(file_name)[0]

    # default files
    if not peers_file:
        peers_file = os.path.join(file_dir, 'peers.k3')

    if not partmap_file:
        partmap_file = os.path.join(file_dir, file_no_ext + '_part.k3')

    network_s = "-n" if args.use_network else ""

    # read in k3 peers file
    with open(peers_file, 'r') as peerfile:
        peer_file_s = peerfile.read()
    peer_file_s_oneline = peer_file_s.replace('\n', '')

    # Create short pre.k3 string
    pre_s = 'include "Core/Builtins.k3"\n' +  \
            'include "Annotation/Set.k3"\n' + \
            'include "Annotation/Seq.k3"\n' + \
            'include "Core/Time.k3"\n' +  \
            '\n'

    # parse the peers file to create a peer command line string
    reg = r'declare my_peers[^=]+= \{\|[^|]+\|(.+)\|\} @.+'
    match_obj = re.match(reg, peer_file_s_oneline)
    inner_str = match_obj.group(1)
    # find peers
    peers_l = re.findall(r'\{[^}]+}', inner_str)
    peers = []
    for peer_cmd_s in peers_l:
        # read the peer details
        m = re.match(r'{\s*\w+\s*:\s*(.+),\s*\w+\s*:\s*"(.+)",\s*\w+\s*:\s*(.+)}', peer_cmd_s)
        peers.append((m.group(1), m.group(2), m.group(3)))

    # Constrain the peers based on our peer index selection (if any)
    if args.peer_index:
        peers = [peers[args.peer_index - 1]]

    # Create peer_file_s for command
    peer_cmd_s = ""
    for peer in peers:
        if peer[1] == 'switch':
            role = r':role=\"s1\"'
        else:
            role = ''
        peer_cmd_s += '-p {peer[0]}{role} '.format(**locals())

    # Create a temporary k3 file
    s = pre_s + peer_file_s
    with open(partmap_file, 'r') as f:
        s += f.read()
    with open(k3_file, 'r') as f:
        s += f.read()
    # replace instances of data file in k3 file
    default_data_file = file_no_ext + '.csv'
    new_data_file = os.path.join(file_dir, default_data_file)
    # add path to data file
    old = r'file "{default_data_file}" k3'.format(**locals())
    new = r'file "{new_data_file}" k3'.format(**locals())
    s = re.sub(old, new, s)

    # add call to haltEngine at end of switch_main()
    if args.halt_time:
        old = 'else \(\)\)\n\nsource'
        new = '''else
                (sleep {0};
                (__HaltAllNodes, me) <- ()))

    source'''.format(args.halt_time)
        s = re.sub(old, new, s)

    # write the temp file
    file_s = 'temp.k3'
    with open(file_s, 'w') as f:
        f.write(s)

    # Create command line
    log_s = '--log "debug"'
    log_file = file_no_ext + '.log'
    lib_path = os.path.join(base_path, 'K3/lib')
    cmd = 'time {k3_prog_path} -I {lib_path} interpret {network_s} {peer_cmd_s} -b --simple {log_s} {file_s} > {log_file} 2>&1'.format(**locals())
    print_sys(cmd)

    # Create a clean log file
    sanitize_file = os.path.join(base_path, 'K3/scripts/mosaic/sanitize_log.py')
    cmd = '{sanitize_file} --unhash {log_file} > {file_no_ext}_clean.log'.format(**locals())
    print_sys(cmd)

if __name__=='__main__':
    main ()
