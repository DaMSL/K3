#!/usr/bin/env python3
#
# Script to generate command line for running Cumulus
#
# Note: this script assumes it's run from a directory above K3-Core and K3-Driver
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
  parser.add_argument("-m", "--partmap", type=str, dest="part_map", 
                    default=None, help="Specify path to k3 partition map file")
  parser.add_argument("-n", "--network",  action='store_true', dest="use_network", 
                    default=False,       help="Use the network")
  parser.add_argument("-c", "--concat",  action='store_true', dest="concat", 
                    default=False,       help="Concatenate into one file before running")


  args = parser.parse_args()

  # get the plain file
  (file_dir, file_name) = os.path.split(args.k3_file)
  file_no_ext = os.path.splitext(file_name)[0]

  # default files
  if not args.peers_file:
    args.peers_file = os.path.join(file_dir, 'peers.k3')

  if not args.part_map:
    args.part_map = os.path.join(file_dir, file_no_ext + '_part.k3')

  network_txt = ""
  if args.use_network:
    network_txt = "-n"

  #read in k3 peers file
  with open(args.peers_file, 'r') as peerfile:
    peer_str = peerfile.read()
  peer_str_oneline = peer_str.replace('\n', '')

  # parse the peers file to create a peer command line string
  reg = r'declare my_peers[^=]+= \{\|[^|]+\|(.+)\|\} @.+'
  match_obj = re.match(reg, peer_str_oneline)
  inner_str = match_obj.group(1)
  # find peers
  peers_l = re.findall(r'\{[^}]+}', inner_str)
  peers = []
  for peer_s in peers_l:
    # read the peer details
    m = re.match(r'{\s*\w+\s*:\s*(.+),\s*\w+\s*:\s*"(.+)",\s*\w+\s*:\s*(.+)}', peer_s)
    peers.append((m.group(1), m.group(2), m.group(3)))

  # Create short pre.k3 file
  pre_s = 'include "Core/Builtins.k3"\n' +  \
          'include "Annotation/Set.k3"\n' + \
          'include "Annotation/Seq.k3"\n\n'
  with open('pre.k3', 'w') as out:
    out.write(pre_s)

  # Create peer_str for command
  peer_s = ""
  for peer in peers:
    if peer[1]=='switch':
      role=r':role=\"s1\"' 
    else:
      role=''
    peer_s += '-p {0}{1} '.format(peer[0], role)

  # In concat mode, create one temporary k3 file
  if args.concat:
    s = pre_s + peer_str
    with open(args.part_map, 'r') as f:
      s += f.read()
    with open(args.k3_file, 'r') as f:
      s += f.read()
    with open('temp.k3', 'w') as f:
      f.write(s)
    file_s = 'temp.k3'
  else:
    file_s = 'pre.k3,{args.peers_file},{args.part_map},{args.k3_file}'.format(**locals())

  # Create command line
  log_s = '--log "debug"'
  log_file = file_no_ext + '.log'
  cmd = 'time K3-Driver/dist/build/k3/k3 -I K3-Core/lib interpret {peer_s} -b --simple {log_s} {file_s} > {log_file} 2>&1'.format(**locals())
  print_sys(cmd)

  # Create a clean log file
  cmd = 'K3-Driver/scripts/cumulus/sanitize_log.py {log_file} > {file_no_ext}_clean.log'.format(**locals())
  print_sys(cmd)

if __name__=='__main__':
  main ()
