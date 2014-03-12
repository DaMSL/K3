#!/usr/bin/env python3.3
#
# Script to generate command line for running Cumulus
#
# Note: this script assumes it's run from a directory above K3-Core and K3-Driver
# Feel free to change it so it's more portable
import argparse
import os
import re

def main():
  parser = argparse.ArgumentParser()

  parser.add_argument("k3_file", type=str, help="Specify path of k3 file")
  parser.add_argument("-p", "--peerfile", type=str, dest="peers_file", 
                    default="peers.k3", help="Specify path to k3 peers file")
  parser.add_argument("-m", "--partmap", type=str, dest="part_map", 
                    default="partmap.k3", help="Specify path to k3 partition map file")
  parser.add_argument("-n", "--network",  action='store_true', dest="use_network", 
                    default=False,       help="Whether to use the network")

  args = parser.parse_args()

  if args.use_network: network_txt = "-n"
  else: network_txt = ""

  #read in k3 peers file
  with open(args.peers_file, 'r') as peerfile:
    peer_str = peerfile.read().replace('\n', '')
  #print(peer_str)
  reg = r'declare peers .* = \{\|[^|]+\|(.+)\|\} @.+'
  m = re.compile(reg).match(peer_str)
  inner_str = m.group(1)
  #print(inner_str)
  reg = r'\{[^}]+}'
  peers_l = re.findall(reg, inner_str)
  #print(peers_l)
  reg = r'{\s*\w+\s*:\s*(.+),\s*\w+\s*:\s*"(.+)",\s*\w+\s*:\s*(.+)}'
  peers = []
  for peer_s in peers_l:
    m = re.match(reg, peer_s)
    peers.append((m.group(1), m.group(2), m.group(3)))
  #print(peers)

  # Create short pre.k3 file
  pre_s = 'include "Core/Builtins.k3"\n' +  \
          'include "Annotation/Set.k3"\n' + \
          'include "Annotation/Seq.k3"\n\n'
  with open('pre.k3', 'w') as out:
    out.write(pre_s)

  # Create peer_str for command
  peer_s = ""
  for peer in peers:
    if peer[1]=='switch': role=r':role=\"s1\"' 
    else: role=''
    peer_s += '-p {0}{1} '.format(peer[0], role)

  # Create command line
  log_s = '--log "Language.K3.Interpreter#Dispatch:debug" --log "Language.K3/Runtime.Engine#EngineSteps:debug" '
  file_s = 'pre.k3,{0},{1},{2}'.format(args.peers_file, args.part_map, args.k3_file)
  cmd = 'K3-Driver/dist/build/k3/k3 -I K3-Core/lib interpret {0} -b {1} {2}'.format(peer_s, log_s, file_s)
  print(cmd) # so we can see what's going on
  os.system(cmd)
    

if __name__=='__main__':
  main ()
