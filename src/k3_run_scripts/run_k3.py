#!/usr/bin/env python3
import csv
from optparse import OptionParser
import subprocess
import os
import sys
import time

k3_lib_path = "/home/vagrant/K3/K3-Core/k3/lib/"
k3_path = "/home/vagrant/K3/K3-Driver/dist/build/k3/k3"
#this script assumes 'k3' is on your path


def simulation_mode(topology_fns,ssh):
  for topology_fn in topology_fns:  
    time.sleep(2)
    with open(topology_fn, 'rb') as csvfile:
      #reading program path header in file
      program_path = csvfile.readline().replace("\n","")
      #reading description header in file
      header = csvfile.readline().split(",")


      full_cmd = "{1} -I {0} interpret -b".format(k3_lib_path, k3_path)
 
      top_reader = csv.reader(csvfile, delimiter=',', quotechar='~')
      count = 0
      hostname = "localhost"
      for row in top_reader:
        count += 1
        hostname = row[0]
        peer = row[1].replace("\n","")
        
        #construct k3 program parameters
        peer_str = '-p {0}'.format(peer)
        for index in range (2, min(len(header), len(row))):
          peer_str = peer_str + ':{0}={1}'.format(header[index].replace("\n",""), row[index].replace("\n",""))

        for index in range(len(header), len(row)):
          peer_str = peer_str + ':{0}'.format(row[index].replace("\n",""))
         
        full_cmd = full_cmd + " " + peer_str

      
      log = '--log "Language.K3.Interpreter#Dispatch:debug" --log "Language.K3.Runtime.Engine#EngineSteps:debug" '
      full_cmd = full_cmd +  " " + program_path + " " + log
      
      ssh_prefix = "ssh {0} -C '".format(hostname)
      if ssh is 1:
        full_cmd = ssh_prefix + full_cmd + "' "
      
      print full_cmd
      output_fn = "simulation_{0}.output".format(program_path.split("/")[-1]) 
      print output_fn
      with open(output_fn, "w") as logfile:
        subprocess.Popen(full_cmd, shell=True, stdout=logfile, stderr=logfile)






def network_mode(topology_fns):

  for topology_fn in topology_fns:

    time.sleep(10)
    #read in csv
    with open(topology_fn, 'rb') as csvfile:
      #reading program path header in file
      program_path = csvfile.readline().replace("\n","")
      #reading description header in file
      header = csvfile.readline().split(",")

      top_reader = csv.reader(csvfile, delimiter=',', quotechar='~')
      count = 0
      for row in top_reader:    
        count += 1
        hostname = row[0];
        peer = row[1].replace("\n","");
        
        ssh_prefix = "ssh {0} -C '".format(hostname)
        cmd = "{1} -I {0} interpret -b -n ".format(k3_lib_path, k3_path)
        #construct k3 program parameters
        peer_str = '-p {0}'.format(peer)
        for index in range (2, len(header)):
          peer_str = peer_str + ':{0}={1}'.format(header[index].replace("\n",""), row[index].replace("\n",""))
         
        for index in range(len(header), len(row)):
          peer_str = peer_str + ':{0}'.format(row[index].replace("\n",""))

        log = '--log "Language.K3.Interpreter#Dispatch:debug" --log "Language.K3.Runtime.Engine#EngineSteps:debug" '
        output_fn = "output/{0}:{1}.output".format(hostname, peer)
        full_cmd = ssh_prefix + " " + cmd + " " + peer_str + " " + program_path + " " + log + "' "
        print full_cmd
        with open(output_fn, "w") as logfile:
          subprocess.Popen(full_cmd, shell=True, stdout=logfile, stderr=logfile)




#run this script on a non-VM machine

if __name__=='__main__':

  usage = "Usagge: %prog [options] <config file>"
  parser = OptionParser(usage=usage)

  parser.add_option("-s", "--ssh", type="int", dest="ssh",
                    default=1, help="Specify whether to ssh into Hostname or run locally when in simulation mode",
                    metavar="#SSH")

  parser.add_option("-n", "--network", type="int", dest="network",
                    default=1, help="Specify network mode (1) or simulation mode (0)",
                    metavar="#NET")

  parser.add_option("-t", "--topology", type="string", dest="topology_fn", 
                    default="topology.csv", help="Specify comma-separated paths to k3 node topology files", 
                    metavar="#TOPOLOGY")

  (options, args) = parser.parse_args()

  topology_fns = options.topology_fn.split(',')
  ssh_bool = options.ssh
  if options.network is 1:
    network_mode(topology_fns)
  else:
    simulation_mode(topology_fns,ssh_bool)




