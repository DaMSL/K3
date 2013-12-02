import csv
from optparse import OptionParser
import subprocess
import os
import sys
import time

#run this script on a non-VM machine

if __name__=='__main__':

  usage = "Usagge: %prog [options] <config file>"
  parser = OptionParser(usage=usage)

  parser.add_option("-t", "--topology", type="string", dest="topology_fn", 
                    default="topology.csv", help="Specify comma-separated paths to k3 node topology files", 
                    metavar="#TOPOLOGY")

  (options, args) = parser.parse_args()

  topology_fns = options.topology_fn.split(',')
  for topology_fn in topology_fns:

    time.sleep(10)
    #read in csv
    with open(topology_fn, 'rb') as csvfile:
      #reading program path header in file
      program_path = csvfile.readline().replace("\n","")
      #reading description header in file
      header = csvfile.readline().split(",")

      top_reader = csv.reader(csvfile, delimiter=',', quotechar='|')
      count = 0
      for row in top_reader:    
        count += 1
        hostname = row[0];
        peer = row[1].replace("\n","");
        
        ssh_prefix = "ssh {0} -C '".format(hostname)
        cmd = "/home/vagrant/K3/K3-Driver/dist/build/k3/k3 -I /home/vagrant/K3/K3-Core/k3/lib/ interpret -b -n "
        #construct k3 program parameters
        peer_str = '-p {0}'.format(peer)
        for index in range (2, len(header)):
          peer_str = peer_str + ':{0}={1}'.format(header[index].replace("\n",""), row[index].replace("\n",""))
        
        log = '--log "Language.K3.Interpreter#Dispatch:debug" --log "Language.K3.Runtime.Engine#EngineSteps:debug" '
        output_fn = "output/{0}:{1}.output".format(hostname, peer)
        full_cmd = ssh_prefix + " " + cmd + " " + peer_str + " " + program_path + " " + log + "' "
        print full_cmd
        with open(output_fn, "w") as logfile:
          subprocess.Popen(full_cmd, shell=True, stdout=logfile, stderr=logfile)


