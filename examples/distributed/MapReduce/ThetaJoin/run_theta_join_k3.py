#!/usr/bin/python
#
# run_theta_join_k3.py
#
# Created by Kartik Thapar on 04/29/2014 at 01:00:37
# Copyright (c) 2014 Kartik Thapar. All rights reserved.
#

"""
Usage: 
    run_theta_join_k3.py -s -t <topologyFile>
    run_theta_join_k3.py -n -t <topologyFile>
    run_theta_join_k3.py -t <topologyFile>

Options:
    -h --help           Show this screen.
    -s --simulation     Create K3 command for simulation mode
    -n --network        Create K3 command for network mode
    -t --topology       Topology file
"""

import docopt
import csv

def createK3Command(topology, mode):
    with open(topologyFile, 'rb') as csvfile:
        # reading program path header in file
        programPath = csvfile.readline().replace("\n","")

        # reading description header in file
        header = csvfile.readline().split(",")

        # reading program execution info
        body = csv.reader(csvfile, delimiter=',', quotechar='~').next()
        (master, role, sMappers, tMappers, reducers, peers, maxS, maxT) = [body[i] for i in range(0,8)]
        peers = peers.split(",")

        peerCommand = " ".join(["-p %s" % (peer) for peer in peers])

        # create and print K3 program
        if mode == "simulation-mode":
            command = "k3 interpret -b -p %s:role=%s:sMappers=%s:tMappers=%s:reducers=%s:maxS=%s:maxT=%s %s %s" \
                % (master, role, sMappers, tMappers, reducers, maxS, maxT, peerCommand, programPath)
        else:
            command = "k3 interpret -b -n -p %s:role=%s:sMappers=%s:tMappers=%s:reducers=%s:maxS=%s:maxT=%s %s %s" \
                % (master, role, sMappers, tMappers, reducers, maxS, maxT, peerCommand, programPath)
        return command

def simulateK3(topologyFile):
    # create and print K3 program
    print createK3Command(topologyFile, "simulation-mode")

def networkK3(topologyFile):
    # create and print K3 program
    print createK3Command(topologyFile, "network-mode")

if __name__ == '__main__':
    s = t = r = None
    try:
        arguments = docopt.docopt(__doc__)
        simulationMode = arguments['--simulation']
        networkMode = arguments['--network']
        topologyFile = arguments['<topologyFile>']
    except docopt.DocoptExit as e:
        print e.message
        exit()

    # if simulation mode required or network mode not specified, run in simulation mode
    if simulationMode or (not networkMode):
        simulateK3(topologyFile) 
    else :
        networkK3(topologyFile)
