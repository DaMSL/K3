#!/usr/bin/python
#
# run_k3.py
#
# Created by Kartik Thapar on 05/13/2014 at 23:48:56
# Copyright (c) 2014 Kartik Thapar. All rights reserved.
#

"""
Usage: 
    run_k3.py -s -t <topologyFile>
    run_k3.py -n -t <topologyFile>
    run_k3.py -t <topologyFile>

Options:
    -h --help           Show this screen.
    -s --simulation     Create K3 command for simulation mode [DEFAULT]
    -n --network        Create K3 command for network mode
    -t --topology       Topology file
"""

import docopt
import csv

k3LibPath = "/Users/kartikthapar/WorkCenter/Projects/K3/core/lib/"
k3Path = "/Users/kartikthapar/WorkCenter/Projects/K3/driver/dist/build/k3/k3"

def createK3Command(topology, mode):
    with open(topologyFile, 'rb') as csvFile:
        # reading program path header in file
        programPath = csvFile.readline().replace("\n","")

        # reading description header in file
        header = csvFile.readline().split(",")

        # command
        command = ""
        if mode == "simulation-mode":
            command = "{1} -I {0} interpret -b".format(k3LibPath, k3Path)
        else:
            command = "{1} -I {0} interpret -n -b".format(k3LibPath, k3Path)

        # reading program execution info
        info = csv.reader(csvFile, delimiter=',', quotechar='~')

        # every row is of type == [peer, role, ...]
        for row in info:
            # get peer
            peer = row[0].replace("\n", "")

            # construct k3 program parameters
            peerString = '-p {0}'.format(peer)
            for index in range (1, min(len(header), len(row))):
                peerString = peerString + ':{0}={1}'.format(header[index].replace("\n",""), row[index].replace("\n",""))
            for index in range(len(header), len(row)):
                peerString = peerString + ':{0}'.format(row[index].replace("\n",""))
            command = command + " " + peerString

        return command

def simulateK3(topologyFile):
    # create and print K3 program
    print createK3Command(topologyFile, "simulation-mode")

def networkK3(topologyFile):
    # create and print K3 program
    print createK3Command(topologyFile, "network-mode")

if __name__ == '__main__':
    simulationMode = networkMode = topologyFile = None
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
