#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import os
import stat
import threading
import shutil
import time
import subprocess
import httplib
import urllib
import yaml
import socket
from collections import namedtuple


import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

import logging
logging.basicConfig(level=logging.DEBUG)

DataFile = namedtuple('datafile', 'path varName policy')


BUFFER_SIZE = 256


# DEBUG = False
# DEBUG_FILE = 'examples/sql/tpch/queries/k3/q1.k3'


def executecmd(cmd):
  task = proc.Popen(cmd, shell=True,
          stdin=None, stdout=proc.PIPE, stderr=proc.STDOUT)
  stdout, stderr = task.communicate()
  return stdout.decode()


def isTrue(boolstr):
  return boolstr.upper() == 'TRUE'

class CompilerExecutor(mesos.interface.Executor):
  def __init__(self):
    logging.debug('Executor is initilizing')
    self.thread = None
    self.status = mesos_pb2.TaskStatus()
    self.status.state = mesos_pb2.TASK_STARTING
    self.success = False
    self.buffer = ""
    self.job_id = None
    self.host = socket.gethostname()


  def sendTaskStatus(self, driver, state, data):
      self.status.state = state
      self.status.data = data
      driver.sendStatusUpdate(self.status)
      logging.info(self.status.data)


  def launchTask(self, driver, task):

    # The compilation task runs in its own thread
    def run_task():

      logging.debug("Called: run_task()")
      logging.debug("run_task()  via print")

      self.status.task_id.value = task.task_id.value
      self.job_id = task.task_id.value.decode()

      # Notify the compiler launcher it's running
      self.sendTaskStatus(driver, mesos_pb2.TASK_RUNNING, "%s is RUNNING" % str(self.job_id))


      #  Start Task Operations
      logging.debug("K3 Task Preparing to launch")

      #  Grab the packed data (in YAML format)
      hostParams = yaml.load(task.data)

      logging.debug("DATA RECEIVED FROM DISPATCHER:")
      for k, v in hostParams.items():
        logging.debug('     %s: %s', str(k), str(v))

      app_name = hostParams["binary"]
      webaddr  = hostParams["archive_endpoint"]



      # Build the K3 Command which will run inside this container
      k3_cmd = "cd $MESOS_SANDBOX && ./" + hostParams["binary"]
      if "logging" in hostParams:
          k3_cmd += " -l INFO "

      if "jsonlog"  in hostParams:
          executecmd("mkdir $MESOS_SANDBOX/json");
          k3_cmd += " -j json "

      if "jsonfinal" in hostParams:
          k3_cmd += " --json_final_only "

      if "resultVar" in hostParams:
          k3_cmd += " --result_path $MESOS_SANDBOX --result_var " + hostParams["resultVar"]


      # Create host-specific set of parameters (e.g. peers, local data files, etc...\
      try:
        # peerParams = hostParams["peers"]
        globalpeers = hostParams["peers"]
        localpeers = hostParams['me']
        dataFilelist = [DataFile(d['path'], d['var'], d['policy']) for d in hostParams['data'][0]]
        logging.debug(' **** Data File List ****')
        for d in dataFilelist:
          logging.debug('      %s', d.path)
        totalPeerCount = hostParams['totalPeers']
        peerStart = hostParams['peerStart']
        peerEnd = hostParams['peerEnd']
      except KeyError as e:
        logging.warning('Key, %s, not found in hostParameters' % key)


    # DATA ALLOCATION: Distribute input files among peers 
      #    (1 file list per local peer)
      inputFiles = [{} for i in range(len(localpeers))]

      logging.debug(" Preparing %d  filelists", len(inputFiles))

      for dataFile in dataFilelist:
        logging.debug('  Processing datafile dir:   %s', dataFile.path)

        try:
          myTotalFiles = 0
          srcfiles = sorted([os.path.join(dataFile.path, f) for f in os.listdir(dataFile.path)])
          logging.debug('   %d source files', len(srcfiles))

          if dataFile.policy == 'global':
            logging.debug('  GLOBAL')
            for i, dfile in enumerate(srcfiles):
              peerNum = i % totalPeerCount
              logging.debug('    Working peer# %d', peerNum)

              if peerNum >= peerStart and peerNum <= peerEnd:
                if dataFile.varName not in inputFiles[peerNum - peerStart]:
                  inputFiles[peerNum - peerStart][dataFile.varName] = []
                inputFiles[peerNum - peerStart][dataFile.varName].append(dfile)
                logging.debug('    local peer# %d  ---> %d', peerNum-peerStart, dfile)

                myTotalFiles += 1

          if dataFile.policy == 'replicate':
            for i in range(len(inputFiles)):
              inputFiles[i][dataFile.varName] = [d for d in srcfiles]
            myTotalFiles = len(srcfiles) * len(localpeers)

          if dataFile.policy == 'pinned':
            inputFiles[0] = {dataFile.varName: [d for d in srcfiles]}
            myTotalFiles = len(srcfiles)

          logging.debug("  %s dir has %d total files", dataFile.path, myTotalFiles)

        except IOError as e:
          logging.error("Failed to open local path: %s  (on %s)", dataFile.path, self.host)
          self.sendTaskStatus(driver, mesos_pb2.TASK_FAILED, 
                "%s FAILED. Error opening local file, %s" % (str(self.job_id), dataFile))

      logging.debug("Input File Allocation (for total of %d files): ", myTotalFiles)

      for i, p in enumerate(inputFiles):
        logging.debug('  Peer #%d ', i)
        for k, v in p.items():
          logging.debug('     %d:  %s --> %s', i, str(k), str(v))

    # Build Parameters for All peers (on this host)
      localMaster = localpeers[0]
      peermastermap = []
      logging.debug('Local Master is %s', str(localMaster))
      peerPort = {}
      for p in hostParams["peers"]:
        if p['addr'][0] not in peerPort:
          peerPort[p['addr'][0]] = []
        peerPort[p['addr'][0]].append(p['addr'][1])
      for ip in peerPort.keys():
        peerPort[ip] = sorted(peerPort[ip])
        for port in peerPort[ip]:
          peermastermap.append(dict(key=[ip, port], value=[ip, peerPort[ip][0]]))
      masters = [{'addr': [ip, peerPort[ip][0]]} for ip in peerPort.keys()]

      logging.debug('Masters list')
      for m in masters:
        logging.debug('    %s', str(m))
      logging.debug('Peer to Masters Map')
      for pm in peermastermap:
        logging.debug('    %s', str(pm))

    # Create one YAML file for each peer on this host
      for i, peer in enumerate(localpeers):

        peerglobals = {}
        logging.debug('globals ---> %d', len(hostParams["globals"]))
        logging.debug(' Peer Globals:')

        for g in hostParams["globals"]:
          for k, v in g.items():
            peerglobals[k] = v

        peerglobals['me'] = peer
        peerglobals['peers'] = globalpeers
        peerglobals['masters'] = masters
        peerglobals['peer_masters'] = peermastermap
        peerglobals["local_peers"] = [hostParams['peers'][p] for p in range(peerStart, peerEnd+1)]
        for var, flist in inputFiles[i].items():
          peerglobals[var] = [{'path': f} for f in flist]

        for k,v in peerglobals.items():
          logging.debug('    %s: %s', str(k), str(v))

        peerFileName = os.path.join(os.getenv('MESOS_SANDBOX'), 'peers%d.yaml' % i)
        with open(peerFileName, 'w') as yamlfile:
          yamlfile.write(yaml.dump(peerglobals))

        # Append peer flag with associated YAML File to run command
        k3_cmd += " -p " + peerFileName;

      # Finish up processing
      k3_cmd += " > stdout_" + self.host + " 2>&1"

      logging.info(" K3 CMD:\n   %s", k3_cmd)



    # Start the service (in current thread with stderr/stdout redirected from Mesos)
      proc = subprocess.Popen(k3_cmd, shell=True,
                              stdin=None,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT)


      # Poll process for new output until finished; buffer output
      # NOTE: Buffered output reduces message traffic via mesos
      while True:
        output = proc.stdout.readline()
        logging.info(output)
        self.buffer += output
        if output == '' and proc.poll() != None:
            # self.status.data = self.buffer
            # driver.sendStatusUpdate(self.status)
            break

        # # If buffer-size is reached: send update message
        # if len(self.buffer) > BUFFER_SIZE:
        #   self.status.data = self.buffer
        #   driver.sendStatusUpdate(self.status)
        #   self.buffer = ""


      # TODO:  Package Sandbox

      exitCode = proc.returncode
      self.success = (exitCode == 0)

      if self.success:
        self.status.state = mesos_pb2.TASK_FINISHED
        self.status.data = "K3 Program terminated normally."
      else:
        self.status.state = mesos_pb2.TASK_FAILED
        self.status.data = "K3 Program FAILED (check logs)"

      driver.sendStatusUpdate(self.status)

    self.thread = threading.Thread(target=run_task)
    self.thread.start()

  def frameworkMessage(self, driver, message):
    logging.info('[FRWK MSG] %s ' % str(message))

  def killTask(self, driver, tid):
    self.status.data = self.buffer
    self.status.state = mesos_pb2.TASK_KILLED
    driver.sendStatusUpdate(self.status)
    logging.warning("Executor was signaled to terminate. Exiting now....")
    sys.exit(1)

  def error(self, driver, code, message):
    print "Error from Mesos: %s (code %s)" % (message, code)

if __name__ == "__main__":
  print('PRINT: Executor is running')
  logging.debug( "Executor has started")
  executor = CompilerExecutor()
  driver = mesos.native.MesosExecutorDriver(executor)
  sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)


