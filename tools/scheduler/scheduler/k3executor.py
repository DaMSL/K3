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
Outpath  = namedtuple('outpath', 'var prefix suffix')
SeqFile  = namedtuple('seqfile', 'num_switches data_dir switch_indexes tables')



BUFFER_SIZE = 256

ALL_TABLES = ["sentinel","customer","lineitem","orders","part","partsupp","supplier"]


# DEBUG = False
# DEBUG_FILE = 'examples/sql/tpch/queries/k3/q1.k3'


def executecmd(cmd):
  task = subprocess.Popen(cmd, shell=True,
          stdin=None, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
  stdout, stderr = task.communicate()
  return stdout.decode()


def isTrue(boolstr):
  return boolstr.upper() == 'TRUE'



class K3Executor(mesos.interface.Executor):
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
      k3_cmd = ''

      # Create host-specific set of parameters (e.g. peers, local data files, etc...\
      #  NOTE: The following keys must appear in provided data from dispatcher
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


      #  NOTE: Remaining Keys are optional
      if "perf_profile" in hostParams:
        frequency = hostParams["perf_frequency"] if "perf_frequency" in hostParams else '10'
        k3_cmd += "perf record -F " + frequency + " -a --call-graph dwarf -- ";

      if 'cmd_prefix' in hostParams:
        k3_cmd += hostParams['cmd_prefix'] + ' '

      k3_cmd += "./" + hostParams["binary"]
      if "logging" in hostParams:
          k3_cmd += " -l INFO "

      if "jsonlog"  in hostParams:
          executecmd("mkdir $MESOS_SANDBOX/json");
          k3_cmd += " -j json "

      if "jsonfinal" in hostParams:
          k3_cmd += " --json_final_only "

      if "resultVar" in hostParams:
          k3_cmd += " --result_path $MESOS_SANDBOX --result_var " + hostParams["resultVar"]

      if 'cmd_suffix' in hostParams:
          k3_cmd += ' ' + hostParams['cmd_suffix']

      #  Outpaths derived from prevousl CPP executor. Not sure why we need it
      #    BUT: retaining as comments for future reference
      # if 'outpaths' in hostParams:
      #     var = hostParams['outpaths']['var']
      #     pre = hostParams['outpaths']['prefix']
      #     suf = hostParams['outpaths']['suffix']
      #     mosaicOutpath = OutPath(var, pre, suf)
      # else:
      mosaicOutpath = None

      if 'seq_files' in hostParams:
          ns  = hostParams['seq_files']["num_switches"]
          dd  = hostParams['seq_files']["data_dir"]
          si  = hostParams['seq_files']["switch_indexes"]
          tb  = hostParams['seq_files']["tables"]
          if len(si) != len(localpeers):
            logging.error("Invalid deployment: number of switch indexes does not match number of local peers")
            logging.error("Switch indexes size: %d", len(si))
            logging.error("Peers size:          %d", len(globalpeers))
            self.sendTaskStatus(driver, mesos_pb2.TASK_FAILED, "Invalid Mosaic Deployment (switch indexes != peers)")
            return
          mosaicSeqfile = SeqFile(ns, dd, si, tb)
          logging.debug('SEQ File Data: ')
          logging.debug('   Num switches:   %s', str(mosaicSeqfile.num_switches))
          logging.debug('   Data Dir    :   %s', str(mosaicSeqfile.data_dir))
          logging.debug('   Switch Indexex:   %s', str(mosaicSeqfile.switch_indexes))
          logging.debug('   Tables        :   %s', str(mosaicSeqfile.tables))
      else:
          mosaicSeqfile = None

      ulimit = "core_dump" in hostParams

    # DATA ALLOCATION: Distribute input files among peers
      #    (1 file list per local peer)
      inputFiles = [{} for i in range(len(localpeers))]

      logging.debug(" Preparing %d  filelists", len(inputFiles))

      myTotalFiles = 0
      for dataFile in dataFilelist:
        logging.debug('  Processing datafile dir:   %s', dataFile.path)

        try:
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

    # Mosaic Seq Files
      if mosaicSeqfile is not None:
        inorderVarByPeer = []
        switchToPeer = {}
        seqFileYamlByPeer = {i: [] for i in mosaicSeqfile.switch_indexes}
        peerFilesList = {idx : {t: [] for t in ALL_TABLES} for idx in mosaicSeqfile.switch_indexes}

        for idx, si in enumerate(mosaicSeqfile.switch_indexes):
          filename = os.path.join(mosaicSeqfile.data_dir, "mux", str(mosaicSeqfile.num_switches), 'mux_' + str(si) + "_" + str(mosaicSeqfile.num_switches))
          for tb in mosaicSeqfile.tables:
            filename += "_" + tb;
          filename += ".csv";
          logging.info(' SeqFile:  %s', filename)
          inorderVarByPeer.append(filename)
          switchToPeer[si] = idx;


        for table in ALL_TABLES:

          path = os.path.join(mosaicSeqfile.data_dir, table)
          if not os.path.exists(path):
            self.sendTaskStatus(driver, mesos_pb2.TASK_FAILED, "Data Dir does not exist")
            return

          for i, filepath in enumerate(sorted(os.listdir(path))):
            for sw_index in mosaicSeqfile.switch_indexes:
              if table == "sentinel" or (i % mosaicSeqfile.num_switches) == sw_index:
                peerFilesList[sw_index][table].append({'path': os.path.join(path, filepath)})
                logging.debug('Switch:  %d  =>  %s', sw_index, filepath)

        for idx in mosaicSeqfile.switch_indexes:
          for table in ALL_TABLES:
            seqFileYamlByPeer[idx].append({'seq': peerFilesList[idx][table]})

        logging.debug("Seq file yaml per peer: ")
        for k, v in  seqFileYamlByPeer.items():
          logging.debug("   SeqFile %d: %s", k, str(v))

        logging.debug("switchToPeer: ")
        for k, v in  switchToPeer.items():
          logging.debug("   Switch %d: %s", k, str(v))


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


        # Mosaic outpath stuff
        peerglobals["peer_idx"] = i;
        if mosaicOutpath is not None:
          peerglobals[mosaicOutpath.var] = mosaicOutpath.prefix + str(i) + mosaicOutpath.suffix;

        # Mosaic seqfile stuff
        if mosaicSeqfile is not None:
          peerglobals["seqfiles"] = seqFileYamlByPeer[mosaicSeqfile.switch_indexes[i]]
          peerglobals["inorder"] = inorderVarByPeer[i]

          for logfile in ['eventlog', 'msgcountlog', 'routelog']:
            peerglobals[logfile] = "%s_%d.csv" % (logfile, i)

        for k,v in peerglobals.items():
          logging.debug('    %s: %s', str(k), str(v))

        peerFileName = os.path.join(os.getenv('MESOS_SANDBOX'), 'peers%d.yaml' % i)
        with open(peerFileName, 'w') as yamlfile:
          yamlfile.write(yaml.dump(peerglobals))

        # Append peer flag with associated YAML File to run command
        k3_cmd += " -p " + peerFileName;

      # Finish up processing
      k3_cmd += " > stdout_" + self.host + " 2>&1"

      # Wrap the k3_cmd with a call to ulimit (if flagged)
      if (ulimit):
          k3_cmd = "bash -c 'ulimit -c unlimited && %s'" % k3_cmd

      k3_cmd = "cd $MESOS_SANDBOX && " + k3_cmd;
      logging.info(" K3 CMD:\n   %s", k3_cmd)

      if hostParams["me"][0] == hostParams["master"]:
        logging.info("I am master")
      logging.info("Launching K3.")

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

      # Package Sandbox
      tarfile = app_name + "_" + str(self.job_id) + "_" + self.host + ".tar"
      tar_cmd = 'cd $MESOS_SANDBOX && tar -cf ' + tarfile + " --exclude='k3executor.py' *"
      archive_endpoint = webaddr + app_name + "/" + str(self.job_id) + "/archive"
      post_curl = 'cd $MESOS_SANDBOX && curl -i -H "Accept: application/json" -X POST '
      curl_cmd  = post_curl + '-F "file=@' + tarfile + '" ' + archive_endpoint
      curl_output = post_curl + '-F "file=@stdout_' + self.host + '" '  + archive_endpoint

      logging.debug("POST-PROCESSING:")

      output = executecmd(tar_cmd)
      logging.debug('TAR CMD: %s\n%s', tar_cmd, output)

      output = executecmd(curl_cmd)
      logging.debug('CURL CMD: %s\n%s', curl_cmd, output)

      output = executecmd(curl_output)
      logging.debug('CURL OUTPUT: %s\n%s', curl_output, output)

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
  executor = K3Executor()
  driver = mesos.native.MesosExecutorDriver(executor)
  sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)


