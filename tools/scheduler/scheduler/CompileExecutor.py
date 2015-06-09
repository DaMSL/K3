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

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

import logging

DEBUG = False
DEBUG_FILE = 'examples/sql/tpch/queries/k3/q1.k3'


class CompilerExecutor(mesos.interface.Executor):
  def __init__(self):
    logging.basicConfig(format='[%(asctime)s %(levelname)s] %(message)s', level=logging.DEBUG, datefmt='%H:%M:%S')
    self.thread = None
    self.status = mesos_pb2.TaskStatus()
    self.status.state = mesos_pb2.TASK_STARTING


  def launchTask(self, driver, task):
    # Create a thread to run the task. Tasks should always be run in new
    # threads or processes, rather than inside launchTask itself.
    def run_task():

      self.status.task_id.value = task.task_id.value

      logging.debug("Executor is running")

      # Mesos v0.22  (Labels)
      logging.debug("Passed Labels received at Executor:")
      labels = task.labels.labels
      daemon = {}
      for l in labels:
        logging.debug("%s = %s" % (l.key, l.value) )
        daemon[l.key] = str(l.value)

      name = str(task.name.encode('utf8', 'ignore'))
      logging.debug("Task Name = %s" % name)

      daemon['k3src'] = DEBUG_FILE if DEBUG else '$MESOS_SANDBOX/%s.k3' % daemon['name']

      if daemon['role'] == 'client':
        cmd = './tools/scripts/run/service.sh submit --svid %(svid)s --host %(host)s --port %(port)s  --blocksize %(blocksize)s -%(compileto)s %(k3src)s' % daemon
      else:
        cmd = './tools/scripts/run/service.sh %(role)s --svid %(svid)s --host %(host)s --port %(port)s' % daemon

      logging.info("CMD = %s" % cmd)


      # self.status.data = "%s is STARTING" % daemon['svid']
      # driver.sendStatusUpdate(self.status)

      # self.status.state = mesos_pb2.TASK_RUNNING
      self.status.message = daemon['role']
      self.status.data = "%s is ready." % daemon['svid']
      self.status.task_id.value = task.task_id.value
      driver.sendStatusUpdate(self.status)

      os.chdir('/k3/K3')
      proc = subprocess.Popen(cmd, shell=True,
                              stdin=None,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT)

      self.status.state = mesos_pb2.TASK_RUNNING
      self.status.data = "%s is RUNNING" % daemon['svid']
      driver.sendStatusUpdate(self.status)
      logging.info(self.status.data)
      
      # Poll process for new output until finished
      while True:
          # output = 'Running .... %s' % daemon['svid'] #proc.stdout.readline()
          output = proc.stdout.readline()
          
          # outerr = proc.stderr.readline()
          # if output == '' and outerr == '' and proc.poll() != None:
          if output == '' and proc.poll() != None:
              break
          logging.debug(output)
          self.status.data = output
          driver.sendStatusUpdate(self.status)

      self.status.message = "complete"
      driver.sendStatusUpdate(self.status)
      logging.info("Compile Task COMPLETED for %s" % daemon['svid'])

      exitCode = proc.returncode

      destfile = ''

      #  TODO: Need a way to check success/failure
      if daemon['role'] == 'client':
        endpoint = daemon['webaddr'] + '/apps/' + daemon['name'] + '/' + daemon['uid']
        logging.debug("Client is copying all files in build dir")
        curlcmd = 'curl -i -X POST -H "Accept: application/json" -F "file=@/k3/K3/__build/%s" ' + endpoint

        logging.debug("  CLIENT __build: ")
        for f in os.listdir('/k3/K3/__build/'):
          #  Upload binary to flask server & copy fo sandbox
          # shutil.copyfile('/k3/K3/__build/' + f, '$MESOS_SANDBOX/' + f)
          logging.debug('  Shipping FILE: ' + f)
          curl = curlcmd % f
          logging.debug('  CURL:  ' + curl)
          subprocess.call(curl, shell=True)

        haltcmd = './tools/scripts/run/service.sh halt --svid %(svid)s --host %(host)s --port %(port)s' % daemon
        subprocess.call(curl, shell=True)
        logging.info("Client sent HALT command and is terminating")
      else:
        logging.info("`%s` Terminated", daemon['svid'])

      self.status.state = mesos_pb2.TASK_FINISHED if exitCode == 0 else mesos_pb2.TASK_FAILED
      driver.sendStatusUpdate(self.status)

      # driver.sendStatusUpdate(self.status)

    self.thread = threading.Thread(target=run_task)
    self.thread.start()

  def frameworkMessage(self, driver, message):
    logging.info('[FRWK MSG] %s ' % str(message))

    # driver.sendFrameworkMessage(message)

  def killTask(self, driver, tid):

    sys.exit(1)

  def error(self, driver, code, message):
    print "Error from Mesos: %s (code %s)" % (message, code)

if __name__ == "__main__":
  print "Executor has started"
  executor = CompilerExecutor()
  driver = mesos.native.MesosExecutorDriver(executor)
  sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)



