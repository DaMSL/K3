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


def genScript(d):
    if d['role'] == 'client':
      cmd = './tools/scripts/run/service.sh submit --svid %(svid)s --host %(host)s --port %(port)s  --blocksize %(blocksize)s examples/sql/tpch/queries/k3/q6.k3' % daemon
    else:
      cmd = './tools/scripts/run/service.sh %(role)s --svid %(svid)s --host %(host)s --port %(port)s' % d
    script = '''
#!/bin/bash
cd /k3/K3
%s
''' % cmd
    logging.debug(script)
    with open('run.sh', 'w') as f:
      f.write(script)
    st = os.stat('run.sh')
    os.chmod('run.sh', st.st_mode | stat.S_IEXEC)

class CompilerExecutor(mesos.interface.Executor):
  def __init__(self):
    logging.basicConfig(format='[%(asctime)s %(levelname)s] %(message)s', level=logging.DEBUG, datefmt='%H:%M:%S')
    self.thread = None
    self.status = mesos_pb2.TaskStatus()


  def launchTask(self, driver, task):
    # Create a thread to run the task. Tasks should always be run in new
    # threads or processes, rather than inside launchTask itself.
    def run_task():

      logging.debug("RUNNING Executor")

      # Mesos v0.22  (Labels)
      logging.debug("LABELS:")
      labels = task.labels.labels
      daemon = {}
      for l in labels:
        logging.debug("%s = %s" % (l.key, l.value) )
        daemon[l.key] = str(l.value)


      name = str(task.name.encode('utf8', 'ignore'))
      logging.debug("NAME = %s" % name)
      self.status.task_id.value = task.task_id.value

      #  TODO: Set up State & task messages for synchronizing job
      self.status.state = mesos_pb2.TASK_RUNNING
      self.status.data = "STARTING role: %s " % daemon['role']
      self.status.message = daemon['role']
      driver.sendStatusUpdate(self.status)

      if daemon['role'] == 'client':
        cmd = './tools/scripts/run/service.sh submit --svid %(svid)s --host %(host)s --port %(port)s  --blocksize %(blocksize)s -1 examples/sql/tpch/queries/k3/q6.k3' % daemon
      else:
        cmd = './tools/scripts/run/service.sh %(role)s --svid %(svid)s --host %(host)s --port %(port)s' % daemon

      # cmd = './tools/scripts/run/service.sh --help'
      logging.info("CMD = %s" % cmd)

      os.chdir('/k3/K3')

      # self.status.state = mesos_pb2.TASK_RUNNING
      self.status.data = "RUNNING role: %s " % daemon['role']
      driver.sendStatusUpdate(self.status)


      proc = subprocess.Popen(cmd, shell=True, 
                              stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT)
      logging.info('Compile task for %s piped Successfully.' % daemon['svid'])

      # self.status.state = mesos_pb2.TASK_RUNNING
      self.status.data = "RUNNING role: %s " % daemon['role']
      driver.sendStatusUpdate(self.status)

      self.status.message = ''
      # Poll process for new output until finished
      while True:
          # output = 'Running .... %s' % daemon['svid'] #proc.stdout.readline()
          output = proc.stdout.readline()
          # outerr = proc.stderr.readline()
          # if output == '' and outerr == '' and proc.poll() != None:
          if output == '' and proc.poll() != None:
              break
          if output != '':
            logging.debug(output)
          # if outerr != '':
          #   logging.debug(outerr)
          self.status.data = output
          # self.status.message = outerr
          driver.sendStatusUpdate(self.status)

      self.status.message = "Task is complete -- post processing %s" % daemon['svid']
      driver.sendStatusUpdate(self.status)
      logging.info("Compile Task COMPLETED for %s" % daemon['svid'])

      exitCode = proc.returncode

      #  TODO: Need a way to check success/failure
      if daemon['role'] != 'client':
        self.status.state = mesos_pb2.TASK_FINISHED
      else:
        if os.path.exists('/k3/K3/__build/A') and exitCode == 0:
          
          shutil.copyfile('/k3/K3/__build/A', daemon['name'])
          self.status.state = mesos_pb2.TASK_FINISHED
          self.status.data = "COMPILER Completed Successfully."

          #  Upload binary to flask server
          headers = {"Accept": "application/json"}
          data = {"file": open(daemon['name'])}
          conn = httplib.HTTPConnection(daemon['webaddr'])
          conn.request("POST", "/apps", data, headers)
          response = conn.getresponse()
          logging.info ('%s: %s\n%s' % (response.status, response.reason, response.read()))
          conn.close()

        else:
          self.status.state = mesos_pb2.TASK_FAILED
          self.status.data = "COMPILER Failed. Check output for details."

      driver.sendStatusUpdate(self.status)


      # stdout_value, stderr_value = proc.communicate('through stdin to stdout')
      # print '\tcombined output:', repr(stdout_value)
      # print '\tstderr value   :', repr(stderr_value)


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
  executor = CompilerExecutor()
  driver = mesos.native.MesosExecutorDriver(executor)
  sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)

