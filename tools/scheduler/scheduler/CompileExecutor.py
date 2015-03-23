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
import threading
import time
import subprocess

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

class CompilerExecutor(mesos.interface.Executor):
  def __init__(self):
    self.thread = None
    self.status = mesos_pb2.TaskStatus()

  def launchTask(self, driver, task):
    # Create a thread to run the task. Tasks should always be run in new
    # threads or processes, rather than inside launchTask itself.
    def run_task():

      name = str(task.name.encode('ascii', 'ignore'))
      cmd = '$MESOS_SANDBOX/compile_%s.sh' % name

      self.frameworkMessage(driver, "Python Executor Test Message")
      self.status.task_id.value = task.task_id.value
      self.status.state = mesos_pb2.TASK_RUNNING
      self.status.data = "Compiling K3: %s " % name
      driver.sendStatusUpdate(self.status)

      # Mesos v0.22  (Labels)
      # labels = task.labels
      # for l in labels:
      #   print l

      proc = subprocess.Popen(cmd,
                              shell=True,
                              stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)

      # Poll process for new output until finished
      while True:
          output = proc.stdout.readline()
          if output == '' and proc.poll() != None:
              break
          self.status.data = output
          driver.sendStatusUpdate(self.status)

      exitCode = proc.returncode

      if os.path.exists('/k3/K3/__build/A') and exitCode == 0:
        self.status.state = mesos_pb2.TASK_FINISHED
        self.status.data = "COMPILER Completed Successfully."
      else:
        self.status.state = mesos_pb2.TASK_FAILED
        self.status.data = "COMPILER Failed. Check output for details."

      driver.sendStatusUpdate(self.status)


      stdout_value, stderr_value = proc.communicate('through stdin to stdout')
      print '\tcombined output:', repr(stdout_value)
      print '\tstderr value   :', repr(stderr_value)


    self.thread = threading.Thread(target=run_task)
    self.thread.start()

  def frameworkMessage(self, driver, message):
    driver.sendFrameworkMessage(message)

  def killTask(self, driver, tid):

    sys.exit(1)

  def error(self, driver, code, message):
    print "Error from Mesos: %s (code %s)" % (message, code)

if __name__ == "__main__":
  executor = CompilerExecutor()
  driver = mesos.native.MesosExecutorDriver(executor)
  sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
