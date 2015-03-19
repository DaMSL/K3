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
import time
import threading

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

import db
from mesosutils import *
from core import *

MASTER = 'zk://192.168.0.10:2181,192.168.0.11:2181,192.168.0.18:2181/mesos'

#TODO: Make this dynamic to auto-pull specific K3 git hash (if needed) and compile flags

task_state = {
    6: "TASK_STAGING",  # Initial state. Framework status updates should not use.
    0: "TASK_STARTING",
    1: "TASK_RUNNING",
    2: "TASK_FINISHED", # TERMINAL.
    3: "TASK_FAILED",   # TERMINAL.
    4: "TASK_KILLED",   # TERMINAL.
    5: "TASK_LOST"      # TERMINAL.
}

# TODO: Consider splitting into two separate tasks: pre & post compilation
def genScript(c):
  script = '''
#!/bin/bash

cd /k3/K3
./tools/scripts/run/compile.sh %(options)s $MESOS_SANDBOX/%(name)s.k3

echo Collecting binary and CPP files
mv __build/A $MESOS_SANDBOX/%(name)s
mv __build/%(name)s.cpp $MESOS_SANDBOX/
cd $MESOS_SANDBOX
echo Uploading New K3 binary to server
curl -i -H "Accept: application/json" -F file=@%(name)s -F uid=%(uid)s http://qp1:5000/apps
echo Uploading Project Archive to server
tar -cf %(name)s.tar --exclude='k3compexec' *
curl -i -H "Accept: application/json" -F "file=@%(name)s.tar" http://qp1:5000/apps/%(name)s/%(uid)s
echo Finished compilation job
'''  % (c)
  return script

#(options, app, app, app, app, uid, app, uname, uname)

class CompileLauncher(mesos.interface.Scheduler):
    def __init__(self, job, **kwargs):
        self.launched   = False
        self.terminate  = False
        self.job        = job
        # self.name    = kwargs.get('name', 'CompileTask')
        # self.uid      = kwargs.get('uid', '')
        # self.path     = kwargs.get('path', '')
        # self.git_hash = kwargs.get('git_hash', 'latest')
        # self.user     = kwargs.get('user', '')
        # self.options   = kwargs.get('options', '')

        self.source   = kwargs.get('source', None)
        self.script   = kwargs.get('script', None)
        self.status   = JobStatus.INITIATED
        self.log      = []



    def registered(self, driver, frameworkId, masterInfo):
        print ("[COMPILER REGISTERED] ID %s" % frameworkId.value)

    #  This Gets invoked when Mesos offers resources to my framework & we decide what to do with the recources (e.g. launch task, set mem/cpu, etc...)
    def resourceOffers(self, driver, offers):
        print ("[COMPILER RESOURCE OFFER] Got %d resource offers" % len(offers))

        #TODO: Det where to compile K3... rightnow, use qp4
        for offer in offers:
          if self.launched or offer.hostname != 'qp4':
            driver.declineOffer(offer.id)
            continue

          task = compileTask(name=self.job.name,
                             uid=self.job.uid,
                             script=self.script,
                             source=self.source,
                             slave=offer.slave_id.value)

          # db.insertCompile(dict(name=self.name, uid=self.uid, user=self.user, options=self.options, git_hash=self.git_hash))
          db.insertCompile(self.job.__dict__())
          driver.launchTasks(offer.id, [task])
          self.status   = JobStatus.SUBMITTED
          self.launched = True
          

    def statusUpdate(self, driver, update):
        # print ("[COMPILE STATUS] %s" % update.data)
        self.log.append(update.data)
        with open(os.path.join(self.path, 'output'), 'a') as out:
          out.write(update.data)

        if update.state == mesos_pb2.TASK_RUNNING:
            if self.status == JobStatus.SUBMITTED:
              self.status = JobStatus.COMPILING
              db.updateCompile(self.job.uid, status=self.status)

            elif self.status == JobStatus.COMPILING and \
                  update.data.startswith('Created binary file:'):
              self.status = JobStatus.ARCHIVING
              db.updateCompile(self.job.uid, status=self.status)


        if update.state == mesos_pb2.TASK_FAILED:
            self.status = JobStatus.FAILED
            db.updateCompile(self.job.uid, status=self.status, done=True)
            self.terminate = True

        if update.state == mesos_pb2.TASK_FINISHED:
            self.status = JobStatus.FINISHED
            db.updateCompile(self.job.uid, status=self.status, done=True)
            self.terminate = True

        if update.state == mesos_pb2.TASK_LOST:
            self.status = JobStatus.FAILED
            db.updateCompile(self.job.uid, status=self.status, done=True)
            self.terminate = True



	# Mesos invokes this method when an executor sends a message
    def frameworkMessage(self, driver, executorId, slaveId, message):
      print ("[COMPILER FRAMEWORK MESSAGE]: %s" % str(message))

class CompileDriver:
  def __init__(self, job, **kwargs):
    self.job = job
    # app      = kwargs.get('name', 'CompileTask')
    source   = kwargs.get('source', None)
    script   = kwargs.get('script', None)
    # uid      = kwargs.get('uid', '')
    # path     = kwargs.get('path', '')
    # user     = kwargs.get('user', '')
    # options   = kwargs.get('options', '')

    f = mesos_pb2.FrameworkInfo()
    f.user = "" # Have Mesos fill in the current user.
    f.name = "Compiling %s" % job.name
    f.principal = "framework-k3-compile"

    print "Framework name is %s" % f.name

    self.framework = f
    # self.launcher = CompileLauncher(job, name=job.name, source=source, user=job.user,
    #                                 script=script, uid=job.uid, path=path, options=options)
    self.launcher = CompileLauncher(job, source=source, script=script)
    self.driver = None

  def getLog(self):
    return self.launcher.log

  def getStatus(self):
    return self.launcher.status

  def launch(self):
    print "Creating Driver"
    self.driver = mesos.native.MesosSchedulerDriver(
            self.launcher, self.framework,  MASTER)
    print "Creating Thread"
    t = threading.Thread(target=self.driver.run)

    try:
      print "Trying to Launch..."
      t.start()
      terminate = False
      wait_debug = 10
      print "Checking if launched..."
      while not terminate and wait_debug > 0:
        time.sleep(1)
        terminate = self.launcher.terminate
        if not self.launcher.launched:
          print ("Waiting to launch task....")
          wait_debug -= 1
      self.driver.stop()
      t.join()

    except KeyboardInterrupt:
      print("Terminating")
      self.driver.stop()
      t.join()


  def stop(self):
    self.launcher.terminate = True
    self.driver.stop()


  def getSandboxURL(self, jobId):
    master = resolve(MASTER).strip()
    url = os.path.join('http://', master, "#/frameworks", self.frameworkId.value)
    return url


