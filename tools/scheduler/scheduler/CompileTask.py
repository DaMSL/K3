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

from mesosutils import *


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


def genScript(app, options=''):
  script = '''
#!/bin/bash

cd /k3/K3
./tools/scripts/run/compile.sh %s $MESOS_SANDBOX/%s.k3
mv __build/A $MESOS_SANDBOX/%s
mv __build/%s.cpp $MESOS_SANDBOX/
cd $MESOS_SANDBOX
curl -i -H "Accept: application/json" -F "file=@%s" http://qp1:5000/apps
tar -cf %s.tar --exclude='k3compexec' *
curl -i -H "Accept: application/json" -F "file=@%s.tar" http://qp1:5000/apps/%s/archive
'''  % (options, app, app, app, app, app, app, app)
  return script



class CompileLauncher(mesos.interface.Scheduler):
    def __init__(self, **kwargs):
        self.launched   = False
        self.terminate  = False
        self.appId    = kwargs.get('appId', 'CompileTask')
        self.source   = kwargs.get('source', None)
        self.script   = kwargs.get('script', None)
        self.uid      = kwargs.get('uid', '')
        self.path     = kwargs.get('path', '')
        self.log      = []

        print "Compile Launcher CREATED"


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

          task = compileTask(appId=self.appId,
                             uid=self.uid,
                             script=self.script,
                             source=self.source,
                             slave=offer.slave_id.value)

          print (task.name, task.task_id.value)

          driver.launchTasks(offer.id, [task])

          print ("COMPILE TASK IS LAUNCHED")
          self.launched = True
          

    def statusUpdate(self, driver, update):
        print ("[COMPILE STATUS] %s" % update.data)

        if update.state == mesos_pb2.TASK_RUNNING:
            self.log.append(update.data)
            with open(os.path.join(self.path, 'output'), 'a') as out:
              out.write(update.data)
            #self.log += update.data.encode('utf-8')


        if update.state == mesos_pb2.TASK_FAILED:
            print ("Compilation has failed :(")
            self.terminate = True

        if update.state == mesos_pb2.TASK_FINISHED:
            print ("Compilation has finished")
            self.terminate = True

        if update.state == mesos_pb2.TASK_LOST:
            print ("Compilation Task is lost... terminating")
            self.terminate = True



	# Mesos invokes this method when an executor sends a message
    def frameworkMessage(self, driver, executorId, slaveId, message):
      print ("[COMPILER FRAMEWORK MESSAGE]: %s" % str(message))

class CompileDriver:
  def __init__(self, **kwargs):
    app      = kwargs.get('name', 'CompileTask')
    source   = kwargs.get('source', None)
    script   = kwargs.get('script', None)
    uid      = kwargs.get('uid', '')
    path     = kwargs.get('path', '')

    f = mesos_pb2.FrameworkInfo()
    f.user = "" # Have Mesos fill in the current user.
    f.name = "Compiling %s" % app
    f.principal = "framework-k3-compile"

    print "Framework name is %s" % f.name

    self.framework = f
    self.launcher = CompileLauncher(appId=app, source=source,
                                    script=script, uid=uid, path=path)
    self.driver = None

  def getLog(self):
    return self.launcher.log


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
      wait_debug = 5
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



