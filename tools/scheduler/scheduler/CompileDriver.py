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


# TODO: Consider splitting into two separate tasks: pre & post compilation
def genScript(c):
  script = '''
#!/bin/bash

cd /k3/K3
./tools/scripts/run/compile.sh %(options)s $MESOS_SANDBOX/%(name)s.k3

echo Collecting binary and CPP files
cp __build/A $MESOS_SANDBOX/%(name)s
cp __build/%(name)s.cpp $MESOS_SANDBOX/
cd $MESOS_SANDBOX
echo
echo
echo Uploading New K3 binary to server
curl -i -H "Accept: application/json" -F file=@%(name)s -F uid=%(uid)s http://%(host)s:%(port)d/apps
echo
echo
echo Uploading Project Archive to server
tar -cf %(name)s.tar --exclude='k3compexec' *
curl -i -H "Accept: application/json" -F "file=@%(name)s.tar" http://%(host)s:%(port)d/apps/%(name)s/%(uid)s
echo
echo
echo Finished compilation job
'''  % (c)
  return script

#(options, app, app, app, app, uid, app, uname, uname)

class CompileLauncher(mesos.interface.Scheduler):
    def __init__(self, job, **kwargs):
        self.launched   = False
        self.terminate  = False
        self.job        = job

        self.source   = kwargs.get('source', None)
        self.script   = kwargs.get('script', None)
        self.webaddr  = kwargs.get('webaddr', None)
        self.status   = JobStatus.INITIATED
        self.log      = []

    def registered(self, driver, frameworkId, masterInfo):
        print ("[COMPILER REGISTERED] ID %s" % frameworkId.value)

    #  This Gets invoked when Mesos offers resources to my framework & we decide what to do with the recources (e.g. launch task, set mem/cpu, etc...)
    def resourceOffers(self, driver, offers):
        print ("[COMPILER RESOURCE OFFER] Got %d resource offers" % len(offers))

        #TODO: Det where to compile K3... rightnow, use qp3
        for offer in offers:
          if self.launched or offer.hostname != 'qp3':
            driver.declineOffer(offer.id)
            continue

          print "Accepted compile offer"
          task = compileTask(name=self.job.name,
                             uid=self.job.uid,
                             script=self.script,
                             source=self.source,
                             webaddr=self.webaddr,
                             slave=offer.slave_id.value)

          db.insertCompile(self.job.__dict__)
          driver.launchTasks(offer.id, [task])
          self.status   = JobStatus.SUBMITTED
          self.launched = True

    def statusUpdate(self, driver, update):
        # print ("[COMPILE STATUS] %s" % update.data)
        self.log.append(update.data)
        if not os.path.exists(self.job.path):
          os.mkdir(self.job.path)
        with open(os.path.join(self.job.path, 'output'), 'a') as out:
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


