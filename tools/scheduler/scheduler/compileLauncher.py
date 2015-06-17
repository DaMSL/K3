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
import datetime
import socket
import logging

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

from common import *
from core import *
from mesosutils import *
import db



class CompileLauncher(mesos.interface.Scheduler):
    def __init__(self, job, **kwargs):
        logging.debug ("[COMPILER] Initializing........")

        self.state    = CompileState.INIT
        self.launched   = False
        self.terminate  = False
        self.driver = None

        self.name = job.name
        self.localpath = job.path
        self.uid  = job.uid
        self.numworkers = job.numworkers
        self.blocksize = job.blocksize
        self.source   = kwargs.get('source', None)
        self.webaddr  = kwargs.get('webaddr', None)
        self.compilestage = job.compilestage

        self.tasks    = {}  # Map (svid: taskInfo) 
        self.offers   = {}  # Map (svid: offer.id)

        self.master = None
        self.workers = []
        self.client  = None
        self.masterHost = None
        self.masterPort = 0
        self.readyworkers = 0
        self.idle = 0
        self.success = False


        self.wslog = logging.getLogger("compiler")

        logging.info("[COMPILER] Posting new job into DB: %s", self.name)
        logging.info("    Name:          " + self.name)
        logging.info("    uid:           " + str(self.uid))
        logging.info("    blocksize:     " + str(self.blocksize))
        logging.info("    # Workers:     " + str(self.numworkers))
        logging.info("    Compile Stage: " + str(self.compilestage))
        logging.info("    Build Dir:     " + str(self.localpath))

        db.insertCompile(job.__dict__)
        # for c in db.getCompiles():
        #   logging.debug("COMPILE JOB FOUND!!!  --- " + c['name'])



    def registered(self, driver, frameworkId, masterInfo):
        logging.info("[COMPILER] Compiler is registered with Mesos. ID %s" % frameworkId.value)
        self.driver = driver

    #  This Gets invoked when Mesos offers resources to my framework & we decide what to do with the recources (e.g. launch task, set mem/cpu, etc...)
    def resourceOffers(self, driver, offers):
        if time.time() > self.idle + heartbeat_delay:
          logging.info("[COMPILER] HeartBeatting with Mesos. `%s` Current state: %s.  (offers available)", self.name, self.state)
          self.idle = time.time()

        accepted = []

        # Iterate through each offer & either accept & hold OR decline it
        for offer in offers:
          if self.launched  or self.state == CompileState.DISPATCH or offer.hostname not in allNodes:
            driver.declineOffer(offer.id)
            # logging.debug("DECLINING Offer from %s" % offer.hostname)
            continue

          logging.info("ACCEPTING Offer from %s" % offer.hostname)

          # For now, allocate all resourced in the offer
          mem = getResource(offer.resources, "mem")
          cpu = getResource(offer.resources, "cpus")
          port = None

          logging.info('  Resources:  cpu=%s, mem=%s' % (cpu, mem))

          daemon = None

          # Match offers with requirements (master, client, & all workers) 
          if self.master == None and offer.hostname in masterNodes:
            logging.debug("Creating Master role")

            #update master host
            self.masterHost = socket.gethostbyname(offer.hostname)

            # bind to the first port in the first list of port ranges
            portRanges = getResource(offer.resources, "ports")
            self.masterPort = portRanges[0][0]
            port = self.masterPort

            daemon = dict(role='master', svid='master', hostname=offer.hostname)
            self.master = daemon

          elif len(self.workers) < self.numworkers and offer.hostname in workerNodes:
            workerNum = len(self.workers) + 1
            logging.debug("Creating Worker #%d role" % workerNum)
            daemon = dict(role='worker', svid='worker%d' % workerNum, hostname=offer.hostname,)
            self.workers.append (daemon)
            logging.debug ("Worker %d out of %d assigned" % (len(self.workers), self.numworkers))

          elif self.client == None and offer.hostname in clientNodes:
            logging.debug("Creating Client role")
            daemon = dict(role='client', svid='client', 
              hostname=offer.hostname, blocksize=self.blocksize, compilestage=self.compilestage)
            self.client = daemon

          else:
            logging.debug("DECLINING Offer from %s (unneeded resource). Current state: %s" % (offer.hostname, self.state))
            driver.declineOffer(offer.id)

          if daemon == None:
            continue

          # Complete common daemon info for each node
          daemon.update(dict(hostname=offer.hostname, webaddr=self.webaddr, 
                             name=self.name, uid=self.uid))

          # Define the protobuf taskInfo message which will launch the task via Mesos
          task = compileTask(name=self.name,
                             uid=self.uid,
                             source=self.source,
                             webaddr=self.webaddr,
                             mem=mem,
                             cpu=cpu,
                             port=port,
                             slave=offer.slave_id.value,
                             daemon=daemon)
          logging.debug("CREATED TASK for:  %s " % daemon['svid'])

          # Store this offer & taskInfo message
          self.offers[daemon['svid']] = offer.id 
          self.tasks[daemon['svid']]  = task

          # Check if all requirements are met
          if self.master and self.client and len(self.workers) == self.numworkers:
            db.updateCompile(self.uid, status=self.state.name)
            self.state = CompileState.DISPATCH
            
        # If in the DISPATCH state: initiation compilation by starting the master
        if self.state == CompileState.DISPATCH:    

          # Update all nodes with master address
          for svid, task in self.tasks.items():
            addTaskLabel(task, 'host', self.masterHost)
            addTaskLabel(task, 'port', self.masterPort)
          logging.info ("ACCEPTED OFFER LIST:")
          logging.info ("    master  ----> %s" % self.master['hostname'])
          logging.info ("    client  ----> %s" % self.client['hostname'])
          for w in self.workers:
            logging.info("    %s ----> %s" % (w['svid'], w['hostname']))
          logging.debug("LAUNCHING MASTER")
          self.state = CompileState.MASTER_WAIT

          # Launch the master
          driver.launchTasks(self.offers['master'], [self.tasks['master']])
          self.launched = True


    def statusUpdate(self, driver, update):

        # Ignore Heart Beat Messages
        if "eartbeat" in update.data:
          return
          
        self.wslog.info(update.data)
        #  TODO: CHANGE this to logging module
        with open(os.path.join(self.localpath, 'output'), 'a') as out:
          out.write(update.data)

        # Tasks' Starting States are used to synchronize the compilation sequence
        if update.state == mesos_pb2.TASK_STARTING:

          # Master has started: launch all workers
          if self.state == CompileState.MASTER_WAIT and update.message == 'master':
            logging.info("MASTER is READY. Launching Workers")
            self.state = CompileState.WORKER_WAIT
            for worker in [w for w in self.offers if w.startswith('worker')]:
              driver.launchTasks(self.offers[worker], [self.tasks[worker]])
            db.updateCompile(self.uid, status=self.state.name)
  
          # A worker has started:
          if self.state == CompileState.WORKER_WAIT and update.message == 'worker':
            self.readyworkers += 1
            logging.debug("Worker reported READY. Readiness status: %d out of %d Total Worker" % (self.readyworkers,self.numworkers))

            # If all workers are ready: launch the client
            if self.readyworkers == self.numworkers:
              logging.info("ALL WORKERS READY. Launching Client")
              self.state = CompileState.CLIENT_WAIT
              db.updateCompile(self.uid, status=self.state.name)
              driver.launchTasks(self.offers['client'], [self.tasks['client']])

          # Client has started. 
          if self.state == CompileState.CLIENT_WAIT and update.message == 'client':
            logging.info("CLIENT is READY. Submitting Compilation Task")
            self.state = CompileState.SUBMIT
            db.updateCompile(self.uid, status=self.state.name)

        if update.state == mesos_pb2.TASK_RUNNING:
          # Client is running: the system is compiling
          if update.message == 'client':
            self.state = CompileState.COMPILE
            db.updateCompile(self.uid, status=self.state.name)

          # The client should return the first 'complete' message
          if update.message.startswith('complete'):
            self.state = CompileState.UPLOAD
            db.updateCompile(self.uid, status=self.state.name)

        # Someone has failed. Stop everything
        if update.state == mesos_pb2.TASK_FAILED:
            logging.warning("[COMPILER]  -- FAILED TASK [%s]: %s.  Killing the job", update.message, update.data)
            self.state = CompileState.FAILED
            db.updateCompile(self.uid, status=self.state.name, done=True)
            self.terminate = True
            self.driver.stop()

        # Someone has finished (successfully)
        if update.state == mesos_pb2.TASK_FINISHED:
            logging.info("[COMPILER]  -- FINISHED TASK [%s]: %s", update.message, update.data)
            self.state = CompileState.COMPLETE
            db.updateCompile(self.uid, status=self.state.name, done=True)
            self.terminate = True
            if update.message == 'master':
              self.driver.stop()

        # Mesos has lost the task
        if update.state == mesos_pb2.TASK_LOST:
            logging.warning("[COMPILER]  -- LOST TASK [%s]: %s.  Killing the job", update.message, update.data)
            db.updateCompile(self.uid, status=self.state.name, done=True)
            self.terminate = True
            self.driver.stop()  


    def kill(self):
      logging.warning("Asked to kill compilation job for %s", self.name)
      for svid, task in self.tasks.items():
        tid = mesos_pb2.TaskID()
        tid.value = svid
        logging.info("[DISPATCHER] Killing task: %s", svid)
        self.driver.killTask(tid)
      self.driver.stop()


	# Mesos invokes this method when an executor sends a message
    def frameworkMessage(self, driver, executorId, slaveId, message):
      logging.info('[FRWK MSG] %s ' % str(message))


