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

from enum import enum

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

from common import *
from core import *
from mesosutils import *
import db

State = enum.Enum('State', 'INIT DISPATCH MASTER_WAIT WORKER_WAIT CLIENT_WAIT SUBMIT COMPILE UPLOAD COMPLETE KILLED')

masterNodes =  ['qp3']
# workerNodes =  ['qp-hm' + str(i) for i in range(1,9)]
# workerNodes =  ['qp4']
workerNodes =  ['qp-hd' + str(i) for i in [1, 3, 4, 6, 7, 8, 10, 12]]
clientNodes =  ['qp5']

class CompileLauncher(mesos.interface.Scheduler):
    def __init__(self, job, **kwargs):
        logging.debug ("[COMPILER] Initializing........")

        self.state    = State.INIT
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

        #TODO: Det where to compile K3... rightnow, use qp3
        for offer in offers:
          if self.launched or offer.hostname not in masterNodes + workerNodes + clientNodes:
            driver.declineOffer(offer.id)
            # logging.debug("DECLINING Offer from %s" % offer.hostname)
            continue

          logging.info("ACCEPTING Offer from %s" % offer.hostname)

          mem = getResource(offer.resources, "mem")
          cpu = getResource(offer.resources, "cpus")
          logging.info('  Resources:  cpu=%s, mem=%s' % (cpu, mem))

          daemon = None

          if self.master == None and offer.hostname in masterNodes:
              
            logging.debug("Creating Master role")

            #update master host (& TODO: port):
            self.masterHost = socket.gethostbyname(offer.hostname)
            self.masterPort = 20000   # For now

            daemon = dict(role='master', svid='master', hostname=offer.hostname)
            self.master = daemon

          elif len(self.workers) < self.numworkers and offer.hostname in workerNodes:
          # elif len(self.workers) < self.numworkers:
            workerNum = len(self.workers) + 1
            logging.debug("Creating Worker #%d role" % workerNum)
            daemon = dict(role='worker', svid='worker%d' % workerNum, hostname=offer.hostname,)
            self.workers.append (daemon)
            logging.debug ("Worker %d out of %d assigned" % (len(self.workers), self.numworkers))

          elif self.client == None and offer.hostname in clientNodes:
          # elif len(self.workers) < self.numworkers:
            logging.debug("Creating Client role")
            daemon = dict(role='client', svid='client', 
              hostname=offer.hostname, blocksize=self.blocksize, compilestage=self.compilestage)
            self.client = daemon

          else:
            logging.debug("DECLINING Offer from %s (unneeded resource). Current state: %s" % (offer.hostname, self.state))
            driver.declineOffer(offer.id)

          if daemon == None:
            continue

          config = dict(hostname=offer.hostname, 
            webaddr=self.webaddr, name=self.name, uid=self.uid)
          daemon.update(config)

          task = compileTask(name=self.name,
                               uid=self.uid,
                               source=self.source,
                               webaddr=self.webaddr,
                               mem=mem,
                               cpu=cpu,
                               slave=offer.slave_id.value,
                               daemon=daemon)
          logging.debug("CREATED TASK for:  %s " % daemon['svid'])

          self.offers[daemon['svid']] = offer.id 
          self.tasks[daemon['svid']]  = task

          if self.master and self.client and len(self.workers) == self.numworkers:
            db.updateCompile(self.uid, status=self.state.name)
            self.state = State.DISPATCH
            break


        if self.state == State.DISPATCH:    
          for svid, task in self.tasks.items():
            addTaskLabel(task, 'host', self.masterHost)
            addTaskLabel(task, 'port', self.masterPort)
          logging.info ("ACCEPTED OFFER LIST:")
          logging.info ("    master  ----> %s" % self.master['hostname'])
          logging.info ("    client  ----> %s" % self.client['hostname'])
          for w in self.workers:
            logging.info("    %s ----> %s" % (w['svid'], w['hostname']))
          logging.debug("LAUNCHING MASTER")
          self.state = State.MASTER_WAIT
          driver.launchTasks(self.offers['master'], [self.tasks['master']])
          self.launched = True


    def statusUpdate(self, driver, update):
        # state = mesos_pb2.TaskState.Name(update.state)
        # logging.debug("[UPDATE - %s]: %s {%s}" % (update.message, state, update.data))

        self.wslog.info(update.data)
        #  TODO: CHANGE this to logging module
        with open(os.path.join(self.localpath, 'output'), 'a') as out:
          out.write(update.data)

        if update.state == mesos_pb2.TASK_STARTING:
          if self.state == State.MASTER_WAIT and update.message == 'master':
            logging.info("MASTER is READY. Launching Workers")
            self.state = State.WORKER_WAIT
            for worker in [w for w in self.offers if w.startswith('worker')]:
              driver.launchTasks(self.offers[worker], [self.tasks[worker]])
            db.updateCompile(self.uid, status=self.state.name)
  
          if self.state == State.WORKER_WAIT and update.message == 'worker':
            self.readyworkers += 1
            logging.debug("Worker reported READY. Readiness status: %d out of %d Total Worker" % (self.readyworkers,self.numworkers))
            if self.readyworkers == self.numworkers:
              logging.info("ALL WORKERS READY. Launching Client")
              self.state = State.CLIENT_WAIT
              db.updateCompile(self.uid, status=self.state.name)
              driver.launchTasks(self.offers['client'], [self.tasks['client']])

          if self.state == State.CLIENT_WAIT and update.message == 'client':
            logging.info("CLIENT is READY. Submitting Compilation Task")
            self.state = State.SUBMIT
            db.updateCompile(self.uid, status=self.state.name)

        if update.state == mesos_pb2.TASK_RUNNING:
          logging.debug(update.data)
          if update.message == 'client':
            self.state = State.COMPILE
            db.updateCompile(self.uid, status=self.state.name)
          if update.message == 'complete':
            self.state = State.UPLOAD
            db.updateCompile(self.uid, status=self.state.name)

        if update.state == mesos_pb2.TASK_FAILED:
            logging.warning("[COMPILER]  -- FAILED TASK [%s]: %s", update.message, update.data)
            self.state = State.COMPLETE
            db.updateCompile(self.uid, status=self.state.name)
            self.terminate = True

        if update.state == mesos_pb2.TASK_FINISHED:
            logging.info("[COMPILER]  -- FINISHED TASK [%s]: %s", update.message, update.data)
            self.state = State.COMPLETE
            db.updateCompile(self.uid, status=self.state.name)
            self.terminate = True
            if update.message == 'master':
              self.driver.stop()

        if update.state == mesos_pb2.TASK_LOST:
            logging.warning("[COMPILER]  -- LOST TASK [%s]: %s", update.message, update.data)
            self.terminate = True



    def kill(self):
      logging.warning("Asked to kill compilation job for %s", self.name)
      for svid, task in self.tasks.items:
        tid = mesos_pb2.TaskID()
        tid.value = svid
        logging.info("[DISPATCHER] Killing task: %s", svid)
        self.driver.killTask(tid)
      self.driver.stop()



	# Mesos invokes this method when an executor sends a message

    def frameworkMessage(self, driver, executorId, slaveId, message):
      logging.info('[FRWK MSG] %s ' % str(message))


