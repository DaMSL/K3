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

import db
from mesosutils import *
from core import *

State = enum.Enum('State', 'INIT DISPATCH MASTER_WAIT WORKER_WAIT CLIENT_WAIT SUBMIT COMPILE UPLOAD KILL')

class CompileLauncher(mesos.interface.Scheduler):
    def __init__(self, job, **kwargs):
        self.launched   = False
        self.terminate  = False
        self.job        = job

        self.source   = kwargs.get('source', None)
        self.script   = kwargs.get('script', None)
        self.webaddr  = kwargs.get('webaddr', None)
        self.numworkers = kwargs.get('numworkers', 1)
        self.readyworkers = 0
        self.blocksize = kwargs.get('blocksize', 4)
        self.status   = JobStatus.INITIATED
        self.log      = []

        self.tasks    = {}  # Map (svid: taskInfo) 
        self.offers   = {}  # Map (svid: offer.id)

        self.state    = State.INIT
        logging.debug (" COMPILER Initiated")

        self.roles    = {}
        self.master = None
        self.workers = []
        self.client  = None

    def registered(self, driver, frameworkId, masterInfo):
        logging.info("[COMPILER] Compiler is registered with Mesos. ID %s" % frameworkId.value)

    #  This Gets invoked when Mesos offers resources to my framework & we decide what to do with the recources (e.g. launch task, set mem/cpu, etc...)
    def resourceOffers(self, driver, offers):
        logging.info("[COMPILER] Got %d resource offers. Current state = %s" % (len(offers), self.state))


        #TODO: Det where to compile K3... rightnow, use qp3
        for offer in offers:
          if offer.hostname not in ['qp3', 'qp4', 'qp5']:
            driver.declineOffer(offer.id)
            # logging.debug("DECLINING Offer from %s" % offer.hostname)
            continue

          logging.info("ACCEPTING Offer from %s" % offer.hostname)

          mem = getResource(offer.resources, "mem")
          cpu = getResource(offer.resources, "cpus")
          # TODO:  PORT assignment
          port = 20000
          logging.info('  Resources:  cpu=%s, mem=%s, port=%d' % (cpu, mem, port))

          daemon = None

          if self.master == None:
              
            logging.debug("Creating Master role")
            daemon = dict(role='master', svid='master', hostname=offer.hostname,
                port=port, host=socket.gethostbyname(offer.hostname))
            self.master = daemon
            db.insertCompile(self.job.__dict__)

          elif len(self.workers) < self.numworkers:
          # elif len(self.workers) < self.numworkers:
            logging.debug("Creating Worker #%d role" % len(self.workers))
            daemon = dict(role='worker', 
                svid='worker%d' % len(self.workers), hostname=offer.hostname,
                port=port, host=self.master['host'])
            self.workers.append (daemon)

          elif self.client == None:
          # elif len(self.workers) < self.numworkers:
            logging.debug("Creating Client role")
            daemon = dict(role='client', svid='client', 
              hostname=offer.hostname, port=port,
              host=self.master['host'], blocksize=self.blocksize)
            self.client = daemon
            db.updateCompile(self.job.uid, status=self.state)
            self.state = State.DISPATCH

            self.launched = True

          else:
            logging.debug("ALL tasks are assigned. Current state: %s" % self.state)

          if daemon == None:
            continue

          config = dict(hostname=offer.hostname, port=port, webaddr=self.webaddr, name=self.job.name)
          daemon.update(config)

          task = compileTask(name=self.job.name,
                               uid=self.job.uid,
                               source=self.source,
                               webaddr=self.webaddr,
                               mem=mem,
                               cpu=cpu,
                               slave=offer.slave_id.value,
                               daemon=daemon)
          logging.debug("CREATED TASK for:  %s " % daemon['svid'])

          self.offers[daemon['svid']] = offer.id 
          self.tasks[daemon['svid']]  = task


        if self.state == State.DISPATCH:    
          logging.info ("ACCEPTED OFFER LIST:")
          logging.info ("    master ----> %s" % self.master['hostname'])
          logging.info ("    client ----> %s" % self.client['hostname'])
          for w in self.workers:
            logging.info("    %s ----> %s" % (w['svid'], w['hostname']))
          logging.debug("LAUNCHING MASTER")
          self.state = State.MASTER_WAIT
          driver.launchTasks(self.offers['master'], [self.tasks['master']])


    def statusUpdate(self, driver, update):
        logging.debug("[COMPILER UPDATE] %s" % update.data)

        #  TODO: CHANGE this to logging module
        self.log.append(update.data)
        if not os.path.exists(self.job.path):
          os.mkdir(self.job.path)
        with open(os.path.join(self.job.path, 'output'), 'a') as out:
          out.write(update.data)

        if update.state == mesos_pb2.TASK_RUNNING:
          # logging.debug("NODE STARTED: %s  (current state = %s)" % (update.message, self.state))
          if self.state == State.MASTER_WAIT and update.message == 'master':
            logging.info("MASTER is READY. Launching Workers")
            self.state = State.WORKER_WAIT
            driver.launchTasks(self.offers['worker0'], [self.tasks['worker0']])

            # for worker in [w for w in self.offers if w.startswith('worker')]:
            #   driver.launchTasks(self.offers[worker], [self.tasks[worker]])
            db.updateCompile(self.job.uid, status=self.state)
  

          if self.state == State.WORKER_WAIT and update.message == 'worker':
            self.readyworkers += 1
            logging.debug("Worker reported READY. Readiness status: %d out of %d Total Worker" % (self.readyworkers,self.numworkers))
            if self.readyworkers == self.numworkers:
              logging.info("ALL WORKERS READY. Launching Client")
              self.state = State.CLIENT_WAIT
              db.updateCompile(self.job.uid, status=self.state)
              driver.launchTasks(self.offers['client'], [self.tasks['client']])

          if self.state == State.CLIENT_WAIT and update.message == 'client':
            logging.info("CLIENT is READY. Submitting Compilation Task")
            self.state = State.SUBMIT


        if update.state == mesos_pb2.TASK_RUNNING:
          logging.debug("[UPDATE %s STDOUT] %s" % (update.task_id.value, update.data))
          logging.debug("[UPDATE %s STDERR] %s" % (update.task_id.value, update.message))
            # if self.status == JobStatus.SUBMITTED:
            #   self.status = JobStatus.COMPILING
            #   db.updateCompile(self.job.uid, status=self.status)

            # elif self.status == JobStatus.COMPILING and \
            #       update.data.startswith('Created binary file:'):
            #   self.status = JobStatus.ARCHIVING
            #   db.updateCompile(self.job.uid, status=self.status)


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
      logging.info('[FRWK MSG] %s ' % str(message))


