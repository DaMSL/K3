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
from threading import Event
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


DEFAULT_MEM = 4
DEFAULT_CPU = 4


class CompileJob():
  def __init__(self, name, uid, builddir, settings, **kwargs):
      logging.debug ("[COMPILER] Initializing new Compilation Job for " + name)

      # Initialize framework state
      self.state      = CompileState.INIT
      self.launched   = False
      self.success    = False
      self.driver     = None

      # Set compiler meta-date and compile configuration settings
      self.name         = name
      self.uid          = uid
      self.svid         = name + '-' + uid

      # Set mem & cpu
      self.cpu          = kwargs.get('cpu', DEFAULT_CPU)
      if not self.cpu:
        self.cpu = DEFAULT_CPU

      self.mem          = kwargs.get('mem', DEFAULT_MEM)
      if not self.mem:
        self.mem = DEFAULT_MEM
      self.mem *= 1024

      logging.debug ("  JOB MEM=%d, CPU=%d", self.mem, self.cpu)

      self.localpath    = builddir
      self.settings     = settings
      self.settings.update(role='client')

      self.compilestage = getCompileStage(settings['compilestage']).value

      # TODO: Merge logs / and/or split log output
      # self.wslog = logging.getLogger("compiler")

      compile_fmt = ServiceFormatter('[%(asctime)s] %(message)s')
      fh = logging.FileHandler(os.path.join(self.localpath, 'output'))
      fh.setFormatter(compile_fmt)
      self.log = logging.getLogger(self.svid)
      self.log.setLevel(logging.DEBUG)
      self.log.propagate = False
      self.log.addHandler(fh)


      self.log.info("[COMPILER] Posting new job into DB: %s", self.name)
      self.log.info("------------------  NEW COMPILATION JOB --------------------")

      for l in [logging, self.log]:
        l.info("    %-15s" % 'Name:' + self.name)
        l.info("    %-15s" % 'UID:' + str(self.uid))
        l.info("    %-15s" % 'Build Dir:' + str(self.localpath))
        for k, v in self.settings.items():
          l.info("    %-15s" % (k+':') + str(v))

      db.insertCompile(dict(self.settings, name=self.name, uid=self.uid, path=self.localpath))

  def getItems(self):
    return dict(self.settings, name=self.name, uid=self.uid, status=self.state.name,
      svid=self.svid, path=self.localpath, compilestage=self.compilestage,
      mem=self.mem, cpu=self.cpu)

  def update(self, data):
    self.settings.update(data)

  def updateState(self, state):
    self.state = state
    db.updateCompile(self.uid, status=self.state.name)            

  def kill(self, driver):
    tid = mesos_pb2.TaskID()
    tid.value = self.svid
    logging.warning("[COMPILER] Killing compilation job: " + self.svid)
    driver.killTask(tid)



class CompileServiceManager(mesos.interface.Scheduler):
    # def __init__(self, job, **kwargs):

    @classmethod
    def compileSettings(cls):
      return dict(
        options='', 
        user='',  
        tag='',
        path='',
        url='',
        webaddr='',
        blocksize=8,
        compilestage='both',
        branch='development',
        gitpull=True,
        cabalbuild=False,
        compileargs='',
        cppthread=12,
        m_workerthread=1,
        w_workerthread=1,
        heartbeat=300)

    def __init__(self, logdir, webaddr):
        logging.debug ("[COMPILER] Compiler service is initializing........")

        self.state        = CompileServiceState.DOWN #CompileState.INIT  # "DOWN", "INIT", "UP-IDLE", "UP-COMPILING"
        self.jobs         = {}
        self.ready        = False
        self.launched     = False
        self.gracefulHalt = False

        self.driver       = None
        self.numworkers   = len(workerNodes)
        self.idle       = 0
        self.settings     = self.compileSettings()
        self.webaddr      = webaddr

        # Master & Workers
        self.nodes    = {}  # Map (svid: taskInfo) 
        self.offers   = {}  # Map (svid: offer.id)

        self.master     = None
        self.workers    = []
        self.masterHost = None
        self.masterPort = 0
        self.readyworkers = 0

        # Clients
        self.compileJobs = 0
        self.pending = []
        self.active  = {}

        self.running = Event()

        #  Configure logging for both ws & rotating filer
        self.log = logging.getLogger("compiler")
        self.gc()


    def update(self, data):
      self.settings.update(data)

    def getItems(self):
      return dict(self.settings, ready=self.ready, gracefulHaltFlag=self.gracefulHalt,
        launched=self.launched, numworkers=len(self.workers),
        master=self.masterHost, port=self.masterPort)

    def gc(self):
      compiles = db.getCompiles()
      for c in compiles:
        svid = c['name'] + '-' + c['uid']

        if c['status'] not in compileTerminatedStates and svid not in self.active:
          db.updateCompile(c['uid'], status='KILLED', done=True)


    def goUp(self, workers):
      if self.state == CompileServiceState.DOWN:
        self.numworkers = workers
        self.state = CompileServiceState.INIT
      self.running.set()

    def goDown(self):
      self.log.info("Asked to Shut Down Compiler Service.")

      del self.pending[:]

      for svid, job in self.active.items():
        job.kill(self.driver)
        del self.active[svid]

      # TODO: Start a halt client sentinel to properly shut down the service

      # Stop the Workers
      for node in self.workers:
        tid = mesos_pb2.TaskID()
        tid.value = node['svid']
        self.log.info("[COMPILER] Killing node: %s", node['svid'])
        self.driver.killTask(tid)

      # Stop the Master
      tid = mesos_pb2.TaskID()
      tid.value = 'master'
      self.log.info("[COMPILER] Killing the master node")
      self.driver.killTask(tid)

      self.master = None
      del self.workers[:]
      self.readyworkers = 0
      self.ready = False
      self.gracefulHalt = False
      self.nodes.clear()
      self.state = CompileServiceState.DOWN
      self.log.info("[COMPILER] Compiler Service is DOWN")
      self.gc()

      # self.running.clear()
      # self.running.wait()

    def goDownGracefully(self):
      self.log.info("[COMPILER] Received signal to Halt")
      self.log.info("     Active jobs remaining to complete:  %d", len(self.active.keys()))
      self.log.info("     Flushing %d JOBS from PENDING QUEUE:", len(self.pending))
      for job in self.pending:
        self.log.info("       - %s", job.svid)

      del self.pending[:]
      self.gracefulHalt = True
      if len(self.active.keys()) == 0:
        self.goDown()

    def kill(self):
      self.terminate = True
      if self.driver:
        self.driver.stop()

    def isUp(self):
      return self.state == CompileServiceState.UP

    def isDown(self):
      return self.state == CompileServiceState.DOWN

    def isActive(self):
      return (len(self.pending) + len(self.active.keys())) == 0

    def getActiveState(self):
      return {svid: job.state.name for svid, job in self.active.items()}

    def submit(self, job):
      if self.state == CompileServiceState.DOWN:
        self.log.warning("[COMPILER] Received compilation request, but the service is down")
      elif self.gracefulHalt:
        self.log.warning("[COMPILER] Received compilation request, service will be gracefully going down  and is not accepting any new jobs.")
      else:
        self.log.info("[COMPILER] Receive new COMPILATION request")
        # = compileJob (name, uid, builddir, masterhost, masterport, settings)

        self.pending.append(job)

    def initiateCompilerNode(self, offer):

      mem = getResource(offer.resources, "mem")
      cpu = getResource(offer.resources, "cpus")
      port = None

      self.log.info('  Resources:  cpu=%s, mem=%s' % (cpu, mem))

      role = None

      # Match offers with requirements (master, client, & all workers) 
      if self.master == None and offer.hostname in masterNodes:
        self.log.debug("Creating Master role")

        #update master host
        self.masterHost = socket.gethostbyname(offer.hostname)

        # bind to the first port in the first list of port ranges
        portRanges = getResource(offer.resources, "ports")
        self.masterPort = portRanges[0][0]
        port = self.masterPort

        role = dict(role='master', svid='master', hostname=offer.hostname)
        self.master = role

      elif len(self.workers) < self.numworkers and offer.hostname in workerNodes:
        workerNum = len(self.workers) + 1
        self.log.debug("Creating Worker #%d role" % workerNum)
        role = dict(role='worker', svid='worker%d' % workerNum, hostname=offer.hostname)
        self.workers.append (role)
        self.log.debug ("Worker %d out of %d assigned" % (len(self.workers), self.numworkers))

      else:
        return None

      role.update(self.settings, name='CompileService')
      return compileTask(AppID("CompileService", "0"),offer.slave_id.value, role, cpu, mem, port)

    def launchNext(self, driver, offer):

      # Get the first job on the queue
      job = self.pending[0]

      # CHECK available resources & allocate min necessary OR TODO: decline & return
      mem = min(int(getResource(offer.resources, "mem")), job.mem)
      cpu = min(int(getResource(offer.resources, "cpus")), job.cpu)
      logging.info('  Resources:  cpu=%s, mem=%s' % (cpu, mem))

      logging.debug("Creating Client role")

      self.compileJobs += 1
      job.update(dict(hostname=offer.hostname, host=self.masterHost, port=self.masterPort))
      
      task = compileTask(AppID(job.name, job.uid),offer.slave_id.value, job.getItems(), cpu, mem, None)
      logging.debug("CREATED TASK for:  %s " % job.svid)

      db.updateCompile(job.uid, status=job.state.name)

      # Launch the master
      driver.launchTasks(offer.id, [task])
      job.state = CompileState.CLIENT_WAIT
      job.launched = True
      del self.pending[0]
      self.active[job.svid] = job

    def killJob(self, svid):
      self.log.info('[COMPILER] Asked to kill job: %s', svid)
      if svid in self.pending:
        self.pending.remove(svid)
      elif svid in self.active:
        self.active[svid].kill(self.driver)
        del self.active[svid] 


    def registered(self, driver, frameworkId, masterInfo):
        self.log.info("[COMPILER] Compiler Service is registered with Mesos. ID %s" % frameworkId.value)

        # Save the driver, in case we need to stop it later
        self.driver = driver
        # self.running.wait()

    #  This Gets invoked when Mesos offers resources to my framework & we decide what to do with the recources (e.g. launch task, set mem/cpu, etc...)
    def resourceOffers(self, driver, offers):


        if time.time() > self.idle + heartbeat_delay:
          self.log.info("[COMPILER] SERVICE is HeartBeatting with Mesos. State=%s. (offers available)", self.state.name)
          self.idle = time.time()

        # Iterate through each offer & either accept & hold OR decline it
        for offer in offers:

          if (self.state == CompileServiceState.DOWN) or (offer.hostname not in compilerNodes):
            driver.declineOffer(offer.id)
            # self.log.debug("DECLINING Offer from %s" % offer.hostname)
            continue


          if self.state == CompileServiceState.INIT:
            task = self.initiateCompilerNode(offer)
            if task == None:
              # self.log.debug("DECLINING Offer from %s (unneeded resource). Current state: %s" % (offer.hostname, self.state))
              driver.declineOffer(offer.id)
              continue

            self.log.info("ACCEPTING Offer from %s" % offer.hostname)
            svid = task.task_id.value
            self.log.debug("CREATED TASK for: " + svid)

            # Store this offer & taskInfo message
            self.offers[svid] = offer.id 
            self.nodes[svid]  = task

            # Check if all requirements are met
            if self.master and len(self.workers) == self.numworkers:
              self.state = CompileServiceState.DISPATCH

          # Service is Running: Process a Compilation Task (of any are pending)
          elif self.state == CompileServiceState.UP and offer.hostname in clientNodes:
            if len(self.pending) == 0:
              driver.declineOffer(offer.id)
              continue
            self.log.info("ACCEPTING Offer from %s for COMPILATION JOB" % offer.hostname)
            self.launchNext(driver, offer)

          else:
            driver.declineOffer(offer.id)

            
        # If in the DISPATCH state: initiation compilation by starting the master
        if self.state == CompileServiceState.DISPATCH:    
          # Update all nodes with master address
          for s, task in self.nodes.items():
            addTaskLabel(task, 'host', self.masterHost)
            addTaskLabel(task, 'port', self.masterPort)
          self.log.info ("ACCEPTED OFFER LIST:")
          self.log.info ("    master  ----> %s" % self.master['hostname'])
          for w in self.workers:
            self.log.info("    %s ----> %s" % (w['svid'], w['hostname']))
          self.log.debug("LAUNCHING MASTER")
          self.state = CompileServiceState.MASTER_WAIT

          # Launch the master
          driver.launchTasks(self.offers['master'], [self.nodes['master']])
          self.launched = True

    def clientResponse(self, svid, update):

      clientTerminated = False

      for line in update.data.split('\n'):
        self.active[svid].log.info(line)

      if update.state == mesos_pb2.TASK_STARTING:
        self.log.info("[COMPILER] CLIENT is READY. Submitting Compilation Task")
        self.active[svid].updateState(CompileState.SUBMIT)

      if update.state == mesos_pb2.TASK_RUNNING and self.active[svid].state == CompileState.SUBMIT:
        self.active[svid].updateState(CompileState.COMPILE)

      # Client has failed or is lost
      if update.state == mesos_pb2.TASK_FAILED or update.state == mesos_pb2.TASK_LOST:
        clientTerminated = True  
        self.log.warning("[COMPILER]  -- FAILED TASK [%s]: %s.  Killing the job", update.message, update.data)
        self.active[svid].state = CompileState.FAILED
        self.active[svid].kill()

      # Client has finished (successfully)
      if update.state == mesos_pb2.TASK_FINISHED:
        clientTerminated = True  
        self.log.info("[COMPILER]  -- FINISHED TASK [%s]: %s", update.message, update.data)
        self.active[svid].state = CompileState.COMPLETE

      if clientTerminated:
        db.updateCompile(self.active[svid].uid, status=self.active[svid].state.name, done=True)
        del self.active[svid]
        if len(self.active.keys()) == 0 and self.gracefulHalt:
          self.goDown()


    def statusUpdate(self, driver, update):

        # Ignore Heart Beat Messages
          
        # For now, log everything (except heartbeat messages)
        for line in update.data.split('\n'):
          # self.wslog.info(line)
          if "eartbeat" in line:
            continue
          self.log.info(line)


        if update.message == 'client':
          svid = update.task_id.value
          if svid in self.active.keys():
            self.clientResponse(svid, update)
          else:
            self.log.warning("[COMPILER] Received update message for inactive compilation job: " + svid)
        else: 
  
          # Tasks' Starting States are used to synchronize the compilation sequence
          if update.state == mesos_pb2.TASK_STARTING:

            # Master has started: launch all workers
            if self.state == CompileServiceState.MASTER_WAIT and update.message == 'master':
              self.log.info("MASTER is READY. Launching Workers")
              self.state = CompileServiceState.WORKER_WAIT
              for worker in [w for w in self.offers if w.startswith('worker')]:
                driver.launchTasks(self.offers[worker], [self.nodes[worker]])
    
            # A worker has started:
            if self.state == CompileServiceState.WORKER_WAIT and update.message == 'worker':
              self.readyworkers += 1
              self.log.debug("Worker reported READY. Readiness status: %d out of %d Total Worker" % (self.readyworkers,self.numworkers))

              # If all workers are ready: launch the client
              if self.readyworkers == self.numworkers:
                self.log.info("ALL WORKERS READY. Compiler Service is READY to receive jobs.")
                self.state = CompileServiceState.UP
                self.ready = True

          elif update.state == mesos_pb2.TASK_RUNNING:
            pass

          # Someone has failed. Stop everything
          elif update.state == mesos_pb2.TASK_FAILED or update.state == mesos_pb2.TASK_LOST:
              self.log.warning("[COMPILER]  -- FAILED NODE [%s]: %s.  Killing the job", update.message, update.data)
              # self.terminate = True
              # self.driver.stop()

          # Someone has finished (successfully)
          elif update.state == mesos_pb2.TASK_FINISHED:
              self.log.info("[COMPILER]  -- NODE HAS FINISHED [%s]: %s", update.message, update.data)
              # self.terminate = True
              # if update.message == 'master':
              #   self.driver.stop()

  # Mesos invokes this method when an executor sends a message
    def frameworkMessage(self, driver, executorId, slaveId, message):
      self.log.info('[FRWK MSG] %s ' % str(message))