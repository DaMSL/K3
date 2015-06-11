# scheduler.dispatcher: Scheduler logic for matching resource offers to job requests.
import os
import sys
import time
import threading
import socket
from collections import deque

from common import *
from core import *
from mesosutils import *
import db

#from protobuf_to_dict import protobuf_to_dict
import json

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

import logging

DEFAULT_MEM = 4 * 1024


class Dispatcher(mesos.interface.Scheduler):
  def __init__(self, master, webaddr, daemon=True):

    self.mesosmaster  = master      # Mesos Master (e.g. zk://host1:2181,host2:2181/mesos)
    self.webaddr     = webaddr

    self.pending = deque()     # Pending jobs. First job is popped once there are enough resources available to launch it.
    self.active = {}           # Active jobs keyed on jobId.
    self.finished = {}         # Finished jobs keyed on jobId.
    self.offers = {}           # Offers from Mesos keyed on offerId. We assume they are valid until they are rescinded by Mesos.
    self.jobsCreated = 0       # Total number of jobs created for generating job ids.

    self.daemon = daemon       # Run as a daemon (or finish when there are no more pending/active jobs)
    self.connected = False
    self.terminate = False     # Flag to signal termination to the owner of the dispatcher
    self.frameworkId = None    # Will get updated when registering with Master

    self.idle = 0
    self.gc = time.time()
    self.offerRelease = 0

    logging.info("[DISPATCHER] Initializing with master at %s" % master)

 
  def submit(self, job):
    logging.info("[DISPATCHER] Received new Job for Application %s, Job ID= %d" % (job.appName, job.jobId))
    self.pending.append(job)


  def getActiveJobs(self):
    return self.active

  def getFinishedJobs(self):
    return self.finished


  def getJob(self, jobId):
    if jobId in self.active:
      return self.active[jobId]
    if jobId in self.finished:
      return self.finished[jobId]
    return None

  def fullId(self, jobId, taskId):
    return "%d.%d" % (jobId, taskId)

  def jobId(self, fullid):
    s = fullid.split(".")
    return int(s[0])

  def taskId(self, fullid):
    s = fullid.split(".")
    return int(s[1].strip())

  def getTask(self, fullid):
    jobId = self.jobId(fullid)
    tid = self.taskId(fullid)
    job = self.active[jobId]
    for t in job.tasks:
      if t.taskid == tid:
        return t
    return None

  def tryTerminate(self):
    if not self.daemon and len(self.pending) == 0 and len(self.active) == 0:
      self.terminate = True
      logging.info("[DISPATCHER] Terminating")

  def allocateResources(self, nextJob): #, driver):

    committedResources = {}
    availableCPU = {i: int(getResource(o.resources, "cpus")) for i, o in self.offers.items()}
    availableMEM = {i: getResource(o.resources, "mem") for i, o in self.offers.items()}
    availablePorts = {i: PortList(getResource(o.resources, "ports")) for i, o in self.offers.items()}

    # Try to satisy each role, sequentially
    for roleId in nextJob.roles.keys():
      unassignedPeers = nextJob.roles[roleId].peers
      committedResources[roleId] = {}

      for offerId in self.offers:
        if availableMEM[offerId] == None or availableMEM[offerId] == 0:
          continue
        if availableCPU[offerId] == 0:
          continue

        #  Check Hostmask requirement
        hostmask = nextJob.roles[roleId].hostmask
        host = self.offers[offerId].hostname.encode('utf8','ignore')
        r = re.compile(hostmask)
        if not r.match(host):
          logging.debug("%s does not match hostmask. DECLINING offer" % host)
          continue
        logging.debug("%s MATCHES hostmask. Checking offer" % host)

        # Allocate CPU Resource
        requestedCPU = min(unassignedPeers, availableCPU[offerId])
        if 'peers_per_host' in nextJob.roles[roleId].params:
          peer_per_host = nextJob.roles[roleId].params['peers_per_host']
          if peer_per_host > availableCPU[offerId]:
            # Cannot satisfy specific peers-to-host requirement
            continue
          else:
            requestedCPU = peer_per_host

        # Allocate Memory Resource
        requestedMEM = availableMEM[offerId]
        if 'mem' in nextJob.roles[roleId].params:
          memPolicy = nextJob.roles[roleId].params['mem']
          if memPolicy == 'some':
            requestedMEM = min(DEFAULT_MEM, availableMEM[offerId]/4)
          elif memPolicy == 'all':
            pass
          elif str(memPolicy).isdigit():
            requestedMEM = memPolicy * 1024
            if requestedMEM > availableMEM[offerId]:
              # Cannot satisfy user's memory request
              continue

        # Commit Resources for this offer with this role
        committedResources[roleId][offerId] = {}

        # Assumes a 1:1 Peer:CPU ratio & USE ALL MEM (for now)
        committedResources[roleId][offerId]['cpus'] = requestedCPU
        committedResources[roleId][offerId]['peers'] = requestedCPU
        unassignedPeers -= requestedCPU
        availableCPU[offerId] -= requestedCPU

        committedResources[roleId][offerId]['mem'] = requestedMEM
        availableMEM[offerId] -= requestedMEM

        committedResources[roleId][offerId]['ports'] = availablePorts[offerId]

        logging.debug("UNASSIGNED PEERS = %d" % unassignedPeers)
        if unassignedPeers <= 0:
          # All peers for this role have been assigned
          break

      if unassignedPeers > 0:
        # Could not commit all peers for this role with current set of offers
        logging.warning("Failed to satisfy role %s. Left with %d unassigned Peers" % (roleId, unassignedPeers))
        return None


    return committedResources

  # See if the next job in the pending queue can be launched using the current offers.
  # Upon failure, return None. Otherwise, return the Job object with fresh k3 tasks attached to it
  def prepareNextJob(self):
    logging.info("[DISPATCHER] Attempting to prepare the next pending job. Currently have %d offers" % len (self.offers))
    if len(self.pending) == 0:
      logging.info("[DISPATCHER] No pending jobs to prepare")
      return None
  
    index = 0
    nextJob = self.pending[0]
    reservation = self.allocateResources(nextJob)
    
    #  If no resources were allocated and jobs are waiting, try to launch each in succession
    while nextJob and reservation == None:
      index += 1
      if index >= len(self.pending):
        logging.info ("[DISPATCHER] No jobs in the queue can run with current offers")
        return None
      nextJob = self.pending[index]
      reservation = self.allocateResources(nextJob)
      
    #  Iterate through the reservations for each role / offer: create peers & tasks
    allPeers = []
    for roleId, role in reservation.items():
      logging.debug("[DISPATCHER] Preparing role, %s" % roleId)
      vars = nextJob.roles[roleId].variables
      for offerId, offer in role.items():
        peers = []
        host = self.offers[offerId].hostname.encode('utf8','ignore')
        ip = socket.gethostbyname(host)
        if len(allPeers) == 0:
          nextJob.master = host
        for n in range(offer['peers']):
          nextPort = offer['ports'].getNext()

          # CHECK:  Switched to hostnames
          p = Peer(len(allPeers), vars, ip, nextPort)
          peers.append(p)
          allPeers.append(p)

        taskid = len(nextJob.tasks)
        t = Task(taskid, offerId, host, offer['mem'], peers, roleId)
        nextJob.tasks.append(t)

    logging.debug("PEER LIST:")
    for p in peers:
      logging.debug("    %-2s  %s:%s" % (p.index, p.ip, p.port))


    # ID Master for collection Stdout TODO: Should we auto-default to offer 0 for stdout?
    populateAutoVars(allPeers)
    nextJob.all_peers = allPeers
    del self.pending[index]  #.popleft()
    return nextJob


  def launchJob(self, nextJob, driver):
    #jobId = self.genJobId()
    logging.info("[DISPATCHER] Launching job %d" % nextJob.jobId)
    self.active[nextJob.jobId] = nextJob
    self.jobsCreated += 1
    nextJob.status = "RUNNING"
    db.updateJob(nextJob.jobId, status=nextJob.status)
    # Build Mesos TaskInfo Protobufs for each k3 task and launch them through the driver
    for taskNum, k3task in enumerate(nextJob.tasks):

      task = taskInfo(nextJob, taskNum, self.webaddr, self.offers[k3task.offerid].slave_id)

      oid = mesos_pb2.OfferID()
      oid.value = k3task.offerid
      driver.launchTasks(oid, [task])
      # Stop considering the offer, since we just used it.
      del self.offers[k3task.offerid]

    # # Decline all remaining offers
    logging.info("[DISPATCHER] DECLINING remaining offers (Task Launched)")
    for oid, offer in self.offers.items():
      driver.declineOffer(offer.id)
    for oid in self.offers.keys():
      del self.offers[oid]

  def cancelJob(self, jobId, driver):
    logging.warning("[DISPATCHER] Asked to cancel job %d. Killing all tasks" % jobId)
    job = self.active[jobId]
    job.status = "FAILED"
    db.updateJob(jobId, status=job.status, done=True)
    for t in job.tasks:
      t.status = "TASK_FAILED"
      fullid = self.fullId(jobId, t.taskid)
      tid = mesos_pb2.TaskID()
      tid.value = fullid
      logging.warning("[DISPATCHER] Killing task: " + fullid)
      driver.killTask(tid)
    del self.active[jobId]
    self.finished[jobId] = job
#    self.tryTerminate()

  def getSandboxURL(self, jobId):

    # For now, return Mesos URL to Framework:
#    return 'http://' + self.mesosmaster
    master = resolve(self.mesosmaster).strip()
    url = os.path.join('http://', master)
    return url


  def taskFinished(self, fullid):
    jobId = self.jobId(fullid)
    job = self.active[jobId]
    runningTasks = False
    for t in job.tasks:
      if t.taskid == self.taskId(fullid):
        t.status = "TASK_FINISHED"
      if t.status != "TASK_FINISHED":
        runningTasks = True

    # If all tasks are finished, clean up the job
    if not runningTasks:
      logging.info("[DISPATCHER] All tasks finished for job %d" % jobId)
      # TODO Move the job to a finished job list
      job = self.active[jobId]
      job.status = "FINISHED"
      db.updateJob(jobId, status=job.status, done=True)
      del self.active[jobId]
      self.finished[jobId] = job
      self.tryTerminate()

  # --- Mesos Callbacks ---
  def registered(self, driver, frameworkId, masterInfo):
    logging.info ("[DISPATCHER] Registered with framework ID %s" % frameworkId.value)
    self.connected = True
    self.frameworkId = frameworkId

  def statusUpdate(self, driver, update):
    s = update.task_id.value.encode('utf8','ignore')
    jobId = self.jobId(update.task_id.value)
    if jobId not in self.active:
      logging.warning("[DISPATCHER] Received a status update for an old job: %d" % jobId)
      return

    k3task = self.getTask(s)
    host = k3task.host
    state = mesos_pb2.TaskState.Name(update.state)
    logging.info ("[TASK UPDATE] TaskID %s on host %s. Status: %s   [%s]"% (update.task_id.value, host, state, update.data))

    # TODO: Check STDOUT flag, capture stream in update.data, & append to appropriate file
    #   will need to update executor and ensure final output archive doesn't overwrite
    # if update.state == mesos_pb2.TASK_RUNNING and self.active[jobId].stdout:
    #     stdout =
    #     if not os.path.exists(????):
    #       os.mkdir(???)
    #     with open(os.path.join(self.job.path, 'output'), 'a') as out:
    #       out.write(update.data)

    if update.state == mesos_pb2.TASK_KILLED or \
       update.state == mesos_pb2.TASK_FAILED or \
       update.state == mesos_pb2.TASK_LOST:
         jobId = self.jobId(update.task_id.value)
         self.cancelJob(jobId, driver)

    if update.state == mesos_pb2.TASK_FINISHED:
      self.taskFinished(update.task_id.value)

  def frameworkMessage(self, driver, executorId, slaveId, message):
    logging.info("[FRMWK MSG] %s" % message[:-1])

  # Handle a resource offers from Mesos.
  # If there is a pending job, add all offers to self.offers
  # Then see if pending jobs can be launched with the offers accumulated so far
  def resourceOffers(self, driver, offers):
    # logging.info("[DISPATCHER] Got %d resource offers. %d jobs in the queue" % (len(offers), len(self.pending)))
    ts = time.time()

    # Heart Beat logging
    if ts > self.idle:
      logging.info("[DISPATCHER] HeartBeatting with Mesos. # Offers: %d", len(offers))
      self.idle = ts + heartbeat_delay

    # Crude Garbage Collection to police up jobs in bad state
    if ts > self.gc:
      for job in db.getJobs():
        if job['jobId'] in self.pending or job['jobId'] in self.active or JobStatus.done(job['status']):
          continue
        else:
          logging.info("[GARBAGE COLLECTION] Job `%(jobId)s` is listed as %(status)s, \
but is neither pending nor active. Killing it now." % job)
          db.updateJob(job['jobId'], status=JobStatus.KILLED, done=True)
      self.gc = ts + gc_delay


    if len(self.pending) == 0:
      self.offerRelease = ts + offer_wait
      for offer in offers:
        driver.declineOffer(offer.id)
        # logging.debug("DECLINING Offer from %s" % offer.hostname)
      return

    for offer in offers:
      self.offers[offer.id.value] = offer
    nextJob = self.prepareNextJob()

    if nextJob != None:
      self.launchJob(nextJob, driver)
    else:
      if len(self.pending) > 0:
        logging.info("[DISPATCHER] Not enough resources to launch next job")
      if ts > self.offerRelease + offer_wait:
        logging.info("[DISPATCHER] I've waited %s seconds and cannot lauch. Releasing all offers", offer_wait)
        for offer in offers:
          driver.declineOffer(offer.id)
          del self.offers[offer.id.value]
        self.offerRelease = ts + offer_wait
      else:
        logging.info("[DISPATCHER]  HOLDING %d Offers for %s Jobs and waiting for more offers", len(self.offers), len(self.pending))



  def offerRescinded(self, driver, offer):
    logging.warning("[DISPATCHER] Previous offer '%d' invalidated" % offer.id.value)
    if offer.id in self.offers:
      del self.offers[offer.id.value]
   

