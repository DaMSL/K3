# scheduler.dispatcher: Scheduler logic for matching resource offers to job requests.
import os
import sys
import time
import threading
from collections import deque

from core import *
from mesosutils import *
import db

from protobuf_to_dict import protobuf_to_dict
import json

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

# Constants
MASTER = 'zk://192.168.0.10:2181,192.168.0.11:2181,192.168.0.18:2181/mesos'

DEFAULT_MEM = 4 * 1024

IP_ADDRS = { "qp1":"192.168.0.10",
             "qp2":"192.168.0.11",
             "qp3":"192.168.0.15",
             "qp4":"192.168.0.16",
             "qp5":"192.168.0.17",
             "qp6":"192.168.0.18",
             "mddb":"192.168.0.20",
             "qp-hd1":"192.168.0.24",
             "qp-hd2":"192.168.0.25",
             "qp-hd3":"192.168.0.26",
             "qp-hd4":"192.168.0.27",
             "qp-hd5":"192.168.0.28",
             "qp-hd6":"192.168.0.29",
             "qp-hd7":"192.168.0.30",
             "qp-hd8":"192.168.0.31",
             "qp-hd9":"192.168.0.32",
             "qp-hd10":"192.168.0.33",
             "qp-hd11":"192.168.0.34",
             "qp-hd12":"192.168.0.35",
             "qp-hd13":"192.168.0.36",
             "qp-hd14":"192.168.0.37",
             "qp-hd15":"192.168.0.38",
             "qp-hd16":"192.168.0.39",
             "qp-hm1":"192.168.0.40",
             "qp-hm2":"192.168.0.41",
             "qp-hm3":"192.168.0.42",
             "qp-hm4":"192.168.0.43",
             "qp-hm5":"192.168.0.44",
             "qp-hm6":"192.168.0.45",
             "qp-hm7":"192.168.0.46",
             "qp-hm8":"192.168.0.47"}

task_state = { 6: "TASK_STAGING",  # Initial state. Framework status updates should not use.
	       0: "TASK_STARTING",
	       1: "TASK_RUNNING",
	       2: "TASK_FINISHED", # TERMINAL.
	       3: "TASK_FAILED",   # TERMINAL.
	       4: "TASK_KILLED",   # TERMINAL.
	       5: "TASK_LOST"}      # TERMINAL.


class Dispatcher(mesos.interface.Scheduler):
  def __init__(self, daemon=True):
    self.pending = deque()     # Pending jobs. First job is popped once there are enough resources available to launch it.
    self.active = {}           # Active jobs keyed on jobId.
    self.finished = {}         # Finished jobs keyed on jobId.
    self.offers = {}           # Offers from Mesos keyed on offerId. We assume they are valid until they are rescinded by Mesos.
    self.jobsCreated = 0       # Total number of jobs created for generating job ids.

    self.daemon = daemon       # Run as a daemon (or finish when there are no more pending/active jobs)
    self.terminate = False     # Flag to signal termination to the owner of the dispatcher
    self.frameworkId = None    # Will get updated when registering with Master
 
  def submit(self, job):
    print ("Received new Job for Application %s, Job ID= %d" % (job.appName, job.jobId))
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
      print("Terminating")



  def allocateResources(self, nextJob): #, driver):

    # Keep track of how many cpus have been used per role and per offer
    # and which roles have been assigned to an offer
    committedResources = {}

    # committedCPU = {}
    # assignedCPU_toRole = {}
    # rolesPerOffer = {}
    # for roleId in nextJob.roles:
    #   assignedCPU_toRole[roleId] = 0
    # for offerId in self.offers:
    #   committedCPU[offerId] = 0
    #   rolesPerOffer[offerId] = []

    availableCPU = {i: int(getResource(o.resources, "cpus")) for i, o in self.offers.items()}
    availableMEM = {i: getResource(o.resources, "mem") for i, o in self.offers.items()}
    availablePorts = {i: PortList(getResource(o.resources, "ports")) for i, o in self.offers.items()}

    for i, p in availablePorts.items():
      print i, p

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
        host = self.offers[offerId].hostname.encode('ascii','ignore')
        r = re.compile(hostmask)
        if not r.match(host):
          print("%s does not match hostmask. DECLINING offer" % host)
          continue
        print("%s MATCHES hostmask. Checking offer" % host)

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

        # committedCPU[offerId] += requestedCPU
        # rolesPerOffer[offerId].append((roleId, requestedCPU))
        # assignedCPU_toRole[roleId] += requestedCPU

        print "UNASSIGNED PEERS = %d" % unassignedPeers
        if unassignedPeers <= 0:
          # All peers for this role have been assigned
          break

      if unassignedPeers > 0:
        # Could not commit all peers for this role with current set of offers
        print("Failed to satisfy role %s. Left with %d unassigned Peers" % (roleId, unassignedPeers))
        return None

    # # Check if all roles were satisfied
    # for roleId in nextJob.roles:
    #   if assignedCPU_toRole[roleId] != nextJob.roles[roleId].peers:
    #     debug = (roleId, assignedCPU_toRole[roleId], nextJob.roles[roleId].peers)
    #     print("Failed to satisfy role %s. Used %d cpus out of %d peers" % debug)
    #     return None

    return committedResources #(assignedCPU_toRole, committedCPU, rolesPerOffer, committedResources)

  # See if the next job in the pending queue can be launched using the current offers.
  # Upon failure, return None. Otherwise, return the Job object with fresh k3 tasks attached to it
  def prepareNextJob(self):
    print("Attempting to prepare the next pending job. Currently have %d offers" % len (self.offers))
    if len(self.pending) == 0:
      print("No pending jobs to prepare")
      return None
  
    index = 0
    nextJob = self.pending[0]
    reservation = self.allocateResources(nextJob)
    
    while nextJob and reservation == None:
      index += 1
      if index >= len(self.pending):
        print ("No jobs in the queue can run with current offers")
        return None
      nextJob = self.pending[index]
      reservation = self.allocateResources(nextJob)
      

    # TODO: Determine if job CAN launch; if not try next job in QUEUE
    # result = self.allocateResources(nextJob) #, driver) #, self.offers)
    #   reservation = self.allocateResources(nextJob)
    #   if reservation == None:
    #     return None
    #  
    #   (assignedCPU_toRole, committedCPU, rolesPerOffer, reservation) = result
  
    print "RESOURCE TO ROLES"
    allPeers = []
    for roleId, role in reservation.items():
      print roleId
      peers = []
      vars = nextJob.roles[roleId].variables
      for offerId, offer in role.items():
        print offer
        host = self.offers[offerId].hostname.encode('ascii','ignore')
        if len(allPeers) == 0:
          nextJob.master = host
        for n in range(offer['peers']):
          nextPort = offer['ports'].getNext()
          print ("PEER PORT = %s" % str(nextPort))
          p = Peer(len(allPeers), vars, IP_ADDRS[host], nextPort)
          peers.append(p)
          allPeers.append(p)

        taskid = len(nextJob.tasks)
        t = Task(taskid, offerId, host, offer['mem'], peers, roleId)
        nextJob.tasks.append(t)

    # ID Master for collection Stdout TODO: Should we auto-default to offer 0 for stdout?
    populateAutoVars(allPeers)
    nextJob.all_peers = allPeers
    del self.pending[index]  #.popleft()
    return nextJob

    # # TODO port management
    # curPeerIndex = 0
    # nextJob.tasks = []
    # allPeers = []
    #
    # # Succesful. Accept any used offers. Build tasks, etc.
    # for offerId in self.offers:
    #   curPort = 40000
    #   if committedCPU[offerId] > 0:
    #
    #     host = self.offers[offerId].hostname.encode('ascii','ignore')
    #     debug = (host, str(rolesPerOffer[offerId]))
    #     print("Accepted Roles for offer on %s: %s" % debug)
    #     if len(allPeers) == 0:
    #       nextJob.master = host
    #
    #     peers = []
    #     for (roleId, n) in rolesPerOffer[offerId]:
    #       for i in range(n):
    #         vs = nextJob.roles[roleId].variables
    #         p = Peer(curPeerIndex, vs, IP_ADDRS[host], curPort)
    #         peers.append(p)
    #         allPeers.append(p)
    #         curPeerIndex = curPeerIndex + 1
    #         curPort = curPort + 1
    #
    #     # Use all MEM (for now)
    #     resources = self.offers[offerId].resources
    #     mem = getResource(resources, "mem")
    #
    #     taskid = len(nextJob.tasks)
    #     t = Task(taskid, offerId, host, mem, peers)
    #     nextJob.tasks.append(t)



  def launchJob(self, nextJob, driver):
    #jobId = self.genJobId()
    print("Launching job %d" % nextJob.jobId)
    self.active[nextJob.jobId] = nextJob
    self.jobsCreated += 1
    nextJob.status = "RUNNING"
    db.updateJob(nextJob.jobId, status=nextJob.status)
    # Build Mesos TaskInfo Protobufs for each k3 task and launch them through the driver
    for taskNum, k3task in enumerate(nextJob.tasks):

      task = taskInfo(nextJob, taskNum, self.offers[k3task.offerid].slave_id)

      oid = mesos_pb2.OfferID()
      oid.value = k3task.offerid
      driver.launchTasks(oid, [task])
      # Stop considering the offer, since we just used it.
      del self.offers[k3task.offerid]

    # # Decline all remaining offers
    for oid, offer in self.offers.items():
      print "DECLINING remaining offer (Task Launched)"
      driver.declineOffer(offer.id)
    for oid in self.offers.keys():
      del self.offers[oid]

  def cancelJob(self, jobId, driver):
    print("Asked to cancel job %d. Killing all tasks" % jobId)
    job = self.active[jobId]
    job.status = "FAILED"
    db.updateJob(jobId, status=job.status)
    for t in job.tasks:
      t.status = "TASK_FAILED"
      fullid = self.fullId(jobId, t.taskid)
      tid = mesos_pb2.TaskID()
      tid.value = fullid
      print("Killing task: " + fullid)
      driver.killTask(tid)
    del self.active[jobId]
    self.finished[jobId] = job
#    self.tryTerminate()

  def getSandboxURL(self, jobId):

    # For now, return Mesos URL to Framework:
    master = resolve(MASTER).strip()
    url = os.path.join('http://', master, "#/frameworks", self.frameworkId.value)
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
      print("All tasks finished for job %d" % jobId)
      # TODO Move the job to a finished job list
      job = self.active[jobId]
      job.status = "FINISHED"
      db.updateJob(jobId, status=job.status, done=True)
      del self.active[jobId]
      self.finished[jobId] = job
      self.tryTerminate()

  # --- Mesos Callbacks ---
  def registered(self, driver, frameworkId, masterInfo):
    print("Registered with framework ID %s" % frameworkId.value)
    self.frameworkId = frameworkId

  def statusUpdate(self, driver, update):
    s = update.task_id.value.encode('ascii','ignore')
    jobId = self.jobId(update.task_id.value)
    if jobId not in self.active:
      print("Received a status update for an old job: %d" % jobId)
      return

    k3task = self.getTask(s)
    host = k3task.host
    print("[TASK UPDATE] TaskID %s on host %s. Status: %s" % (update.task_id.value, host, task_state[update.state]))

    if task_state[update.state] == "TASK_KILLED" or \
       task_state[update.state] == "TASK_FAILED" or \
       task_state[update.state] == "TASK_LOST":
         jobId = self.jobId(update.task_id.value)
         self.cancelJob(jobId, driver)

    if task_state[update.state] == "TASK_FINISHED":
      self.taskFinished(update.task_id.value)
   
  def frameworkMessage(self, driver, executorId, slaveId, message):
    print("[FRMWK MSG] %s" % message)
  
  # Handle a resource offers from Mesos.
  # If there is a pending job, add all offers to self.offers
  # Then see if pending jobs can be launched with the offers accumulated so far
  def resourceOffers(self, driver, offers):
    print("[RESOURCE OFFER] Got %d resource offers. %d jobs in the queue" % (len(offers), len(self.pending)))

    if len(self.pending) == 0:
      for offer in offers:
        driver.declineOffer(offer.id)
    else:
      for offer in offers:
        self.offers[offer.id.value] = offer
      while len(self.pending) > 0:
        nextJob = self.prepareNextJob()
        if nextJob != None:
          self.launchJob(nextJob, driver)
        else:
          print("Not enough resources to launch next job. Waiting for more offers")
          return


  def offerRescinded(self, driver, offer):
    print("[OFFER RESCINDED] Previous offer '%d' invalidated" % offer.id.value)
    if offer.id in self.offers:
      del self.offers[offer.id]
   
