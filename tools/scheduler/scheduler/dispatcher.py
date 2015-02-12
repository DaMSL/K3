# scheduler.dispatcher: Scheduler logic for matching resource offers to job requests.
import os
import sys
import time
import threading
from collections import deque

from core import *
from mesosutils import *

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

TOTAL_TASKS = 2

TASK_CPUS = 1
TASK_MEM = 32

MASTER = 'zk://192.168.0.10:2181,192.168.0.11:2181,192.168.0.18:2181/mesos'

IP_ADDRS = { "qp1":"192.168.0.10",
             "qp2":"192.168.0.11",
             "qp3":"192.168.0.15",
             "qp4":"192.168.0.16",
             "qp5":"192.168.0.17",
             "qp6":"192.168.0.18",
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


task_state = {
	6: "TASK_STAGING",  # Initial state. Framework status updates should not use.
	0: "TASK_STARTING",
	1: "TASK_RUNNING",
	2: "TASK_FINISHED", # TERMINAL.
	3: "TASK_FAILED",   # TERMINAL.
	4: "TASK_KILLED",   # TERMINAL.
	5: "TASK_LOST"      # TERMINAL.
}

# Helper functions. TODO move elsewhere
def getResource(resources, tag, convF):
  for resource in resources:
    if resource.name == tag:
      return convF(resource.scalar.value)

class Dispatcher(mesos.interface.Scheduler):
  def __init__(self):
    self.active = {}
    self.pending = deque()
    self.offers = {}
    self.jobsCreated = 0

  def genJobId(self):
    x = self.jobsCreated
    self.jobsCreated = x + 1
    return x

  def submit(self, job):
    self.pending.append(job)

  # For the first pending job, attempt to construct tasks given the current set of offers
  # If the job can't be run: return None
  # Otherwise: accept any used offer and remove it from the dict.
  # Attach tasks to the job, launch them on Mesos, then return the Job
  # TODO if first job can't be launched, should we try the next one instead?
  def prepareNextJob(self):
    if len(self.pending) == 0:
      return None
   
    nextJob = self.pending[0]

    # Keep track of how many cpus have been used per role and per offer
    # and which roles have been assigned to an offer
    cpusUsedPerOffer = {}
    cpusUsedPerRole = {}
    rolesPerOffer = {}
    for roleId in nextJob.roles:
      cpusUsedPerRole[roleId] = 0
    for offerId in self.offers:
      cpusUsedPerOffer[offerId] = 0
      rolesPerOffer[offerId] = []

    # Try to satisy each role, sequentially
    # TODO consider constraints, such as hostmask
    for roleId in nextJob.roles:
      for offerId in self.offers:
        
        # TODO remove hd restriction
        host = self.offers[offerId].hostname.encode('ascii','ignore')
        if "hd" not in host:
          print("hd not in %s" % host)
          continue
        resources = self.offers[offerId].resources
        offeredCpus = int(getResource(resources, "cpus", float))
        offeredMem = getResource(resources, "mem", float)
        
        if cpusUsedPerOffer[offerId] >= offeredCpus:
          # All cpus for this offer have already been used
          continue

        cpusRemainingForOffer = offeredCpus - cpusUsedPerOffer[offerId]
        cpusToUse = min([cpusRemainingForOffer, nextJob.roles[roleId].peers])
        
        cpusUsedPerOffer[offerId] += cpusToUse
        rolesPerOffer[offerId].append((roleId, cpusToUse))
        cpusUsedPerRole[roleId] += cpusToUse
      
        if cpusUsedPerRole[roleId] == nextJob.roles[roleId].peers:
          # All peers for this role have been assigned
          break      
    
    # Check if all roles were satisfied
    for roleId in nextJob.roles:
      if cpusUsedPerRole[roleId] != nextJob.roles[roleId].peers:
        debug = (roleId, cpusUsedPerRole[roleId], nextJob.roles[roleId].peers)
        print("Failed to satisfy role %s. Used %d cpus out of %d peers" % debug)
        return None

    # TODO port management
    curPort = 40000
    curPeerIndex = 0
    nextJob.tasks = []
    allPeers = []
    # Succesful. Accept any used offers. Build tasks, etc.
    for offerId in self.offers:
      if cpusUsedPerOffer[offerId] > 0:
        host = self.offers[offerId].hostname.encode('ascii','ignore')

        debug = (host, str(rolesPerOffer[offerId]))
        
        print("Accepted Roles for offer on %s: %s" % debug)

        peers = []
        for (roleId, n) in rolesPerOffer[offerId]:
          for i in range(n):
            vs = nextJob.roles[roleId].variables
            p = Peer(curPeerIndex, vs, IP_ADDRS[host], curPort)
            peers.append(p)
            allPeers.append(p)
            curPeerIndex = curPeerIndex + 1
            curPort = curPort + 1
        
        taskid = len(nextJob.tasks)
        mem = getResource(self.offers[offerId].resources, "mem", float) 
        t = Task(taskid, offerId, host, mem, peers)
        print("offer id %s " %  offerId)
        nextJob.tasks.append(t)
        print("new offer id %s " %  nextJob.tasks[-1].offerid)
   
    # Fill in any "auto" variables
    # to be the address first peer in the allPeers
    for p in allPeers:
      for v in p.variables:
        if p.variables[v] == "auto":
          p.variables[v] = [allPeers[0].ip, allPeers[0].port]  

    nextJob.all_peers = allPeers
    self.pending.popleft()
    return nextJob
     
  # --- Mesos Callbacks ---
  def registered(self, driver, frameworkId, masterInfo):
    print("Registered with framework ID %s" % frameworkId.value)

  def statusUpdate(self, driver, update):
    print("[TASK UPDATE] TaskID %s. Data: %s" % (update.task_id.value, repr(str(update.data))))
    
  def frameworkMessage(self, driver, executorId, slaveId, message):
    print("[FRMWK MSG] %s" % message)
   
  # Handle a resource offers from Mesos.
  # If there is a pending job, add all offers to self.offers
  # Then see if the pending job can be launched with the offers accumulated so far
  def resourceOffers(self, driver, offers):
    print("[RESOURCE OFFER] Got %d resource offers" % len(offers))
    if len(self.pending) == 0:
      print("No Pending jobs. Declining all offers")
      for offer in offers:
        driver.declineOffer(offer.id)
      return

    print("Adding %d offers to offer dict" % len(offers))
    for offer in offers:
      self.offers[offer.id.value] = offer

    nextJob = self.prepareNextJob()
    if nextJob != None:
      jobId = self.genJobId()
      self.active[jobId] = nextJob
      nextJob.status = "ACTIVE"
      for k3task in nextJob.tasks:
        print("offer id now: %s" % k3task.offerid)
        task = taskInfo(k3task, jobId, self.offers[k3task.offerid].slave_id, nextJob.binary_url, nextJob.all_peers, nextJob.inputs)
         
        oid = mesos_pb2.OfferID()
        oid.value = k3task.offerid 
        driver.launchTasks(oid, [task])

        
    else:
      print("Not enough resources to launch next job. Waiting for more offers")

  def offerRescinded(self, driver, offer):
    print("[OFFER RESCINDED] Previous offer '%d' invalidated" % offer.id.value)
    if offer.id in self.offers:
      del self.offers[offer.id]
    
if __name__ == "__main__":
  framework = mesos_pb2.FrameworkInfo()
  framework.user = "" # Have Mesos fill in the current user.
  framework.name = "K3 Dispatcher"
  d = Dispatcher()
  driver = mesos.native.MesosSchedulerDriver(d, framework, MASTER)
  status = 0
 
  t = threading.Thread(target = driver.run)
  variables = {"role": "rows", "master": "auto"}
  r = Role(128, variables)
  roles = {"role1": r}

  inputs = [{"var": "dataFiles", "path": "/local/data/tpch/tpch10g/lineitem", "policy": "global"}]
  j = Job(roles, "http://192.168.0.11:8002/tpchq1")
  j.inputs = inputs

  d.submit(j)
  try:
    t.start() 
    # Sleep until interrup
    while True:
      time.sleep(1)
  except KeyboardInterrupt:
    print("INTERRUPT")
    driver.stop()
    t.join()


  print(len(d.pending))

