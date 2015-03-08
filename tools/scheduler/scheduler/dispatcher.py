# scheduler.dispatcher: Scheduler logic for matching resource offers to job requests.
import os
import sys
import time
import threading
import re
from collections import deque

from core import *
from mesosutils import *
from opts import *

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

# Constants
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

  def submit(self, job):
    self.pending.append(job)

  # Use the next available jobId and then generate a fresh one.
  def genJobId(self):
    x = self.jobsCreated
    self.jobsCreated = x + 1
    return x

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

  # See if the next job in the pending queue can be launched using the current offers.
  # Upon failure, return None. Otherwise, return the Job object with fresh k3 tasks attached to it
  def prepareNextJob(self):
    print("Attempting to prepare the next pending job")
    if len(self.pending) == 0:
      print("No pending jobs to prepare")
      return None

    nextJob = self.pending[0]
    result = assignRolesToOffers(nextJob, self.offers)
    if result == None:
      return None

    (cpusUsedPerRole, cpusUsedPerOffer, rolesPerOffer) = result

    # TODO port management
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
          curPort = 40000
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
        nextJob.tasks.append(t)

    populateAutoVars(allPeers)
    nextJob.all_peers = allPeers
    self.pending.popleft()
    return nextJob

  def launchJob(self, nextJob, driver):
    jobId = self.genJobId()
    print("Launching job %d" % jobId)
    self.active[jobId] = nextJob
    nextJob.status = "RUNNING"
    # Build Mesos TaskInfo Protobufs for each k3 task and launch them through the driver
    for k3task in nextJob.tasks:
      task = taskInfo(k3task, jobId, self.offers[k3task.offerid].slave_id, nextJob.binary_url, nextJob.all_peers, nextJob.inputs)

      oid = mesos_pb2.OfferID()
      oid.value = k3task.offerid
      driver.launchTasks(oid, [task])
      # Stop considering the offer, since we just used it.
      del self.offers[k3task.offerid]

  def cancelJob(self, jobId, driver):
    print("Asked to cancel job %d. Killing all tasks" % jobId)
    job = self.active[jobId]
    job.status = "FAILED"
    for t in job.tasks:
      t.status = "TASK_FAILED"
      fullid = self.fullId(jobId, t.taskid)
      tid = mesos_pb2.TaskID()
      tid.value = fullid
      driver.killTask(tid)
    del self.active[jobId]
    self.finished[jobId] = job

    self.tryTerminate()



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
      del self.active[jobId]
      self.finished[jobId] = job
      self.tryTerminate()

  # --- Mesos Callbacks ---
  def registered(self, driver, frameworkId, masterInfo):
    print("Registered with framework ID %s" % frameworkId.value)

  def statusUpdate(self, driver, update):
    s = update.task_id.value.encode('ascii','ignore')
    jobId = self.jobId(update.task_id.value)
    if jobId not in self.active:
      print("Receieved a status update for an old job: %d" % jobId)
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
    print("[FRMWK MSG] %s" % message[:-1])

  # Handle a resource offers from Mesos.
  # If there is a pending job, add all offers to self.offers
  # Then see if pending jobs can be launched with the offers accumulated so far
  def resourceOffers(self, driver, offers):
    print("[RESOURCE OFFER] Got %d resource offers" % len(offers))

    print("Adding %d offers to offer dict" % len(offers))
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

if __name__ == "__main__":
  framework = mesos_pb2.FrameworkInfo()
  framework.user = "" # Have Mesos fill in the current user.
  framework.name = "K3 Dispatcher"

  d = parseArgs()
  if d == None:
    print("Failed to create dispatcher. Aborting")
    sys.exit(1)

  driver = mesos.native.MesosSchedulerDriver(d, framework, MASTER)
  t = threading.Thread(target = driver.run)

  try:
    t.start()
    # Sleep until interrupt
    terminate = False
    while not terminate:
      time.sleep(1)
      terminate = d.terminate
    driver.stop()
    t.join()
  except KeyboardInterrupt:
    print("INTERRUPT")
    driver.stop()
    t.join()
