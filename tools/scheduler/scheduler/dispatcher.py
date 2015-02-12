# scheduler.dispatcher: Scheduler logic for matching resource offers to job requests.
import os
import sys
import time
import threading
from collections import deque

from core import *

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

TOTAL_TASKS = 2

TASK_CPUS = 1
TASK_MEM = 32

MASTER = 'zk://192.168.0.10:2181,192.168.0.11:2181,192.168.0.18:2181/mesos'
FILESERVER = 'http://192.168.0.10:8002'
K3_DOCKER_NAME = "k3-mesos2"

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
        resources = self.offers[offerId].resources
        offeredCpus = getResource(resources, "cpus", float)
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

    curPeerIndex = 0
    nextJob.tasks = []
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
            inputs = nextJob.roles[roleId].inputs
            p = Peer(curPeerIndex, vs, inputs)
            peers.append(p)
            curPeerIndex = curPeerIndex + 1

        t = Task(peers)
        nextJob.tasks.append(t)
        
    return True
     
  # --- Mesos Callbacks ---
  def registered(self, driver, frameworkId, masterInfo):
    print("Registered with framework ID %s" % frameworkId.value)

  def statusUpdate(self, driver, update):
    print("[TASK UPDATE] Task state: %s.  %s" %s (task_state[update.task_id.value], repr(str(update.data))))
    
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

    if self.prepareNextJob() != None:
      # TODO run the job
      print("Next job ready to launch")
    else:
      print("Not enough resources to launch next job. Waiting for more offers")

  def offerRescinded(self, driver, offer):
    print("[OFFER RESCINDED] Previous offer '%d' invalidated" % offer.id.value)
    if offer.id in self.offers:
      del self.offers[offer.id]
    
  def addExecutor (programBinary, hostParams, mounts):
    # Create the Executor
    executor = mesos_pb2.ExecutorInfo() 
    executor.executor_id.value = "K3 Executor"
    executor.name = programBinary
    executor.data = hostParams
    #executor.source = "NOT USED"    

    # Create the Command
    command = mesos_pb2.CommandInfo()
    command.value = '$MESOS_SANDBOX/k3executor'
    exec_binary = command.uris.add()
    exec_binary.value = FILESERVER + '/' + programBinary
    exec_binary.URI.executable = True
    exec_binary.URI.extract = False

    k3_binary = command.uris.add()
    k3_binary.value = FILESERVER + '/' + programBinary
    k3_binary.URI.executable = True
    k3_binary.URI.extract = False
    
    executor.command.MergeFrom(command)
    executor.container.MergeFrom(container)

    # Create the docker object
    docker = mesos_pb2.ContainerInfo.DockerInfo()
    docker.image = K3_DOCKER_NAME
    docker.network = docker.HOST
      
    # Create the Container
    container = mesos_pb2.ContainerInfo()
    container.type = container.DOCKER
    container.docker.MergeFrom(docker)
    #container.volumes = []    # FOR: Mounting Volumes
          
    # Map the volume to the Docker Container
  #  for m in mounts:    # TODO: UPDATE mounts
  #    volume = container.volumes.add()
  #    volume.container_path = m.______'/mnt'
  #    volume.host_path = m.______'/local/mesos'
      # TODO RO vs RW
      #volume.mode = volume.RO
      #volume.mode = volume.RW
    volume = container.volumes.add()
    volume.container_path = '/local/mesos'
    volume.host_path = '/mnt/out'
  
  
if __name__ == "__main__":
  framework = mesos_pb2.FrameworkInfo()
  framework.user = "" # Have Mesos fill in the current user.
  framework.name = "K3 Dispatcher"
  d = Dispatcher()
  driver = mesos.native.MesosSchedulerDriver(d, framework, MASTER)
  status = 0
 
  t = threading.Thread(target = driver.run)
  variables = {"role": "rows", "master": "auto"}
  r = Role(10, variables)
  roles = {"role1": r}

  j = Job(roles, "tpchq1")

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

#			if self.tasksLaunched < TOTAL_TASKS:
#				# Generate a new Task ID
#				tid = self.tasksLaunched
#				self.tasksLaunched += 1
#				
#				print "Creating the Task object"
#				# Create the Task to Launch the Docker Container
#				task = mesos_pb2.TaskInfo()
#				task.task_id.value = str(tid)
#				task.slave_id.value = offer.slave_id.value
##        task.name = programBinary + '@' + hostname
#				task.executor.MergeFrom(executor)
#
#				cpus = task.resources.add()
#				cpus.name = "cpus"
#				cpus.type = mesos_pb2.Value.SCALAR
#				cpus.scalar.value = TASK_CPUS
#
#				mem = task.resources.add()
#				mem.name = "mem"
#				mem.type = mesos_pb2.Value.SCALAR
#				mem.scalar.value = TASK_MEM
#				
#				# Set the command to run
##				task.command.value = '/bin/sleep 15'
#
#				# SET Task's Container to the task
##				task.container.MergeFrom(container)
#				
#				print "Task Created."
#				
#				tasks.append(task)
#				
#				# This call actually requests to launch the task via the Mesos driver
#				#  It executed the task.executor which was passed to the schedule during construction
#				self.taskData[task.task_id.value] = (
#					offer.slave_id, task.executor.executor_id)
#
#			print "Launching your task"
#			driver.launchTasks(offer.id, tasks)
