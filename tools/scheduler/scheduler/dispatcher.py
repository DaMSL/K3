# scheduler.dispatcher: Scheduler logic for matching resource offers to job requests.
import os
import sys
import time

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


class Dispatcher(mesos.interface.Scheduler):
  def __init__(self):
    self.active = {}
    self.pending = deque()
    self.offers = {}

  def submit(self, job):
    self.pending.append(job)

  # For the first pending job, attempt to construct tasks given the current set of offers
  # If the job cannot be run, return None
  # Otherwise, accept any used offer, reject any unused offer,
  # Attach tasks to the job, launch them on Mesos, then return the Job
  # TODO if first job can't be launched, should we try the next one instead?
  def prepareNextJob(self):
    return None

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
      # TODO decline all offers
      return

    for offer in offers:
      self.offers[offer.id.value] = offer

    if prepareNextJob() != None:
      # TODO run the job
      pass

  def offerRescinded(self, driver, offer):
    print("[OFFER RESCINDED] Previous offer '%d' invalidated" % offer.id.value)
    if offer.id.value in self.offers:
      del self.offers[offer.id.value]
    
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
  driver = mesos.native.MesosSchedulerDriver(Dispatcher(), framework, MASTER)
	
  status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1

  variables = {"role": "rows", "master": "auto"}
  r = Role(10, variables)
  roles = {"role1": r}

  j = Job(roles, "tpchq1")

  d = Dispatcher()
  d.submit(j)

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
