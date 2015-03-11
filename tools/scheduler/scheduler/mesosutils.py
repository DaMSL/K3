# mesosutils: Constructors for Mesos Protocol Buffers
import yaml
import re
import subprocess

from protobuf_to_dict import protobuf_to_dict
import json


import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

# TODO how should we determine the executor url
EXECUTOR_URL = "http://qp1:8002/k3executor"
K3_DOCKER_NAME = "damsl/k3-deployment:stable"

def getResource(resources, tag, convF):
  for resource in resources:
    if resource.name == tag:
      return convF(resource.scalar.value)

# Fill in any "auto" variables
# to be the address first peer in allPeers
def populateAutoVars(allPeers):
  for p in allPeers:
    for v in p.variables:
      if p.variables[v] == "auto":
        p.variables[v] = [allPeers[0].ip, allPeers[0].port]

def assignRolesToOffers(nextJob, offers):

  # Keep track of how many cpus have been used per role and per offer
  # and which roles have been assigned to an offer
  cpusUsedPerOffer = {}
  cpusUsedPerRole = {}
  rolesPerOffer = {}
  for roleId in nextJob.roles:
    cpusUsedPerRole[roleId] = 0
  for offerId in offers:
    cpusUsedPerOffer[offerId] = 0
    rolesPerOffer[offerId] = []

  # Try to satisy each role, sequentially
  # TODO consider constraints, such as hostmask
  for roleId in nextJob.roles:
    totalPeersPerRole = nextJob.roles[roleId].peers
    for offerId in offers:
     
      hostmask = nextJob.roles[roleId].hostmask
      host = offers[offerId].hostname.encode('ascii','ignore')
      r = re.compile(hostmask)
      if not r.match(host):
        print("%s does not match hostmask" % host)
        continue
     
      resources = offers[offerId].resources
      offeredCpus = int(getResource(resources, "cpus", float))
      offeredMem = getResource(resources, "mem", float)
     
      if cpusUsedPerOffer[offerId] >= offeredCpus:
        # All cpus for this offer have already been used
        continue


      cpusRemainingForOffer = offeredCpus - cpusUsedPerOffer[offerId]

      cpusToUse = 0
      if 'peers_per_host' in nextJob.roles[roleId].params:
        cpusToUse = nextJob.roles[roleId].params['peers_per_host']
        if cpusToUse > cpusRemainingForOffer:
          # Cannot satisfy specific peers-to-host requirement
          continue
      else:
        cpusToUse = min([cpusRemainingForOffer, totalPeersPerRole])


      totalPeersPerRole -= cpusToUse
     
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

  return (cpusUsedPerRole, cpusUsedPerOffer, rolesPerOffer)

def executorInfo(k3task, jobid, binary_url, volumes=[]):

  # Create the Executor
  executor = mesos_pb2.ExecutorInfo()
  executor.executor_id.value = k3task.getId(jobid)
  executor.name = k3task.getId(jobid)
  executor.data = ""

  # Create the Command
  command = mesos_pb2.CommandInfo()
  command.value = '$MESOS_SANDBOX/k3executor'
  exec_binary = command.uris.add()
  exec_binary.value = EXECUTOR_URL
  exec_binary.executable = True
  exec_binary.extract = False

  k3_binary = command.uris.add()
  k3_binary.value = binary_url
  k3_binary.executable = True
  k3_binary.extract = False
 
  executor.command.MergeFrom(command)

  # Create the docker object
  docker = mesos_pb2.ContainerInfo.DockerInfo()
  docker.image = K3_DOCKER_NAME
  docker.network = docker.HOST
   
  # Create the Container
  container = mesos_pb2.ContainerInfo()
  container.type = container.DOCKER
  container.docker.MergeFrom(docker)

  for v in volumes:
    volume = container.volumes.add()
    volume.container_path = v['container']
    volume.host_path = v['host']
#    volume.mode = volume.RW if v['RW'] else volume.RW
    volume.mode = volume.RW

  executor.container.MergeFrom(container)
       
  return executor

# def taskInfo(k3task, jobId, slaveId, binary_url, all_peers, inputs, volumes):
def taskInfo(k3job, k3task, slaveId):
  task_data = {}
  # TODO fix this hack
  task_data["binary"] = k3job.appId
  task_data["totalPeers"] = len(k3job.all_peers)
  task_data["peerStart"] = k3task.peers[0].index
  task_data["peerEnd"] = k3task.peers[-1].index
  task_data["me"] = [ [p.ip, p.port] for p in k3task.peers]
  task_data["peers"] = [ {"addr": [p.ip, p.port] } for p in k3job.all_peers]
  task_data["globals"] = [p.variables for p in k3task.peers]
  task_data["master"] = [ k3job.all_peers[0].ip, k3job.all_peers[0].port ]
  task_data["data"] = [ k3job.inputs for p in range(len(k3task.peers)) ]

  executor = executorInfo(k3task, k3job.jobId, k3job.binary_url, k3job.volumes)

  task = mesos_pb2.TaskInfo()
  task.task_id.value = k3task.getId(k3job.jobId)
  task.slave_id.value = slaveId.value
  task.name = str(k3job.jobId) + "@" + k3task.host
  task.executor.MergeFrom(executor)
  task.data = yaml.dump(task_data)
 
  cpus = task.resources.add()
  cpus.name = "cpus"
  cpus.type = mesos_pb2.Value.SCALAR
  cpus.scalar.value = len(k3task.peers)

  mem = task.resources.add()
  mem.name = "mem"
  mem.type = mesos_pb2.Value.SCALAR
  mem.scalar.value = k3task.mem

  return task   



# Helper that uses 'mesos-resolve' to resolve a master IP:port from
# one of:
#     zk://host1:port1,host2:port2,.../path
#     zk://username:password@host1:port1,host2:port2,.../path
#     file://path/to/file (where file contains one of the above)
def resolve(master):

    process = subprocess.Popen(
        ['mesos-resolve', master],
        stdin=None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=False)

    status = process.wait()
    if status != 0:
        raise Exception('Failed to execute \'mesos-resolve %s\':\n%s'
                        % (master, process.stderr.read()))

    result = process.stdout.read()
    process.stdout.close()
    process.stderr.close()
    return result
