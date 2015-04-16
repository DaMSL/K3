# mesosutils: Constructors for Mesos Protocol Buffers
import yaml
import re
import subprocess
import httplib

#from protobuf_to_dict import protobuf_to_dict
import json


import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native


# EXECUTOR_URL = "http://qp1:8002/k3executor"
# COMPEXEC_URL = "http://qp1:8002/CompileExecutor.py"

# EXECUTOR_URL =
# COMPEXEC_URL =
K3_DOCKER_NAME = "damsl/k3-run:exec"



def getResource(resources, tag):
  for resource in resources:
    if resource.name == tag:
      if resource.type == mesos_pb2.Value.SCALAR:
        return float(resource.scalar.value)
      elif resource.type == mesos_pb2.Value.RANGES:
        range_list = []
        for r in resource.ranges.range:
          range_list.append((r.begin, r.end))
        return (range_list)
      elif resource.type == mesos_pb2.Value.SET:
        resource.set.value
      else:
        resource.text.value
  return 0


# Fill in any "auto" variables
# to be the address first peer in allPeers
def populateAutoVars(allPeers):
  for p in allPeers:
    for v in p.variables:
      if p.variables[v] == "auto":
        p.variables[v] = [allPeers[0].ip, allPeers[0].port]



# TODO: Add role to this to enable varying roles for volumes & envars
def executorInfo(k3job, tnum, webaddr): #, jobid, binary_url, volumes=[], environs=[]):


  k3task = k3job.tasks[tnum]
  role = k3job.roles[k3task.roleId]

  # Create the Executor
  executor = mesos_pb2.ExecutorInfo()
  executor.executor_id.value = str(k3job.jobId)
  executor.name = str(k3job.jobId)
  executor.data = ""

  # Create the Command
  command = mesos_pb2.CommandInfo()
  command.value = 'k3/k3executor'
#  command.value = '$MESOS_SANDBOX/k3executor'
#  exec_binary = command.uris.add()
#  exec_binary.value = "%s/fs/k3executor" % webaddr
#  exec_binary.executable = True
#  exec_binary.extract = False

  k3_binary = command.uris.add()
  k3_binary.value = k3job.binary_url
  k3_binary.executable = True
  k3_binary.extract = False

  if len(role.envars) > 0:
    environment = mesos_pb2.Environment()
    for e in role.envars:
      var = environment.variables.add()
      var.name = e['name']
      var.value = e['value']
    command.environment.MergeFrom(environment)

  executor.command.MergeFrom(command)

  # Create the docker object
  docker = mesos_pb2.ContainerInfo.DockerInfo()
  docker.image = K3_DOCKER_NAME
  docker.network = docker.HOST
  docker.privileged = k3job.privileged

  if docker.privileged:
    print ("NOTE: running privileged mode")

   
  # Create the Container
  container = mesos_pb2.ContainerInfo()
  container.type = container.DOCKER

  # For version 0.22 -- request to force docker pull
  # container.force_pull_image = True
  container.docker.MergeFrom(docker)

  for v in role.volumes:
    volume = container.volumes.add()
    volume.container_path = v['container']
    volume.host_path = v['host']
#    volume.mode = volume.RW if v['RW'] else volume.RO
    volume.mode = volume.RW

  executor.container.MergeFrom(container)
       
  return executor

def taskInfo(k3job, tnum, webaddr, slaveId):

  k3task = k3job.tasks[tnum]
  role   = k3job.roles[k3task.roleId]

  peerStart = k3task.peers[0].index,
  peerEnd   = k3task.peers[-1].index,

  task_data = {"binary": str(k3job.appName),
      "totalPeers": len(k3job.all_peers),
      "peerStart": k3task.peers[0].index,
      "peerEnd": k3task.peers[-1].index,
      "me": [ [p.ip, p.port] for p in k3task.peers],
      "peers": [ {"addr": [p.ip, p.port] } for p in k3job.all_peers],
      "globals": [p.variables for p in k3task.peers],
      "master": [k3job.all_peers[0].ip, k3job.all_peers[0].port ],
      "archive_endpoint" : "%s/jobs/" % webaddr,
      "data": [role.inputs for p in range(len(k3task.peers))]}
  # if k3job.logging:
  task_data['logging'] = True

  print 'ARCHIVE ADDR: %s' % webaddr
  executor = executorInfo(k3job, tnum, webaddr)

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

  ports = task.resources.add()
  ports.name = "ports"
  ports.type = mesos_pb2.Value.RANGES

  # To handle non-contiguous ranges
  peerPorts = sorted([p.port for p in k3task.peers])
  portlist = ports.ranges.range.add()
  portlist.begin = min(peerPorts)
  portlist.end = max(peerPorts)


  print "PEERS IN MESOSUTIL:"
  for p in k3task.peers:
    print p.ip, p.port
  # nextPort = peerPorts[0]
  # portlist = ports.ranges.range.add()
  #
  # print "PROTOBUFF PORTS:"
  # portlist.begin = nextPort
  # print ("  Range: %d -" % nextPort,)
  # prevPort = nextPort
  # for port in peerPorts[1:]:
  #   if port != prevPort + 1:
  #     portlist.end = prevPort
  #     print prevPort
  #     portlist = ports.ranges.range.add()
  #     portlist.begin = port
  #     print "  Range: %d " % port,
  #   prevPort = port
  # portlist.end = prevPort
  # print prevPort

  return task



def compileTask(**kwargs):
  app     = kwargs.get('name', 'myprog')
  webaddr = kwargs.get('webaddr', 'http://localhost:5000')
  script  = kwargs.get('script', None)
  source  = kwargs.get('source', None)
  slave   = kwargs.get('slave', None)
  uid     = kwargs.get('uid', None)
  r_cpu   = kwargs.get('cpu', 4)
  r_mem   = kwargs.get('mem', 4*1024)

  print 'WEB ADDR: %s' % webaddr

  if script == None or source == None:
    print ("Error. Cannot run compiler (missing source and/or compiler script)")
    return

  if slave == None:
    print ("Error. No mesos slave provided")

  uname = app if uid == None else "%s-%s" % (app, uid)

  executor = mesos_pb2.ExecutorInfo()
  executor.executor_id.value = app
  executor.name = 'K3-Compiler-%s' % app

  command = mesos_pb2.CommandInfo()
  command.value = 'python $MESOS_SANDBOX/CompileExecutor.py'

  # TODO:  Pull Specific K3-Build Version
  # env = mesos_pb2.Environment()
  # var = environment.variables.add()
  # var.name = 'K3_BUILD'
  # var.value = <git-hash>
  # command.environment.MergeFrom(environment)

  comp_exec = command.uris.add()
  comp_exec.value = "%s/fs/CompileExecutor.py" % webaddr
  comp_exec.executable = False
  comp_exec.extract = False

  sh = command.uris.add()
  sh.value = script
  sh.executable = True
  sh.extract = False

  src = command.uris.add()
  src.value = source
  src.executable = False
  src.extract = False

  docker = mesos_pb2.ContainerInfo.DockerInfo()
  docker.image = K3_DOCKER_NAME
  docker.network = docker.HOST

  container = mesos_pb2.ContainerInfo()
  container.type = container.DOCKER
  container.docker.MergeFrom(docker)

  executor.command.MergeFrom(command)
  executor.container.MergeFrom(container)

  task = mesos_pb2.TaskInfo()
  task.name = app
  task.task_id.value = uname
  task.slave_id.value = slave

  # Labels:  New In Mesos v 0.22
  # config = mesos_pb2.Labels()
  # l_app = config.labels.add()
  # l_app.key = 'appName'
  # l_app.value = app
  # task.labels.MergeFrom(config)

  task.executor.MergeFrom(executor)

  cpus = task.resources.add()
  cpus.name = "cpus"
  cpus.type = mesos_pb2.Value.SCALAR
  cpus.scalar.value = r_cpu

  mem = task.resources.add()
  mem.name = "mem"
  mem.type = mesos_pb2.Value.SCALAR
  mem.scalar.value = r_mem

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

    server = process.stdout.read()
    process.stdout.close()
    process.stderr.close()

    conn = httplib.HTTPConnection(server.strip())
    conn.request('GET', '/')
    return server if conn.getresponse().status in [200, 201, 202] else None

