# mesosutils: Constructors for Mesos Protocol Buffers
import yaml
import re
import subprocess
import httplib
import logging

#from protobuf_to_dict import protobuf_to_dictd
import json


import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

DEBUG_EXECUTOR = True

# K3_DOCKER_NAME = "damsl/k3-run:exec"
K3_COMPILER_IMAGE = "damsl/k3-compiler"
K3_DOCKER_NAME = "damsl/k3-app"
#K3_COMPILER_IMAGE = "damsl/k3-run:compile"

def addLabel(labelMap, key, value):
  # Labels:  New In Mesos v 0.22
  # config = mesos_pb2.Labels()
  lab = labelMap.labels.add()
  lab.key = key
  lab.value = value

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

  if DEBUG_EXECUTOR:
    command.value = 'python $MESOS_SANDBOX/k3executor.py'
    exec_binary = command.uris.add()
    exec_binary.value = "%s/fs/k3executor.py" % webaddr
    exec_binary.executable = True
    exec_binary.extract = False
  else:
    command.value = 'python k3/k3executor.py'

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
    logging.warning ("NOTE: running privileged mode")


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

  task_data = {
      "binary"           : str(k3job.appName),
      "totalPeers"       : len(k3job.all_peers),
      "peerStart"        : k3task.peers[0].index,
      "peerEnd"          : k3task.peers[-1].index,
      "me"               : [ [p.ip, p.port] for p in k3task.peers],
      "peers"            : [ {"addr": [p.ip, p.port] } for p in k3job.all_peers],
      "globals"          : [p.variables for p in k3task.peers],
      "master"           : [k3job.all_peers[0].ip, k3job.all_peers[0].port ],
      "archive_endpoint" : "%s/jobs/" % webaddr,
      "data"             : [role.inputs for p in range(len(k3task.peers))],
      "outpaths"         : role.outpaths,
      "seq_files"        : role.seq_files,
      "logging"          : k3job.logging,
      "jsonlog"          : k3job.jsonlog,
      "jsonfinal"        : k3job.jsonfinal,
      "perf_profile"     : k3job.perf_profile,
      "perf_frequency"   : k3job.perf_frequency,
      "cmd_prefix"       : k3job.cmd_prefix,
      "cmd_infix"        : k3job.cmd_infix,
      "cmd_suffix"       : k3job.cmd_suffix,
      "core_dump"        : k3job.core_dump,
      "stdout"           : k3job.stdout }

  # TODO:  When logging is fully implemented, remove this & update executor to accept
  #       the logging level
  if task_data['logging'] == False:
    del task_data['logging']

  if task_data['jsonlog'] == False:
    del task_data['jsonlog']

  if task_data['jsonfinal'] == False:
    del task_data['jsonfinal']

  if task_data['perf_profile'] == False:
    del task_data['perf_profile']

  if task_data['core_dump'] == False:
    del task_data['core_dump']

  if task_data['perf_frequency'] == '':
    del task_data['perf_frequency']

  if task_data['cmd_prefix'] is None:
    del task_data['cmd_prefix']

  if task_data['cmd_infix'] is None:
    del task_data['cmd_infix']

  if task_data['cmd_suffix'] is None:
    del task_data['cmd_suffix']

  if task_data['seq_files'] == []:
    del task_data['seq_files']



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


  for p in k3task.peers:
    logging.debug("   %s, %s" % (p.ip, p.port))
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



def addTaskLabel(task, key, value):
  # Labels:  New In Mesos v 0.22
  # config = mesos_pb2.Labels()
  # lab = config.labels.add()
  lab = task.labels.labels.add()
  lab.key = str(key)
  lab.value = str(value)

# def compileTask(**kwargs):
def compileTask(app, slave, daemon, r_cpu, r_mem, r_port):

  if slave == None:
    logging.error("Error. No mesos slave provided")

  # uname = app if uid == None else "%s-%s" % (app, uid)

  executor = mesos_pb2.ExecutorInfo()
  executor.executor_id.value = app.get()
  executor.name = 'K3-Compiler-%s' % app.get()

  command = mesos_pb2.CommandInfo()
  command.value = 'python $MESOS_SANDBOX/CompileExecutor.py'

  comp_exec = command.uris.add()
  comp_exec.value = "%s/fs/CompileExecutor.py" % daemon['webaddr']
  comp_exec.executable = False
  comp_exec.extract = False

  if daemon['role'] == 'client':
    src = command.uris.add()
    src.value = "%s/fs/build/%s/%s" % (daemon['webaddr'], app.get(), daemon['source'])
    src.executable = False
    src.extract = False

  docker = mesos_pb2.ContainerInfo.DockerInfo()
  docker.image = K3_COMPILER_IMAGE
  docker.network = docker.HOST

  container = mesos_pb2.ContainerInfo()
  container.type = container.DOCKER
  container.docker.MergeFrom(docker)

  executor.command.MergeFrom(command)
  executor.container.MergeFrom(container)

  #  TO set ENVAR for K3_BRANCH  (may remove)
  # environment = mesos_pb2.Environment()
  # envar = environment.variables.add()
  # envar.name = 'K3_BRANCH'
  # envar.value = daemon['branch']
  # command.environment.MergeFrom(environment)

  task = mesos_pb2.TaskInfo()
  task.name = app.get()
  task.task_id.value = daemon['svid']
  task.slave_id.value = slave

  # ADD ALL LABELS HERE to pass to executor
  daemon['name'] = app.name
  daemon['uid'] = app.uid
  config = mesos_pb2.Labels()
  for k, v in daemon.items():
    lab = config.labels.add()
    lab.key = str(k)
    lab.value = str(v)

  task.labels.MergeFrom(config)
  task.executor.MergeFrom(executor)

  cpus = task.resources.add()
  cpus.name = "cpus"
  cpus.type = mesos_pb2.Value.SCALAR
  cpus.scalar.value = r_cpu

  mem = task.resources.add()
  mem.name = "mem"
  mem.type = mesos_pb2.Value.SCALAR
  mem.scalar.value = r_mem

  # Ports are only added if set and are defined as a range of 1 port
  if r_port:
    port = task.resources.add()
    port.name = "ports"
    port.type = mesos_pb2.Value.RANGES
    portrange = port.ranges.range.add()
    portrange.begin = r_port
    portrange.end = r_port
    logging.debug("Compile Task Resources: cpu=%s, mem=%s, port=%s" % (r_cpu, r_mem, r_port))
  else:
    logging.debug("Compile Task Resources: cpu=%s, mem=%s" % (r_cpu, r_mem))

  return task

# Helper that uses 'mesos-resolve' to resolve a master IP:port from
# one of:
#     zk://host1:port1,host2:port2,.../path
#     zk://username:password@host1:port1,host2:port2,.../path
#     file://path/to/file (where file contains one of the above)

def cmd_exists(cmd):
    return subprocess.call("type " + cmd, shell=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE) == 0

def resolve(master):
    if subprocess.call("type mesos-resolve", shell=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE) != 0:

      host = ''
      for h in master.split('/')[1:]:
        if ':' in h:
          host = h.split(':')[0]
          break
      if host == '':
        host = "localhost"

      logging.debug('HOST  ---> %s', host)
      return "http://%s:5050" % host


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

    status = conn.getresponse().status
    if int(status) in [200, 201, 202]:
      return server

    else:
      logging.warning("MESOS Master Not responding")
      return None

