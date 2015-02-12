# mesosutils: Constructors for Mesos Protocol Buffers
import yaml

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

# TODO how should we determine this?
EXECUTOR_URL = "http://192.168.0.11:8002/k3executor"
K3_DOCKER_NAME = "damsl/k3-mesos2"

def executorInfo(k3task, jobid, binary_url):

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
  
  volume = container.volumes.add()
  volume.container_path = '/local/data'
  volume.host_path = '/local/data'
  volume.mode = volume.RO
 
  executor.container.MergeFrom(container)
        
  return executor

def taskInfo(k3task, jobId, slaveId, binary_url, all_peers, inputs):
  task_data = {}
  # TODO fix this hack
  task_data["binary"] = binary_url.split("/")[-1].strip()
  task_data["totalPeers"] = len(all_peers)
  task_data["peerStart"] = k3task.peers[0].index
  task_data["peerEnd"] = k3task.peers[-1].index
  task_data["me"] = [ [p.ip, p.port] for p in k3task.peers] 
  task_data["peers"] = [ {"addr": [p.ip, p.port] } for p in all_peers] 
  task_data["globals"] = [p.variables for p in k3task.peers]  
  task_data["master"] = [ all_peers[0].ip, all_peers[0].port ]
  task_data["data"] = [ inputs for p in range(len(k3task.peers)) ]

  executor = executorInfo(k3task, jobId, binary_url)
  
  task = mesos_pb2.TaskInfo()
  task.task_id.value = k3task.getId(jobId)
  task.slave_id.value = slaveId.value
  task.name = str(jobId) + "@" + k3task.host
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

