# scheduler.core: Data structures for managing K3 jobs.

class Job:
  def __init__(self, roles, binary_url):
    self.roles = roles
    self.binary_url = binary_url
    self.tasks = None
    self.status = None
    self.all_peers = None
    self.inputs = None

# TODO inputs per Roles instead of per Job
class Role:
  def __init__(self, peers = 0, variables = {}, inputs = {}, hostmask = r"*"):
    self.peers = peers
    self.variables = variables
    #self.inputs = inputs
    self.hostmask = hostmask

class Peer:
  def __init__(self, index, variables, ip, port):
    self.index = index
    self.variables = variables
    self.ip = ip
    self.port = port

class Task:
  def __init__(self, taskid, offerid, host, mem, peers):
    self.taskid = taskid
    self.offerid = offerid
    self.status = None
    self.host = host
    self.mem = mem
    self.peers = peers

  def getId(self, jobid):
    return "%s.%s" % (jobid, self.taskid)

