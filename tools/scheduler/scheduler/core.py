# scheduler.core: Data structures for managing K3 jobs.

class Job:
  def __init__(self, roles, binary_url):
    self.roles = roles
    self.binary_url = binary_url
    self.tasks = None
    self.status = None

class Role:
  def __init__(self, peers = 0, variables = {}, inputs = {}, hostmask = r"*"):
    self.peers = peers
    self.variables = variables,
    self.inputs = inputs
    self.hostmask = hostmask

class Peer:
  def __init__(self, index, variables, inputs):
    self.index = index
    self.variables = variables
    self.inputs = inputs

class Task:
  def __init__(self, peers):
    self.status = None
    self.peers = peers
