# scheduler.core: Data structures for managing K3 jobs.

class Job:
  def __init__(self, roles):
    self.roles = roles
    self.tasks = None
    self.status = None

class Role:
  def __init__(self, peers = 0, variables {}, inputs {}, hostmask = r"*"):
    self.peers = peers
    self.variables = variables,
    self.inputs = inputs
    self.hostmask = hostmask

class Task:
  def __init__(self):
    self.status = None
