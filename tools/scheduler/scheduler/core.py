# scheduler.core: Data structures for managing K3 jobs.
import os, re, yaml
import tarfile




# TODO inputs per Roles instead of per Job
class Role:
  def __init__(self, peers = 0, variables = {}, inputs = {}, hostmask = r".*"):
    self.peers = peers
    self.variables = variables
    #self.inputs = inputs
    self.hostmask = hostmask

  def to_string(self):
    print ("  ROLE ")
    print ("    # peers = %d" % self.peers)
    print ("    # vars  = ", self.variables)



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



class Job:
  def __init__(self, **kwargs):
    self.archive    = kwargs.get("archive", None)
    self.binary_url = kwargs.get("binary", None)
    self.appId      = kwargs.get("appId", '2222')   #TODO: AppId system
    roleFile        = kwargs.get("rolefile", None)
    self.roles      = {}
    self.inputs     = []
    self.tasks      = None
    self.status     = None
    self.all_peers  = None

    if self.archive == None and self.binary_url == None:
      print ("No Archive or Binary found")
      return

    if self.archive != None and self.binary_url != None:
      print ("Create job with EITHER archive OR binary")
      return

    if self.archive:
      # Unzip archive into binary & yaml file
      self.path = "jobs/" + self.appId
      if not os.path.exists(self.path):
        os.mkdir(self.path)
      tf = tarfile.open(self.archive)
      tf.extractall(self.path)

      files = os.listdir(self.path)

      # TODO: Better way to ID binary, & split YAML upload perhaps via JSON REST input
      for f in files:
         if not re.match('.*\..*', f):
           self.binary_url = "http://qp1:8002/" + self.path + "/" + f
           break

      if self.binary_url == None:
        print ("Error. Binary file not found in archive")
        return

      roleFiles = [f for f in os.listdir(self.path) if re.match('.*\.(yml|yaml)$', f)]
      if len(roleFiles) == 0:
        print("Error. No YAML files found in archive")
        return

      #TODO:  Multiple role files??? (if needed)
      roleFile = roleFiles[0]

    else:
      if roleFile == None:
        print ("Error. No YAML file provided to Job")
        return

    print ("Creating Job ID %s, with Binary: %s, using Role: %s, ")
    self.createRoles(roleFile)


  def createRoles(self, path):
    y = None
    with open(path, "r") as f:
      contents = f.read()
      y = yaml.load_all(contents)

    for doc in y:
      if "name" not in doc:
        print("Error. 'name' not specified in YAML file")
        return None
      name = doc['name']

      if "peers" not in doc:
        print("Error. 'peers' not specified in YAML file")
        return None
      peers = int(doc['peers'])

      variables = {}
      if doc['k3_globals']:
        variables = doc['k3_globals']
      else:
        print("Warning. No k3_globals found in YAML file")

      mask = r".*"
      if "hostmask" in doc:
        mask = doc['hostmask']

      if "k3_data" in doc:
        self.inputs = doc['k3_data']

      r = Role(peers, variables, hostmask=mask)
      self.roles[name] = r

