# scheduler.core: Data structures for managing K3 jobs.
import os, re, yaml
import tarfile



class JobStatus:
  INITIATED = 'INITIATED'
  SUBMITTED = 'SUBMITTED'
  RUNNING   = 'RUNNING'
  COMPILING = 'COMPILING'
  ARCHIVING = 'ARCHIVING'
  FAILED    = 'FAILED'
  KILLED    = 'KILLED'
  FINISHED  = 'FINISHED'

  @classmethod
  def done(cls, s):
    return s in [JobStatus.FAILED, JobStatus.KILLED, JobStatus.FINISHED]



class AppID:
  def __init__(self, name, uid):
    self.name = name
    self.uid = uid

  def get(self):
    return '%s-%s' % (self.name, self.uid)

  @classmethod
  def getAppId(cls, name, uid):
    # TODO: Error check for a dash in the name
    return '%s-%s' % (name, uid)

  @classmethod
  def getName(cls, appId):
    return appId.split('-')[0]

  @classmethod
  def getUID(cls, appId):
    return appId.split('-')[1]


# TODO inputs per Roles instead of per Job
class Role:
  def __init__(self, **kwargs):
    self.peers      = kwargs.get("peers", 0)
    self.variables  = kwargs.get("variables", {})
    self.hostmask   = kwargs.get("hostmask", r".*")
    self.params     = kwargs.get("params", {})
    self.volumes    = kwargs.get("volumes", [])
    self.envars     = kwargs.get("envars", [])

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


class CompileJob:
  def __init__(self, **kwargs):
    self.name    = kwargs.get('name', 'CompileTask')
    self.uid      = kwargs.get('uid', '')
    self.path     = kwargs.get('path', '')
    self.url      = kwargs.get('url', '')
    self.git_hash = kwargs.get('git_hash', 'latest')
    self.user     = kwargs.get('user', '')
    self.tag       = kwargs.get('tag', '')
    self.options   = kwargs.get('options', '')
  def __dict__(self):
    return dict(name=self.name, uid=self.uid, path=self.path, tag=self.tag,
                git_hash=self.git_hash, user=self.user, options=self.options)


class Job:
  def __init__(self, **kwargs):
    self.archive    = kwargs.get("archive", None)
    self.binary_url = kwargs.get("binary", None)
#    self.appId      = kwargs.get("appId", 'None')   #TODO: AppId system
    self.appName     = kwargs.get("appName", 'None')   #TODO: AppId system
    self.appUID      = kwargs.get("appUID", 'None')   #TODO: AppId system
    self.jobId      = kwargs.get("jobId", '1000')   #TODO: AppId system
    roleFile        = kwargs.get("rolefile", None)
    self.roles      = {}
    self.inputs     = []
    self.volumes    = []
    self.envars   = []
    self.tasks      = None
    self.status     = None
    self.all_peers  = None
    self.master     = None

    if self.archive == None and self.binary_url == None:
      print ("No Archive or Binary found")
      return

    if self.archive != None and self.binary_url != None:
      print ("Create job with EITHER archive OR binary")
      return

    if self.archive:
      # Unzip archive into binary & yaml file
      self.path = "jobs/" + self.appName
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
      if 'k3_globals' in doc:
        variables = doc['k3_globals']
      else:
        print("Warning. No k3_globals found in YAML file")

      params = {}
      if 'peers_per_host' in doc:
        params['peers_per_host'] = doc['peers_per_host']

      # TODO:  CHANGE TO ROLE-BASED VOLUMES for volumes
      volumes = [] if 'volumes' not in doc else doc['volumes']
      self.volumes = volumes

      envars = [] if 'envars' not in doc else doc['envars']
      self.envars = envars


      # TODO:  CHANGE TO ROLE-BASED INPUTS for k3_data
      if 'k3_data' in doc:
        self.inputs = doc['k3_data']

      mask = r".*"
      if "hostmask" in doc:
        mask = doc['hostmask']


      r = Role(peers=peers, variables=variables, hostmask=mask, volumes=volumes, params=params, envars=envars)
      self.roles[name] = r



