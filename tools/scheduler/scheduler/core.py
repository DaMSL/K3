# scheduler.core: Data structures for managing K3 jobs.
import os, re, yaml
import tarfile
import logging
from common import *

class K3JobError(Exception):
  def __init__(self, msg):
    self.value = msg
  def __str__ (self):
    return self.value

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
    self.inputs     = kwargs.get("inputs", [])

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
  def __init__(self, taskid, offerid, host, mem, peers, roleId):
    self.taskid = taskid
    self.offerid = offerid
    self.status = None
    self.host = host
    self.mem = mem
    self.peers = peers
    self.roleId = roleId

  def getId(self, jobid):
    return "%s.%s" % (jobid, self.taskid)


class CompileJob:
  def __init__(self, **kwargs):
    self.name    = kwargs.get('name', 'CompileTask')
    self.uid      = kwargs.get('uid', '')
    self.user     = kwargs.get('user', '')
    self.tag       = kwargs.get('tag', '')
    self.options   = kwargs.get('options', '')
    self.blocksize = kwargs.get('blocksize', 4)
    self.numworkers = kwargs.get('numworkers', 1)

    stage = kwargs.get('compilestage', CompileStage.BOTH)
    self.compilestage = stage.value
    
    self.path     = kwargs.get('path', '')

    self.url      = kwargs.get('path', '')

    # os.path.join(kwargs.get('webaddr'), 'fs', 'archive', self.name, self.uid)

  def __dict__(self):
    return dict(name=self.name, uid=self.uid, path=self.path, tag=self.tag,
                user=self.user, options=self.options, blocksize=self.blocksize,
                numworkers=self.numworkers, url=self.url, uname=self.uname())

  def uname(self):
    return self.name + '-' + self.uid



class Job:
  def __init__(self, **kwargs):
    # self.archive    = kwargs.get("archive", None)
    self.binary_url = kwargs.get("binary", None)
    self.appName     = kwargs.get("appName", 'None')
    self.appUID      = kwargs.get("appUID", 'None')
    self.jobId      = kwargs.get("jobId", '1000')
    roleFile        = kwargs.get("rolefile", None)
    self.logging    = kwargs.get("logging", False)
    self.jsonlog    = kwargs.get("jsonlog", False)
    self.jsonfinal    = kwargs.get("jsonfinal", False)
    self.stdout     = kwargs.get("stdout", False)
    self.roles      = {}
    self.tasks      = []
    self.status     = None
    self.all_peers  = None
    self.master     = None

    if self.binary_url == None:
      logging.error("[FLASKWEB] Error. No binary provided to Job")
      return

    print "BINARY URL = %s " % self.binary_url

    if roleFile == None:
      logging.error("[FLASKWEB] Error. No YAML file provided to Job")
      return

    roles = None
    with open(roleFile, "r") as f:
      try:
        contents = f.read()
        roles = yaml.load_all(contents)
      except yaml.YAMLError, exc:
        if hasattr(exc, 'problem_mark'):
          mark = exc.problem_mark
          logging.error("[FLASKWEB] YAML Format Error position: (%s:%s)" % (mark.line+1, mark.column+1))

    for doc in roles:
      try:
        name = doc['name']
        peers = int(doc['peers'])
        variables = doc['k3_globals']
      except KeyError as err:
        raise K3JobError('Input YAML File missing entry for: %s' % err.message)

      self.privileged = False if 'privileged' not in doc else doc['privileged']

      mask = r".*" if "hostmask" not in doc else doc['hostmask']
      volumes = [] if 'volumes' not in doc else doc['volumes']
      envars = [] if 'envars' not in doc else doc['envars']
      inputs = [] if 'k3_data' not in doc else doc['k3_data']

      # Parameters:  Just add additional parameters here to receive them
      #  from YAML -- the dispather will need to handle them
      params = {}
      if 'peers_per_host' in doc:
        params['peers_per_host'] = doc['peers_per_host']
      if 'mem' in doc:
        params['mem'] = doc['mem']

      # self.volumes.extend(volumes)
      # self.envars.extend(envars)
      # self.inputs.extend(inputs)

      r = Role(peers=peers, variables=variables, hostmask=mask,
               volumes=volumes, params=params, envars=envars, inputs=inputs)
      self.roles[name] = r


class PortList():
   def __init__(self, ranges=[]):
       self.index = 0
       self.offset = 0
       self.ports = [] if ranges == 0 else ranges
       
   def addRange(self, r):
       ports.append(r)

   def __iter__(self):
       return self

   def __next__(self):
     try:
       next = self.getNext()
       if next == None:
         raise StopIteration
     except IndexError:
       raise StopIteration
     return next

   def getNext(self):
     if len(self.ports) == 0 or self.index >= len(self.ports):
       return None
     result = self.ports[self.index][0] + self.offset
     if result == self.ports[self.index][1]:
         self.index += 1
         self.offset = 0
     else:
         self.offset += 1
     return int(result)




