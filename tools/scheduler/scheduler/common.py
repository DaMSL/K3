import logging
import datetime
import uuid
from enum import enum, Enum


heartbeat_delay = 60  # secs. move to common
gc_delay = 15
offer_wait = 5


# Returns unique time stamp uid (unique to this machine only)
def getUID():
  return str(uuid.uuid1()).split('-')[0]


# masterNodes =  ['qp-hd2']
# workerNodes =  ['qp-hd9', 'qp-hd15']
# clientNodes =  ['qp6']
masterNodes =  ['qp-hm1']
workerNodes =  ['qp3', 'qp4', 'qp5', 'qp6'] + ['qp-hm' + str(i) for i in range(2,9)] + ['qp-hd' + str(i) for i in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13]]
# workerNodes =  ['qp' + str(i) for i in range(3,6)] + ['qp-hm' + str(i) for i in range(2,9)]
clientNodes =  ['qp-hd14', 'qp-hd15', 'qp-hd16']


compilerNodes    =  masterNodes + workerNodes + clientNodes


CompileServiceState = enum.Enum('Service', 'DOWN INIT DISPATCH MASTER_WAIT WORKER_WAIT UP')

CompileState = enum.Enum('CompileState', 'INIT DISPATCH CLIENT_WAIT SUBMIT COMPILE UPLOAD COMPLETE FAILED KILLED')
compileTerminatedStates = ['COMPLETE', 'FAILED', 'KILLED']




compileStageValues = ['both', 'cpp', 'bin']
class CompileStage(Enum):
  FIRST = '-1'
  SECOND = '-2'
  BOTH = ''
def getCompileStage(stage):
    inputmap = dict(both=CompileStage.BOTH, 
                  cpp=CompileStage.FIRST, 
                  bin=CompileStage.SECOND)
    return inputmap[stage]


workloadOptions = {'balanced': '',
                   'moderate': '--workerfactor hm=3 --workerblocks hd=4:qp3=4:qp4=4:qp5=4:qp6=4',
                   'moderate2': '--workerfactor hm=3 --workerblocks hd=2:qp3=2:qp4=2:qp5=2:qp6=2',
                   'extreme': '--workerfactor hm=4 --workerblocks hd=1:qp3=1:qp4=1:qp5=1:qp6=1'}


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


# TODO: move to common
class ServiceFormatter(logging.Formatter):
  """
  Custom formatter to customize milliseconds in formats
  """
  converter=datetime.datetime.fromtimestamp
  def formatTime(self, record, datefmt=None):
      ct = self.converter(record.created)
      if datefmt:
          s = ct.strftime(datefmt)
      else:
          t = ct.strftime("%H:%M:%S")
          s = "%s.%03d" % (t, record.msecs)
      return s


class ParseName:

  @classmethod
  def splituname(cls, uname):
    return uname.split('-')

  @classmethod
  def uname2name(cls, uname):
    return uname.split('-')[0]

  @classmethod
  def uname2uid(cls, uname):
    return uname.split('-')[1]

  @classmethod
  def makeuname(cls, name, uid):
    return name + '-' + uid

