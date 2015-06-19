import logging
import datetime
import uuid
from enum import enum, Enum


heartbeat_delay = 60  # secs. move to common
gc_delay = 8
offer_wait = 8


# Returns unique time stamp uid (unique to this machine only)
def getUID():
  return str(uuid.uuid1()).split('-')[0]


masterNodes =  ['qp-hm1']
workerNodes =  ['qp' + str(i) for i in range(3,6)] + ['qp-hm' + str(i) for i in range(2,9)] + ['qp-hd' + str(i) for i in [1, 3, 4, 6, 7, 8, 10, 12]]
clientNodes =  ['qp-hd16']
allNodes    =  masterNodes + workerNodes + clientNodes


CompileState = enum.Enum('CompileState', 'INIT DISPATCH MASTER_WAIT WORKER_WAIT CLIENT_WAIT SUBMIT COMPILE UPLOAD COMPLETE FAILED KILLED')

class CompileStage(Enum):
  FIRST = '-1'
  SECOND = '-2'
  BOTH = ''

compileStageValues = ['both', 'cpp', 'bin']
 
def getCompileStage(stage):
    inputmap = dict(both=CompileStage.BOTH, 
                  cpp=CompileStage.FIRST, 
                  bin=CompileStage.SECOND)
    return inputmap[stage]



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

