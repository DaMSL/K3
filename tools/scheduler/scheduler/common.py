import logging
import datetime
import uuid
from enum import enum


heartbeat_delay = 10  # secs. move to common
gc_delay = 8
offer_wait = 8


# Returns unique time stamp uid (unique to this machine only (for now)
def getUID():
  return str(uuid.uuid1()).split('-')[0]



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
