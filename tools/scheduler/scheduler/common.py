import logging
import datetime


heartbeat_delay = 10  # secs. move to common


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
