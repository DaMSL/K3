# scheduler.dispatcher: Scheduler logic for matching resource offers to job requests.

from multiprocessing import Queue

class Dispatcher:
  def __init__(self):
    self.active = {}
    self.pending = Queue()
