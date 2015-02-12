# scheduler.dispatcher: Scheduler logic for matching resource offers to job requests.

from collections import deque
from core import *

class Dispatcher:
  def __init__(self):
    self.active = {}
    self.pending = deque()
    self.offers = {}

  def submit(self, job):
    self.pending.append(job)

if __name__ == "__main__":
  variables = {"role": "rows", "master": "auto"}
  r = Role(10, variables)
  roles = {"role1": r}

  j = Job(roles, "tpchq1")

  d = Dispatcher()
  d.submit(j)

  print(len(d.pending))
