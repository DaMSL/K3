from classes.k3peer import K3Peer
from threading import Thread
import time

class K3Deployer:
  def __init__(self,peers):
    self.peers = peers    
  
  def run(self):
    self.parallelize(K3Peer.deployBinary)
    self.parallelize(K3Peer.generateScript)
    self.parallelize(K3Peer.deployScript)
    self.parallelize(K3Peer.runRemoteScript)

  def parallelize(self,f):
    ts = []
    master = True
    for peer in self.peers:
        t = Thread(target=f,args=(peer,))
        t.start()
        ts.append(t)
        if master:
          time.sleep(.100)
          master = False
    for t in ts:
        t.join()
