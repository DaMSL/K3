import yaml
import copy
from classes.k3peer import K3Peer

def loadYmlFile(path):
  with open(path,"r") as f:
    return yaml.load(f.read())

def genK3Peers(path):
  config = loadYmlFile(path)
  peers = config.get("k3_peers",None)
  if peers == None:
    print("No peers!")
    return # TODO error!
    
  configs = []
  peer_strs = []
  for peer in peers:
    # Merge config dictionary. Merge k3 bindings. Using peer to overwrite
    cp = copy.deepcopy(config)
    peer_overwrites = peer.get("k3_bindings",{})
    peer["k3_bindings"] = dict(cp["k3_bindings"].items() + peer_overwrites.items())
    peer_config = dict(cp.items() + peer.items()) 
    
    # Bind the "me" and "peers" variables
    me_str = "<" + peer_config["ip"] + ":" + peer_config["k3_port"] + ">"
    peer_config["k3_bindings"]["me"] = me_str
    peer_str = "{addr:%s}" % me_str
    peer_strs.append(peer_str)
    configs.append(peer_config)

  result = []
  for config in configs:
    config["k3_bindings"]["peers"] = "[" + (",".join(peer_strs)) + "]"
    result.append(K3Peer(config))
  return result
