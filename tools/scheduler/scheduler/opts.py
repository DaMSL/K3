import argparse
import yaml
from dispatcher import *

def createRoles(path):
  y = None
  with open(path, "r") as f:
    contents = f.read()
    y = yaml.load_all(contents)

  roles = {}
  inputs = []
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
    if doc['k3_globals']:
      variables = doc['k3_globals']
    else:
      print("Warning. No k3_globals found in YAML file")

    mask = r".*"
    if "hostmask" in doc:
      mask = doc['hostmask']

    if "k3_data" in doc:
      inputs = doc['k3_data']
    
    r = Role(peers, variables, hostmask=mask)
    roles[name] = r

  return (roles, inputs)

# Parse command line arguments and create a dispatcher
def parseArgs():
  parser = argparse.ArgumentParser() 
  parser.add_argument("--daemon", help="Run the Dispatcher as a daemon. Can not be used with other flags")
  parser.add_argument("--binary", nargs=1, help="URL of the K3 Binary to run. Only allowed in non-daemon mode")
  parser.add_argument("--roles", nargs=1, help="Path to a YAML files specifying roles. Only allowed in non-daemon mode")
  args = parser.parse_args()

  if not (args.daemon or args.binary or args.roles):
    print("ERROR: No flags specified")
    return None

  if args.daemon and (args.binary or args.roles):
    print("ERROR: Daemon mode can not be used with other flags")
    return None
  
  if args.daemon:
    d = Dispatcher(daemon=True)
    return d
    
  if not (args.binary and args.roles):
    print("ERROR: Must specify both --binary and --roles for non-daemon mode")
    return None
  
  d = Dispatcher(daemon=False)
  
  result = createRoles(args.roles[0])
  if result == None:
    print("Failed to create roles from YAML")
    return None

  (roles, inputs) = result
  
  j = Job(roles, args.binary[0])
  j.inputs = inputs
  d.submit(j)
  return d
