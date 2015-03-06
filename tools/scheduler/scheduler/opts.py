import argparse
import yaml
from dispatcher import *



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



