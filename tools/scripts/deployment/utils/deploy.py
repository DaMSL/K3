import subprocess
import sys
import os
import copy
import time
from threading import Thread

from utils.ssh import *
from utils.address import *
# Deployment Utilities for K3 Programs.

# Run the specified function with each of the specified configurations, in parallel. Then join.
# The function must expect a config dictionary as a single argument
def parallelize(f, configs):
    ts = []
    master = True
    for config in configs:
        t = Thread(target=f,args=(config,))
        t.start()
        ts.append(t)
        if master:
          time.sleep(.050)
          master = False
    for t in ts:
        t.join()

def runDistK3(configs):
    parallelize(generateScript, configs)
    parallelize(sendScriptToPeer, configs)
    parallelize(runRemoteScript, configs)

# Generate a script that will execute the K3 binary with proper command line args.
# Required config keys: me_ip, k3_args, k3_binary_path, k3_binary_name
def generateScript(config):
    k3_args = config["k3_args"]
    arg_str = ("'" + (",".join([k + ":" + v for (k,v) in k3_args.items()]))  + "'") % k3_args
    config["arg_str"] = arg_str    

    # Generate a script to kill existing k3 program if running, then run program.
    script = ("""#!/bin/bash\n"""
              """kill -9 $(pidof %(k3_binary_name)s)\n"""
              """%(k3_binary_path)s%(k3_binary_name)s  -p %(arg_str)s\n""") % config 
    local_script_path = config["me_ip"] + ".txt"

    # Dump to a local file
    with open(local_script_path,"wb") as script_file:
      script_file.write(script)

# Send local script to peer
# Required config keys: me_ip, remote_script_path, 
def sendScriptToPeer(config):

# Run script on a remote peer.
# Required config keys: me_ip, remote_script_path
