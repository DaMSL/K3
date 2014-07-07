from utils.ssh import *
import os

class K3Peer:
  def __init__(self,config):
    self.local_binary_path = config.get("local_binary_path", None)
    self.remote_binary_path = config["k3_binary_path"]
    self.binary_name = config["k3_binary_name"]
    self.ip = config["ip"]
    self.port = config["k3_port"]
    self.remote_script_path = config["remote_script_path"]
    self.k3_bindings = config.get("k3_bindings",{})
    self.loggingEnabled = bool(config.get("enable_logging", False))
    # Constants
    self.local_script_path =  "%s.txt" % self.ip
    self.host = "root@%s" % self.ip
  
  def deployBinary(self):
    if(self.local_binary_path != None):
      remote_file = self.remote_binary_path + self.binary_name
      
      scpTo(self.host, self.local_binary_path, remote_file) # TODO use rsync

  def generateScript(self):
    k3_bindings = self.k3_bindings
    self.k3_bindings_str = ("'" + (",".join([k + ":" + v for (k,v) in k3_bindings.items()]))  + "'")
  
    logStr = ""
    if self.loggingEnabled:
      logStr = "-l some"
    # Generate a script to kill existing k3 program if running, then run program.
    script = ("""#!/bin/bash\n"""
              """kill -9 $(pidof %(bn)s)\n"""
              """%(bp)s%(bn)s %(ls)s -p %(bs)s\n""") % {"bn":self.binary_name,"bp":self.remote_binary_path, "bs":self.k3_bindings_str, "ls": logStr}
    
    # Dump to a local file
    with open(self.local_script_path,"wb") as script_file:
      script_file.write(script)

  def deployScript(self):
    # SCP local file to remote host
    scpTo(self.host, self.local_script_path, self.remote_script_path)

    # chmod +x remote_script
    chmod_cmd = "chmod +x %s" % self.remote_script_path
    ssh(self.host, chmod_cmd)

    # clean up
    os.system("rm " + self.local_script_path)

  def runRemoteScript(self):
    run_script_cmd = self.remote_script_path
    ssh(self.host,run_script_cmd)
