from utils.ssh import *
import os
import time

class K3Peer:
  def __init__(self,config):
    self.local_binary_path = config.get("local_binary_path")
    self.ip = config["ip"]
    self.port = config["k3_port"]
    self.remote_binary_path = "/tmp/"
    self.binary_name = "k3binary_" + str(self.ip) + str(self.port)
    self.remote_log_dir  = "/tmp/log/" + self.binary_name + "/"

    self.remote_log_file = self.remote_log_dir + "stdouterr.log"
    self.remote_script_path = "/tmp/run" + self.binary_name + ".txt"
    self.k3_bindings = config.get("k3_bindings",{})
    self.loggingEnabled = bool(config.get("enable_logging", False))
    self.stashOutput = bool(config.get("stash_output", False))
    self.local_output_dir = config["output_dir"]
    output_dir_str = '"%s"' % self.remote_log_dir
    self.k3_bindings["output_dir"] = output_dir_str

    # Constants
    self.local_script_path =  "%s.txt" % self.binary_name
    self.host = "root@%s" % self.ip

  def deployBinary(self):
    # Kill any running instance of the binary to unlock all files
    local_kill_file = "kill_" + self.binary_name + ".txt"
    remote_kill_file = self.remote_binary_path + "kill_" + self.binary_name;
    script = ("""#!/bin/bash\n"""
              """pkill -f k3binary\n""")


    # Dump to a local file
    with open(local_kill_file,"wb") as script_file:
      script_file.write(script)

    # copy local to remote
    scpTo(self.host, local_kill_file, remote_kill_file)

    # chmod +x remote_script
    chmod_cmd = "chmod +x %s" % remote_kill_file
    ssh(self.host, chmod_cmd)

    # cleanup the local file
    cleanup_cmd = "rm %s" % local_kill_file
    os.system(cleanup_cmd)

    # call kill command
    ssh(self.host, remote_kill_file)

    # give the poor process time to die in peace (and unlock the script file)
    time.sleep(10)

    # copy run script over to remote
    remote_file = self.remote_binary_path + self.binary_name

    scpTo(self.host, self.local_binary_path, remote_file) # TODO use rsync
    ssh(self.host, "chmod +x %s" % remote_file)

  def generateScript(self):
    k3_bindings = self.k3_bindings
    self.k3_bindings_str = ("'" + (",".join([k + ":" + v for (k,v) in k3_bindings.items()]))  + "'")

    logStr = ""
    if self.loggingEnabled:
      logStr = "-l some"

    stashStr = ""
    if self.stashOutput:
        stashStr = ">%(sf)s 2>&1" % {"sf": self.remote_log_file} 
    #perf record -g -o /tmp/perf/%(bn)s.data
    # Generate a script to run program.
    script = ("""#!/bin/bash\n"""
              """mkdir -p %(logdir)s\nrm -rf %(logdir)s/*\nnice -n 19 %(bp)s%(bn)s %(ls)s -p %(bs)s %(ss)s\n""") % {"bn":self.binary_name,"bp":self.remote_binary_path, "bs":self.k3_bindings_str, "ls": logStr, "ss": stashStr, "logdir": self.remote_log_dir}

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

  def collectResults(self):
    if (self.stashOutput):
      os.system("mkdir -p %s" % self.local_output_dir)
      scpDirFrom(self.host, self.remote_log_dir, self.local_output_dir) 
