import os

def ssh(usrAtHost, cmd):
  ssh_cmd = "ssh " + usrAtHost + " " + cmd
  os.system(ssh_cmd)

def scpTo(usrAtHost, local_file, remote_path):
  scp_cmd = "scp " + local_file + " " + usrAtHost + ":" + remote_path 
  os.system(scp_cmd) 
