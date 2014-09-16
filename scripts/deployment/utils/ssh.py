import os

def ssh(usrAtHost, cmd):
  ssh_cmd = "ssh -q " + usrAtHost + " " + cmd
  return os.system(ssh_cmd)

def scpTo(usrAtHost, local_file, remote_path):
  scp_cmd = "scp -q " + local_file + " " + usrAtHost + ":" + remote_path 
  return os.system(scp_cmd)

def scpFrom(usrAtHost, remote_file, local_path):
  scp_cmd = "scp -q " + usrAtHost + ":" + remote_file + " " + local_path
  return os.system(scp_cmd)

def scpDirFrom(usrAtHost, remote_dir, local_path):
  scp_cmd = "scp -q -r " + usrAtHost + ":" + remote_dir + " " + local_path
  return os.system(scp_cmd)
