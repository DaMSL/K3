import threading
import os
import sys
import subprocess
import time

globalconfig = {
    'benchmark_dir': None,
    'sender_ip'    : None,
    'receiver_ip'  : None,
    'num_messages' : None,
    'message_len'  : None
  }

globalresults = {}

# mode should be a string: "sender" or "receiver"
def buildCommand(mode, config):
  # copy config for safe mutation
  conf = dict(config)
  ip_key = mode + "_ip"
  conf['ssh_ip'] = config[ip_key]
  conf['mode'] = mode
  command = "ssh %(user)s@%(ssh_ip)s %(benchmark_dir)s/benchmark %(num_messages)s %(message_len)s %(mode)s %(receiver_ip)s %(sender_ip)s" % conf
  return command

class ReceiverThread(threading.Thread):
  def run(self):
    command = buildCommand("receiver", globalconfig)
    print(command)
    subprocess.call(command, shell=True)

class SenderThread(threading.Thread):
  def run(self):
    command = buildCommand("sender", globalconfig)
    print(command)
    subprocess.call(command, shell=True)
    # fetch results file
    fetch = "scp %(user)s@%(sender_ip)s:results.txt ." % globalconfig
    print(fetch)
    subprocess.call(fetch, shell=True)
    # read results
    f = open("results.txt","r")
    run_time = f.readline()
    f.close()
    ## stash results
    stashResult(run_time)

def stashResult(run_time):
  num_messages = globalconfig['num_messages']
  message_len = globalconfig['message_len']
  k = (num_messages, message_len)
  if  k not in globalresults:
    globalresults[k] = []

  print(run_time)
  globalresults[k].append(run_time)

def sleep():
  time.sleep(2)

def runExperiment(num_messages, message_len):
  # config
  globalconfig['num_messages'] = num_messages
  globalconfig['message_len'] = message_len

  t1 = ReceiverThread()
  t2 = SenderThread()
  t1.start()
  sleep()
  t2.start()
  t1.join()
  t2.join()

if __name__ == "__main__":
  # Args
  if len(sys.argv) < 5:
    print("usage:", sys.argv[0],": ssh_user receiver_ip sender_ip benchmark_dir")

  globalconfig["user"] = sys.argv[1]
  globalconfig["receiver_ip"] = sys.argv[2]
  globalconfig["sender_ip"] = sys.argv[3]
  globalconfig["benchmark_dir"] = sys.argv[4]

  # Config
  nums = [100000]
  lens = [16, 1024, 32*1024, 64*1024 , 128*1024, 256*1024, 512*1024, 1024*1024]
  trials = range(3)
  # Run
  for num_messages in nums:
    for message_len in lens:
      for t in trials:
        runExperiment(num_messages, message_len)
        sleep()
  # Print
  print(globalresults)

  exit(0)
