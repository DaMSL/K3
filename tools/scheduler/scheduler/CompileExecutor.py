#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import os
import stat
import threading
import shutil
import time
import subprocess
import httplib
import urllib

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

import logging

BUFFER_SIZE = 256

DEBUG = False
DEBUG_FILE = 'examples/sql/tpch/queries/k3/q1.k3'


def isTrue(boolstr):
    return boolstr.upper() == 'TRUE'


class CompilerExecutor(mesos.interface.Executor):

    def __init__(self):
        logging.basicConfig(
            format='[%(asctime)s %(levelname)s] %(message)s', level=logging.DEBUG, datefmt='%H:%M:%S')
        self.thread = None
        self.status = mesos_pb2.TaskStatus()
        self.status.state = mesos_pb2.TASK_STARTING
        self.success = False
        self.buffer = ""

    def launchTask(self, driver, task):

        # The compilation task runs in its own thread
        def run_task():

            self.status.task_id.value = task.task_id.value

            logging.debug("Executor is running")

            # Mesos v0.22  (Labels)
            logging.debug("Passed Labels received at Executor:")
            labels = task.labels.labels
            daemon = {}
            for l in labels:
                logging.debug("%s = %s" % (l.key, l.value))
                daemon[l.key] = str(l.value)

            self.status.message = daemon['role']

            name = str(task.name.encode('utf8', 'ignore'))
            logging.debug("Task Name = %s" % name)
            os.chdir('/k3/K3')

            if isTrue(daemon['gitpull']):
                gitpull = 'cd /k3/K3 && git pull && git checkout %s' % daemon[
                    'branch']
                logging.info("GIT PULL Requested: %s " % gitpull)
                gitpulled = subprocess.call(gitpull, shell=True)
                if gitpulled == 0:
                    logging.info('Local K3 successfully updated')
                else:
                    logging.warning(
                        'Local K3 update FAILED (continuing with compilation)')

            if isTrue(daemon['cabalbuild']):

                logging.info("CABAL BUILD requested")
                path = '/opt/ghc/7.10.1/bin:/opt/cabal/1.22/bin:/opt/alex/3.1.4/bin:/opt/happy/1.19.5/bin:/root/.cabal/bin/:$PATH'
                cabalbuilt = subprocess.call(
                    'export PATH=%s && echo $PATH && cd /k3/K3 && cabal build -j' % path, shell=True)
                if cabalbuilt == 0:
                    logging.info('Cabal build complete')
                else:
                    logging.warning(
                        'CABAL build has FAILED (continuing with compilation)')

            # Set Compiling service command line
            if daemon['role'] == 'client':
                daemon[
                    'k3src'] = DEBUG_FILE if DEBUG else '$MESOS_SANDBOX/%s.k3' % daemon['name']
                cmd = './tools/scripts/run/service.sh submit --svid %(svid)s --host %(host)s --port %(port)s  --blocksize %(blocksize)s -j %(cppthread)s %(compilestage)s %(compileargs)s %(k3src)s +RTS -A10M -s -N -RTS' % daemon
            elif daemon['role'] == 'master':
                # limit to 73Gigs of RAM (hm)
                cmd = './tools/scripts/run/service.sh %(role)s --svid %(svid)s --host %(host)s --port %(port)s --workers %(m_workerthread)s +RTS -s -A10M -N -M73G -RTS' % daemon
            else:
                cmd = './tools/scripts/run/service.sh %(role)s --svid %(svid)s --host %(host)s --port %(port)s --workers %(w_workerthread)s --heartbeat %(heartbeat)s +RTS -A10M -s -N -RTS' % daemon

            logging.info("CMD = %s" % cmd)

            self.status.data += "%s is ready.\n" % daemon['svid']
            if isTrue(daemon['gitpull']):
                self.status.data += 'Pulled GIT BRANCH: %s.\n' % daemon[
                    'branch']
            if isTrue(daemon['cabalbuild']):
                self.status.data += 'CABAL Re-built K3.\n'
            self.status.task_id.value = task.task_id.value
            driver.sendStatusUpdate(self.status)

            # Start the service (in current thread with stderr/stdout
            # redirected from Mesos)
            proc = subprocess.Popen(cmd, shell=True,
                                    stdin=None,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT)

            # Notify the compiler launcher it's running
            self.status.state = mesos_pb2.TASK_RUNNING
            self.status.data = "%s is RUNNING" % daemon['svid']
            driver.sendStatusUpdate(self.status)
            logging.info(self.status.data)

            # Poll process for new output until finished; buffer output
            # NOTE: Buffered output reduces message traffic via mesos
            while True:
                output = proc.stdout.readline()
                logging.info(output)
                self.buffer += output
                if output == '' and proc.poll() != None:
                    self.status.data = self.buffer
                    driver.sendStatusUpdate(self.status)
                    break

                # If buffer-size is reached: send update message
                if len(self.buffer) > BUFFER_SIZE:
                    self.status.data = self.buffer
                    driver.sendStatusUpdate(self.status)
                    self.buffer = ""

            exitCode = proc.returncode
            self.success = (exitCode == 0)
            driver.sendStatusUpdate(self.status)
            logging.info("Compile Task COMPLETED for %s" % daemon['svid'])

            destfile = ''

            curlPrefix = 'curl -i -X POST -H "Accept: application/json"'
            builddir = '/k3/K3/__build/'

            #  TODO: Need a way to check success/failure
            if daemon['role'] == 'client':
                logging.debug("Client is copying all files in build dir")
                self.status.data = "CLIENT ----> EXIT CODE=%d Uploading Application, %s" % (
                    exitCode, daemon['name'])
                driver.sendStatusUpdate(self.status)
                archive_target = daemon[
                    'webaddr'] + '/apps/' + daemon['name'] + '/' + daemon['uid']
                curlcmd = curlPrefix + ' -F "file=@' + builddir + '%s" ' + archive_target

                # User requested binary compilation. Try to upload new app
                if daemon['compilestage'] == '' and os.path.exists(builddir):

                    # Compilation Successful: upload it with same UID
                    if os.path.exists(builddir + 'A'):
                        # Binary was successfully built
                        # NOTE: compilation "succeeded" even if client exited
                        # w/non-zero status
                        self.success = True
                        daemon['binfile'] = builddir + daemon['name']
                        shutil.move(builddir + 'A', daemon['binfile'])
                        submit = curlPrefix + \
                            ' -F "file=@%(binfile)s" -F "uid=%(uid)s" %(webaddr)s/apps' % daemon
                        logging.debug("SUBMIT New Application: " + submit)
                        subprocess.call(submit, shell=True)
                    else:
                        # NOTE: compilation "fails" with no binary, even if
                        # client exited w/zero status
                        self.success = False

                if os.path.exists(builddir):
                    logging.debug("CLIENT __build: ")
                    self.status.data = "CLIENT ----> Sending build dir to server"
                    driver.sendStatusUpdate(self.status)
                    for f in os.listdir(builddir):
                        #  Upload binary to flask server & copy to sandbox
                        logging.debug('  Shipping FILE: ' + f)
                        curl = curlcmd % f
                        subprocess.call(curl, shell=True)

            # #  SET IF CASE FOR A "HALT ROLE"
            #   haltcmd = './tools/scripts/run/service.sh halt --svid %(svid)s --host %(host)s --port %(port)s' % daemon
            #   subprocess.call(haltcmd, shell=True)
            #   logging.info("Client sent HALT command and is terminating")

            else:
                logging.info("`%s` Terminated", daemon['svid'])

            if self.success:
                self.status.state = mesos_pb2.TASK_FINISHED
                self.status.data = "terminated normally."
                if daemon['role'] == 'client':
                    self.status.data += " Compilation Job was SUCCESSFUL."
            else:
                self.status.state = mesos_pb2.TASK_FAILED
                self.status.data = "Terminated"
                if daemon['role'] == 'client':
                    self.status.data += " Compilation FAILED."
            driver.sendStatusUpdate(self.status)

        self.thread = threading.Thread(target=run_task)
        self.thread.start()

    def frameworkMessage(self, driver, message):
        logging.info('[FRWK MSG] %s ' % str(message))

    def killTask(self, driver, tid):
        self.status.data = self.buffer
        self.status.state = mesos_pb2.TASK_KILLED
        driver.sendStatusUpdate(self.status)
        logging.warning("Executor was signaled to terminate. Exiting now....")
        sys.exit(1)

    def error(self, driver, code, message):
        print "Error from Mesos: %s (code %s)" % (message, code)

if __name__ == "__main__":
    print "Executor has started"
    executor = CompilerExecutor()
    driver = mesos.native.MesosExecutorDriver(executor)
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)

    #  Upload binary to flask server ILO curl
    # headers = {"Accept": "application/json"}
    # data = {"file": open(daemon['name'])}
    # endpoint = "/apps/%s/%s" % (daemon['name'], daemom['uid'])
    # conn = httplib.HTTPConnection(daemon['webaddr'])
    # conn.request("POST", endpoint, data, headers)
    # response = conn.getresponse()
    # logging.info ('%s: %s\n%s' % (response.status, response.reason, response.read()))
    # conn.close()
