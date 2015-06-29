import os
import threading
import yaml
import json
import hashlib
import datetime
import shutil
import argparse
import socket
import BaseHTTPServer
import SimpleHTTPServer
import httplib
import logging, logging.handlers

import socket
from threading import Thread, Event
from time import sleep

from flask import (Flask, request, redirect, url_for, jsonify,
                     render_template, send_from_directory)
from werkzeug import (secure_filename, SharedDataMiddleware)
from flask.ext.socketio import SocketIO, emit

from common import *
from core import *
from dispatcher import *
from compileLauncher import *
from mesosutils import *
import db


webapp = Flask(__name__, static_url_path='')
socketio = SocketIO(webapp)
logger = logging.getLogger("")

dispatcher = None

dispatcherTerminate = Event()
webserverTerminate  = Event()
haltFlag = Event()

# driver = None
# driver_t = None
# driverInitilize = Event()

compileService = None
lastCompile = None
compile_tasks = {}

index_message = 'Welcome to K3'


class SocketIOHandler(logging.Handler):
    def emit(self, record):
        socketio.emit('my response', record.getMessage(), namespace='/compile')

#===============================================================================
#   General Web Service Functions
#===============================================================================
def initWeb(port, **kwargs):
  """
    Peforms web service initialization
  """
  # Configure logging
  # logging.Formatter(fmt='[%(asctime)s %(levelname)-5s %(name)s] %(message)s',datefmt='%H:%M:%S')
  log_fmt = ServiceFormatter('[%(asctime)s %(levelname)6s] %(message)s')
  log_console = logging.StreamHandler()
  log_console.setFormatter(log_fmt)
  logger.setLevel(logging.DEBUG)
  logger.addHandler(log_console)


  logger.debug("Setting up directory structure")

  # LOCAL DIR : Local path for storing all K3 Applications, job files, executor, output, etc.
  LOCAL_DIR  = kwargs.get('local', '/k3/web')

  # SERVER URL : URL for serving static content. Defaults to using Flask as static handler via /fs/ endpoint
  SERVER_URL = kwargs.get('server', '/fs/')

  host   = kwargs.get('host', socket.gethostname())
  master = kwargs.get('master', None)

  JOBS_TARGET    = 'jobs'
  APPS_TARGET    = 'apps'
  ARCHIVE_TARGET = 'archive'
  BUILD_TARGET   = 'build'
  LOG_TARGET     = 'log'

  #  TODO: Either do away with this Flask_request  (python vers limitation)
  #       or simplify this
  APPS_DEST = os.path.join(LOCAL_DIR, APPS_TARGET)
  APPS_URL = os.path.join(SERVER_URL, APPS_TARGET)
  JOBS_DEST = os.path.join(LOCAL_DIR, JOBS_TARGET)
  JOBS_URL = os.path.join(SERVER_URL, JOBS_TARGET)
  ARCHIVE_DEST = os.path.join(LOCAL_DIR, ARCHIVE_TARGET)
  ARCHIVE_URL = os.path.join(SERVER_URL, ARCHIVE_TARGET)
  BUILD_DEST = os.path.join(LOCAL_DIR, BUILD_TARGET)
  BUILD_URL = os.path.join(SERVER_URL, BUILD_TARGET)
  LOG_DEST  = os.path.join(LOCAL_DIR, LOG_TARGET)

  #  Store dir structures in web context
  webapp.config['DIR']      = LOCAL_DIR
  webapp.config['PORT']     = port
  webapp.config['HOST']     = host
  webapp.config['ADDR']     = 'http://%s:%d' % (host, port)
  webapp.config['MESOS']    = master
  webapp.config['UPLOADED_APPS_DEST']     = APPS_DEST
  webapp.config['UPLOADED_APPS_URL']      = APPS_URL
  webapp.config['UPLOADED_JOBS_DEST']     = JOBS_DEST
  webapp.config['UPLOADED_JOBS_URL']      = JOBS_URL
  webapp.config['UPLOADED_ARCHIVE_DEST']  = ARCHIVE_DEST
  webapp.config['UPLOADED_ARCHIVE_URL']   = ARCHIVE_URL
  webapp.config['UPLOADED_BUILD_DEST']  = BUILD_DEST
  webapp.config['UPLOADED_BUILD_URL']   = BUILD_URL
  webapp.config['LOG_DEST']             = LOG_DEST
  webapp.config['COMPILE_OFF']  = not(kwargs.get('compile', False))

  # Create dirs, if necessary
  for p in [LOCAL_DIR, JOBS_TARGET, APPS_TARGET, ARCHIVE_TARGET, BUILD_TARGET, LOG_TARGET]:
    path = os.path.join(LOCAL_DIR, p)
    if not os.path.exists(path):
      os.mkdir(path)

  #  Configure rotating log file
  logfile = os.path.join(LOG_DEST, 'web.log')
  webapp.config['LOGFILE'] = logfile
  log_file = logging.handlers.RotatingFileHandler(logfile, maxBytes=2*1024*1024, backupCount=5, mode='w')
  log_file.setFormatter(log_fmt)
  logger.addHandler(log_file)

  logger.info("\n\n\n\n\n====================  <<<<< K3 >>>>> ===================================")
  logger.info("FLASK WEB Initializing:\n" +
    "    Host  : %s\n" % host +
    "    Port  : %d\n" % port +
    "    Master: %s\n" % master +
    "    Local : %s\n" % LOCAL_DIR +
    "    Server: %s\n" % SERVER_URL +
    "    Port  : %d\n" % port)


  # Configure Compilation Service
  if not webapp.config['COMPILE_OFF']:

    compileLogger = logging.getLogger("compiler")

    sio_fmt = ServiceFormatter('\n' + u'[%(asctime)s] %(message)s\n'.encode("utf8", errors='ignore'))
    sio = SocketIOHandler()
    sio.setFormatter(sio_fmt)
    compileLogger.addHandler(sio)

    compileFile = os.path.join(LOG_DEST, 'compiler.log')
    webapp.config['COMPILELOG'] = compileFile

    compile_fmt = ServiceFormatter(u'[%(asctime)s] %(message)s'.encode("utf8", errors='ignore'))
    compile_file = logging.handlers.RotatingFileHandler(compileFile, maxBytes=1024*1024, backupCount=5, mode='w')
    compile_file.setFormatter(compile_fmt)
    compileLogger.addHandler(compile_file)

    compileLogger.info("\n\n\n\n\n====================  <<<<< Compiler Service Initiated >>>>> ===================================")

    # Check for executor(s), build if necessary
    compiler_exec = os.path.join(LOCAL_DIR, 'CompileExecutor.py')
    if not os.path.exists(compiler_exec):
      logger.info("Compiler executor not found. Copying to web root dir.")
      shutil.copyfile('CompileExecutor.py', compiler_exec)

  launcher_exec = os.path.join(LOCAL_DIR, 'k3executor')
  if not os.path.exists(launcher_exec):
    logger.info("K3 Executor not found. Making & Copying to web root dir.")
    os.chdir('executor')
    os.system('cmake .')
    os.system('make')
    shutil.copyfile('k3executor', launcher_exec)
    os.chdir('..')


def returnError(msg, errcode):
  """
    returnError -- Helper function to format & return error messages & codes
  """
  logger.warning("[FLASKWEB] Returning error code %d, `%s`" % (errcode, msg))
  if request.headers['Accept'] == 'application/json':
    return msg, errcode
  else:
    return render_template("error.html", message=msg, code=errcode)


def shutdown_server():
    global dispatcher
    logging.warning ("[FLASKWEB] Attempting to kill the server")
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError("Not running the server")
    func()
    webserverTerminate.set()
    dispatcher.terminate = True

def shutdown_dispatcher():
    # driver.stop()
    # driver_t.join() 
    for k, v in compile_tasks.items():
      v.kill()
    logging.info ("[FLASKWEB] Detached from Mesos")
    dispatcherTerminate.set()



#===============================================================================
#   General Web Service End Points
#===============================================================================
@webapp.route('/')
def root():
  """
  #------------------------------------------------------------------------------
  #  / - Home (welcome msg)
  #------------------------------------------------------------------------------
  """
  if request.headers['Accept'] == 'application/json':
    return "Welcome\n\n", 200
  else:
    return redirect(url_for('index'))


@webapp.route('/index')
def index():
  if request.headers['Accept'] == 'application/json':
    return "Welcome\n\n", 200
  else:
    return render_template('index.html')


@webapp.route('/about')
def about():
  """
  #------------------------------------------------------------------------------
  #  /about - Display about page
  #------------------------------------------------------------------------------
  """
  logger.debug('[FLASKWEB  /about]')
  if request.headers['Accept'] == 'application/json':
    return redirect(url_for('staticFile', filename="rest.txt"))
  else:
    return render_template('about.html')


@webapp.route('/restapi')
def restapi():
  """
  #------------------------------------------------------------------------------
  #  /restapi - Display complete list of API EndPoints
  #------------------------------------------------------------------------------
  """
  logger.debug('[FLASKWEB  /restapi] API Reference request')
  if request.headers['Accept'] == 'application/json':
    return redirect(url_for('staticFile', filename="rest.txt"))
  else:
    with open('static/rest.txt') as restapi:
      rest = restapi.read()
      return render_template('rest.html', api=rest)


@webapp.route('/log')
def getLog():
  """
  #------------------------------------------------------------------------------
  #  /log - Displays current log file
  #------------------------------------------------------------------------------
  """
  with open(webapp.config['LOGFILE'], 'r') as logfile:
    output = logfile.read()
  if request.headers['Accept'] == 'application/json':
    return output, 200
  else:
    return render_template("output.html", output=output)


@webapp.route('/trace')
def trace():
  """
  #------------------------------------------------------------------------------
  #  /trace - Debugging respose. Returns the client's HTTP request data in json
  #------------------------------------------------------------------------------
  """
  logger.debug('[FLASKWEB  /trace] Trace debug request')
  output = {}
  output['args'] = request.args
  output['form'] = request.form
  output['method'] = request.method
  output['url'] = request.url
  output['client_ip']  = request.remote_addr
  output['headers'] = {k: str(v) for k,v in request.headers.items()}
  return jsonify(output), 200


@webapp.route('/restart')
def restart():
    """
    #------------------------------------------------------------------------------
    #  /restart - Restart the K3 Dispatch Service (kills/cleans all running tasks)
    #------------------------------------------------------------------------------
    """
    logging.warning ("[FLASKWEB] Shutting down K3 Dispatcher....")
    shutdown_dispatcher()
    return 'Dispatcher is restarting.....Give me a millisec'



@webapp.route('/kill')
def shutdown():
    """
    #------------------------------------------------------------------------------
    #  /kill - Kill the server  (TODO: Clean this up)
    #------------------------------------------------------------------------------
    """
    logging.warning ("[FLASKWEB] Shutting down Flask Web Server....")
    shutdown_server()
    logging.warning ("[FLASKWEB] Shutting down K3 Dispatcher....")
    shutdown_dispatcher()

    haltFlag.set()
    return 'Server is going down...'



#===============================================================================
#   Static Content Service
#===============================================================================
@webapp.route('/fs/<path:path>/')
def staticFile(path):
  """
  #------------------------------------------------------------------------------
  #  /fs - File System Exposure for the local webroot folder
  #        Note: Direct file access via curl should include a trailing slash (/) 
  #           Otherwise, you will get a 302 redirect to the actual file
  #------------------------------------------------------------------------------
  """
  logger.info('[FLASKWEB  /fs] Static File Request for `%s`' % path)
  local = os.path.join(webapp.config['DIR'], path)
  if not os.path.exists(local):
    return returnError("File not found: %s" % path, 404)
  if os.path.isdir(local):
    contents = sorted(os.listdir(local))
    for i, f in enumerate(contents):
      if os.path.isdir(f):
        contents[i] += '/'

    if request.headers['Accept'] == 'application/json':
      return jsonify(dict(cwd=local, contents=contents)), 200
    else:
      return render_template('listing.html', cwd=path, listing=contents), 200

  else:
    if 'stdout' in local or 'output' in local or local.split('.')[-1] in ['txt', 'yaml', 'yml', 'json', 'log']:
      with open(local, 'r') as file:
        # output = unicode(file.read(), 'utf-8')
        output = file.read()

        if request.headers['Accept'] == 'application/json':
          return output, 200
        else:
          return render_template("output.html", output=output)

    return send_from_directory(webapp.config['DIR'], path)


#===============================================================================
#   Application End Points
#===============================================================================
@webapp.route('/app', methods=['GET', 'POST'])
def uploadAppRedir():
  """
    Redirect to uploadApp (/apps)
  """
  logger.debug('[FLASKWEB  /app] Redirect to /apps')
  return uploadApp()

@webapp.route('/apps', methods=['GET', 'POST'])
def uploadApp():
  """
  #------------------------------------------------------------------------------
  #  /apps, /app - Application Level interface    
  #     POST   Upload new application
  #       curl -i -X POST -H "Accept: application/json" 
  #           -F file=@<filename> http://<host>:<port>/apps
  #
  #     GET    Display both list of loaded apps and form to upload new ones
  #
  #     /app will redirect to /apps
  #------------------------------------------------------------------------------
  """
  if request.method == 'POST':
      logger.debug("[FLASKWEB  /apps] POST request to upload new application")
      file = request.files['file']
      if file:
          name = secure_filename(file.filename)
          path = os.path.join(webapp.config['UPLOADED_APPS_DEST'], name)
          if not os.path.exists(path):
            os.mkdir(path)
          uid = getUID() if 'uid' not in request.form or not request.form['uid'] else request.form['uid']
          path =  os.path.join(path, uid)
          if not os.path.exists(path):
            os.mkdir(path)
          fullpath = os.path.join(path, name)
          file.save(fullpath)

          jobdir = os.path.join(webapp.config['UPLOADED_JOBS_DEST'], name)
          if not os.path.exists(jobdir):
            os.mkdir(jobdir)
          # hash = hashlib.md5(open(fullpath).read()).hexdigest()

          hash = request.form['tag'] if 'tag' in request.form else ''

          # if (db.checkHash(hash)):
          db.insertApp(dict(uid=uid, name=name, hash=hash))
          logger.info("[FLASKWEB] Added new application: `%s`, uid=`%s`", name, uid)
          #TODO:  Complete with paths for archive & app

          if request.headers['Accept'] == 'application/json':
              output = dict(name=name, uid=uid, status='SUCCESS', greeting='Thank You!')
              return jsonify(output), 200
          else:
              return redirect(url_for('uploadApp'))

  logger.debug('[FLASKWEB  /apps] GET request for list of apps')
  applist = db.getAllApps()
  versions = {a['name']: db.getVersions(a['name'], limit=5) for a in applist}

  # TODO: Add 2nd link on AppList: 1 to launch latest, 1 to show all versions
  if request.headers['Accept'] == 'application/json':
      return jsonify(dict(apps=applist)), 200
  else:
      return render_template('apps.html', applist=applist, versions=versions)


@webapp.route('/app/<appName>')
def getAppRedir(appName):
  """
    Redirect to uploadApp (/apps/<appName>)
  """
  logger.debug('[FLASKWEB  /app/<appName>] Redirect to /apps/%s' % appName)
  return getApp(appName)

@webapp.route('/apps/<appName>')
def getApp(appName):
  """
  #------------------------------------------------------------------------------
  #  /apps/<appName> - Specific Application Level interface
  #         GET    Display all versions for given application
  #------------------------------------------------------------------------------
  """
  logger.debug('[FLASKWEB  /apps/<appName>] GET request for app, `%s`' % appName)
  applist = [a['name'] for a in db.getAllApps()]
  if appName in applist:
    versionList = db.getVersions(appName)
    if request.headers['Accept'] == 'application/json':
      return jsonify(dict(name=appName, versions=versionList)), 200
    else:
      return render_template("apps.html", name=appName, versionList=versionList)
  else:
    return returnError("Application %s does not exist" % appName, 404)


@webapp.route('/app/<appName>/<appUID>', methods=['GET', 'POST'])
def archiveAppRedir(appName, appUID):
  """
    Redirect to archiveApp
  """
  logger.debug('[FLASKWEB  /app/<appName>/<appUID>] Redirec to /apps/%s/%s' 
      % (appName, appUID))
  return archiveApp(appName, appUID)


@webapp.route('/apps/<appName>/<appUID>', methods=['GET', 'POST'])
def archiveApp(appName, appUID):
  """
  #------------------------------------------------------------------------------
  #  /apps/<appName>/<appUID> - Specific Application Level interface
  #         POST   (Upload archive data (C++, Source, etc....)
  #            curl -i -X POST -H "Accept: application/json"
  #                    -F "file=<filename>" http://qp1:5000/apps/<addName>/<addUID>
  #
  #         GET    (TODO) Display archived files...  NotImplemented
  #------------------------------------------------------------------------------
  """
  logger.debug('[FLASKWEB  /app/<appName>/<appUID>] %s Request for App Archive `%s`, UID=`%s`' % (request.method, appName, appUID))
  applist = [a['name'] for a in db.getAllApps()]
  uname = AppID.getAppId(appName, appUID)

  # if appName not in applist:
  #   logger.warning("Archive request for app that does not exist: %s", appName)
  #   return returnError("Application %s does not exist" % appName, 404)

  if request.method == 'POST':
      file = request.files['file']
      if file:
          filename = secure_filename(file.filename)
          path = os.path.join(webapp.config['UPLOADED_BUILD_DEST'], uname).encode(encoding='utf8', errors='ignore')
          logger.debug("Archiving file, %s, to %s" % (filename, path))
          if not os.path.exists(path):
            os.mkdir(path)
          file.save(os.path.join(path, filename))
          return "File Uploaded & archived\n", 202
      else:
          logger.warning("Archive request, but no file provided.")
          return "No file received\n", 400

  elif request.method == 'GET':
    path = os.path.join(webapp.config['UPLOADED_BUILD_URL'], uname)
    return redirect(path, 302)


@webapp.route('/delete/app/<appName>', methods=['POST'])
def deleteApp(appName):
  """
  #------------------------------------------------------------------------------
  #  /delete/app/<appName>
  #     POST     Deletes an app from the web server 
  #         NOTE: Data files will remain in webroot on the server, but
  #           the app will be inaccessible through the interface
  #           (metadata is removed from the internal db)
  #------------------------------------------------------------------------------
  """
  logger.debug('[FLASKWEB  /delete/app/<appName>] Request to delete App `%s`', appName)
  applist = [a['name'] for a in db.getAllApps()]
  if appName not in applist:
    return returnError("Application %s does not exist" % appName, 404)

  logger.info("[FLASKWEB]  DELETING all versions of app, `%s`")
  db.deleteAllApps(appName)

  if request.headers['Accept'] == 'application/json':
    return jsonify(dict(app=appName, status='DELETED, files remain on server')), 200
  else:
    applist = db.getAllApps()
    versions = {a['name']: db.getVersions(a['name'], limit=5) for a in applist}
    return render_template('apps.html', applist=applist, versions=versions)


#===============================================================================
#   Job End Points
#===============================================================================
@webapp.route('/job')
def listJobsRedir():
  """
    Redirect to listJobs
  """
  logger.debug('[FLASKWEB  /job] Redirecting to /jobs')
  return listJobs()

@webapp.route('/jobs')
def listJobs():
  """
  #------------------------------------------------------------------------------
  #  /jobs - Current runtime & completed job Interface
  #         GET    Display currently executing & recently completed jobs
  #------------------------------------------------------------------------------
  """
  logger.debug('[FLASKWEB  /jobs] Request for job listing')
  jobs = db.getJobs()
  for job in jobs:
    job['time'] = datetime.datetime.strptime(job['time'], db.TS_FMT).replace(tzinfo=db.timezone('UTC')).isoformat()

  #  Garbage Collect Orpahened jobs
  compiles = db.getCompiles()
  for compile in compiles:
    if compile['submit']:
      compile['submit'] = datetime.datetime.strptime(compile['submit'], db.TS_FMT).replace(tzinfo=db.timezone('UTC')).isoformat()
    if compile['complete']:
      compile['complete'] = datetime.datetime.strptime(compile['complete'], db.TS_FMT).replace(tzinfo=db.timezone('UTC')).isoformat()
  # for c in compiles:
  #   if c['uid'] not in compile_tasks.keys():
  #     db.updateCompile(c['uid'], status='KILLED', done=True)
  # compiles = db.getCompiles()

  if request.headers['Accept'] == 'application/json':
    return jsonify(dict(LaunchJobs=jobs, CompilingJobs=compiles)), 200
  else:
    return render_template("jobs.html", joblist=jobs, compilelist=compiles)


@webapp.route('/jobs/<appName>', methods=['GET', 'POST'])
def createJobLatest(appName):
  """
    Redirect createJob using the latest uploaded application version
  """
  logger.debug('[FLASKWEB  /jobs/<appName>] Redirect to current version of /jobs/%s' % appName)
  app = db.getApp(appName)
  if app:
    return createJob(appName, app['uid'])
  else:
    return returnError("Application %s does not exist" % appName, 404)
    
@webapp.route('/jobs/<appName>/<appUID>', methods=['GET', 'POST'])
def createJob(appName, appUID):
  """
  #------------------------------------------------------------------------------
  #  /jobs/<appName>
  #  /jobs/<appName>/<appUID  - Launch a new K3 Job
  #         POST    Create new K3 Job
  #          curl -i -X POST -H "Accept: application/json" 
  #             -F "file=@<rolefile>" 
  #             -F logging=[True | False]
  #             -F jsonlog=[True | False]
  #             -F jsonfinal=[True | False]
  #             -F http://<host>:<port>/jobs/<appName>/<appUID>
  #             NOTE: if appUID is omitted, job will be submitted to latest version of this app
  #
  #         GET    Display job list for this application
  #------------------------------------------------------------------------------
  """
  logger.debug('[FLASKWEB  /jobs/<appName>/<appUID>] Job Request for %s' % appName)
  global dispatcher
  applist = [a['name'] for a in db.getAllApps()]
  if appName in applist:
    if request.method == 'POST':
        logger.debug("POST Request for a new job")
        # TODO: Get user
        file = request.files['file']
        text = request.form['text'] if 'text' in request.form else None
        k3logging = True if 'logging' in request.form else False
        jsonlog = True if 'jsonlog' in request.form else False
        jsonfinal = True if 'jsonfinal' in request.form else False
        stdout = request.form['stdout'] if 'stdout' in request.form else False
        user = request.form['user'] if 'user' in request.form else 'anonymous'
        tag = request.form['tag'] if 'tag' in request.form else ''

        # User handling: jsonfinal is a qualifier flag for the json logging flag
        if jsonfinal and not jsonlog:
          jsonlog = True

        logger.debug("K3 LOGGING is :  %s" %  ("ON" if k3logging else "OFF"))
        logger.debug("JSON LOGGING is :  %s" %  ("ON" if jsonlog else "OFF"))
        logger.debug("JSON FINAL LOGGING is :  %s" %  ("ON" if jsonfinal else "OFF"))
        # trials = int(request.form['trials']) if 'trials' in request.form else 1

        # Check for valid submission
        if not file and not text:
          logger.error('Error. Cannot create job: No input file and no YAML for enviornment configuration provided. ')
          return render_template("error.html", code=404, message="Invalid job request")

        # Post new job request, get job ID & submit time
        thisjob = dict(appName=appName, appUID=appUID, user=user, tag=tag)
        jobId, time = db.insertJob(thisjob)
        thisjob = dict(jobId=jobId, time=time)

        # Save yaml to file (either from file or text input)
        path = os.path.join(webapp.config['UPLOADED_JOBS_DEST'], appName)
        if not os.path.exists(path):
          os.mkdir(path)

        path = os.path.join(path, str(jobId))
        filename = 'role.yaml'
        if not os.path.exists(path):
          os.mkdir(path)

        if file:
            file.save(os.path.join(path, filename))
        else:
            with open(os.path.join(path, filename), 'w') as file:
              file.write(text)

        # Create new Mesos K3 Job
        apploc = webapp.config['ADDR']+os.path.join(webapp.config['UPLOADED_APPS_URL'], appName, appUID, appName)

        newJob = Job(binary=apploc, appName=appName, jobId=jobId,
                     rolefile=os.path.join(path, filename), logging=k3logging, 
                     jsonlog=jsonlog, jsonfinal=jsonfinal)

        # Submit to Mesos
        dispatcher.submit(newJob)

        thisjob = dict(thisjob, url=dispatcher.getSandboxURL(jobId), status='SUBMITTED')



        if 'application/json' in request.headers['Accept']:
          return jsonify(thisjob), 202
        else:
          return render_template('last.html', appName=appName, lastjob=thisjob)

    elif request.method == 'GET':
      jobs = db.getJobs(appName=appName)
      if 'application/json' in request.headers['Accept']:
        return jsonify(dict(jobs=jobs))
      else:
        preload = request.args['preload'] if 'preload' in request.args else 'Sample'
        logger.debug("YAML file preload = %s" % preload)
        if preload == 'Instructions':
            yamlFile = 'role_file_template.yaml'
        elif preload == 'Last':
            lastJobId = max([ d['jobId'] for d in jobs])
            path = os.path.join(webapp.config['UPLOADED_JOBS_DEST'], appName, "%d" % lastJobId, 'role.yaml')
            logger.debug("   YAML PATH= %s" % path)
            if os.path.exists(path):
              yamlFile = path
            else:
              yamlFile = None
        else:
          yamlFile = 'sample.yaml'
        if yamlFile:
          with open(yamlFile, 'r') as f:
            sample = f.read()
        else:
          sample = "(No YAML file to display)"
        return render_template("newjob.html", name=appName, uid=appUID, sample=sample)

  else:
    return returnError("There is no application, %s" % appName, 404)


@webapp.route('/job/<jobId>')
def getJobRedir(jobId):
    """
      Redirect to getJob
    """
    jobs = db.getJobs(jobId=jobId)
    job = None if len(jobs) == 0 else jobs[0]
    if job == None:
      return returnError("Job ID, %s, does not exist" % jobId, 404)
    appName = job['appName']
    return getJob(appName, jobId)

@webapp.route('/jobs/<appName>/<jobId>/status')
def getJob(appName, jobId):
    """
    #------------------------------------------------------------------------------
    #  /jobs/<appName>/<jobId>/status - Detailed Job info
    #         GET     Display detailed job info  (default for all methods)
    #------------------------------------------------------------------------------
    """
    jobs = db.getJobs(jobId=jobId)
    job = None if len(jobs) == 0 else jobs[0]
    k3job = dispatcher.getJob(int(jobId))

    if job == None:
      return returnError("Job ID, %s, does not exist" % jobId, 404)

    thisjob = dict(job, url=dispatcher.getSandboxURL(jobId))
    if k3job != None:
      thisjob['master'] = k3job.master
    local = os.path.join(webapp.config['UPLOADED_JOBS_DEST'], appName, str(jobId)).encode(encoding='utf8', errors='ignore')
    path = os.path.join(webapp.config['UPLOADED_JOBS_DEST'], appName, str(jobId),'role.yaml').encode(encoding='utf8', errors='ignore')
    if os.path.exists(local) and os.path.exists(path):
      with open(path, 'r') as role:
        thisjob['roles'] = role.read()
    else:
      return returnError("Job Data no longer exists", 400)

    thisjob['sandbox'] = sorted (os.listdir(local))

    if 'application/json' in request.headers['Accept']:
      return jsonify(thisjob)
    else:
      return render_template("last.html", appName=appName, lastjob=thisjob)


@webapp.route('/job/<jobId>/replay')
def replayJobRedir(jobId):
    """
      Redirect to replayJob
    """
    jobs = db.getJobs(jobId=jobId)
    job = None if len(jobs) == 0 else jobs[0]
    if job == None:
      return returnError("Job ID, %s, does not exist" % jobId, 404)
    appName = job['appName']
    logging.info ("[FLASKWEB] REPLAYING JOB # %s" % jobId)
    return replayJob(appName, jobId)

@webapp.route('/jobs/<appName>/<jobId>/replay', methods=['GET', 'POST'])
def replayJob(appName, jobId):
  """
  #------------------------------------------------------------------------------
  #  /jobs/<appName>/<appUID/replay  - Replay a previous K3 Job
  #         POST    Create new K3 Job
  #          curl -i -X POST -H "Accept: application/json" http://<host>:<port>/jobs/<appName>/<appUID>/replay
  #------------------------------------------------------------------------------
  """
  global dispatcher
  joblist = db.getJobs(jobId=jobId)
  oldjob = None if len(joblist) == 0 else joblist[0]
  if oldjob:
      logger.info("[FLASKWEB] REPLAYING %s" % jobId),
      # Post new job request, get job ID & submit time
      thisjob = dict(appName=oldjob['appName'],
                     appUID=oldjob['hash'],
                     user=oldjob['user'],
                     tag='REPLAY: %s' % oldjob['tag'])

      new_jobId, time = db.insertJob(thisjob)
      thisjob = dict(jobId=new_jobId, time=time)
      logging.info ("[FLASKWEB] new Replay JOBID: %s" % new_jobId),


      # Save yaml to file (either from file or text input)
      role_src = os.path.join(webapp.config['UPLOADED_JOBS_DEST'], appName, str(jobId), 'role.yaml').encode('utf8', 'ignore')
      path = os.path.join(webapp.config['UPLOADED_JOBS_DEST'], appName, str(new_jobId)).encode('utf8', 'ignore')
      os.mkdir(path)
      role_copy = os.path.join(path, 'role.yaml')
      shutil.copyfile(role_src, os.path.join(path, role_copy))

      # Create new Mesos K3 Job
      try:
        newJob = Job(binary=webapp.config['ADDR']+os.path.join(webapp.config['UPLOADED_APPS_URL'], appName, oldjob['hash'], appName),
                     appName=appName, jobId=new_jobId, rolefile=role_copy)
      except K3JobError as err:
        db.deleteJob(jobId)
        logger.error("JOB ERROR: %s" % err)
        return returnError(err.value, 400)

      logging.info ("[FLASKWEB] NEW JOB ID: %s" % newJob.jobId)

      # Submit to Mesos
      dispatcher.submit(newJob)
      thisjob = dict(thisjob, url=dispatcher.getSandboxURL(new_jobId), status='SUBMITTED')

      if 'application/json' in request.headers['Accept']:
          return jsonify(thisjob), 202
      else:
          return render_template('last.html', appName=appName, lastjob=thisjob)

  else:
    return returnError("There is no Job, %s\n" % jobId, 404)


@webapp.route('/job/<jobId>/archive', methods=['GET', 'POST'])
def archiveJobRedir(jobId):
    """
      Redirect to archiveJob
    """
    jobs = db.getJobs(jobId=jobId)
    job = None if len(jobs) == 0 else jobs[0]
    appName = job['appName']
    if job == None:
      return returnError("Job ID, %s, does not exist" % jobId, 404)
    return archiveJob(appName, jobId)


@webapp.route('/jobs/<appName>/<jobId>/archive', methods=['GET', 'POST'])
def archiveJob(appName, jobId):
    """"
    #------------------------------------------------------------------------------
    #  /jobs/<appName>/<jobId>/archive - Endpoint to receive & archive files
    #         GET     returns curl command
    #         POST    Accept files for archiving here
    #           curl -i -X POST -H "Accept: application/json"
    #              -F file=@<filename> http://<host>:<post>/<appName>/<jobId>/archive
    #------------------------------------------------------------------------------
    """
    job_id = str(jobId).encode('utf8', 'ignore')
    if job_id.find('.') > 0:
      job_id = job_id.split('.')[0]
    jobs = db.getJobs(jobId=job_id)
    job = None if len(jobs) == 0 else jobs[0]
    if job == None:
      return returnError ("Job ID, %s, does not exist" % job_id, 404)

    if request.method == 'POST':
        file = request.files['file']
        if file:
            filename = secure_filename(file.filename)
            path = os.path.join(webapp.config['UPLOADED_JOBS_DEST'], appName, job_id, filename).encode(encoding='utf8', errors='ignore')
            file.save(path)
            return "File Uploaded & archived", 202
        else:
            return "No file received", 400

    elif request.method == 'GET':
        return '''
Upload your file using the following CURL command:\n\n
   curl -i -X POST -H "Accept: application/json" -F file=@<filename> http://<server>:<port>/<appName>/<jobId>/archive
''', 200


@webapp.route('/job/<jobId>/kill', methods=['GET'])
def killRedir(jobId):
    """
      Redirect to killJob
    """
    jobs = db.getJobs(jobId=jobId)
    job = None if len(jobs) == 0 else jobs[0]
    appName = job['appName']
    if job == None:
      return returnError("Job ID, %s, does not exist" % jobId, 404)
    return killJob(appName, jobId)

@webapp.route('/jobs/<appName>/<jobId>/kill', methods=['GET'])
def killJob(appName, jobId):
    """
    #------------------------------------------------------------------------------
    #  /jobs/<appName>/<jobId>/kill - Job Interface to cancel a job
    #         GET     Kills a Job (if orphaned, updates status to killed)
    #           curl -i -H "Accept: application/json" http://qp1:5000/jobs/<appName>/<jobId>/kill
    #------------------------------------------------------------------------------
    """
    jobs = db.getJobs(jobId=jobId)
    job = None if len(jobs) == 0 else jobs[0]

    if job == None:
      return returnError ("Job ID, %s, does not exist" % jobId, 404)

    logging.info ("[FLASKWEB] Asked to KILL job #%s. Current Job status is %s" % (jobId, job['status']))
    # Separate check to kill orphaned jobs in Db
    # TODO: Merge Job with experiments to post updates to correct table
    if job['status'] == 'RUNNING' or job['status'] == 'SUBMITTED':
      db.updateJob(jobId, status='KILLED')

    if int(jobId) in dispatcher.getActiveJobs():
      status = 'KILLED'
      logging.debug('[FLASKWEB] Job %s is active. Signaling to kill in mesos.' % jobId)
      dispatcher.cancelJob(int(jobId), driver)
    else:
      status = 'ORPHANED and CLEANED'
      logging.debug('[FLASKWEB] Job # %s is ORPHANED and does not exist in current state. Cleaning up.' % jobId)

    ts = db.getTS_est()  #datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    thisjob = dict(jobId=jobId, time=ts, url=dispatcher.getSandboxURL(jobId), status=status)
    if 'application/json' in request.headers['Accept']:
      return jsonify(thisjob)
    else:
      return render_template("last.html", appName=appName, lastjob=thisjob)

@webapp.route('/delete/jobs', methods=['POST'])
def deleteJobs():
  """
  #------------------------------------------------------------------------------
  #  /delete/jobs
  #         POST     Deletes list of K3 jobs
  #------------------------------------------------------------------------------
  """
  deleteList = request.form.getlist("delete_job")
  for jobId in deleteList:
    job = db.getJobs(jobId=jobId)[0]
    path = os.path.join(webapp.config['UPLOADED_JOBS_DEST'], job['appName'], jobId)
    shutil.rmtree(path, ignore_errors=True)
    db.deleteJob(jobId)
  return redirect(url_for('listJobs')), 302

#------------------------------------------------------------------------------
#  /jobs/<appName>/<jobId>/stdout - Job Interface for specific job
#         GET     TODO: Consolidate STDOUT for current job (from all tasks)
#         POST    TODO: Accept STDOUT & append here (if desired....)
#------------------------------------------------------------------------------
# @webapp.route('/jobs/<appName>/<jobId>/stdout', methods=['GET'])
# def stdout(appName, jobId):
#     jobs = db.getJobs(appName=appName)
#     link = resolve(MASTER)
#     print link
#     sandbox = dispatcher.getSandboxURL(jobId)
#     if sandbox:if 
#       print sandbox
#       return '<a href="%s">%s</a>' % (sandbox, sandbox)
#     else:
#       return 'test'



#===============================================================================
#   Compile End Points
#===============================================================================
@webapp.route('/compileservice')
def compService():
  """
  #------------------------------------------------------------------------------
  #  /compileservice
  #         GET       Return compile service status
  #------------------------------------------------------------------------------
  """
  return jsonify(compileService.getItems())


@webapp.route('/compileservice/check')
def compServiceCheck():
  """
  #------------------------------------------------------------------------------
  #  /compileservice/check
  #         GET       Quick check for status
  #------------------------------------------------------------------------------
  """
  # global compileService
  return compileService.state.name

@webapp.route('/compileservice/up', methods=['GET', 'POST'])
def compServiceUp():
  """
  #------------------------------------------------------------------------------
  #  /compileservice/up
  #        POST     Starts compile service
  #           curl -i -X POST -H "Accept: application/json"
  #                   -F numworkers=<numworkers> 
  #                   -F gitpull=[True|False]
  #                   -F branch=<k3_branch>
  #                   -F cabalbuild=[True|False] 
  #                   -F m_workerthread=<#master_service_threads> 
  #                   -F w_workerthread=<#worker_service_threads> 
  #                   -F heartbeat=<heartbeat_interval_in_secs> 
  #                   http://<host>:<port>/compile
  #
  #    Default vals:  numworkers=(max workers), gitpull=True, 
  #                   branch=development, cabalbuild=False, 
  #                   m_workerthread=1, w_workerthread=1, 
  #                   heartbeat=300, cppthread=12 

  #
  #        GET     Redirect to Compile Form (html) or compile service setting (json)
  #------------------------------------------------------------------------------
  """
  global compileService
  if request.method == 'POST' and compileService.isDown():
    settings = dict(webaddr=webapp.config['ADDR'])

    settings['branch'] = request.form.get('branch', 'development')
    gitpull = request.form.get('gitpull', True)
    settings['gitpull'] = gitpull if isinstance(gitpull, bool) else (gitpull.upper() == 'TRUE')
    cabalbuild = request.form.get('cabalbuild', False)
    settings['cabalbuild'] = cabalbuild if isinstance(cabalbuild, bool) else (cabalbuild.upper() == 'TRUE')

    if 'm_workerthread' in request.form:
      settings['m_workerthread'] = request.form['m_workerthread']

    if 'w_workerthread' in request.form:
      settings['w_workerthread'] = request.form['w_workerthread']

    if 'heartbeat' in request.form:
      settings['heartbeat'] = request.form['heartbeat']

    # if 'cppthread' in request.form:
    #   settings['cppthread'] = request.form['cppthread']


    logger.debug ("[FLASKWEB] User GIT PULL Request    (?): %s" % str(settings['gitpull']))
    logger.debug ("[FLASKWEB] User CABAL BUILD Request (?): %s" % str(settings['cabalbuild']))

    # TODO: Set Worker Nodes here (dynamic allocation of workers)
    if 'numworkers' not in request.form or request.form['numworkers'] == '':
      settings['numworkers'] = len(workerNodes)
    else:
      settings['numworkers'] = int(request.form['numworkers'])

    compileService.update(settings)
    compileService.goUp(settings['numworkers'])

  if request.headers['Accept'] == 'application/json':
    return jsonify(compileService.getItems()), 200
  else:
    return render_template("compile.html", status=compileService.state.name)    



@webapp.route('/compileservice/stop')
def compServiceStop():
  """
  #------------------------------------------------------------------------------
  #  /compileservice/stop
  #         GET     Stop compile service Immediately
  #
  #       NOTE:  Be careful, it kills all jobs (There is no confirmation check)
  #------------------------------------------------------------------------------
  """
  global compileService
  compileService.goDown()

  if request.headers['Accept'] == 'application/json':
    return jsonify(compileService.getItems()), 200
  else:
    return render_template("compile.html", status=compileService.state.name)    


@webapp.route('/compileservice/down')
def compServiceDown():
  """
  #------------------------------------------------------------------------------
  #  /compileservice/down
  #         GET     Shuts down compile service gracefully
  #
  #       NOTE:  Clears all pendings tasks
  #------------------------------------------------------------------------------
  """
  global compileService
  compileService.goDownGracefully()

  if request.headers['Accept'] == 'application/json':
    return jsonify(compileService.getItems()), 200
  else:
    return render_template("compile.html", status=compileService.state.name)



#===============================================================================
#   Compile End Points
#===============================================================================
@webapp.route('/compile', methods=['GET', 'POST'])
def compile():
    global lastCompile
    """
    #------------------------------------------------------------------------------
    #  /compile
    #        POST    Submit new K3 Compile task
    #           curl -i -X POST -H "Accept: application/json"
    #                   -F name=<appName>  
    #                   -F file=@<sourceFile> 
    #                   -F blocksize=<blocksize> 
    #                   -F compilestage=['both'|'cpp'|'bin']
    #                   -F compileargs=<compile_args>
    #                   -F mem=<mem_in_GB>
    #                   -F cpu=<#cores>
    #                   -F workload=['balanced'|'moderate'|'moderate2'|'extreme']
    #                   -F user=<userName> http://<host>:<port>/compile
    #
    #    NOTE:  -user & compileargs are optional. 
    #           -If name is omitted, it is inferred from filename
    #    Default vals:  blocksize=8, compilestage='both', workload='balanced'
    #
    #         GET     Form for compiling new K3 Executable OR status of compiling tasks
    #------------------------------------------------------------------------------
    """
    if webapp.config['COMPILE_OFF']:
      logger.warning("Compilation requested, but denied (Feature not turned on)")
      return returnError("Compilation Features are not available", 400)

    logging.debug("[FLASKWEB /compile]   REQUEST ")

    if request.method == 'POST':

      if not compileService.isUp():
        logger.warning("Compilation requested, but denied (Compiler Service not ready)")
        return returnError("Compiler Service is not running. Ensure it is up before initiating a compilation task.", 400)

      if compileService.gracefulHalt:
        logger.warning("Compilation requested, but denied (Compiler Service is flagged to shut down, gracefully)")
        return returnError("Sorry. Your compilation request is denied because the Compiler Service is flagged to shut down, gracefully.", 400)


      file = request.files['file']
      text = request.form.get('text', None)
      name = request.form.get('name', None)

      # Create a unique ID
      uid = getUID()

      # Set default settings for a Compile Job
      # TODO: Update Settings
      settings = compileService.getItems()

      # update settings & error check where necessary
      settings['compileargs'] = request.form.get('compileargs', settings['compileargs'])
      settings['user'] = request.form.get('user', settings['user'])
      settings['tag'] = request.form.get('tag', settings['tag'])

      workload = request.form.get('workload', '').lower()

      if settings['compileargs'] == '':
        settings['compileargs'] = workloadOptions[workload]

      mem = int(request.form['mem']) if 'mem' in request.form else None
      cpu = int(request.form['cpu']) if 'cpu' in request.form else None

      stage = request.form.get('compilestage', settings['compilestage'])
      if stage not in ['both', 'cpp', 'bin']:
        return returnError("Invalid Input on key `compilestage`. Valid entries are ['both', 'cpp', 'bin']", 400)
      else:
        settings['compilestage'] = stage #getCompileStage(stage).value

      blocksize = request.form.get('blocksize', settings['blocksize'])
      if isinstance(blocksize, int):
        settings['blocksize'] = blocksize
      elif blocksize.isdigit():
        settings['blocksize'] = int(blocksize)

      # Determine application name (for pass-thru naming)
      if not name:
        if file:
          srcfile = secure_filename(file.filename)
          name = srcfile.split('.')[0]
        else:
          return returnError("No name provided for K3 program", 400)

      app = AppID(name, uid)
      uname = '%s-%s' % (name, uid)
      path = os.path.join(webapp.config['UPLOADED_BUILD_DEST'], uname).encode(encoding='utf8', errors='ignore')

      # Save K3 source to file (either from file or text input)
      settings['source'] = ('%s.k3' % name)
      settings['webaddr'] = webapp.config['ADDR']
      if not os.path.exists(path):
        os.mkdir(path)

      if file:
          file.save(os.path.join(path, settings['source']))
      else:
          file = open(os.path.join(path, settings['source']).encode(encoding='utf8', errors='ignore'), 'w')
          file.write(text)
          file.close()

      # Create Symlink for easy access to latest compiled task
      link = os.path.join(webapp.config['UPLOADED_BUILD_DEST'], name).encode(encoding='utf8', errors='ignore')
      if os.path.exists(link):
        os.remove(link)

      os.symlink(uname, link)
      url = os.path.join(webapp.config['UPLOADED_BUILD_URL'], uname).encode(encoding='utf8', errors='ignore')

      job = CompileJob(name, uid, path, settings, mem=mem, cpu=cpu)
      lastCompile = job.getItems()

      logger.info("[FLASKWEB] Submitting Compilation job for " + name)
      compileService.submit(job)

      # Prepare results to send back to the user
      outputurl = "/compile/%s" % uname
      cppsrc = os.path.join(webapp.config['UPLOADED_BUILD_URL'], uname, settings['source'])

      thiscompile = dict(lastCompile, url=dispatcher.getSandboxURL(uname),
                         status='SUBMITTED', outputurl=outputurl, cppsrc=cppsrc,uname=uname)


      # Return feedback to user
      if request.headers['Accept'] == 'application/json':
        return jsonify(thiscompile), 200
      else:
        return render_template("last.html", appName=name, lastcompile=thiscompile, status=compileService.state.name)

    else:
      # TODO: Return list of Active/Completed Compiling Tasks
      if request.headers['Accept'] == 'application/json':
        return ('TO SUBMIT A NEW COMPILATION:\n\n\tcurl -i -X POST -H "Accept: application/json" \
-F name=<appName> -F file=@<sourceFile> -F blocksize=<blocksize> -F compilestage=<compilestage> \
http://<host>:<port>/compile' if compileService.isUp() 
          else 'START THE SERVICE: curl -i -H "Accept: application/json" http://<host>:<port>/compileservice/up')

      else:
        return render_template("compile.html", status=compileService.state.name)


def getCompilerOutput(uname):
    """
      Retrieves the compiler output from local file
    """
    fname = os.path.join(webapp.config['UPLOADED_BUILD_DEST'], uname, 'output').encode('utf8')
    if os.path.exists(fname):
      stdout_file = open(fname, 'r')
      output = unicode(stdout_file.read(), 'utf-8')
      stdout_file.close()
      return output
    else:
      return returnError("Output not available for " + uname, 404)


@webapp.route('/compile/<uid>', methods=['GET'])
def getCompile(uid):
  """
  #------------------------------------------------------------------------------
  #  /compile/<uid>
  #         GET     displays STDOUT & STDERR consolidated output for compile task
  #------------------------------------------------------------------------------
  """
  if webapp.config['COMPILE_OFF']:
    return returnError("Compilation Features are not available", 400)

  logger.debug("[FLASKWEB] Retrieving last compilation status")
  
  result = db.getCompiles(uid=uid)
  if len(result) == 0:
    result = db.getCompiles(uid=AppID.getUID(uid))

  if len(result) == 0:
    return returnError("No output found for compilation, %s\n\n" % uid, 400)
  else:
    output = result[0]
    output['uname'] = AppID.getAppId(output['name'], output['uid'])
    local = os.path.join(webapp.config['UPLOADED_BUILD_DEST'], output['uname'])
    output['sandbox'] = sorted (os.listdir(local))

    if request.headers['Accept'] == 'application/json':
      return jsonify(output), 200
    else:
      return render_template("last.html", lastcompile=output)



@webapp.route('/compilestatus')
def getCompileStatus():
  """
  #------------------------------------------------------------------------------
  #  /compilestatus - Short list of active jobs & current statuses
  #------------------------------------------------------------------------------
  """
  logger.debug("[FLASKWEB] Retrieving current active compilation status")

  jobs = compileService.getActiveState()
  title = "Active Compiling Tasks" if jobs else "NO Active Compiling Jobs"

  if request.headers['Accept'] == 'application/json':
    return jsonify(jobs), 200
  else:
    return render_template("keyvalue.html", title=title, store=jobs)


@webapp.route('/compilelog')
def getCompileLog():
  """
  #------------------------------------------------------------------------------
  #  /compilelog - Connects User to compile log websocket
  #------------------------------------------------------------------------------
  """
  if webapp.config['COMPILE_OFF']:
    return returnError("Compilation Features are not available", 400)

  logger.debug("[FLASKWEB] Connecting user to Compile Log WebSocket")

  with open(webapp.config['COMPILELOG'], 'r') as logfile:
    output = logfile.read().split('<<<<< Compiler Service Initiated >>>>>')[-1]

  if request.headers['Accept'] == 'application/json':
    return output, 200
  else:
    return render_template("socket.html", namespace='/compile', prefetch=output)



@webapp.route('/compile/<uid>/kill', methods=['GET'])
def killCompile(uid):
  """
  #------------------------------------------------------------------------------
  #  /compile/<uid>/kill
  #         GET     Kills an active compiling tasks (or removes an orphaned one from DB)
  #------------------------------------------------------------------------------
  """
  if webapp.config['COMPILE_OFF']:
    return returnError("Compilation Features are not available", 400)


  complist = db.getCompiles(uid=uid)
  if len(complist) == 0:
    complist = db.getCompiles(uid=AppID.getUID(uid))

  if len(complist) == 0:
    return returnError("Not currently tracking the compile task %s" % uid, 400)

  else:
    c = complist[0]
    logging.info ("[FLASKWEB] Asked to KILL Compile UID #%s. Current status is %s" % (c['uid'], c['status']))

    if c['status'] not in compileTerminatedStates:
      logging.info ("[FLASKWEB] KILLING Compile UID #%s. " % (c['uid']))
      c['status'] = CompileState.KILLED.name
      db.updateCompile(c['uid'], status=c['status'], done=True)

    svid = AppID.getAppId(c['name'], c['uid'])
    compileService.killJob(svid)

    if request.headers['Accept'] == 'application/json':
      return jsonify(c), 200
    else:
      return redirect(url_for('listJobs')), 302

@webapp.route('/delete/compiles', methods=['POST'])
def deleteCompiles():
  """
  #------------------------------------------------------------------------------
  #  /delete/compiles
  #         POST     Deletes list of inactive compile jobs
  #------------------------------------------------------------------------------
  """
  if webapp.config['COMPILE_OFF']:
    return returnError("Compilation Features are not available", 400)


  deleteList = request.form.getlist("delete_compile")
  for uid in deleteList:
    logger.info("[FLASKWEB /delete/compiles] DELETING compile job uid=" + uid)
    job = db.getCompiles(uid=uid)[0]
    db.deleteCompile(job['uid'])
  return redirect(url_for('listJobs')), 302

@socketio.on('connect', namespace='/compile')
def test_connect():
    logger.info('[FLASKWEB] Client is connected to /connect stream')
    emit('my response', 'Connected to Compile Log Stream')

@socketio.on('message', namespace='/log')
def test_message(message):
    emit('my response', "Hello User!")




if __name__ == '__main__':

  #  Parse Args
  parser = argparse.ArgumentParser()
  parser.add_argument('-p', '--port', help='Flaskweb Server Port', default=5000, required=False)
  parser.add_argument('-m', '--master', help='URL for the Mesos Master (e.g. zk://localhost:2181/mesos', default='zk://localhost:2181/mesos', required=False)
  parser.add_argument('-d', '--dir', help='Local directory for hosting application and output files', default='/k3/web/', required=False)
  parser.add_argument('-c', '--compile', help='Enable Compilation Features (NOTE: will require a capable image)', action='store_true', required=False)
  parser.add_argument('--wipedb', help='Wipe the Database clean before running', action='store_true', required=False)
  parser.add_argument('--ip', help='Public accessible IP for connecting to flask', required=False)
  args = parser.parse_args()

  #  TODO:  Move to a Common module
  # 
  # console.setFormatter(formatter)
  # log.addHandler(console)

  # logging.basicConfig(format='[%(asctime)s %(levelname)-5s %(name)s] %(message)s', level=logging.DEBUG, datefmt='%H:%M:%S')
  logger.info("K3 Flask Web Service is initiating.....")


  #  Program Initialization
  webapp.debug = True

  if args.wipedb:
    logger.info("Wiping database and exiting")
    db.dropTables()
    sys.exit(0)

  db.createTables()
  master = args.master
  port = int(args.port)


  initWeb(
    host = socket.gethostname() if not args.ip else args.ip,
    port=port,
    master=master,
    local=args.dir,
    compile=args.compile
  )


  #  Create long running framework, dispatcher, & driver
  frameworkDispatch = mesos_pb2.FrameworkInfo()
  frameworkDispatch.user = "" # Have Mesos fill in the current user.
  frameworkDispatch.name = "[DEV] Dispatcher"

  # Note: Each compile job runs as as a separate framework
  frameworkCompiler = mesos_pb2.FrameworkInfo()
  frameworkCompiler.user = ""
  frameworkCompiler.name = "[DEV] Compiler"

  # Start mesos schedulers & flask web service
  try:


    # Create Job Dispatcher
    logging.debug("[FLASKWEB] Dispatch Driver is initializing")
    dispatcher = Dispatcher(master, webapp.config['ADDR'], daemon=True)
    if dispatcher == None:
      logger.error("[FLASKWEB] Failed to create dispatcher. Aborting")
      sys.exit(1)
    driverDispatch = mesos.native.MesosSchedulerDriver(dispatcher, frameworkDispatch, master)
    threadDispatch = threading.Thread(target=driverDispatch.run)
    threadDispatch.start()

    # Create Compiler Service 
    logging.debug("[FLASKWEB] Compiler Service is initializing")
    compileService = CompileServiceManager(webapp.config['LOG_DEST'], webapp.config['ADDR'])
    if compileService == None:
      logger.error("[FLASKWEB] Failed to create compiler service. Aborting")
      sys.exit(1)
    driverCompiler = mesos.native.MesosSchedulerDriver(compileService, frameworkCompiler, master)
    threadCompiler = threading.Thread(target=driverCompiler.run)
    threadCompiler.start()

    logger.info("[FLASKWEB] Starting FlaskWeb Server...")
    socketio.run(webapp, host='0.0.0.0', port=port, use_reloader=False)
    webserverTerminate.clear()
    initSocketIO = False

    # Block until flagged to halt
    haltFlag.wait()

    compileService.kill()
    logger.info("[FLASKWEB] Server is terminating")
    driverDispatch.stop()
    threadDispatch.join()
    logging.debug("[FLASKWEB] Driver thread complete")
    driverCompiler.stop()
    threadCompiler.join()
    logging.debug("[FLASKWEB] Compiler thread complete")

  except socket.error:
    logger.error("[FLASKWEB] Flask web cannot start: Port not available.")
    compileService.kill()
    driverDispatch.stop()
    threadDispatch.join()
    driverCompiler.stop()
    threadCompiler.join()

  except KeyboardInterrupt:
    logger.warning("[FLASKWEB] KEYBOARD INTERRUPT -- Shutting Down")
    compileService.kill()
    driverDispatch.stop()
    threadDispatch.join()
    driverCompiler.stop()
    threadCompiler.join()


