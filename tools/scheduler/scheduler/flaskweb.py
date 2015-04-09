import os
import threading
import yaml
import json
import hashlib
import uuid
import datetime
import shutil
from flask import (Flask, request, redirect, url_for, jsonify, render_template)
# from flask.ext.uploads import delete, init, save, Upload
from werkzeug import secure_filename
from core import *
from mesosutils import *
from dispatcher import *
from CompileDriver import *

import db


class Server(Flask):
  def __init__(self, name):
    self.state = {}
    self.up = True
    Flask.__init__(self, name)

LOCAL_DIR  = '/web'
SERVER_URL = 'http://qp1:8002/'

JOBS_TARGET    = 'jobs'
APPS_TARGET    = 'apps'
ARCHIVE_TARGET = 'archive'

# DEFAULT_UPLOAD_TARGET  = 'uploads'
# UPLOADS_DEFAULT_DEST = os.path.join(LOCAL_DIR, DEFAULT_UPLOAD_TARGET)
# UPLOADS_DEFAULT_URL = os.path.join(SERVER_URL, DEFAULT_UPLOAD_TARGET)

APPS_DEST = os.path.join(LOCAL_DIR, APPS_TARGET)
APPS_URL = os.path.join(SERVER_URL, APPS_TARGET)
JOBS_DEST = os.path.join(LOCAL_DIR, JOBS_TARGET)
JOBS_URL = os.path.join(SERVER_URL, JOBS_TARGET)
ARCHIVE_DEST = os.path.join(LOCAL_DIR, ARCHIVE_TARGET)
ARCHIVE_URL = os.path.join(SERVER_URL, ARCHIVE_TARGET)


# TODO: DIR Structure
webapp = Flask(__name__)
webapp.config['UPLOADED_APPS_DEST']     = APPS_DEST
webapp.config['UPLOADED_APPS_URL']      = APPS_URL
webapp.config['UPLOADED_JOBS_DEST']     = JOBS_DEST
webapp.config['UPLOADED_JOBS_URL']      = JOBS_URL
webapp.config['UPLOADED_ARCHIVE_DEST']  = ARCHIVE_DEST
webapp.config['UPLOADED_ARCHIVE_URL']   = ARCHIVE_URL
webapp.config['nextJobId']    = 1001


dispatcher = None
driver = None
driver_t = None
web_up = True
debug_trace = []
compile_tasks = {}

joblist = {}
index_message = 'Welcome to K3'


def initWeb():
  for p in [LOCAL_DIR, JOBS_TARGET, APPS_TARGET, ARCHIVE_TARGET]:
    path = os.path.join(LOCAL_DIR, p)
    if not os.path.exists(path):
      os.mkdir(path)
  # Check for executor(s)
  compiler_exec = os.path.join(LOCAL_DIR, 'CompileExecutor.py')
  if not os.path.exists(compiler_exec):
    shutil.copyfile('CompileExecutor.py', compiler_exec)
  launcher_exec = os.path.join(LOCAL_DIR, 'k3executor')
  if not os.path.exists(compiler_exec):
    os.chdir('executor')
    os.system('cmake .')
    os.system('make')
    shutil.copyfile('k3executor', launcher_exec)
    os.chdir('..')



# Returns unique time stamp uid (unique to this machine only (for now)
def getUID():
  return str(uuid.uuid1()).split('-')[0]


def returnError(msg, errcode):
  if request.headers['Accept'] == 'application/json':
    return msg, errcode
  else:
    return render_template("errors/404.html", message=msg)



#------------------------------------------------------------------------------
#  / - Home (welcome msg)
#------------------------------------------------------------------------------
@webapp.route('/')
def root():
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

#------------------------------------------------------------------------------
#  /about - TODO: about message
#------------------------------------------------------------------------------
@webapp.route('/about')
def about():
  if request.headers['Accept'] == 'application/json':
    return redirect(url_for('static', filename="API.txt"))
  else:
    return redirect(url_for('static', filename="API.html"))


#------------------------------------------------------------------------------
#  /trace - Debugging respose
#------------------------------------------------------------------------------
@webapp.route('/trace')
def trace():
  global dispatcher
  if 'application/json' in request.headers['Accept']:
    output = {}
    output['headers'] = request.headers
    output['args'] = request.args
    output['form'] = request.args
    output['data'] = request.args
    return jsonify(output)
  else:
    output = dict(active=dispatcher.active.__dict__,
                  finished=dispatcher.finished.__dict__,
                  offers=dispatcher.offers.__dict__)
    return jsonify(output)



#------------------------------------------------------------------------------
#  /apps - Application Level interface
#         POST   Upload new application
#             curl -i -H "Accept: application/json" -F file=@<filename> http://qp1:5000/apps
#
#         GET    Display both list of loaded apps and form to upload new ones
#------------------------------------------------------------------------------
@webapp.route('/apps', methods=['GET', 'POST'])
def upload_app():
  global debug_trace
  if request.method == 'POST':
      print ("UPLOAD NEW APP.....")
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
          #TODO:  Complete with paths for archive & app

          if request.headers['Accept'] == 'application/json':
              return "FILE UPLOADED", 200
          else:
              return redirect(url_for('upload_app'))

  applist = db.getAllApps()
  versions = {a['name']: db.getVersions(a['name'], limit=5) for a in applist}


  # TODO: Add 2nd link on AppList: 1 to launch latest, 1 to show all versions
  if request.headers['Accept'] == 'application/json':
      return jsonify(dict(apps=applist)), 200
  else:
      return render_template('apps.html', applist=applist, versions=versions)


#------------------------------------------------------------------------------
#  /apps/<appName> - Specific Application Level interface
#         GET    Display all versions for given application
#------------------------------------------------------------------------------
@webapp.route('/apps/<appName>')
def get_app(appName):
    applist = [a['name'] for a in db.getAllApps()]

    if appName in applist:
      versionList = db.getVersions(appName)
      if request.headers['Accept'] == 'application/json':
        return jsonify(dict(name=appName, versions=versionList)), 200
      else:
        return render_template("apps.html", name=appName, versionList=versionList)
    else:
      return render_template("errors/404.html", message="Application %s does not exist" % appName)


#------------------------------------------------------------------------------
#  /apps/<appName> - Specific Application Level interface
#         POST   (Upload archive data (C++, Source, etc....)
#            curl -i -H "Accept: application/json"
#                    -F "file=<filename>" http://qp1:5000/apps/<addName>/<addUID>
#
#         GET    (TODO) Display archived files...  NotImplemented
#------------------------------------------------------------------------------
@webapp.route('/apps/<appName>/<appUID>', methods=['GET', 'POST'])
def archive_app(appName, appUID):
    applist = [a['name'] for a in db.getAllApps()]
    uname = AppID.getAppId(appName, appUID)

    if appName not in applist:
      msg = "Application %s does not exist" % appName
      if request.headers['Accept'] == 'application/json':
        return msg, errcode
      else:
        return render_template("errors/404.html", message=msg)

    if request.method == 'POST':
        file = request.files['file']
        if file:
            filename = secure_filename(file.filename)
            path = os.path.join(webapp.config['UPLOADED_ARCHIVE_DEST'], uname).encode(encoding='ascii')
            if not os.path.exists(path):
              os.mkdir(path)
            file.save(os.path.join(path, filename))
            return "File Uploaded & archived", 202
        else:
            return "No file received", 400

    elif request.method == 'GET':
      path = os.path.join(webapp.config['UPLOADED_ARCHIVE_URL'], uname)
      return redirect(path, 302)

      # if request.headers['Accept'] == 'application/json':
      #   return jsonify(db.getApp(appName))
      # else:
      #   return redirect(path, 302)



#------------------------------------------------------------------------------
#  /jobs - Current runtime & completed job Interface
#         GET    Display currently executing & recently completed jobs
#------------------------------------------------------------------------------
@webapp.route('/jobs')
def list_jobs():
  jobs = db.getJobs()
  compiles = db.getCompiles()
  if request.headers['Accept'] == 'application/json':
    return jsonify(dict(LaunchJobs=jobs, CompilingJobs=compiles)), 200
  else:
    return render_template("jobs.html", joblist=jobs, compilelist=compiles)


#------------------------------------------------------------------------------
#  /jobs/<appName>
#  /jobs/<appName>/<appUID  - Launch a new K3 Job
#         POST    Create new K3 Job
#          curl -i -X POST -H "Accept: application/json" -F "file=@<rolefile>" http://qp1:5000/jobs/<appName>/<appUID>
#             NOTE: if appUID is omitted, job will be submitted to latest version of this app
#
#         GET    Display job list for this application
#------------------------------------------------------------------------------
@webapp.route('/jobs/<appName>/<appUID>', methods=['GET', 'POST'])
def create_job(appName, appUID):
  global dispatcher
  applist = [a['name'] for a in db.getAllApps()]
  if appName in applist:
    if request.method == 'POST':
        # TODO: Get user
        file = request.files['file']
        text = request.form['text'] if 'text' in request.form else None
        user = request.form['user'] if 'user' in request.form else 'anonymous'
        tag = request.form['tag'] if 'tag' in request.form else ''
        # trials = int(request.form['trials']) if 'trials' in request.form else 1

        # Check for valid submission
        if not file and not text:
          return render_template("errors/404.html", message="Invalid job request")

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
        apploc = os.path.join(webapp.config['UPLOADED_APPS_URL'], appName, appUID, appName)

        newJob = Job(binary=apploc, appName=appName, jobId=jobId, rolefile=os.path.join(path, filename))

        print ("NEW JOB ID: %s" % newJob.jobId)

        # Submit to Mesos
        dispatcher.submit(newJob)
        thisjob = dict(thisjob, url=dispatcher.getSandboxURL(jobId), status='SUBMITTED')

        # while trials > 1:
        #   def replay_this_job():
        #     while active:
        #       time.sleep(5)
        #       active = len(dispatcher.getActiveJobs) > 0
        #     replay_job(appName, jobId)
        #   t = threading.Thread(target = replay_this_job)
        #   try:
        #     t.start()
        #     t.join()
        #
        #   except KeyboardInterrupt:
        #     print("INTERRUPT")
        #     driver.stop()
        #     driver_t.join()
        #     break
        #
        #   trials -= 1


        if 'application/json' in request.headers['Accept']:
          return jsonify(thisjob), 202
        else:
          return render_template('jobs.html', appName=appName, lastjob=thisjob)

    elif request.method == 'GET':
      jobs = db.getJobs(appName=appName)
      if 'application/json' in request.headers['Accept']:
        return jsonify(jobs)
      else:
        if len(jobs) > 0:
          lastjobId = max([j['jobId'] for j in jobs])

          path = os.path.join(webapp.config['UPLOADED_JOBS_DEST'], appName, str(lastjobId),'role.yaml').encode(encoding='ascii')
          if os.path.exists(path):
            with open(path, 'r') as role:
              lastrole = role.read()
            return render_template("newjob.html", name=appName, uid=appUID, lastrole=lastrole)
        return render_template("newjob.html", name=appName, uid=appUID)

  else:
    msg =  "There is no application, %s" % appName
    if request.headers['Accept'] == 'application/json':
      return msg, errcode
    else:
      return render_template("errors/404.html", message=msg)


@webapp.route('/jobs/<appName>', methods=['GET', 'POST'])
def create_job_latest(appName):
    app = db.getApp(appName)
    print app
    if request.method == 'POST':
        appUID = app['uid']
        file = request.files['file']
        text = request.form['text'] if 'text' in request.form else None
        user = request.form['user'] if 'user' in request.form else 'anonymous'
        tag = request.form['tag'] if 'tag' in request.form else ''

        # Check for valid submission
        if not file and not text:
          return returnError("Invalid job request", 404)

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
        apploc = os.path.join(webapp.config['UPLOADED_APPS_URL'], appName, appUID, appName)
        try:
          newJob = Job(binary=apploc, appName=appName, jobId=jobId, rolefile=os.path.join(path, filename))
        except K3JobError as err:
          db.deleteJob(jobId)
          return returnError(err.value, 400)

        print ("NEW JOB ID: %s" % newJob.jobId)

        # Submit to Mesos
        dispatcher.submit(newJob)
        thisjob = dict(thisjob, url=dispatcher.getSandboxURL(jobId), status='SUBMITTED')


        if 'application/json' in request.headers['Accept']:
          return jsonify(thisjob), 202
        else:
          return render_template('jobs.html', appName=appName, lastjob=thisjob)

    elif request.method == 'GET':
      return redirect(url_for('create_job', appName=appName, appUID=app['uid']))


#------------------------------------------------------------------------------
#  /jobs/<appName>/<jobId>/status - Detailed Job info
#         GET     Display detailed job info  (default for all methods)
#------------------------------------------------------------------------------
@webapp.route('/jobs/<appName>/<jobId>/status')
def get_job(appName, jobId):
    jobs = db.getJobs(jobId=jobId)
    job = None if len(jobs) == 0 else jobs[0]
    k3job = dispatcher.getJob(int(jobId))

    if job == None:
      return render_template("errors/404.html", message="Job ID, %s, does not exist" % jobId)

    thisjob = dict(job, url=dispatcher.getSandboxURL(jobId))
    if k3job != None:
      thisjob['master'] = k3job.master
    thisjob['sandbox'] = os.path.join(webapp.config['UPLOADED_JOBS_URL'], appName, str(jobId)).encode(encoding='ascii')

    path = os.path.join(webapp.config['UPLOADED_JOBS_DEST'], appName, str(jobId),'role.yaml').encode(encoding='ascii')
    if os.path.exists(path):
      with open(path, 'r') as role:
        thisjob['roles'] = role.read()

    if 'application/json' in request.headers['Accept']:
      return jsonify(thisjob)
    else:
      return render_template("jobs.html", appName=appName, joblist=jobs, lastjob=thisjob)



#------------------------------------------------------------------------------
#  /jobs/<appName>/<appUID/replay  - Replay a previous K3 Job
#         POST    Create new K3 Job
#          curl -i -X POST -H "Accept: application/json" http://qp1:5000/jobs/<appName>/<appUID>/replay
#------------------------------------------------------------------------------
@webapp.route('/jobs/<appName>/<jobId>/replay', methods=['GET', 'POST'])
def replay_job(appName, jobId):
  global dispatcher
  joblist = db.getJobs(jobId=jobId)
  oldjob = None if len(joblist) == 0 else joblist[0]
  if oldjob:
      print ("REPLAYING %s" % jobId),
      # Post new job request, get job ID & submit time
      thisjob = dict(appName=oldjob['appName'],
                     appUID=oldjob['hash'],
                     user=oldjob['user'],
                     tag='REPLAY: %s' % oldjob['tag'])

      new_jobId, time = db.insertJob(thisjob)
      thisjob = dict(jobId=new_jobId, time=time)
      print (" as new JOBID: %s" % new_jobId),


      # Save yaml to file (either from file or text input)
      role_src = os.path.join(webapp.config['UPLOADED_JOBS_DEST'], appName, str(jobId), 'role.yaml').encode('ascii', 'ignore')
      path = os.path.join(webapp.config['UPLOADED_JOBS_DEST'], appName, str(new_jobId)).encode('ascii', 'ignore')
      os.mkdir(path)
      role_copy = os.path.join(path, 'role.yaml')
      shutil.copyfile(role_src, os.path.join(path, role_copy))

      # Create new Mesos K3 Job
      try:
        newJob = Job(binary=os.path.join(webapp.config['UPLOADED_APPS_URL'], appName, oldjob['hash'], appName),
                     appName=appName, jobId=new_jobId, rolefile=role_copy)
      except K3JobError as err:
        db.deleteJob(jobId)
        print err
        return returnError(err.value, 400)

      print ("NEW JOB ID: %s" % newJob.jobId)

      # Submit to Mesos
      dispatcher.submit(newJob)
      thisjob = dict(thisjob, url=dispatcher.getSandboxURL(new_jobId), status='SUBMITTED')

      if 'application/json' in request.headers['Accept']:
          return jsonify(thisjob), 202
      else:
          return render_template('jobs.html', appName=appName, lastjob=thisjob)

  else:
    msg =  "There is no Job, %s\n" % jobId
    if request.headers['Accept'] == 'application/json':
      return msg, 404
    else:
      return render_template("errors/404.html", message=msg)

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
#     if sandbox:
#       print sandbox
#       return '<a href="%s">%s</a>' % (sandbox, sandbox)
#     else:
#       return 'test'


#------------------------------------------------------------------------------
#  /jobs/<appName>/<jobId>/archive - Endpoint to receive & archive files
#         GET     returns curl command
#         POST    Accept files for archiving here
#           curl -i -H "Accept: application/json"
#              -F file=@<filename> http://qp1:5000/<appName>/<jobId>/archive
#------------------------------------------------------------------------------
@webapp.route('/jobs/<appName>/<jobId>/archive', methods=['GET', 'POST'])
def archive_job(appName, jobId):
    job_id = str(jobId).encode('ascii', 'ignore')
    if job_id.find('.') > 0:
      job_id = job_id.split('.')[0]
    jobs = db.getJobs(jobId=job_id)
    job = None if len(jobs) == 0 else jobs[0]
    if job == None:
      return render_template("errors/404.html", message="Job ID, %s, does not exist" % job_id)

    if request.method == 'POST':
        file = request.files['file']
        if file:
            filename = secure_filename(file.filename)
            path = os.path.join(webapp.config['UPLOADED_JOBS_DEST'], appName, job_id, filename).encode(encoding='ascii')
            file.save(path)
            return "File Uploaded & archived", 202
        else:
            return "No file received", 400

    elif request.method == 'GET':
        return '''
Upload your file using the following CURL command:\n\n
   curl -i -H "Accept: application/json" -F file=@<filename> http://qp1:5000/<appName>/<jobId>/archive
''', 200

#------------------------------------------------------------------------------
#  /jobs/<appName>/<jobId>/kill - Job Interface to cancel a job
#         GET     Kills a Job (if orphaned, updates status to killed)
#           curl -i -H "Accept: application/json" http://qp1:5000/jobs/<appName>/<jobId>/kill
#------------------------------------------------------------------------------
@webapp.route('/jobs/<appName>/<jobId>/kill', methods=['GET'])
def kill_job(appName, jobId):
    jobs = db.getJobs(jobId=jobId)
    job = None if len(jobs) == 0 else jobs[0]

    if job == None:
      return render_template("errors/404.html", message="Job ID, %s, does not exist" % jobId)

    print ("Asked to KILL job #%s. Current Job status is %s" % (jobId, job['status']))
    # Separate check to kill orphaned jobs in Db
    # TODO: Merge Job with experiments to post updates to correct table
    if job['status'] == 'RUNNING' or job['status'] == 'SUBMITTED':
      db.updateJob(jobId, status='KILLED')

    status = 'KILLED' if jobId in dispatcher.active else 'ORPHANED and CLEANED'


    if status == 'KILLED':
      dispatcher.cancelJob(int(jobId), driver)
    ts = db.getTS_est()  #datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    thisjob = dict(jobId=jobId, time=ts, url=dispatcher.getSandboxURL(jobId), status=status)
    if 'application/json' in request.headers['Accept']:
      return jsonify(thisjob)
    else:
      return render_template("jobs.html", appName=appName, lastjob=thisjob)


#------------------------------------------------------------------------------
#  /job/<jobId> - Detailed Job info
#         GET     Display detailed job info  (default for all methods)
#------------------------------------------------------------------------------
@webapp.route('/job/<jobId>')
def get_job_id(jobId):
    jobs = db.getJobs(jobId=jobId)
    job = None if len(jobs) == 0 else jobs[0]
    if job == None:
      return render_template("errors/404.html", message="Job ID, %s, does not exist" % jobId)
    appName = job['appName']
    return get_job(appName, jobId)

#------------------------------------------------------------------------------
#  /job/<jobId>/replay - Replays this job
#         GET     Display detailed job info  (default for all methods)
#------------------------------------------------------------------------------
@webapp.route('/job/<jobId>/replay')
def replay_job_id(jobId):
    jobs = db.getJobs(jobId=jobId)
    job = None if len(jobs) == 0 else jobs[0]
    if job == None:
      return render_template("errors/404.html", message="Job ID, %s, does not exist" % jobId)
    appName = job['appName']
    print ("REPLAYING JOB # %s" % jobId)
    return replay_job(appName, jobId)

#------------------------------------------------------------------------------
#  /jobs/<appName>/<jobId>/kill - Job Interface to cancel a job
#         GET     Kills a Job (if orphaned, updates status to killed)
#           curl -i -H "Accept: application/json" http://qp1:5000/jobs/<appName>/<jobId>/kill
#------------------------------------------------------------------------------
@webapp.route('/job/<jobId>/kill', methods=['GET'])
def kill_job_id(jobId):
    jobs = db.getJobs(jobId=jobId)
    job = None if len(jobs) == 0 else jobs[0]
    appName = job['appName']
    if job == None:
      return render_template("errors/404.html", message="Job ID, %s, does not exist" % jobId)
    return kill_job(appName, jobId)
#------------------------------------------------------------------------------
#  /compile
#         GET     Form for compiling new K3 Executable OR status of compiling tasks
#         POST    Submit new K3 Compile task
#           curl -i -H "Accept: application/json"
#                   -F name=<appName> -F file=@<sourceFile>
#                   -F options=<compileOptions> -F user=<userName> http://qp1:5000/compile
#           NOTE: Username & Options are optional fields
#------------------------------------------------------------------------------
@webapp.route('/compile', methods=['GET', 'POST'])
def compile():

    print ("COMPILE REQUEST....")

    if request.method == 'POST':
      file = request.files['file']
      text = request.form['text']
      name = request.form['name']
      options = request.form['options'] if 'options' in request.form else ''
      user = request.form['user'] if 'user' in request.form else 'someone'
      tag = request.form['tag'] if 'tag' in request.form else ''

      if not file and not text:
          return render_template("errors/404.html", message="Invalid Compile request")

      # Determine application name
      uid = getUID()

      if not name:
        if file:
          srcfile = secure_filename(file.filename)
          name = srcfile.split('.')[0]
        else:
          return render_template("errors/404.html", message="No name provided for K3 program")

      uname = '%s-%s' % (name, uid)

      path = os.path.join(webapp.config['UPLOADED_ARCHIVE_DEST'], uname).encode(encoding='ascii')
      url = os.path.join(webapp.config['UPLOADED_ARCHIVE_URL'], uname).encode(encoding='ascii')
      src_file = ('%s.k3' % name)
      if not os.path.exists(path):
        os.mkdir(path)

      # Save K3 source to file (either from file or text input)
      if file:
          file.save(os.path.join(path, src_file))
      else:
          file = open(os.path.join(path, src_file).encode(encoding='ascii'), 'w')
          file.write(text)
          file.close()

      # Generate compilation script  TODO: (Consider) Move to executor
      sh_file = 'compile_%s.sh' % name
      sh = genScript(dict(name=name, uid=uid, options=options, uname=uname))
      compscript = open(os.path.join(path, sh_file).encode(encoding='ascii'), 'w')
      compscript.write(sh)
      compscript.close()

      source = os.path.join(url, src_file)
      script = os.path.join(url, sh_file)

      # TODO: Add in Git hash
      print ("Create the compile job...")
      compileJob    = CompileJob(name=name, uid=uid, path=path, url=url, options=options, user=user, tag=tag)
      print ("Create the driver...")
      compileDriver = CompileDriver(compileJob, source=source, script=script)

      # TODO: get/set K3 build source
      compile_tasks[uname] = compileDriver

      print ("Starting compilation thread")

      t = threading.Thread(target=compileDriver.launch)
      try:
        t.start()
      except e:
        compileDriver.stop()
        t.join()

      outputurl = "http://qp1:5000/compile/%s" % uname
      thiscompile = dict(compileJob.__dict__, url=dispatcher.getSandboxURL(uname),
                         status='SUBMITTED', outputurl=outputurl)

      if request.headers['Accept'] == 'application/json':
        return outputurl, 200
      else:
        return render_template("jobs.html", appName=name, lastcompile=thiscompile)


    else:
      # TODO: Return list of Active/Completed Compiling Tasks
      if request.headers['Accept'] == 'application/json':
        return 'CURL FORMAT:\n\n\tcurl -i -H "Accept: application/json" -F name=<appName> -F file=@<sourceFile> -F options=<compileOptions> -F user=<userName> http://qp1:5000/compile'
      else:
        return render_template("compile.html")



#------------------------------------------------------------------------------
#  /compile/<uname>
#         GET     displays STDOUT & STDERR consolidated output for compile task
#------------------------------------------------------------------------------
@webapp.route('/compile/<uname>', methods=['GET'])
def get_compile(uname):
    fname = os.path.join(webapp.config['UPLOADED_ARCHIVE_DEST'], uname, 'output').encode('ascii')
    if os.path.exists(fname):
      stdout_file = open(fname, 'r')
      output = stdout_file.read()
      stdout_file.close()

      if request.headers['Accept'] == 'application/json':
        return output, 200
      else:
        return render_template("output.html", output=output)

    else:
      msg = "No output found for compilation, %s\n\n" % uname
      if request.headers['Accept'] == 'application/json':
        return msg, errcode
      else:
        return render_template("errors/404.html", message=msg)


#------------------------------------------------------------------------------
#  /compile/<uname>/kill
#         GET     Kills an active compiling tasks (or removes and orphaned one
#------------------------------------------------------------------------------
@webapp.route('/compile/<uname>/kill', methods=['GET'])
def kill_compile(uname):

    name = AppID.getName(uname)
    uid = AppID.getUID(uname)

    complist = db.getCompiles(uid=uid)
    if len(complist) == 0:
      msg = "Not currently tracking the compile task %s" % uname
      if request.headers['Accept'] == 'application/json':
        return msg, errcode
      else:
        return render_template("errors/404.html", message=msg)
    else:
      c = complist[0]
      print ("Asked to KILL Compile UID #%s. Current Job status is %s" % (c['uid'], c['status']))

      if not JobStatus.done(c['status']):
        db.updateCompile(jobId, status=JobStatus.KILLED, done=True)

      if uname in compile_tasks:
        del compile_tasks[uname]
        c['status'] = JobStatus.KILLED
      else:
        c['status'] = 'ORPHANED and CLEANED'

      if request.headers['Accept'] == 'application/json':
        return jsonify(c), 200
      else:
        apps = getAllApps(appName=appName)
        return render_template("jobs.html", appName=appName, applist=apps, lastcompile=c, complist=complist)




#------------------------------------------------------------------------------
#  /delete/jobs
#         POST     Deletes list of K3 jobs
#------------------------------------------------------------------------------
@webapp.route('/delete/jobs', methods=['POST'])
def delete_jobs():
  deleteList = request.form.getlist("delete_job")
  for jobId in deleteList:
    job = db.getJobs(jobId=jobId)[0]
    path = os.path.join(webapp.config['UPLOADED_JOBS_DEST'], job['appName'], jobId)
    shutil.rmtree(path, ignore_errors=True)
    db.deleteJob(jobId)
  return redirect(url_for('list_jobs')), 302


#------------------------------------------------------------------------------
#  /delete/compiles
#         POST     Deletes list of compile jobs
#------------------------------------------------------------------------------
@webapp.route('/delete/compiles', methods=['POST'])
def delete_compiles():
  deleteList = request.form.getlist("delete_compiles")
  for uid in deleteList:
    job = db.getCompiles(uid=uid)[0]
    # TODO: COMPLETE
    # path = os.path.join(webapp.config['UPLOADED_JOBS_DEST'], job['appName'], jobId)
    # shutil.rmtree(path, ignore_errors=True)
    # db.deleteJob(jobId)
  return redirect(url_for('list_jobs')), 302



# @webapp.route('/upload', methods=['GET', 'POST'])
# def upload_archive():
#     global dispatcher
#     global index_message
#     index_message = "File uploading...."
#     print ("Received an upload request...")
#     if request.method == 'POST':
#         file = request.files['file']
#         if file:
#             filename = "FOO"        # TODO: Filenaming system
#             file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
#
#             #TODO:  appName tracker goes here
#             app_id = '4444'
#             print ("Creating new Job")
#             newJob = Job(archive=UPLOAD_FOLDER + '/' + filename, appName=app_id)
#
#             # TODO: Insert job into DB
#             joblist[app_id] = newJob.binary_url
#
#             print ("Submitting new job")
#             dispatcher.submit(newJob)
#             return "File Uploaded!"
#
#     elif request.method == 'GET':
#         return 'Upload your file using a POST request'


#------------------------------------------------------------------------------
#  /kill - Kill the server  (TODO: Clean this up)
#------------------------------------------------------------------------------

def shutdown_server():
    global web_up
    print ("Attempting to kill the server")
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError("Not running the server")
    func()
    web_up = False
    print ("Web is down")
    driver.stop()
    driver_t.join() 
    for k, v in compile_tasks.items():
      v.stop()
    print ("Mesos is down")


@webapp.route('/kill')
def shutdown():
    print ("Shutting down the driver")
    shutdown_server()
    return 'Server is going down...'

if __name__ == '__main__':
  webapp.debug = True

  db.createTables()
  initWeb()

  framework = mesos_pb2.FrameworkInfo()
  framework.user = "" # Have Mesos fill in the current user.
  framework.name = "K3 FlaskWeb Dispatcher"

  dispatcher = Dispatcher(daemon=True)
  if dispatcher == None:
    print("Failed to create dispatcher. Aborting")
    sys.exit(1)

  driver = mesos.native.MesosSchedulerDriver(dispatcher, framework, MASTER)
  driver_t = threading.Thread(target = driver.run)

  try:
    index_message = 'K3 Dispatcher attempting to run'
    driver_t.start()
    webapp.run(host='0.0.0.0', threaded=True)
    index_message = 'K3 Dispatcher currently running'
    print ("Web Server is currently online")

    terminate = False
    while not terminate:
      time.sleep(1)
      terminate = dispatcher.terminate
    print ("Server is terminating")
    driver.stop()
    driver_t.join()

  except KeyboardInterrupt:
    print("INTERRUPT")
    driver.stop()
    driver_t.join()
    for k, v in compile_tasks.items():
      v.stop()

