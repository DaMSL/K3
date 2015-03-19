import os
import threading
import yaml
import json
import hashlib
import uuid
import datetime
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

LOCAL_DIR  = '.'
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


# Returns unique time stamp uid (unique to this machine only (for now)
def getUID():
  return str(uuid.uuid1()).split('-')[0]


def return_error(msg, errcode):
  if request.headers['Accept'] == 'application/json':
    return msg, errcode
  else:
    # TODO: Make other error template files
    return render_template("errors/404.html", message=msg)



@webapp.route('/')
def root():
  return redirect(url_for('index'))


@webapp.route('/index')
def index():
    # Renders index.html.
    return render_template('index.html')

@webapp.route('/about')
def about():
    # Renders author.html.
    return render_template('about.html')


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
#         GET    Display both list of loaded app and form to upload new one
#------------------------------------------------------------------------------
@webapp.route('/apps', methods=['GET', 'POST'])
def upload_app():
  global debug_trace
  if request.method == 'POST':
      print ("UPLOAD NEW APP.....")
      file = request.files['file']
      if file:
          print ("GOT A FILE.....")
          name = secure_filename(file.filename)
          fullpath = os.path.join(webapp.config['UPLOADED_APPS_DEST'], name)
          file.save(fullpath)

          print ("CHECKING UID.....")

          uid = getUID() if 'uid' not in request.form or not request.form['uid'] else request.form['uid']

          print ("UID = %s" % uid)

          jobdir = os.path.join(webapp.config['UPLOADED_JOBS_DEST'], name)
          if not os.path.exists(jobdir):
            os.mkdir(jobdir)
          hash = hashlib.md5(open(fullpath).read()).hexdigest()

          print ('hash = %s' % hash)
          # if (db.checkHash(hash)):
          db.insertApp(dict(uid=uid, name=name, hash=hash))
          #TODO:  Complete with paths for archive & app

          if request.headers['Accept'] == 'application/json':
              return "FILE UPLOADED", 200
          else:
              return redirect(url_for('upload_app'))

  applist = db.getAllApps()
  print ("SHOWING ALL APPS")

  # TODO: Add 2nd link on AppList: 1 to launch latest, 1 to show all versions
  if request.headers['Accept'] == 'application/json':
      return jsonify(applist), 200
  else:
      return render_template('apps.html', applist=applist)


#------------------------------------------------------------------------------
#  /apps/<appName> - Specific Application Level interface
#         GET    Display all versions, executed jobs, etc...  (TODO)
#------------------------------------------------------------------------------
@webapp.route('/apps/<appName>')
def get_app(appName):
    applist = [a['name'] for a in db.getAllApps()]

    # TODO:  Get/List all application versions for this App
    if appName in applist:

      versionList = db.getVersions(appName)

      if request.headers['Accept'] == 'application/json':
        return jsonify(dict(name=appname, versions=versionList))
      else:
        return render_template("apps.html", name=appName, versionList=versionList)
    else:
      return render_template("errors/404.html", message="Application %s does not exist" % appName)


#------------------------------------------------------------------------------
#  /apps/<appName> - Specific Application Level interface
#         POST   (Upload archive data (C++, Source, etc....)
#         GET    Display all versions, executed jobs, etc...  (TODO)
#------------------------------------------------------------------------------
@webapp.route('/apps/<appName>/<appUID>', methods=['GET', 'POST'])
def archive_app(appName):
    applist = [a['name'] for a in db.getAllApps()]
    uname = AppID.getAppId(appName, appUID)

    if appName not in applist:
      msg = "Application %s does not exist" % appName
      return_error(msg)

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
      # TODO: Return the archive for this specific build
      if request.headers['Accept'] == 'application/json':
        return jsonify(db.getApp(appName))
      else:
        return render_template("index.html", message="TODO: Archive Contents for application %s" % uname)


#------------------------------------------------------------------------------
#  /jobs - Current runtime & completed job Interface
#         GET    Display currently executing & recently completed jobs
#------------------------------------------------------------------------------
@webapp.route('/jobs')
def list_jobs():
  jobs = db.getJobs()
  if request.headers['Accept'] == 'application/json':
    return jsonify(jobs)
  else:
    return render_template("jobs.html", joblist=jobs)


#------------------------------------------------------------------------------
#  /jobs/<appName> - Job Interface for specific application
#         POST    Create new K3 Job
#         GET    (TODO: Display detailed job info)
#------------------------------------------------------------------------------

@webapp.route('/jobs/<appName>/<appUID>', methods=['GET', 'POST'])    # IS THIS POSSIBLE????
def create_job(appName, appUID):
  global dispatcher
  applist = [a['name'] for a in db.getAllApps()]
  if appName in applist:
    if request.method == 'POST':
        # TODO: Get user
        file = request.files['file']
        text = request.form['text']

        # Check for valid submission
        if not file and not text:
          return render_template("errors/404.html", message="Invalid job request")

        # Post new job request, get job ID & submit time

        thisjob = dict(appName=appName, appUID=appUID, user='DEV')
        jobId, time = db.insertJob(thisjob)
        thisjob = dict(jobId=jobId, time=time)

        # Save yaml to file (either from file or text input)
        path = os.path.join(webapp.config['UPLOADED_JOBS_DEST'], appName, str(jobId))
        filename = 'role.yaml'
        if not os.path.exists(path):
          os.mkdir(path)

        if file:
            file.save(os.path.join(path, filename))
        else:
            with open(os.path.join(path, filename), 'w') as file:
              file.write(text)

        # Create new Mesos K3 Job
        newJob = Job(binary=os.path.join(webapp.config['UPLOADED_APPS_URL'], appName),
                     appName=appName, jobId=jobId, rolefile=os.path.join(path, filename))
        print ("NEW JOB ID: %s" % newJob.jobId)

        # Submit to Mesos
        dispatcher.submit(newJob)
        thisjob = dict(thisjob, url=dispatcher.getSandboxURL(jobId), status='SUBMITTED')
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
            return render_template("newjob.html", appName=appName, lastrole=lastrole)
        return render_template("newjob.html", appName=appName)

  else:
    #TODO:  Error handle
    return "There is no app %s" % appName


@webapp.route('/jobs/<appName>', methods=['GET', 'POST'])
def create_job_latest(appName):
    app = db.getApp(appName)
    print app
    return redirect(url_for('create_job', appName=appName, appUID=app['uid']))


#------------------------------------------------------------------------------
#  /jobs/<appName>/<jobId> - Job Interface for specific job
#         GET     (TODO: Display detailed job info)
#         DELETE  (TODO: Kill a current K3Job)
#------------------------------------------------------------------------------
@webapp.route('/jobs/<appName>/<jobId>', methods=['GET', 'POST'])
def get_job(appName, jobId):
    jobs = db.getJobs(jobId=jobId)
    job = None if len(jobs) == 0 else jobs[0]
    k3job = dispatcher.getJob(int(jobId))

    if job == None:
      return render_template("errors/404.html", message="Job ID, %s, does not exist" % jobId)

    thisjob = dict(job, url=dispatcher.getSandboxURL(jobId))
    if k3job != None:
      thisjob = dict(thisjob, master=k3job.master)

    path = os.path.join(webapp.config['UPLOADED_JOBS_DEST'], appName, str(jobId),'role.yaml').encode(encoding='ascii')
    if os.path.exists(path):
      with open(path, 'r') as role:
        thisjob['roles'] = role.read()

    if 'application/json' in request.headers['Accept']:
      return jsonify(thisjob)
    else:
      return render_template("jobs.html", appName=appName, joblist=jobs, lastjob=thisjob)


#------------------------------------------------------------------------------
#  /jobs/<appName>/<jobId>/stdout - Job Interface for specific job
#         GET     TODO: Consolidate STDOUT for current job (from all tasks)
#         POST    TODO: Accept STDOUT & append here (if desired....)
#------------------------------------------------------------------------------
@webapp.route('/jobs/<appName>/<jobId>/stdout', methods=['GET'])
def stdout(appName, jobId):
    jobs = db.getJobs(appName=appName)
    link = resolve(MASTER)
    print link
    sandbox = dispatcher.getSandboxURL(jobId)
    if sandbox:
      print sandbox
      return '<a href="%s">%s</a>' % (sandbox, sandbox)
    else:
      return 'test'


#------------------------------------------------------------------------------
#  /jobs/<appName>/<jobId>/archive - Endpoint to receive & archive files
#         GET     TODO: Consolidate STDOUT for current job (from all tasks)
#         POST    TODO: Accept STDOUT & append here (if desired....)
#------------------------------------------------------------------------------
@webapp.route('/jobs/<appName>/<jobId>/archive', methods=['GET', 'POST'])
def archive_job(appName, jobId):
    jobs = db.getJobs(jobId=jobId)
    job = None if len(jobs) == 0 else jobs[0]
    if job == None:
      return render_template("errors/404.html", message="Job ID, %s, does not exist" % jobId)

    if request.method == 'POST':
        file = request.files['file']
        if file:
            filename = secure_filename(file.filename)
            path = os.path.join(webapp.config['UPLOADED_JOBS_DEST'], appName, jobId, filename).encode(encoding='ascii')
            file.save(path)
            return "File Uploaded & archived", 202
        else:
            return "No file received", 400

    elif request.method == 'GET':
        return 'Upload your file using a POST request'



#------------------------------------------------------------------------------
#  /jobs/<appName>/<jobId>/kil - Job Interface to cancel a job
#         GET     Reads STDOUT for current job
#         POST    TODO: Consolidate STDOUT & append here (if desired....)
#------------------------------------------------------------------------------
@webapp.route('/jobs/<appName>/<jobId>/kill', methods=['GET'])
def kill_job(appName, jobId):
    jobs = db.getJobs(jobId=jobId)
    job = None if len(jobs) == 0 else jobs[0]

    if job == None:
      return render_template("errors/404.html", message="Job ID, %s, does not exist" % jobId)

    print ("Asked to KILL job #%s. Current Job status is %s" % (jobId, job['status']))
    # Separate check to kill orphaned jobs in Db
    # TODO: Merge Job withs experiments to post updates to correct table
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
#  /compile
#         GET     Form for compiling new K3 Executable OR status of compiling tasks
#         POST    Submit new K3 Compile task
#------------------------------------------------------------------------------
@webapp.route('/compile', methods=['GET', 'POST'])
def compile():

    if request.method == 'POST':
      file = request.files['file']
      text = request.form['text']
      name = request.form['name']
      options = request.form['options']
      user = request.form['user']

      if not file and not text:
          return render_template("errors/404.html", message="Invalid Compile request")

      # Determine application name  TODO: Change to uname
      # TODO:  (if desired) use full UUID -- using shorter for simplicity
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
      compileJob    = CompileJob(name=name, uid=uid, path=path, url=url, options=options, user=user)
      compileDriver = CompileDriver(compileJob, source=source, script=script)

      # TODO: Enter Compile task into db, get/set K3 build source, use UUID for key
      compile_tasks[uname] = compileDriver

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
        return render_template("compile.html", appName=name, lastcompile=thiscompile)


    else:
      # TODO: Return list of Active/Completed Compiling Tasks
      if request.headers['Accept'] == 'application/json':
        return 'CURL FORMAT:\n\n\tcurl -i -F "name=<appName>;options=<compileOptions>;file=@<sourceFile>" http://qp1:5000/compile'
      else:
        return render_template("compile.html")



@webapp.route('/compile/<uname>', methods=['GET', 'POST'])
def get_compile(uname):
    if uname not in compile_tasks:
      msg = "Not currently tracking the compile task %s" % uname
      return_error (msg, 404)
    else:
      c = compile_tasks[uname]
      fname = os.path.join(webapp.config['UPLOADED_ARCHIVE_DEST'], uname, 'output').encode('ascii')
      stdout_file = open(fname, 'r')
      output = stdout_file.read()
      stdout_file.close()

      if request.headers['Accept'] == 'application/json':
        return output, 200
      else:
        return render_template("output.html", output=output)


# TODO: KILL A COMPILE TASK
# @webapp.route('/compile/<uname>/kill', methods=['GET'])
# def kill_compile(uname):
#     if uname not in compile_tasks:
#       msg = "Not currently tracking the compile task %s" % uname
#       return_error (msg, 404)
#     else:
#       c = compile_tasks[uname]
#       fname = os.path.join(webapp.config['UPLOADED_ARCHIVE_DEST'], uname, 'output').encode('ascii')
#       stdout_file = open(fname, 'r')
#       output = stdout_file.read()
#       stdout_file.close()
#
#       if request.headers['Accept'] == 'application/json':
#         return output, 200
#       else:
#         return render_template("output.html", output=output)





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

  framework = mesos_pb2.FrameworkInfo()
  framework.user = "" # Have Mesos fill in the current user.
  framework.name = "K3 Dispatcher"

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
      print ("Server is running")
    print ("Server is terminating")
    driver.stop()

    driver_t.join()
  except KeyboardInterrupt:
    print("INTERRUPT")
    driver.stop()
    driver_t.join()
    for k, v in compile_tasks.items():
      v.stop()

