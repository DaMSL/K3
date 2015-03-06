import os
import threading
import yaml
import json
import hashlib
from flask import (Flask, request, redirect, url_for, jsonify, render_template)
from flask.ext.uploads import delete, init, save, Upload
from werkzeug import secure_filename
from core import *
from mesosutils import *
from dispatcher import *
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

UPLOADED_APPS_DEST = os.path.join(LOCAL_DIR, APPS_TARGET)
UPLOADED_APPS_URL = os.path.join(SERVER_URL, APPS_TARGET)
UPLOADED_JOBS_DEST = os.path.join(LOCAL_DIR, JOBS_TARGET)
UPLOADED_JOBS_URL = os.path.join(SERVER_URL, JOBS_TARGET)
UPLOADED_ARCHIVE_DEST = os.path.join(LOCAL_DIR, ARCHIVE_TARGET)
UPLOADED_ARCHIVE_URL = os.path.join(SERVER_URL, ARCHIVE_TARGET)


# TODO: DIR Structure
webapp = Flask(__name__)
webapp.config['UPLOADED_APPS_DEST']     = UPLOADED_APPS_DEST
webapp.config['UPLOADED_APPS_URL']      = UPLOADED_APPS_URL
webapp.config['UPLOADED_JOBS_DEST']     = UPLOADED_JOBS_DEST
webapp.config['UPLOADED_JOBS_URL']      = UPLOADED_JOBS_URL
webapp.config['UPLOADED_ARCHIVE_DEST']  = UPLOADED_ARCHIVE_DEST
webapp.config['UPLOADED_ARCHIVE_URL']   = UPLOADED_ARCHIVE_URL
webapp.config['nextJobId']    = 1001


dispatcher = None
driver = None
driver_t = None
web_up = True
debug_trace = []

joblist = {}
index_message = 'Welcome to K3'



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
    print (dispatcher.active)
    print (dispatcher.finished)
    print (dispatcher.offers)
    output = dict(active=dispatcher.active.__dict__,
                  finished=dispatcher.finished.__dict__,
                  offers=dispatcher.offers.__dict__)
    return jsonify(output)



#------------------------------------------------------------------------------
#  /apps - Application Level interface
#         POST   Upload new application (TODO: Change to K3 source)
#         GET    Display both list of loaded app and form to upload new one
#------------------------------------------------------------------------------
@webapp.route('/apps', methods=['GET', 'POST'])
def upload_app():
  global debug_trace
  if request.method == 'POST':
      file = request.files['file']
      if file:
          filename = secure_filename(file.filename)
          fullpath = os.path.join(webapp.config['UPLOADED_APPS_DEST'], filename)
          file.save(fullpath)

          jobdir = os.path.join(webapp.config['UPLOADED_JOBS_DEST'], filename)
          if not os.path.exists(jobdir):
            os.mkdir(jobdir)
          hash = hashlib.md5(open(fullpath).read()).hexdigest()
          if not (db.checkHash(hash)):
            # TODO: Insert app into DB
            db.insertApp(filename, hash)
          return redirect(url_for('upload_app'))
  applist = db.getAllApps()
  return render_template('apps.html', applist=applist)


#------------------------------------------------------------------------------
#  /apps/<appId> - Specific Application Level interface
#         POST   (TODO: Upload archive data (C++, Source, etc....)
#         GET    Display all versions, executed jobs, etc...  (TODO)
#------------------------------------------------------------------------------
@webapp.route('/apps/<appId>')
def get_app(appId):
    applist = [a['name'] for a in db.getAllApps()]
    if appId in applist:
      if request.headers['Accept'] == 'application/json':
        return jsonify(db.getApp(appId))
      else:
        return render_template("index.html", message="Details for application %s" % appId)
    else:
      return render_template("errors/404.html", message="Application %s does not exist" % appId)


#------------------------------------------------------------------------------
#  /jobs - Current runtime & completed job Interface
#         GET    (TODO: Display currently executing & recently completed jobs)
#------------------------------------------------------------------------------
@webapp.route('/jobs')
def list_jobs():
  jobs = db.getJobs()
  if request.headers['Accept'] == 'application/json':
    return jsonify(jobs)
  else:
    return render_template("jobs.html", joblist=jobs)


#------------------------------------------------------------------------------
#  /jobs/<appId> - Job Interface for specific application
#         POST    Create new K3 Job
#         GET    (TODO: Display detailed job info)
#------------------------------------------------------------------------------
@webapp.route('/jobs/<appId>', methods=['GET', 'POST'])
def create_job(appId):
  global dispatcher
  applist = [a['name'] for a in db.getAllApps()]
  if appId in applist:
    # Get General App info
    thisapp = db.getApp(appId)
    if request.method == 'POST':
        # TODO: Get user
        file = request.files['file']
        text = request.form['text']

        # Check for valid submission
        if not file and not text:
          return "Error on job submission"

        # Post new job request, get job ID & submit time
        thisjob = dict(appId=appId, hash=thisapp['hash'], user='DEV')
        jobId, time = db.insertJob(thisjob)
        thisjob = dict(jobId=jobId, time=time)

        # Save yaml to file (either from file or text input)
        path = os.path.join(webapp.config['UPLOADED_JOBS_DEST'], appId, str(jobId))
        filename = 'role.yaml'
        if not os.path.exists(path):
          os.mkdir(path)
        if file:
            file.save(os.path.join(path, filename))
        else:
            file = open(os.path.join(path, filename), 'w')
            file.write(text)
            file.close()

        # Create new Mesos K3 Job
        newJob = Job(binary=os.path.join(webapp.config['UPLOADED_APPS_URL'], appId),
                     appId=jobId, rolefile=os.path.join(path, filename))

        # Submit to Mesos
        dispatcher.submit(newJob)
        return render_template('jobs.html', appId=appId, lastjob=thisjob)
    elif request.method == 'GET':
      jobs = db.getJobs(appId=appId)
      if 'application/json' in request.headers['Accept']:
        return jsonify(jobs)
      else:
        return render_template("newjob.html", appId=appId, joblist=jobs)

  else:
    #TODO:  Error handle
    return "There is no app %s" % appId


#------------------------------------------------------------------------------
#  /jobs/<appId>/<jobId> - Job Interface for specific job
#         GET     (TODO: Display detailed job info)
#         DELETE  (TODO: Kill a current K3Job)
#------------------------------------------------------------------------------
@webapp.route('/jobs/<appId>/<jobId>', methods=['GET'])
def get_job(appId, jobId):
    jobs = db.getJobs(appId=appId)
    if 'application/json' in request.headers['Accept']:
      return jsonify(jobs)
    else:
      return render_template("jobs.html", appId=appId, joblist=jobs)




@webapp.route('/upload', methods=['GET', 'POST'])
def upload_archive():
    global dispatcher
    global index_message
    index_message = "File uploading...."
    print ("Received an upload request...")
    if request.method == 'POST':
        file = request.files['file']
        if file:
            filename = "FOO"        # TODO: Filenaming system
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))

            #TODO:  AppId tracker goes here
            app_id = '4444'
            print ("Creating new Job")
            newJob = Job(archive=UPLOAD_FOLDER + '/' + filename, appId=app_id)

            # TODO: Insert job into DB
            joblist[app_id] = newJob.binary_url

            print ("Submitting new job")
            dispatcher.submit(newJob)
            return "File Uploaded!"

    elif request.method == 'GET':
        return 'Upload your file using a POST request'


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

