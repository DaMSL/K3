# scheduler.server: HTTP server to manage K3 job submission and listing.
import os
import threading
from flask import Flask, request, redirect, url_for
from werkzeug import secure_filename
from core import *
from mesosutils import *
from dispatcher import *


# TODO: Check if dir exists
UPLOAD_FOLDER = './uploads'

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


dispatcher = None


def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError("Not running the server")
    func()

@app.route('/')
def index():
    return 'Welcome Page for K3'

@app.route('/new/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        file = request.files['file']
        if file:
            filename = "FOO"        # TODO: Filenaming system
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))

            #TODO:  AppId tracker goes here
            newJob = Job(archive=UPLOAD_FOLDER + '/' + filename, appId='3333')
            dispatcher.submit(newJob)
            return "K3 Job Launched"

    elif request.method == 'GET':
        return 'Upload your file using a POST request'


@app.route('/kill')
def shutdown():
    shutdown_server()
    return 'Server is going down...'


# @app.route('/apps/')
# def apps():
#     return 'Placeholder for App List'
#

@app.route('/apps/<appId>')
def app(appId):
    return 'Your app ID is: %s!' % appId





if __name__ == '__main__':

  app.run(host='0.0.0.0')

  framework = mesos_pb2.FrameworkInfo()
  framework.user = "" # Have Mesos fill in the current user.
  framework.name = "K3 Dispatcher"

  dispatcher = Dispatcher(daemon=False)
  if dispatcher == None:
    print("Failed to create dispatcher. Aborting")
    sys.exit(1)

  driver = mesos.native.MesosSchedulerDriver(dispatcher, framework, MASTER)
  t = threading.Thread(target = driver.run)
  # w = threading.Thread(target = app.run)

  try:
    t.start()
    print ("Driver has started")
    # w.start()
    print ("Web Server has started")
    # Sleep until interrupt
    terminate = False
    while not terminate:
      time.sleep(1)
      terminate = dispatcher.terminate
    print ("Server is terminating")
    driver.stop()

    t.join()
    # w.join()
  except KeyboardInterrupt:
    print("INTERRUPT")
    driver.stop()
    t.join()
    # w.join()
