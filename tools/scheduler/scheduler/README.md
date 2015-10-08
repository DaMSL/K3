K3 Dispatcher 
==========
The K3 Dispatcher provides a user interface for launching and monitoring distributed K3 Applications. It is built on top of the Apache Mesos Framework by integrating a Flask-based web interface and REST API to manage applications across a distributed cluster of host machines. The dispatcher uses a minimal set of program parameters to allocate resources, configure and launch all K3 peers on multiple hosts. In additin, we also provide a set of EC2 CloudFormation templates which will establish a mesos cluser and run the web service for you (includes steps 1-3 below, see separate folder for EC2 in this repository). 

The basic steps to launching a K3 program:

1. Establish a Mesos cluser 
2. Install docker on all slave nodes
3. Install / Launch K3 Flask Web Dispatching service 
4. Develop/Compile your own K3 program and save the binary file
5. Upload K3 Binary to Web Interface (or via command line REST end point)
6. Launch K3 Job for your application with a given set of program parameters (e.g. # peers, global vars, data files, etc...)


Establish Mesos Cluster
----------------------
We are currently support Mesos v0.22. The easiest and quickest way to install mesos is via [Mesosphere](https://docs.mesosphere.com/getting-started/) installation package.  Or, go to the Apache Mesos [Getting Started](http://mesos.apache.org/gettingstarted/) page for other installing options. You will need to install mesos on all your host machines. Notes on configuring Mesos:
    * Ensure you set `--containerizers=docker,mesos` on all slaves
    * The K3 dispatcher leverages Mesos resource management to conduct port management for all peers. Set `resources="ports:[40000-40999]"` to block a set of ports for K3 (feel free to use any unallocated port range)


Install Docker
--------------
See the official [Docker Website](https://www.docker.com) for instructions on installing and running docker.

We maintain two docker containers for use with K3: a development and a execution container. The development version contains all the packages, libraries, and support software to build and complile K3. The execution container is much smaller (<400MB) and includes the necessary software to run a K3 program. It it specifically packaged with a Mesos Executor application which dispatches K3 peers on a local host machine.  You can pull either or both containers from the Docker regristry at:

    damsl/k3-deployment:latest   (complete version)
    damsl/k3-run:exec            (execution only)


Install / Launch K3-Flask Web Service
-------------------------------------

The Flask Web Service runs on Python v2.7 (minimum 2.7.6). Because Flask is not fully supported on Python v3, we recommend using Python v2.7. To run the flask web service, install the following dependencies either via pip or easy install:

- pyyaml
- flask
- pytz
- [google.protobuf](https://pypi.python.org/packages/2.7/p/protobuf/protobuf-2.5.0-py2.7.egg)

In addition, depending on your method of installing Mesos, you may need to manually install the Python libraries for mesos:

- mesos
- [mesos.interface](https://pypi.python.org/packages/2.7/m/mesos.interface/mesos.interface-0.22.0-py2.7.egg)
- [mesos.native](https://mesosphere.com/downloads/#)

Once you have installed all dependencies, git clone this repository to your mesos master node. 

```
cd k3/tools/scheduler/scheduler
python flaskweb.py
```

Notes:
    - `-m (--master)`: By default, the flask service will attempt to register with mesos master via zookeeper on the local host (`zk://localhost:2181/mesos`). If you have a different configuration, use the -m flag to set the master location for registering the K3 dispatcher with Mesos.
    - `-d (--dir)` Data Directory: The K3 Dispatcher will save all input/output data to a common directory location. This includes binary files, stdout, archives, etc... The default is `/k3/web`. Also ensure you have permissions set up to read/write to this directory. This feature is designed to enable Flask to run within a docker container and retain all data to a persistent, mounted volume.
    - `-p (--port)` You can optionally set the port using the  flag. The default is port 5000.
    - `--ip` Flask web will bind to all ip addresses on the host by default. You can restrict this using the --ip flag.


Develop/Compile K3 Program
--------------------------
Develop K3 locally on your own computer or feel free to pull and use the provided K3 development container we provide. To integrate your executable binary with this distributed framework, ensure your binary is compiled using a 64-bit debian or ubuntu based operating system. For consistency, we utilize Ubuntu 14.04 and recommend this version (which is provided in the K3-deployment container). 


Upload K3 Binary
----------------
Once you have a compiled binary file, use the web interface end point (http://<hostame>:5000/app) to upload the file. The curl command is provided below:

```
curl -i -H "Accept: application/json" -F file=@<filename> http://<hostname>:5000/apps
```


Launch a K3 Job
---------------
Launching a K3 Application consists of uploading a yaml-formatted configuration environment for your application. This includes the # of peers, data file locations, and other program specific settings. For full details on the yaml file format, please see the **role_file_template.yaml**. Use the following curl command to send a role file and launch via command line where **appName** is the name  of the K3 Binary you uploaded in the previous step:

```
curl -i -X POST -H "Accept: application/json" -F "file=@<rolefile>" http://<hostname>:5000/jobs/<appName>
```

Monitoring
---------------
You can view the updated status of your application at `http://<hostname>/job/<jobId>` When the program is finished, it will tar and send the contents of your sandbox to the flask web service for archiving.