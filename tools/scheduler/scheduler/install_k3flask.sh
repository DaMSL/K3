#!/bin/bash

#  READ ME FIRST!!!!!

#  Flask Installer script
#  - This is designed to be run from INSIDE THE CONTAINER!!!!
#  - You must use Python v2.7  (2.7.10 preferably, flask limitation)

#  Run the container to map following volumes for persistence:
#      your local K3 --> /k3
#      local web dir --> /web

#  Verify ZK, IP, and ports below for rhe run command

echo "Installing Python Dependencies"

apt-get install -y python-dev python-pip vim
pip install pyyaml
pip install flask
pip install pytz
pip install enum34
pip install Flask-SocketIO

mkdir /web/log
cp k3executor.py /web

echo "Setting up Environment"
MYIP=$(hostname -i)

echo "export K3_HOME=/k3/K3" > /env
echo "export K3_FLASK=/k3/K3/tools/scheduler/scheduler" >> /env
echo "alias ll='ls -al --color'" >> /env

#  RUN COMMAND HERE (edit as needed)
echo "alias run='cd /k3/K3/tools/scheduler/scheduler && python flaskweb.py -m zk://qp2:2181,qp4:2181,qp6:2181/mesos -d /web --ip ${MYIP} -p 5000 -c'" >> /env

source /env

echo
echo "Flask Service Configured to run on  IP: ${MYIP}  and   PORT: 5000"
