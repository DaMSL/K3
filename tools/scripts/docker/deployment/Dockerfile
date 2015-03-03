FROM damsl/k3-profiling:latest
WORKDIR /software

RUN apt-get update && apt-get install -y python-pip libyaml-dev libprotobuf-dev libgoogle-glog-dev libsvn-dev libapr1-dev python-dev

ADD libmesos.tar.gz /software/
ADD mesos-eggs.tar.gz /software/
ADD mesos-include.tar.gz /software/

RUN mv libmesos/* /usr/local/lib
RUN cp -r mesos /usr/local/include
RUN apt-get install -y cmake
RUN easy_install mesos-eggs/mesos.*
RUN pip install pyyaml
