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
RUN git clone https://github.com/3rdparty/stout.git && mv stout/include/stout /usr/include/
ENV LD_LIBRARY_PATH /usr/local/lib/:/usr/lib/x86_64-linux-gnu/
RUN ulimit -c unlimited
