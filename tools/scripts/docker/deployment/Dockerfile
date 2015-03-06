FROM damsl/k3-profiling:latest
WORKDIR /software

RUN apt-get install -y python-pip libyaml-dev 

ADD libmesos.tar.gz /software/
ADD mesos-eggs.tar.gz /software/

RUN mv libmesos/* /usr/local/lib
RUN apt-get install -y cmake
RUN easy_install mesos-eggs/mesos.*
