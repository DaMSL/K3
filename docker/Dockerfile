# Base image is Ubuntu 14.04
FROM ubuntu:14.04

# Create software directory
RUN mkdir /software

# Update apt-get
RUN apt-get update

# Locale setup
RUN apt-get install -y language-pack-en-base
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US.UTF-8
ENV LC_ALL en_US.UTF-8

## Install Clang 3.4
RUN apt-get install -y clang-3.4

## Install Boost libraries
RUN apt-get install -y libboost1.55-all-dev

## Download and Build nanomsg
## TODO cleanup after this build
RUN apt-get install -y wget
RUN apt-get install make
RUN wget http://download.nanomsg.org/nanomsg-0.3-beta.tar.gz
RUN tar -xvzf nanomsg-0.3-beta.tar.gz
RUN mkdir /software/nanomsg
RUN ./nanomsg-0.3-beta/configure --prefix /software/nanomsg && make && make install  

# Install Git
RUN apt-get install -y git

# Clone K3-Core from Git
RUN mkdir /k3
RUN mkdir /k3/core
RUN git clone https://github.com/DaMSL/K3-Core.git /k3/core

## Move a compile script over to the docker image
#RUN mv /k3/core/runtime/cpp/test/compile2 /k3/core/runtime/cpp/test/distributed/compile2
##
#ENV PATH /k3/core/runtime/cpp/test:$PATH

# Install GHC and cabal
RUN apt-get install -y happy
RUN apt-get install -y zlib1g-dev
RUN apt-get install -y ghc
RUN apt-get install -y cabal-install
RUN cabal update
RUN cabal install cabal-install
ENV PATH /.cabal/bin/:$PATH

### Build K3-Core
WORKDIR /k3/core
RUN rm -rf /.ghc
RUN cabal sandbox init
RUN cabal install --only-dependencies
#RUN cabal configure
#RUN cabal build
#RUN cabal install

# Expose Ports
EXPOSE 3000 3001 3002 3003 3004 3005 3006 3007 3008 3009

# Install Vim
RUN apt-get install -y vim

ADD . /k3/core/runtime/cpp/test
RUN mkdir /k3/core/runtime/cpp/test/distributed
RUN mv /k3/core/runtime/cpp/test/compile2 /k3/core/runtime/cpp/test/distributed/compile2
RUN mv /k3/core/runtime/cpp/test/startup /k3/core/startup
RUN chmod +x /k3/core/startup
WORKDIR /k3/core
CMD ["/bin/bash","startup"]
