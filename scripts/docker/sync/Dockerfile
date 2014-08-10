# Base image is Ubuntu 14.04
FROM ubuntu:14.04

# Add configuration files from the Docker context
ADD config /root/config

# Create software directory
RUN mkdir /software

# Add apt-fast ppa and update repositories
RUN add-apt-repository ppa:saiarcot895/myppa && \
    apt-get update

# Locale setup
RUN apt-get install -y language-pack-en-base
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US.UTF-8
ENV LC_ALL en_US.UTF-8

## Install basic utilities
RUN apt-get install -y apt-fast aria2 

## Install development libraries and tools
RUN apt-fast update && \
    apt-fast install -y wget curl git vim make \
                       clang-3.4 \
                       ocaml opam camlp4-extra \
                       happy ghc cabal-install \
                       zlib1g-dev libreadline6 libreadline6-dev libncurses5-dev libboost1.55-all-dev

# Regenerate phusion/baseimage ssh keys
RUN /etc/my_init.d/00_regen_ssh_host_keys.sh

## Download and Build nanomsg
## TODO cleanup after this build
RUN wget http://download.nanomsg.org/nanomsg-0.3-beta.tar.gz
RUN tar -xvzf nanomsg-0.3-beta.tar.gz
RUN mkdir /software/nanomsg
RUN ./nanomsg-0.3-beta/configure --prefix /software/nanomsg && make && make install  

# Install decent vim configuration
RUN mkdir /.vim
RUN git clone https://github.com/tpope/vim-pathogen.git /.vim
RUN echo 'execute pathogen#infect() | syntax on | filetype plugin indent on' > /.vimrc
RUN git clone https://github.com/tpope/vim-sensible.git /.vim/bundle

# Update cabal
RUN cabal update
RUN cabal install cabal-install
ENV PATH /.cabal/bin/:$PATH

# Clone K3-Mosaic from Git
RUN mkdir -p /k3/K3-Mosaic
RUN git clone https://github.com/DaMSL/K3-Mosaic.git /k3/K3-Mosaic

### Build K3-Mosaic
WORKDIR /k3/K3-Mosaic
RUN ./build_opt.sh
RUN ./build_utils.sh

# Clone K3-Core from Git
RUN mkdir -p /k3/K3-Core
RUN git clone https://github.com/DaMSL/K3-Core.git /k3/K3-Core

# Clone K3-Driver from Git
RUN mkdir -p /k3/K3-Driver
RUN git clone https://github.com/DaMSL/K3-Driver.git /k3/K3-Driver

### Build K3-Core
WORKDIR /k3/K3-Core
RUN rm -rf /.ghc
RUN cabal sandbox init
RUN cabal install --only-dependencies --disable-documentation

### Build K3-Driver
WORKDIR /k3/K3-Driver
RUN cabal sandbox init
RUN cabal sandbox add-source /k3/K3-Core
RUN cabal install --ghc-options="-O2" --only-dependencies --disable-documentation
RUN cabal configure --ghc-options="-O2"
RUN cabal build

### Set up Bittorrent Sync
# Download and extract the executable to /usr/bin; set up btsync with runit
RUN curl -o /usr/bin/btsync.tar.gz http://download-lb.utorrent.com/endpoint/btsync/os/linux-x64/track/stable
RUN cd /usr/bin && tar -xzvf btsync.tar.gz && rm btsync.tar.gz
RUN mkdir -p /etc/service/btsync && \
    cp /root/config/run_btsync.sh /etc/service/btsync/run && \
    chmod a+x /etc/service/btsync/run

# BTSync Web GUI
EXPOSE 8888

# BTSync listening port
EXPOSE 55555

CMD ["/sbin/my_init", "--enable-insecure-key"]
