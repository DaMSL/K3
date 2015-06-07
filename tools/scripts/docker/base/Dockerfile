# Ubuntu 14.04 LTS derivative
FROM phusion/baseimage:0.9.13

# Create software directory
RUN mkdir /software

# Update apt-get
RUN echo "cache-bust" && apt-get update

# Locale setup
RUN apt-get install -y language-pack-en-base
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US.UTF-8
ENV LC_ALL en_US.UTF-8

## Install Clang 3.5, Boost and utilities
RUN apt-get update && apt-get install -y \
      linux-tools-3.13.0-32-generic libc6-dbg \
      wget aptitude tmux vim emacs make git mercurial \
      autoconf automake libtool pkg-config \
      zlib1g-dev libreadline6 libreadline6-dev libncurses5-dev libgmp-dev libyaml-dev libzmq3 libzmq3-dev \
      python-dev python-pip ruby \
      clang-3.5 libbz2-dev

# Install and configure GCC 4.9
RUN add-apt-repository ppa:ubuntu-toolchain-r/test && \
    apt-get update && \
    apt-get install -y gcc-4.9 g++-4.9 libstdc++-4.9-dev && \
    update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-4.9 20 && \
    update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-4.9 20

# Install decent vim configuration
RUN mkdir /root/.vim && \
    git clone https://github.com/tpope/vim-pathogen.git /root/.vim && \
    echo 'execute pathogen#infect() | syntax on | filetype plugin indent on' > /root/.vimrc && \
    git clone https://github.com/tpope/vim-sensible.git /root/.vim/bundle

# Install ocaml
RUN add-apt-repository -y ppa:avsm/ppa && apt-get update && \
    apt-get install -y ocaml ocaml-native-compilers camlp4-extra opam time && \
    export OPAMYES=1 && export OCAMLRUNPARAM=b && opam init && eval `opam config env` && opam install ocamlfind

RUN mkdir -p /software/boost-1.56 && \
    cd /software/boost-1.56 && \
    wget --no-verbose -T 10 http://downloads.sourceforge.net/boost/boost_1_56_0.tar.bz2 && \
    tar xvfj boost_1_56_0.tar.bz2 && \
    cd /software/boost-1.56/boost_1_56_0 && \
    ./bootstrap.sh --without-libraries=python --prefix=/usr && ./b2 -j 8 stage threading=multi link=shared && ./b2 -j 8 install threading=multi link=shared

# Install GHC and cabal
ENV PATH /opt/ghc/7.10.1/bin:/opt/cabal/1.22/bin:/opt/alex/3.1.4/bin:/opt/happy/1.19.5/bin:/root/.cabal/bin/:$PATH

WORKDIR /
RUN add-apt-repository ppa:hvr/ghc && \
    apt-get update && \
    apt-get install -y ghc-7.10.1 alex-3.1.4 happy-1.19.5 cabal-install-1.22 && \
    cabal update && \
    cabal install cabal-install -j

## Download and Build nanomsg
RUN mkdir -p /software/nanomsg/build
WORKDIR /software/nanomsg/build
RUN wget --no-verbose -T 10 http://download.nanomsg.org/nanomsg-0.3-beta.tar.gz && \
    tar -xvzf nanomsg-0.3-beta.tar.gz && \
    ./nanomsg-0.3-beta/configure --prefix /software/nanomsg && \
    make -j 4 && make install && \
    rm -rf /software/nanomsg/build

# Install libre2
RUN add-apt-repository -y ppa:pi-rho/security && \
    apt-get update && \
    apt-get install -y libre2-dev

# Install libyaml
RUN apt-get install -y libyaml-cpp-dev cmake

# Install csvpp
RUN git clone https://git01.codeplex.com/forks/wjjt/csvpp /software/csvpp && \
    cd /software/csvpp &&  \
    cmake . && make && mv libcsvpp.so /usr/lib/

# Install libdynamic
RUN git clone https://github.com/DaMSL/libdynamic /software/libdynamic && \
    cd /software/libdynamic &&  \
    ./autogen.sh && ./configure --prefix=/usr && make && make install

# Numa ctl
RUN wget --no-verbose -T 10 ftp://oss.sgi.com/www/projects/libnuma/download/numactl-2.0.10.tar.gz && \
    tar -xvf numactl-2.0.10.tar.gz && \
    cd numactl-2.0.10 && \
    ./autogen.sh && \
    ./configure && \
    make && \
    make install

# python pip installs
RUN pip install pyyaml

# Clone K3, K3-Mosaic from Git
RUN mkdir -p /k3/K3 /k3/K3-Mosaic && \
    git clone https://github.com/DaMSL/K3.git /k3/K3 && \
    git clone https://github.com/DaMSL/K3-Mosaic.git /k3/K3-Mosaic
