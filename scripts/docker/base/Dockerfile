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

## Install Clang 3.4, Boost and utilities
RUN apt-get install -y \
      wget vim emacs make git mercurial \
      zlib1g-dev libreadline6 libreadline6-dev libncurses5-dev libgmp-dev \
      clang-3.4 libboost1.55-all-dev 


# Install and configure GCC 4.9
RUN add-apt-repository ppa:ubuntu-toolchain-r/test
RUN apt-get update
RUN apt-get install -y gcc-4.9
RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-4.9 20

# Install decent vim configuration
RUN mkdir /root/.vim
RUN git clone https://github.com/tpope/vim-pathogen.git /root/.vim
RUN echo 'execute pathogen#infect() | syntax on | filetype plugin indent on' > /root/.vimrc
RUN git clone https://github.com/tpope/vim-sensible.git /root/.vim/bundle

# Install ocaml
RUN apt-get install -y ocaml opam camlp4-extra

# Install GHC and cabal
WORKDIR /
RUN wget --no-verbose http://www.haskell.org/platform/download/2014.2.0.0/haskell-platform-2014.2.0.0-unknown-linux-x86_64.tar.gz
RUN tar xvf haskell-platform-2014.2.0.0-unknown-linux-x86_64.tar.gz
RUN /usr/local/haskell/ghc-7.8.3-x86_64/bin/activate-hs && \
    cabal update && \
    cabal install cabal-install

ENV PATH /root/.cabal/bin/:$PATH

## Download and Build nanomsg
RUN mkdir -p /software/nanomsg/build
WORKDIR /software/nanomsg/build
RUN wget --no-verbose http://download.nanomsg.org/nanomsg-0.3-beta.tar.gz && \
    tar -xvzf nanomsg-0.3-beta.tar.gz && \
    ./nanomsg-0.3-beta/configure --prefix /software/nanomsg && \
    make && make install && \
    rm -rf /software/nanomsg/build

# Install libre2
RUN add-apt-repository -y ppa:pi-rho/security
RUN apt-get update
RUN apt-get install -y libre2-dev

# Clone K3-Core, K3-Driver, K3-Mosaic from Git
RUN mkdir -p /k3/K3-Core /k3/K3-Driver /k3/K3-Mosaic && \
    git clone https://github.com/DaMSL/K3-Core.git /k3/K3-Core && \
    git clone https://github.com/DaMSL/K3-Driver.git /k3/K3-Driver && \
    git clone https://github.com/DaMSL/K3-Mosaic.git /k3/K3-Mosaic

