# This is a dockerfile for deploying previously compiled K3 binaries.
# It is set up for peer discovery
# Ubuntu 14.04 LTS derivative
FROM phusion/baseimage:0.9.13

# Create software directory
RUN mkdir /software

# Update apt-get
RUN apt-get update

# Locale setup
RUN apt-get install -y language-pack-en-base
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US.UTF-8
ENV LC_ALL en_US.UTF-8

RUN apt-get install -y wget vim git

# Install decent vim configuration
RUN mkdir /root/.vim
RUN git clone https://github.com/tpope/vim-pathogen.git /root/.vim
RUN echo 'execute pathogen#infect() | syntax on | filetype plugin indent on' > /root/.vimrc
RUN git clone https://github.com/tpope/vim-sensible.git /root/.vim/bundle

# Install Redis
RUN apt-get install -y redis-server redis-tools libhiredis0.10 libhiredis-dev libhiredis-dbg
EXPOSE 6379
EXPOSE 26379

RUN service ssh start
EXPOSE 22

ADD redis_and_bash.sh
