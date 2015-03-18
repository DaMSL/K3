# Base image is Ubuntu 14.04
FROM damsl/k3-base:latest

### Build K3
WORKDIR /k3/K3
RUN git pull && git checkout development

RUN cabal sandbox init && \
    cabal install --only-dependencies --enable-library-profiling --ghc-options="-O2 -fprof-auto" --disable-documentation -j

RUN cabal configure --enable-library-profiling --enable-profiling --ghc-options="-O2 -fprof-auto" && \
    cabal build -j

### Build K3-Mosaic
WORKDIR /k3/K3-Mosaic
RUN ./build_opt.sh && \
    ./build_utils.sh

RUN apt-get install -y google-perftools
RUN apt-get install -y libgoogle-perftools-dev

RUN mkdir -p /software
ADD intelpcm.tar.gz /software/

RUN mv /software/intelpcm/lib/* /usr/local/lib && mv /software/intelpcm/include/* /usr/local/include 
ENV LD_LIBRARY_PATH /usr/local/lib/
