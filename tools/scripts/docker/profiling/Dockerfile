# Base image is Ubuntu 14.04
FROM damsl/k3-base

### Build K3
WORKDIR /k3/K3
RUN git pull && git checkout development

RUN cabal sandbox init
RUN cabal install --only-dependencies --enable-library-profiling --ghc-options="-O2 -fprof-auto" --disable-documentation
RUN cabal configure --enable-library-profiling --enable-profiling --ghc-options="-O2 -fprof-auto" && \
    cabal build

### Build K3-Mosaic
WORKDIR /k3/K3-Mosaic
RUN ./build_opt.sh && \
    ./build_utils.sh

RUN mkdir -p /software
ADD tcmalloc.tar.gz /software/
ADD intelpcm.tar.gz /software/

RUN mv /software/libtcmalloc/lib/* /usr/local/lib && mv /software/libtcmalloc/include/* /usr/local/include
RUN mv /software/intelpcm/lib/* /usr/local/lib && mv /software/intelpcm/include/* /usr/local/include 
ENV LD_LIBRARY_PATH /usr/local/lib/
