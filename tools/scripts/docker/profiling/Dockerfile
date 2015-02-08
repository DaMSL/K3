# Base image is Ubuntu 14.04
FROM damsl/k3-base

### Build K3
WORKDIR /k3/K3
RUN cabal sandbox init && \
    cabal install --only-dependencies --enable-library-profiling --ghc-options="-O2 -fprof-auto" --disable-documentation && \
    cabal configure --enable-library-profiling --enable-executable-profiling --ghc-options="-O2 -fprof-auto" && \
    cabal build

### Build K3-Mosaic
WORKDIR /k3/K3-Mosaic
RUN ./build_opt.sh && \
    ./build_utils.sh

