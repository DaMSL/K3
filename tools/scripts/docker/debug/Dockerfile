# Base image is Ubuntu 14.04
FROM damsl/k3-base

### Build K3
WORKDIR /k3/K3
RUN git pull && git checkout development

RUN cabal sandbox init && \
    cabal install --only-dependencies --disable-documentation -j && \
    cabal configure --ghc-options="-DDEBUG" --disable-documentation && \
    cabal build

### Build K3-Mosaic
WORKDIR /k3/K3-Mosaic
RUN ./build_opt.sh && \
    ./build_utils.sh

