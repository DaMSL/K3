# Base K3 image
FROM damsl/k3-base

### Build K3 toolchain.
WORKDIR /k3/K3
RUN git pull

RUN cabal sandbox init && \
    cabal install --only-dependencies --ghc-options="-O2" --disable-documentation -j && \
    cabal configure --ghc-options="-O2" && \
    cabal build

### Build K3-Mosaic
WORKDIR /k3/K3-Mosaic
RUN ./build_opt.sh && \
    ./build_utils.sh

