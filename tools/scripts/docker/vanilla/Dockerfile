# Base K3 image
FROM damsl/k3-base

### Build K3-Core
WORKDIR /k3/K3-Core
RUN cabal sandbox init && \
    cabal install --only-dependencies --disable-documentation

### Build K3-Driver
WORKDIR /k3/K3-Driver
RUN cabal sandbox init && \
    cabal sandbox add-source /k3/K3-Core && \
    cabal install --ghc-options="-O2" --only-dependencies --disable-documentation && \
    cabal configure --ghc-options="-O2" && \
    cabal build

### Build K3-Mosaic
WORKDIR /k3/K3-Mosaic
RUN ./build_opt.sh && \
    ./build_utils.sh

