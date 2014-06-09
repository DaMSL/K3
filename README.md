K3/driver
=========
[![Build Status](https://travis-ci.org/DaMSL/K3-Driver.svg?branch=master)](https://travis-ci.org/DaMSL/K3-Driver)

This project contains the driver executable for the K3 programming language.


Build instructions
------------------
Use the following process:

cabal sandbox init
cabal add-source <path to core checkout>
cabal configure
cabal install --only-dependencies
cabal build
