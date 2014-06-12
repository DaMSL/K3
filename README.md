K3/driver
=========
[![Build Status](https://travis-ci.org/DaMSL/K3-Driver.svg?branch=master)](https://travis-ci.org/DaMSL/K3-Driver)
[![Stories in Ready](https://badge.waffle.io/damsl/k3-driver.png?label=ready&title=Ready)](https://waffle.io/damsl/k3-driver)

This project contains the driver executable for the K3 programming language.


Build instructions
------------------
Use the following process:

```bash
cabal sandbox init
cabal add-source <path to core checkout>
cabal configure
cabal install --only-dependencies
cabal build
```
