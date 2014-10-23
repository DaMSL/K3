K3/core
==========

[![Build Status](https://api.shippable.com/projects/543afdb77a7fb11eaa64eaec/badge?branchName=master)](https://app.shippable.com/projects/543afdb77a7fb11eaa64eaec/builds/latest)
[![Stories in Ready](https://badge.waffle.io/damsl/k3-core.png?label=ready&title=Ready)](https://waffle.io/damsl/k3-core)

K3 is a programming language for building large-scale **_data systems_**. Hadoop, Spark, Storm and Graphlab are all examples of such systems. We aim to provide a high-level, declarative way of exploring the design space and tradeoffs in these systems, enabling the construction of specialized, efficient, data processing tools.

K3's features center on separating high-performance systems design and implementation concerns from application and algorithm logic.
We empower K3 developers to create algorithms with the mindset of working on a single machine, and then facilitate the transition to a scalable service with our data systems building blocks (e.g., resource allocation, replication, data partitioning, fault tolerance).
Our language and compiler allows us to leverage powerful program analyses and statistics to realize co-design opportunities that are often lost at API boundaries in library and framework-based approaches.

K3 is a work in progress at all levels (language, compiler, cloud runtime).
You can find examples of language features, and simple algorithms in [damsl/K3-Core/examples/](examples/)

The easiest way to try out K3 is with our docker container:
https://registry.hub.docker.com/u/damsl/k3-vanilla/

From any docker installation, you can grab this with:

    $> docker pull damsl/k3-vanilla

Our docker image contains both the K3-Core and K3-Driver codebases, as well as the dependencies and libraries listed below. We're developing K3 in the Haskell language, and K3 generates C++ code.

Dependencies
-------------
Haskell platform: https://www.haskell.org/platform/
- version: 2014.2.0.0
- alternatively: [ghc == 7.8.3](http://www.haskell.org/ghc/download_ghc_7_8_3) and [cabal >= 1.20](http://www.haskell.org/ghc/download_ghc_7_8_3)

We use [cabal](http://www.haskell.org/cabal/) for managing Haskell package and library dependencies.

For second-stage compilation:
- any Clang/LLVM (>= 3.4) or gcc (>= 4.9) version with C++14 support
- the [Boost C++ libraries](http://www.boost.org/)

For cluster deployment:
- [ansible](http://www.ansible.com)
- [docker](https://www.docker.com)


Full Toolchain Installation
----------------------------
We **strongly** recommend you use our docker container unless you're comfortable with Haskell, cabal and C++ compilers. You'll only need to build this repository if you plan to work directly on the compiler toolchain.

This repository contains the compiler toolchain backend implemented as a Haskell package.
To use the library, you'll also need a frontend, as found in our driver repo: http://github.com/damsl/K3-Driver

Assuming you've installed the Haskell platform:

    $> mkdir K3
    $> git clone git@github.com:DaMSL/K3-Core.git K3/K3-Core
    $> git clone git@github.com:DaMSL/K3-Driver.git K3/K3-Driver

    $> cd K3/K3-Core
    $> cabal sandbox init
    $> cabal configure
    $> cabal install --only-dependencies
    $> cabal build

    $> cd ../K3-Driver
    $> cabal sandbox init
    $> cabal sandbox add-source ../K3-Core
    $> cabal configure
    $> cabal install --only-dependencies
    $> cabal build

This will leave you with a binary in: **K3-Driver/dist/build/k3/k3**

Running and Deploying
----------------------
The K3 driver supports several modes of execution for testing simple programs:
- Interpreted execution
- Network simulation (in a single process)
- Multithreaded and multiprocess network execution

For example, to run [our fibonnacci example](examples/algorithms/fibonacci.k3) on a single local peer:

    $> K3-Driver/dist/build/k3/k3 -I K3-Core/lib/k3 interpret \
                                  -b -p 127.0.0.1:40000:role=\"s1\" \
                                  K3-Core/examples/algorithms/fibonacci.k3

Example interpreter output:

    $> Mode: Interpret
        Batch
           Network: False
           System environment:
           127.0.0.1:40000
             me   => 127.0.0.1:40000
             role => "s1"
           [... snip remainder of config ...]

        Input: "../K3-Core/examples/algorithms/fibonacci.k3"

        [127.0.0.1:40000]               ## Final program state printed at exit
          Value: VTuple []
          Environment:
            final                       => 8
            [... snip other variables ...]

We're actively working on making larger deployments easy to manage.
We deploy K3 on our cluster with ansible and docker (and soon Mesos) and will release our deployment tools following more testing and experimentation. Please contact us if you'd like help deploying K3.


Core Library Installation
--------------------------

This package can be compiled using:

    $> cabal configure
    $> cabal build

Additionally the API documentation can be generated by configuring as above,
followed by:

    $> cabal haddock --hyperlink-source
