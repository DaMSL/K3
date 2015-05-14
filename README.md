K3
==========

[![Build Status](https://api.shippable.com/projects/5451c2f844927f89db3e42eb/badge?branchName=master)](https://app.shippable.com/projects/5451c2f844927f89db3e42eb/builds/latest)
[![Stories in Ready](https://badge.waffle.io/damsl/k3-core.png?label=ready&title=Ready)](https://waffle.io/damsl/k3-core)

K3 is a programming language for building large-scale **_data systems_**. Hadoop, Spark, Storm and Graphlab are all examples of such systems. We aim to provide a high-level, declarative way of exploring the design space and tradeoffs in these systems, enabling the construction of specialized, efficient, data processing tools.

K3's features center on separating high-performance systems design and implementation concerns from application and algorithm logic. We empower K3 developers to create algorithms with the mindset of working on a single machine, and then facilitate the transition to a scalable service with our data systems building blocks (e.g., resource allocation, replication, data partitioning, fault tolerance). Our language and compiler allows us to leverage powerful program analyses and statistics to realize co-design opportunities that are often lost at API boundaries in library and framework-based approaches.

K3 is a work in progress at all levels (language, compiler, runtime). We're developing the K3 compiler in the Haskell language and the runtime environment in C++. The compiler generates C++ code which is further compiled down to an executable. You can find examples of language features and simple algorithms in [damsl/K3/examples/](examples/)

---
##Getting Started
The easiest way to try out K3 is with our docker container:
https://registry.hub.docker.com/u/damsl/k3-vanilla/

1. Our docker image contains the K3 github codebase, as well as the dependencies and libraries listed below. From any docker installation, pull this with:

    `docker pull damsl/k3-vanilla`

2. Run your docker container:

    `docker run -i -t damsl/k3-vanilla /bin/bash`

3. Ensure you are in the correct directory for K3 development. Think of this as your K3 "workspace" :

    `cd /k3/K3`

4.  We provide scripts to both compile and clean your working environment. To build the Count Peers sample K3 program, run the following:

    `tools/scripts/run/compile.sh examples/distributed/count_peers.k3`

    Note that you now have a `__build` directory. When you build a K3 program, the compiler creates this (if doesn't exist); it contains the generated C++ code, object files, and your executable binary file which is simply,  `__build/A`.

5. Before running the K3 program, we will need to set up a YAML-formatted configuration file. The file should contain a document for <i>each</i> peer in the program. Copy and paste the following into a file called `peers.yaml` in your local workspace to set up a simple program which will launch three local peers:

<pre>
    ---
    me: [127.0.0.1, 40000]
    role: go
    peers: [{addr: [127.0.0.1, 40000]}, {addr: [127.0.0.1, 40001]}, {addr: [127.0.0.1, 40002]}]
    ---
    me: [127.0.0.1, 40001]
    role: go
    peers: [{addr: [127.0.0.1, 40000]}, {addr: [127.0.0.1, 40001]}, {addr: [127.0.0.1, 40002]}]
    ---
    me: [127.0.0.1, 40002]
    role: go
    peers: [{addr: [127.0.0.1, 40000]}, {addr: [127.0.0.1, 40001]}, {addr: [127.0.0.1, 40002]}]</pre>
 
`me` (the peer's ip/port address) and `role` (the source to start the peer) are mandatory. The `peers` field is required for any distributed program with 2 or more peers. Optionally, you may add any global variables from your program; include global vars to initialize each peer to a unique value instead of the default value. Also, note the formatting for an address data type (a 2-element sequence) and for the peers list (a sequence of mappings).

6. Finally, run the program with the following command:

    `__build/A -l 1 -p peers.yaml`

    The flag `-l 1` provides logging output. As  you run the program, you should see the `nodeCounter` global variable increment for the rendezvous peer. You will need to use `Ctrl-c` on this program as it does not include a command to halt.

7. Now you're ready to modify, update, & build new programs. Use the clean script before re-compiling to ensure your workspace is fresh.: `tools/scripts/run/clean.sh`.  

Where to go from here:
- View, modify, compile, & run the `count_peers_v2.k3` program which is located in the same folder as the `count_peers.k3` file. It is an updated version of the 



---
##Distributed Deployment
The previous section ran through a single-site, local deployment with three peers. In order to distribute K3 across a cluster of hosts, you will need to set up Mesos, a distributed system framework, and run the flask-based REST API for launching K3 programs. See the [scheduler](https://github.com/DaMSL/K3/tree/development/tools/scheduler/scheduler) on how to set up, configure, and deploy K3 through Mesos in your own cluster.

---
##Cloud Deployment
Alternatively, we have developed EC2 Cloud Formation templates which will automatically set up and launch a Mesos cluster with the Flask-based Scheduler web interface in your AWS account. 

---
## Building K3
We **strongly** recommend you use our docker container unless you're comfortable with Haskell, cabal and C++ compilers. You'll only need to build this repository if you plan to work directly on the compiler toolchain.

####Dependencies
Haskell platform: https://www.haskell.org/platform/
- version: 2014.2.0.0
- alternatively: [ghc == 7.8.3](http://www.haskell.org/ghc/download_ghc_7_8_3) and [cabal >= 1.20](http://www.haskell.org/ghc/download_ghc_7_8_3)

We use [cabal](http://www.haskell.org/cabal/) for managing Haskell package and library dependencies.

For second-stage compilation:
- any Clang/LLVM (>= 3.4) or gcc (>= 4.9) version with C++14 support
- the [Boost C++ libraries](http://www.boost.org/)


####Full Toolchain Installation
Assuming you've installed the Haskell platform:

    $> mkdir K3
    $> git clone git@github.com:DaMSL/K3.git K3/K3

    $> cd K3/K3
    $> cabal sandbox init
    $> cabal install --only-dependencies --disable-documentation -j
    $> cabal configure
    $> cabal build

This will leave you with a binary in: **K3/dist/build/k3/k3**
