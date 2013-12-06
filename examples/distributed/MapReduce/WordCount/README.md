Word Count K3 implementation
============================

For MapReduceV3.k3
----------------

1. simulation mode : k3 -I /home/chao/work/K3/K3-Core/k3/lib/ interpret -b -p 127.0.0.1:40000:role=\"master\" -p 127.0.0.1:51000:role=\"go\" -p 127.0.0.1:52000:role=\"go\" -p 127.0.0.1:53000:role=\"go\" -p 127.0.0.1:61000:role=\"go\" -p 127.0.0.1:62000:role=\"go\" -p 127.0.0.1:63000:role=\"go\" -p 127.0.0.1:70000:role=\"go\" /home/chao/work/K3/K3-Core/examples/distributed/MapReduce/WordCount/MapReduceV3.k3 --log "Language.K3.Interpreter#Dispatch:debug" --log "Language.K3.Runtime.Engine#EngineSteps:debug"

2. simulation mode to log file : k3 -I /home/chao/work/K3/K3-Core/k3/lib/ interpret -b -p 127.0.0.1:40000:role=\"master\" -p 127.0.0.1:51000:role=\"go\" -p 127.0.0.1:52000:role=\"go\" -p 127.0.0.1:53000:role=\"go\" -p 127.0.0.1:61000:role=\"go\" -p 127.0.0.1:62000:role=\"go\" -p 127.0.0.1:63000:role=\"go\" -p 127.0.0.1:70000:role=\"go\" /home/chao/work/K3/K3-Core/examples/distributed/MapReduce/WordCount/MapReduceV3.k3 --log "Language.K3.Interpreter#Dispatch:debug" --log "Language.K3.Runtime.Engine#EngineSteps:debug" >> /home/chao/work/K3/K3-Core/examples/distributed/MapReduce/WordCount/MapReduceV3.log 2>&1
  
TODO
----

1. try nick's script for network mode

2. more generic trigger or add a lib 
