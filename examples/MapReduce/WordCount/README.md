Word Count K3 implementation
============================

For MapReduce.k3 
----------------

1. run the cmd : k3 -I /home/chao/work/K3/K3-Core/k3/lib/ interpret -b -p 127.0.0.1:40001:role=\"mapper\" -p 127.0.0.1:40000 /home/chao/work/K3/K3-Core/examples/MapReduce/WordCount/MapReduce.k3 --log "Language.K3.Interpreter#Dispatch:debug" --log "Language.K3.Runtime.Engine#EngineSteps:debug" 

2. output to log file :  k3 -I /home/chao/work/K3/K3-Core/k3/lib/ interpret -b -p 127.0.0.1:40001:role=\"mapper\" -p 127.0.0.1:40000 /home/chao/work/K3/K3-Core/examples/MapReduce/WordCount/MapReduce.k3 --log "Language.K3.Interpreter#Dispatch:debug" --log "Language.K3.Runtime.Engine#EngineSteps:debug" >> /home/chao/work/K3/K3-Core/examples/MapReduce/WordCount/MapReduce.log 2>&1


For MapReduce.k3
----------------
   
1. simulation mode : k3 -I /home/chao/work/K3/K3-Core/k3/lib/ interpret -b -p 127.0.0.1:40000:role=\"go\" -p 127.0.0.1:51000:role=\"go\" -p 127.0.0.1:52000:role=\"go\" -p 127.0.0.1:53000:role=\"go\" -p 127.0.0.1:61000:role=\"go\" -p 127.0.0.1:62000:role=\"go\" -p 127.0.0.1:63000:role=\"go\" -p 127.0.0.1:70000:role=\"go\" /home/chao/work/K3/K3-Core/examples/MapReduce/WordCount/MapReduceV2.k3 --log "Language.K3.Interpreter#Dispatch:debug" --log "Language.K3.Runtime.Engine#EngineSteps:debug"

2. simulation mode to log file : k3 -I /home/chao/work/K3/K3-Core/k3/lib/ interpret -b -p 127.0.0.1:40000:role=\"go\" -p 127.0.0.1:51000:role=\"go\" -p 127.0.0.1:52000:role=\"go\" -p 127.0.0.1:53000:role=\"go\" -p 127.0.0.1:61000:role=\"go\" -p 127.0.0.1:62000:role=\"go\" -p 127.0.0.1:63000:role=\"go\" -p 127.0.0.1:70000:role=\"go\" /home/chao/work/K3/K3-Core/examples/MapReduce/WordCount/MapReduceV2.k3 --log "Language.K3.Interpreter#Dispatch:debug" --log "Language.K3.Runtime.Engine#EngineSteps:debug" >> /home/chao/work/K3/K3-Core/examples/MapReduce/WordCount/MapReduceV2.log 2>&1

3. network mode : 

k3 -I /home/chao/work/K3/K3-Core/k3/lib/ interpret -b -n -p 127.0.0.1:40000:role=\"go\" /home/chao/work/K3/K3-Core/examples/MapReduce/WordCount/MapReduceV2.k3 --log "Language.K3.Interpreter#Dispatch:debug" --log "Language.K3.Runtime.Engine#EngineSteps:debug" >> /home/chao/work/K3/K3-Core/examples/MapReduce/WordCount/MapReduceV2Log/40000.log 2>&1


k3 -I /home/chao/work/K3/K3-Core/k3/lib/ interpret -b -n -p 127.0.0.1:51000:role=\"go\" /home/chao/work/K3/K3-Core/examples/MapReduce/WordCount/MapReduceV2.k3 --log "Language.K3.Interpreter#Dispatch:debug" --log "Language.K3.Runtime.Engine#EngineSteps:debug" >> /home/chao/work/K3/K3-Core/examples/MapReduce/WordCount/MapReduceV2Log/51000.log 2>&1


k3 -I /home/chao/work/K3/K3-Core/k3/lib/ interpret -b -n -p 127.0.0.1:52000:role=\"go\" /home/chao/work/K3/K3-Core/examples/MapReduce/WordCount/MapReduceV2.k3 --log "Language.K3.Interpreter#Dispatch:debug" --log "Language.K3.Runtime.Engine#EngineSteps:debug" >> /home/chao/work/K3/K3-Core/examples/MapReduce/WordCount/MapReduceV2Log/52000.log 2>&1


k3 -I /home/chao/work/K3/K3-Core/k3/lib/ interpret -b -n -p 127.0.0.1:53000:role=\"go\" /home/chao/work/K3/K3-Core/examples/MapReduce/WordCount/MapReduceV2.k3 --log "Language.K3.Interpreter#Dispatch:debug" --log "Language.K3.Runtime.Engine#EngineSteps:debug" >> /home/chao/work/K3/K3-Core/examples/MapReduce/WordCount/MapReduceV2Log/53000.log 2>&1


k3 -I /home/chao/work/K3/K3-Core/k3/lib/ interpret -b -n -p 127.0.0.1:61000:role=\"go\" /home/chao/work/K3/K3-Core/examples/MapReduce/WordCount/MapReduceV2.k3 --log "Language.K3.Interpreter#Dispatch:debug" --log "Language.K3.Runtime.Engine#EngineSteps:debug" >> /home/chao/work/K3/K3-Core/examples/MapReduce/WordCount/MapReduceV2Log/61000.log 2>&1


k3 -I /home/chao/work/K3/K3-Core/k3/lib/ interpret -b -n -p 127.0.0.1:62000:role=\"go\" /home/chao/work/K3/K3-Core/examples/MapReduce/WordCount/MapReduceV2.k3 --log "Language.K3.Interpreter#Dispatch:debug" --log "Language.K3.Runtime.Engine#EngineSteps:debug" >> /home/chao/work/K3/K3-Core/examples/MapReduce/WordCount/MapReduceV2Log/62000.log 2>&1


k3 -I /home/chao/work/K3/K3-Core/k3/lib/ interpret -b -n -p 127.0.0.1:63000:role=\"go\" /home/chao/work/K3/K3-Core/examples/MapReduce/WordCount/MapReduceV2.k3 --log "Language.K3.Interpreter#Dispatch:debug" --log "Language.K3.Runtime.Engine#EngineSteps:debug" >> /home/chao/work/K3/K3-Core/examples/MapReduce/WordCount/MapReduceV2Log/63000.log 2>&1


k3 -I /home/chao/work/K3/K3-Core/k3/lib/ interpret -b -n -p 127.0.0.1:70000:role=\"go\" /home/chao/work/K3/K3-Core/examples/MapReduce/WordCount/MapReduceV2.k3 --log "Language.K3.Interpreter#Dispatch:debug" --log "Language.K3.Runtime.Engine#EngineSteps:debug" >> /home/chao/work/K3/K3-Core/examples/MapReduce/WordCount/MapReduceV2Log/70000.log 2>&1

  
TODO
----

1. try sink and src

2. try nick's script for network mode

3. more generic trigger or add a lib 
