Word Count K3 implementation
============================

How to run
----------

1. run the cmd : k3 -I /home/chao/work/K3/K3-Core/k3/lib/ interpret -b -p 127.0.0.1:40001:role=\"mapper\" -p 127.0.0.1:40000 /home/chao/work/K3/K3-Core/examples/MapReduce/WordCount/MapReduce.k3 --log "Language.K3.Interpreter#Dispatch:debug" --log "Language.K3.Runtime.Engine#EngineSteps:debug"

Problem
-------
   
1. cannot pass collection from console (or we can setup several mappers)
   eg "k3 -I /home/chao/work/K3/K3-Core/k3/lib/ interpret -b -p 127.0.0.1:40001:role=\"mapper\"::wordsList="{| word:string | "first", "first" |} @ { Collection }" -p 127.0.0.1:40000 /home/chao/work/K3/K3-Core/examples/MapReduce/WordCount/MapReduce.k3 --log "Language.K3.Interpreter#Dispatch:debug" --log "Language.K3.Runtime.Engine#EngineSteps:debug""
   
