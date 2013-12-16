Word Count K3 implementation version 2
======================================

For wordCountV2.k3
---------------------

1. Description : Read collections from a txt file, each
   collection represents a job

2. simulation mode : k3 -I
   /home/chao/work/K3/K3-Core/k3/lib/ interpret -b
   -p 127.0.0.1:40000:role=\"master\"
   -p 127.0.0.1:61000:role=\"go\"
   -p 127.0.0.1:62000:role=\"go\"
   -p 127.0.0.1:63000:role=\"go\"
   -p 127.0.0.1:70000:role=\"go\"  /home/chao/work/K3/K3-Core/examples/distributed/MapReduce/WordCountV2/wordCountV2.k3 --log "Language.K3.Interpreter#Dispatch:debug" --log "Language.K3.Runtime.Engine#EngineSteps:debug"

TODO
----

1. output the intermediate result to a file by using sink function
