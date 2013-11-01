LeaderElectionK3
================
Description:
Leader Election in Asynchronous Network implement by K3

1. LCR (DONE)
   (1) Based on AsynchLCR on page 477 of lynch's book
   (2) cmd to run : " k3 -I /home/chao/work/K3/K3-Core/k3/lib/ interpret -b -p 127.0.0.1:40000:role=\"go\":uid="2":successor="127.0.0.1:50000" -p 127.0.0.1:50000:role=\"go\":uid="3":successor="127.0.0.1:60000" -p 127.0.0.1:60000:role=\"go\":uid="4":successor="127.0.0.1:70000" -p 127.0.0.1:70000:role=\"go\":uid="1":successor="127.0.0.1:40000" /home/chao/work/K3/LeaderElectionK3/LCR.k3 --log "Language.K3.Interpreter#Dispatch:debug" --log "Language.K3.Runtime.Engine#EngineSteps:debug" "

2. ABC (Debugging)
   (2) Based on Broadcast and convergecast algorithm on page 499 of Lynch's book
   (2) cmd to run :  
