from classes.k3deployer import K3Deployer
import utils.parse as parse
import sys

if len(sys.argv) < 2:
  print("usage: %s config_file" % sys.argv[0])
  sys.exit(-1)

config_file = sys.argv[1]
peers = parse.genK3Peers(config_file)
d = K3Deployer(peers)

d.run()
