import yaml
import sys
import os

group_name = "all_peers"


def loadYmlFile(path):
  with open(path,"r") as f:
    return yaml.load(f.read())

def createInventoryFile(path, d):
  with open(path, "w") as f:
    # Create a group
    f.write("[%s]\n" % group_name)
    # Add each host
    for host in d['hosts']:
      f.write(host['name'] + "\n")

def createGroupVars(path, d):
  with open(path, "w") as f:
  # TODO better log options
    new_d = {"k3_bindings": d["k3_bindings"], "enable_logging": False}
    f.write(yaml.dump(new_d))

def createHostVars(path, d):
  for host in d['hosts']:
    with open(path + "%s.yml" % host['name'], "w") as f:
      result = {'ip': host['ip'], 'local_peers':[]}
      print(host['num_peers'])
      for i in range(host['num_peers']):
        peer = {'port': 40000 + i,
                'overrides': {} }
        for d2 in d['data_files']:
          key = d2['variable_name']
          suff = d2['path_suffix_fmt'] % (i + host['file_start_index'])
          val = "\"" +  d2['path_prefix'] + suff + "\""
          peer['overrides'][key] = val
        result['local_peers'].append(peer)
      f.write(yaml.dump(result))




if __name__ == "__main__":
  if (len(sys.argv) < 3):
    print("Usage: %s config_file app_name", sys.argv[0])
    sys.exit(1)

  config_file = sys.argv[1]
  app_name = sys.argv[2]

  os.system("mkdir %s" % app_name)
  os.system("mkdir %s/%s" % (app_name, "group_vars"))
  os.system("mkdir %s/%s" % (app_name, "host_vars"))

  out_folder = app_name + "/"
  d = loadYmlFile(config_file)
  createInventoryFile(out_folder + "inventory.ini", d)
  createGroupVars(out_folder + "group_vars/" + group_name + ".yml", d)
  createHostVars(out_folder + "host_vars/", d)
