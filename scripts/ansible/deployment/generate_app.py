import yaml
import sys
import os

group_name = "all_peers"
run_play = "../../common/run_k3.yml"

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


def createDeploy(path, d):
  result = {}
  result['hosts'] = group_name
  result['tasks'] = [{'include': run_play, 
                      'vars':
                         {'app_name':    d['app_name'], 
                          'binary_path': d['binary_path'] }
                    }]

  with open(path, "w") as f:
    f.write(yaml.dump([result]))

if __name__ == "__main__":
  if (len(sys.argv) < 2):
    print("Usage: %s config_file" % sys.argv[0])
    sys.exit(1)

  config_file = sys.argv[1]

  d = loadYmlFile(config_file)
  app_name = d['app_name']


  out_folder = "apps/" + app_name + "/"

  os.system("mkdir -p %s" % (out_folder) )
  os.system("mkdir -p %s%s" % (out_folder, "group_vars"))
  os.system("mkdir -p %s%s" % (out_folder, "host_vars"))
  
  createInventoryFile(out_folder + "inventory.ini", d)
  createGroupVars(out_folder + "group_vars/" + group_name + ".yml", d)
  createHostVars(out_folder + "host_vars/", d)
  createDeploy(out_folder + "deploy.yml", d)
