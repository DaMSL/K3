from subprocess import check_output
import sys

mainfile = "main.py"

def runTrial(path):
  cmd = "python %s %s" % (mainfile, path)
  print(cmd)
  out = check_output([cmd], shell=True)
  lines = out.split("\n")
  for line in lines:
    if "time:" in line:
      result = line.split(":")[1]
      return result;

if __name__ == "__main__":
  if len(sys.argv) < 3:
    print("usage: %s config_file num_trials")
   
  num_trials = int(sys.argv[2])
  config_path = sys.argv[1]
  results = []
  for i in range(num_trials):
    result = runTrial(config_path)
    results.append(result)

  print results
