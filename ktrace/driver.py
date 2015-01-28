from scripts.ktrace import *
import sys
import os

if __name__ == "__main__":
  if len(sys.argv) < 4:
    print("usage: %s k3_dir k3_source_file result_variable_id result_dir" % sys.argv[0])
    sys.exit(1)


  k3BaseDir = sys.argv[1]
  k3Source = sys.argv[2]
  resultVar = sys.argv[3]
  resultDir = sys.argv[4]

 
  # Check the result directory for the result files
  if not os.path.isdir(resultDir):
    print("Result Directory does not exist: %s" % resultDir)
    sys.exit(1)

  resultFiles = [os.path.join(resultDir, f) for f in os.listdir(resultDir)]

  # Call Ktrace to generate SQL (generates loaders for Result) 
  ktrace_lines  = genKTraceSQL(k3BaseDir,k3Source, resultVar, resultFiles)

  # Generate COPY statements for Globals and Messages
  # TODO incorporate message's back into ktrace
  # copy_lines = genCopyStatements(globFiles, messFiles) 

  # Dump SQL to stdout for now
  for l in ktrace_lines:
    print(l)
