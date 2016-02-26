#!/usr/bin/env pythin
# Plot the output of process-all-latencies.rb

import matplotlib.pyplot as plt

in_dir = "./latencies"
queries = ["3", "4", "12", "17"]
batches = [100, 1000, 10000]

def process(query, batch_sz):
  # Parse the input file (sorted latencies)
  path = "%s/%s/%d.txt" % (in_dir, query, batch_sz)     
  latencies = []
  with open(path, 'r') as f:
    for line in f:
      latencies.append(int(line))

  # Compute the CDF. (Fraction of samples w/ latency < l)
  fracs = []
  i = 1 
  n = len(latencies)
  for l in latencies:
    frac = float(i) / n
    fracs.append(frac)
    i += 1
  return (latencies, fracs)

for query in queries:
  # New Figure per query
  plt.figure()
  f, ax = plt.subplots()

  # One line per batch size
  for batch_sz in batches:
    (latencies, fracs) = process(query, batch_sz)
    plt.plot(latencies, fracs, label="Batch Size %d" % batch_sz)

  # Labels, etc. 
  plt.legend(loc='lower right')
  plt.title("Query %s Latency" % query)
  plt.xlabel("Latency (ms)")
  plt.ylabel("Fraction")

  # Save to file
  tag = "latency-%s.png" % query
  plt.savefig(tag)
