#!/usr/bin/env python
import matplotlib.pyplot as plt
import sys
import numpy as np
import yaml

# Grab yaml path
if len(sys.argv) != 2:
  print ("Usage: %s yaml_data" % sys.argv[0])
  sys.exit(1)
infile = sys.argv[1]

# Load yaml
data = {}
with open(infile, "r") as f:
  data = yaml.load(f)

# Build the heatmap manually
n = 129
arr = []
for i in range(n):
  arr.append([])
  for j in range(n):
    d = 0
    if i in data and j in data[i]:
      d += int(data[i][j]['16']) if '16' in data[i][j] else 0
      d += int(data[i][j]['18']) if '18' in data[i][j] else 0
    arr[-1].append(d)

# Plot
xs = range(n)
ys = range(n)
x, y = np.meshgrid(xs, ys)
intensity = np.array(arr)

plt.pcolormesh(x, y, intensity)
plt.colorbar()
plt.savefig('test.png')
