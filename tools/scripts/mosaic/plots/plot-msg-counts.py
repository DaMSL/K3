#!/usr/bin/env python3
import matplotlib.pyplot as plt
import sys
import numpy as np
import yaml
import argparse

def make_png(max_node, variant, file, data):
    # Build the heatmap manually
    arr = []
    for i in range(max_node):
      arr.append([])
      for j in range(max_node):
        d = 0
        if i in data and j in data[i] and variant in data[i][j]:
          d += int(data[i][j][variant])
        arr[-1].append(d)

    # Plot
    xs = range(max_node)
    ys = range(max_node)
    x, y = np.meshgrid(xs, ys)
    intensity = np.array(arr)

    plt.clf()
    plt.pcolormesh(x, y, intensity)
    plt.colorbar()
    plt.savefig(file)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("in_file", type=str, help="Input file")
    parser.add_argument("--out-prefix", dest='out_prefix', type=str, default="test", help="Choose out files prefix")
    args = parser.parse_args()
    in_file = args.in_file
    out_prefix = args.out_prefix

    # Load yaml
    data = {}
    with open(in_file, "r") as f:
      data = yaml.load(f)

    # get max node number
    max_node = 0
    for i in data:
        if i > max_node: max_node = i
        for j in data[i]:
            if j > max_node: max_node = j

    # unify tags
    for i in data:
        for j in data[i]:
            if not 'msgs' in data[i][j]:
                data[i][j]['msgs'] = 0
            if not 'poly_bytes' in data[i][j]:
                data[i][j]['poly_bytes'] = 0
            if 'mixed_msgs' in data[i][j]:
                data[i][j]['msgs'] += data[i][j]['mixed_msgs']
            if 'poly_msgs' in data[i][j]:
                data[i][j]['msgs'] += data[i][j]['poly_msgs']
            if 'poly_only_bytes' in data[i][j]:
                data[i][j]['poly_bytes'] += data[i][j]['poly_only_bytes']

    variants = [("poly_bytes", out_prefix + "_pbytes.png"),
                ("upoly_bytes", out_prefix + "_ubytes.png"),
                ("msgs", out_prefix + "_m.png")]

    for v, f in variants:
        make_png(max_node, v, f, data)
    
if __name__ == '__main__':
    main()
