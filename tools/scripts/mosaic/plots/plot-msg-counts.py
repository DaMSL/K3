#!/usr/bin/env python
import matplotlib.pyplot as plt
import sys
import numpy as np
import yaml
import argparse
import csv

def make_array(max_node, variant, data):
    size = max_node + 1
    arr = [[0 for x in range(size)] for x in range(size)]
    for i in data:
      for j in data[i]:
        if variant in data[i][j]:
          arr[i][j] += int(data[i][j][variant])
    return arr

def make_png(max_node, variant, file, data):
    # Build the heatmap manually
    size = max_node + 1
    arr = make_array(max_node, variant, data)

    # Plot
    xs = np.arange(size)
    ys = np.arange(size)
    x, y = np.meshgrid(xs, ys)
    intensity = np.array(arr)

    plt.clf()
    plt.pcolormesh(x, y, intensity)
    plt.colorbar()
    plt.ylabel('Sources')
    plt.xlabel('Destinations')
    plt.title(variant)
    plt.savefig(file)

def make_csv(max_node, variant, file, data):
    arr = make_array(max_node, variant, data)
    with open(file, 'wb') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        for row in arr:
            writer.writerow(row)

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

    # make png files
    variants = [("poly_bytes", out_prefix + "_pbytes.png"),
                ("upoly_bytes", out_prefix + "_ubytes.png"),
                ("msgs", out_prefix + "_m.png")]

    for v, f in variants:
        make_png(max_node, v, f, data)

    # make csv files
    variants = [("poly_bytes", out_prefix + "_pbytes.csv"),
                ("upoly_bytes", out_prefix + "_ubytes.csv"),
                ("msgs", out_prefix + "_m.csv")]

    for v, f in variants:
        make_csv(max_node, v, f, data)
    
if __name__ == '__main__':
    main()
