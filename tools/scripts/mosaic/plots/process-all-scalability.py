#!/usr/bin/env python
# Form data into lines for the scalability graph.

import csv
import yaml

# Compute averages over repeated trials
files = ['data/scalability.csv']
queries_by_sf = {}  # SF => Query => #Nodes => [Rows]
for path in files:
    with open(path) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            sf = row['sf']
            if sf not in queries_by_sf:
                queries_by_sf[sf] = {}

            query = row['query']
            if query not in queries_by_sf[sf]:
                queries_by_sf[sf][query] = {}

            nodes = int(row['#nodes'])
            if nodes not in queries_by_sf[sf][query]:
                queries_by_sf[sf][query][nodes] = []
            if row['time'] != '-1':
                queries_by_sf[sf][query][nodes].append(row)

# Group into lines for plotting / Compute averages
lines = {}
for (sf, queries) in sorted(queries_by_sf.items()):
    if sf not in lines:
        lines[sf] = {}
    for (query, nodes) in sorted(queries.items()):
        if query not in lines[sf]:
            lines[sf][query] = []
        for (num, rows) in sorted(nodes.items()):
            avg = float(sum([int(x['time']) for x in rows])) / len(rows) if len(rows) > 0 else 0
            lines[sf][query].append(avg)

print(yaml.dump(lines))
