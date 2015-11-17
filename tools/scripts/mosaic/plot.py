import csv
#import matplotlib.pyplot as plt

# Compute averages over repeated trials
files = ['mosaic_trial1-small/trials1.csv', 'mosaic_trial2-small/trials2.csv', 'mosaic_trial3-small/trials3.csv']
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

for (sf, queries) in sorted(lines.items()):
    print("------- SF %s -----" %sf)
    for (query, avgs) in sorted(queries.items()):
        print("%s: %s" % (query, str(avgs)))

#for (sf, queries) in queries_by_sf.items():
#    for (query, rows) in queries.items():
#        # New figure per (query, sf) combination
#        plt.figure()
#        f, ax = plt.subplots()
#
#        # Plot raw (#nodes, time) points
#        xs = [row['#nodes'] for row in rows]
#        ys = [row['time'] for row in rows]
#        plt.plot(xs, ys, 'o-')
#
#        # Labels
#        plt.title('Query %s Scale Factor %s' % (query, sf))
#        plt.xlabel('Number of Nodes')
#        plt.ylabel('Time (ms)')
#
#        # Dashed-line and Limits
#        max_x = max([int(row['#nodes']) for row in rows])
#        max_y = max([int(row['time']) for row in rows])
#        plt.plot([0, max_x], [0, max_y], '--k')
#        plt.xlim([0, max_x])
#        plt.ylim([0, max_y])
#
#        # Save results to file
#        plt.savefig('results/plot-query-%s-sf-%s.png' % (query, sf))
