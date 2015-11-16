import csv
import matplotlib.pyplot as plt

queries_by_sf = {}  # SF => Query => [Rows]
with open('results.csv') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        sf = row['sf']
        if sf not in queries_by_sf:
            queries_by_sf[sf] = {}
        query = row['query']
        if query not in queries_by_sf[sf]:
            queries_by_sf[sf][query] = []
        queries_by_sf[sf][query].append(row)

for (sf, queries) in queries_by_sf.items():
    for (query, rows) in queries.items():
        # New figure per (query, sf) combination
        plt.figure()
        f, ax = plt.subplots()

        # Plot raw (#nodes, time) points
        xs = [row['#nodes'] for row in rows]
        ys = [row['time'] for row in rows]
        plt.plot(xs, ys, 'o-')

        # Labels
        plt.title('Query %s Scale Factor %s' % (query, sf))
        plt.xlabel('Number of Nodes')
        plt.ylabel('Time (ms)')

        # Dashed-line and Limits
        max_x = max([int(row['#nodes']) for row in rows])
        max_y = max([int(row['time']) for row in rows])
        plt.plot([0, max_x], [0, max_y], '--k')
        plt.xlim([0, max_x])
        plt.ylim([0, max_y])

        # Save results to file
        plt.savefig('results/plot-query-%s-sf-%s.png' % (query, sf))
