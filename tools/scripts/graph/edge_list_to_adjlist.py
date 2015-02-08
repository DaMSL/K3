import sys
import os
import fileinput

if __name__ == '__main__':
    lists = {}
    for line in fileinput.input():
        if not line.strip():
            continue
        (source, dest) = line.split()
        try:
            lists[source].append(dest)
        except KeyError:
            lists[source] = [dest]

    for line in lists:
        print(','.join([line] + lists[line]))
