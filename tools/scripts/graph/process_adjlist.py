import fileinput
import sys
import os

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: {} <INPUT> <PARTITIONS>".format(sys.argv[0]))
    else:
        input_file = sys.argv[1]
        partitions = int(sys.argv[2])

        lists = {i: [] for i in range(partitions)}

        for line in fileinput.input(input_file):
            (head, *tail) = line.split(',')
            lists[int(head) % partitions].append(line)

        for i in range(partitions):
            with open("{}_{:03d}_{:04d}".format(os.path.basename(input_file), partitions, i), "w") as out:
                out.writelines(lists[i])
