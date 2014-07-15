# Read lines of user_vists from sys.argv[1], write date-conversions to stdout.
import calendar
import csv
import sys
import time

def main():
    with open(sys.argv[1]) as input_file:
        reader = csv.reader(input_file)
        for row in reader:
            t = time.strptime(row[2], "%Y-%M-%d")
            row[2] = str(calendar.timegm(t))
            print(','.join(row))

if __name__ == '__main__':
    main()
