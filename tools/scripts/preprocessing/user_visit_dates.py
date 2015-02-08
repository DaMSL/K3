# Read lines of user_vists from sys.argv[1], write date-conversions to stdout.
import calendar
import csv
import sys
import time

def convert(txt):
  t = time.strptime(txt,"%Y-%M-%d")
  return str(calendar.timegm(t))



def main():
    with open(sys.argv[1]) as input_file:
        reader = csv.reader(input_file)
        for row in reader:
            row[2] = convert(row[2])
            print(','.join(row))

if __name__ == '__main__':
    main()
