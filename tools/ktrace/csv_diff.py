import sys
import csv

# Parse a single csv field into a native type (try int, then float, then string)
def parse(field):
  try:
    return int(field)
  except ValueError:
    pass
  try:
    return float(field)
  except ValueError:
    return field

# Load a CSV file into a list. Parsing each row into native types.
def loadCSV(path):
  l = []
  with open(path, 'rb') as csvfile:
    reader =  csv.reader(csvfile, delimiter='|')
    for row in reader:
      l.append(map(lambda x : parse(x.strip()) , row))
  return l

# Check if two fields are approximately equal
def approxEq(field1, field2):
  if (isinstance(field1, float) and isinstance(field2, float)):
    err = abs(field1 - field2) / field1
    if err > .1:
      return False
    return True

  return field1 == field2

# Sort and diff two lists
def diff(correct, actual):
  if len(correct) != len(actual):
    print("Failed: Correct has %d rows. Actual has %d rows" % (len(correct), len(actual)))
    return False

  correct.sort()
  actual.sort()

  for i in xrange(len(correct)):
    cRow = correct[i]
    aRow = actual[i]
    for j in xrange(len(cRow)):
      cField = cRow[j]
      aField = aRow[j]
      if not approxEq(cField, aField):
        print("Comparison failed at row %d: " % i)
        print(cRow)
        print(aRow)
        print("Correct: %s vs. Actual: %s" % (cField, aField))
        return False
  return True

if __name__ == "__main__":
  if len(sys.argv) != 3:
    print("Usage: %s correct_csv actual_csv" % sys.argv[0])
    sys.exit(1)

  correct = loadCSV(sys.argv[1])
  actual = loadCSV(sys.argv[2])
  if diff(correct, actual):
    sys.exit(0)
  sys.exit(1)
