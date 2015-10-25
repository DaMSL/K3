#!/usr/bin/env ruby
# Create in-order files for polyfile maker

require 'optparse'
require 'csv'

$num_files = 6

def main()
  tables = ['sentinel', 'customer', 'lineitem', 'orders', 'part', 'partsupp', 'supplier']
  files = tables.map do |n| File.open(n + '.tbl') end
  remaining = [1..$num_files].to_set # which files have remaining stuff
  start = true
  while remaining.length > 0 do
    choice = rand(remaining.length)
    line = ""
    begin
      line = files[choice].readline()
      if !start then print "," end
      print choice
      start = false
    rescue EOFError # check for EOF
      remaining = remaining.delete(choice)
    end
  end
  print ",0" # add sentinel
  files.iter do |f| f.close end
end

main
