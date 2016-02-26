#!/usr/bin/env ruby
# Invert stacks

require 'optparse'

def run(f)
  hash = {}
  fn_hash = {}
  total = 0
  File.open(f, "r") do |file|
    while (line = file.gets)
      r = /^(.*) (\d+)$/.match(line)
      num = r[2].to_i
      total += num
      stack_line = r[1]
      # do inversion
      elems = stack_line.split(";")
      elems.reverse!
      len = elems.length()
      (0...len - 1).each do |i|
        if hash.has_key?(elems)
          hash[elems] += num
        else
          hash[elems] = num
        end
        hd = elems[0]
        # save functions and their totals
        if fn_hash.has_key?(hd)
          fn_hash[hd] += num
        else
          fn_hash[hd] = num
        end
        # remove the head
        elems = elems[1..-1]
      end
    end
  end
  totalf = total.to_f
  arr = []
  max_pct = $options[:prune_pct].to_f / 100.0
  hash.each_pair do |k, v|
    # check that we pass the percentage
    key_num = fn_hash[k[0]]
    key_pct = key_num / totalf
    if key_pct <= max_pct then 
        arr << (k.join(";") + " " + v.to_s)
    end
  end
  arr.sort!
  arr.each {|s| puts s}
end

def main()
  $options = {}
  $options[:prune_pct] = 10 # default

  usage = "Usage: #{$PROGRAM_NAME} stack_file options"
  parser = OptionParser.new do |opts|
    opts.banner = usage
    opts.on("--prune [INT]", "Prune percentage") { |i| $options[:prune_pct] = i }
  end
  parser.parse!

  unless ARGV.size == 1
    puts parser.help
    exit(1)
  end

  run(ARGV[0])
end

main()
