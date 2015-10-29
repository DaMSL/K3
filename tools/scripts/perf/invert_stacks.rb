#!/usr/bin/env ruby

def run(f)
  hash = {}
  top_hash = {}
  File.open(f, "r") do |file|
    while (line = file.gets)
      r = /^(.*) (\d+)$/.match(line)
      num = r[2].to_i
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
        if top_hash.has_key?(hd)
          hash[hd] += num
        else
          hash[hd] = num
        end
        # remove the head
        elems = elems[1..-1]
      end
    end
  end
  arr = []
  hash.each_pair do |k, v|
    arr << (k.join(";") + " " + v.to_s)
  end
  arr.sort!
  arr.each {|s| puts s}
end

run(ARGV[0])
