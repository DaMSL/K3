#! /usr/bin/env ruby
# Samples randomly from a set of CSV files, creating a null-padded union-schema. Individual rows are
# in alphabetical order of source file names, regardless of order specified.

require 'csv'

if __FILE__ == $0
  begin
    out_file = ARGV.shift
    in_handles = ARGV.sort.collect do |path|
      CSV.open(path, "r", {:col_sep => "|"})
    end
  rescue
    puts "Incorrect filename, verify."
  end

  schema_widths = in_handles.map do |in_handle|
    width = in_handle.shift.length
    in_handle.rewind
    width
  end

  schema_defs = in_handles.map do |in_handle|
    vals = in_handle.shift.map do |val|
        if /^\d+$/ =~ val then 0
        elsif /^\d+\.\d*$/ =~ val then 0
        elsif /^(true|false)$/ =~ val then false
        else ""
        end
    end
    in_handle.rewind
    vals
  end

  active = (0..in_handles.length - 1).to_a

  CSV.open(out_file, "wb", {:col_sep => "|"}) do |out_handle|
    while !active.empty?
      # choose a random in_handle to read from
      index = active.sample
      in_handle = in_handles[index]
      row = in_handle.shift
      if row.nil?
        active.delete(index)
      else
        before = schema_defs[0...index].flatten
        after = schema_defs[index + 1..-1].flatten
        out_handle << [index] + before + row + after
      end
    end
    out_handle << [-1] + schema_defs.flatten
  end
end
