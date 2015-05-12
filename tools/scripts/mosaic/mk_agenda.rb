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

  schema_widths = in_handles.collect do |in_handle|
    width = in_handle.shift.length
    in_handle.rewind
    width
  end

  active = (0..in_handles.length - 1).to_a

  CSV.open(out_file, "wb", {:col_sep => "|"}) do |out_handle|
    while !active.empty?
      index = active.sample
      in_handle = in_handles[index]
      row = in_handle.shift
      if row.nil?
        active.delete(index)
      else
        before = schema_widths[0...index].inject(0, :+)
        after = schema_widths[index + 1..-1].inject(0, :+)
        out_handle << [in_handles.index(in_handle)] + [nil] * before + row + [nil] * after
      end
    end
  end
end
