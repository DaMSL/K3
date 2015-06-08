#! /usr/bin/env ruby
# Parses a SQL file with schema definitions and an agenda mapping, to create a unified agenda.

require 'csv'
require 'pathname'

DATA_PATH = "."
DATA_EXTN = ".tbl"
DATA_DELM = "|"

MAPPING_RXP = /mapping\s*:=\s*'([^']*)'/, 1
STREAM_RXP = /CREATE STREAM (\w*) \((?:.|\n)*?\)\s*FROM FILE '([^']*)'/

if __FILE__ == $0
  begin
    sql_dump = File.new($*.shift).read
  rescue
    puts "usage: #{$0} <path/to/schema.sql> <path/to/output.psv>"
  end

  begin
    out_file = File.new($*.shift, "w")
  rescue
    puts "usage: #{$0} <path/to/schema.sql> <path/to/output.psv>"
    exit
  end

  begin
    table_sources = sql_dump.scan(STREAM_RXP).collect do |name, path|
      next if name == "AGENDA"
      [name, CSV::open(
         Pathname.new(DATA_PATH) + Pathname.new(path).basename.sub_ext(DATA_EXTN),
         "r",
         {:col_sep => ","},
       )]
    end.compact.to_h
  rescue
    puts "Unable to open source stream file."
    exit
  end

  schema_mapping = {}
  sql_dump[*MAPPING_RXP].split(/;\s*/).each do |line|
    table_name, columns = line.split(":")
    schema_mapping[table_name] = columns.split(",").map(&:to_i)
  end

  union_schema_width = schema_mapping.values.flatten.max

  CSV.open(out_file, "wb", {:col_sep => "|"}) do |out_handle|
    while !table_sources.empty?
      index = table_sources.keys.sample
      in_handle = table_sources[index]
      row = in_handle.shift
      if row.nil?
        table_sources.delete(index)
      else
        union_schema = [nil] * union_schema_width
        row.zip(schema_mapping[index]).each do |r, f|
          union_schema[f] = r
        end
        union_schema[0] = index
        union_schema[1] = 1
        out_handle << union_schema
      end
    end
    out_handle << [-1] + schema_defs.flatten
  end
end
