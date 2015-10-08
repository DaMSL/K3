#! /usr/bin/env ruby
require 'csv'
require 'optparse'

if __FILE__ == $0
  options = {}
  options[:suffix] = ""
  options[:field] = 0
  parser = OptionParser.new do |p|
    p.banner = "usage: #{$PROGRAM_NAME} /path/to/multiplexed/agenda.tbl"
    p.on("-s", "--suffix SUFFIX", "Suffix for generated demuxed files.") { |s| options[:suffix] = s}
    p.on("-f", "--field NUMBER", "Field to demux on.") { |f| options[:field] = f.to_i }
  end

  parser.parse!

  unless ARGV.size == 1
    puts parser.help
    exit 1
  end

  output_handles = {}
  CSV.foreach($*.shift, {:col_sep => "|", :quote_char => "\""}) do |row|
    stream_name = row[options[:field]]
    if !output_handles.has_key?(stream_name)
      output_handles[stream_name] = CSV.open(
        "#{stream_name.downcase}#{options[:suffix]}.tbl",
        "wb",
        {:col_sep => "|", :quote_char => "\""}
      )
    end
    output_handles[stream_name] << row
  end
end
