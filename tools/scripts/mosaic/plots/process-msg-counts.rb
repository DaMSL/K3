#!/usr/bin/env ruby
# Post-Process message count data.

require 'csv'
require 'fileutils'
require 'yaml'
require 'optparse'

$tags = { '14' => 'poly_bytes',
          '15' => 'upoly_bytes',
          '16' => 'mixed_msgs',
          '17' => 'poly_only_bytes',
          '18' => 'poly_msgs'
        }

# Look through a peer's config to determine the peer's index globally
def find_peer_index(h)
  me = h['me']
  i = 0
  h['peers'].each do |peer|
    addr = peer['addr']
    return i if addr == me
    i += 1
  end
end

# Given an event log from a particular sender
# Produce a dict mapping each destination to a dict of sum(event_val) group by event_tag
def process_csv(path)
  res = {}
  CSV.foreach(path) do |row|
    (tag_str, _, dest_str, val_str) = row
    next unless $tags.has_key? tag_str
    tag, dest, val = $tags[tag_str], dest_str.to_i, val_str.to_i
    res[dest] = {} unless res.has_key?(dest)
    res[dest][tag] = 0 unless res[dest].has_key?(tag)
    res[dest][tag] += val
  end
  res
end

def run()
  if $options[:job_dir]
    job_dir = $options[:job_dir]
    if not File.directory?(job_dir)
      puts "Cannot find dir at #{job_dir}"
      exit 1
    end

    # List of yaml configurations (1 per peer)
    yamls = `find #{job_dir} -name peers*.yaml | sort`.split("\n")
  else
    yamls = [$options[:job_file]]
  end

  all_res = {}
  for yaml in yamls
    f = File.read(yaml)
    YAML.load_stream f do |h|
      # get the index of the peer in the peer list
      peer_idx = find_peer_index(h)
      peer_dir = $options[:job_file] ? "." : File.dirname(yaml)
      events_file = File.join(peer_dir, h['eventlog'])
      all_res[peer_idx] = process_csv(events_file)
    end
  end

  puts YAML.dump(all_res)
end

def main()
  $options = {}
  usage = "Usage: #{$PROGRAM_NAME} options"
  parser = OptionParser.new do |opts|
    opts.banner = usage
    opts.on("-j", "--job-dir [PATH]", "Path for jobs") {|s| $options[:job_dir] = s}
    opts.on("-f", "--file [FILE]", "Local json file for jobs") {|s| $options[:job_file] = s}
  end
  parser.parse!
  if !($options[:job_dir] || $options[:job_file])
    puts "Job directory or file required"
    exit 1
  end
  run
end

main
