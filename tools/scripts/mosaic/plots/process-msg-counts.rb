#!/usr/bin/env ruby
# Post-Process message count data.

require 'csv'
require 'fileutils'
require 'yaml'

if ARGV.length != 1
  puts "Usage: #{$PROGRAM_NAME} job_dir"
  exit 1
end

job_dir = ARGV[0]
if not File.directory?(job_dir)
  puts "Cannot find dir at #{job_dir}"
  exit 1
end

# List of yaml configurations (1 per peer)
yamls = `find #{job_dir} -name peers*.yaml | sort`.split("\n")

# Look through a peer's config to determine the peer's index globally
def find_peer_index(h)
  me = h['me']
  i = 0
  for peer in h['peers']
    addr = peer['addr']
    if addr == me
      return i
    end
    i += 1
  end
end

# Given an event log from a particular sender
# Produce a dict mapping each destination to a dict of sum(event_val) group by event_tag
def process_csv(path)
  res = {}
  CSV.foreach(path) do |row|
    (tag, _, dest_str, val) = row
    dest = dest_str.to_i
    if not res.has_key?(dest)
      res[dest] = {}
    end
    if not res[dest].has_key?(tag)
      res[dest][tag] = 0
    end
    res[dest][tag] += val.to_i
  end
  return res
end

all_res = {}
for yaml in yamls
  h = YAML.load_file(yaml)
  # get the index of the peer in the peer list
  peer_idx = find_peer_index(h)
  peer_dir = File.dirname(yaml)
  events_file = File.join(peer_dir, h['eventlog'])
  all_res[peer_idx] = process_csv(events_file)
end

puts YAML.dump(all_res)
