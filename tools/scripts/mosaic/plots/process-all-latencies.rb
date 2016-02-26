#!/usr/bin/env ruby
# Post-Process latency profiling data.
# For each query, populate a directory with
# a sorted latency file per batch size.
# Run from K3 top-level directory

require 'optparse'
require 'fileutils'

$options = {
  :run_cmd  => "/k3/K3/tools/scripts/mosaic/run.rb",
  :data_dir => "/local/results/mosaic/latencies/",
  :out_dir => "./latencies",
  :queries => ["3", "4", "12", "17"],
  :batches => [100, 1000, 10000]
}

`mkdir -p /tmp/mosaic/`
for (query, batch_sz) in $options[:queries].product($options[:batches])
  puts "Processing Query #{query} Batch Size #{batch_sz}"

  # Look for the job directory that contains latency profiling data
  tag = "latency-#{query}-#{batch_sz}"
  in_dir = File.join($options[:data_dir], tag)
  if not File.directory?(in_dir)
    puts "Failed to find dir: #{in_dir}"
    exit 1
  end

  # Build a run.rb command to process latencies, then sort and output
  out_dir = File.join($options[:out_dir], query.to_s)
  `mkdir -p #{out_dir}`
  out_file = File.join(out_dir, "#{batch_sz}.txt")
  `#{$options[:run_cmd]} --process-latencies ".*qp3.*" --latency-job-dir #{in_dir} -w tpch4/` # TODO run.rb complains unless there is a source file present, hence the '-w tpch4' hack
  `sort -g latencies.txt > #{out_file}`
end
