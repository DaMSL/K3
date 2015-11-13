#!/usr/bin/env ruby
# Experiment driver for mosaic queries

require 'csv'
require 'fileutils'
require 'optparse'

$options = {
  :workdir       => ".",
  :result_file   => "results.csv",
  :num_machines  => 8, #qp-hd[1-9]$
  :queries       => ["4"],
  :scale_factors => ["0.1"],
  :switch_counts => [1],
  :node_counts   => [8]
}

# Append an entry to the result csv file
def log_csv(sf, query, switches, nodes, time)
  csv_path = File.join($options[:workdir], $options[:result_file])
  CSV.open(csv_path, "a") do |csv|
    csv << [sf, query, switches, nodes, time]
  end
end

# Run a single configuration
def run_trial(sf, query, switches, nodes, perhost)
  # Keep a seperate output file for each trial
  trial_id = "sf-#{sf}-query-#{query}-#{switches}-switch-#{nodes}-nodes-#{perhost}-perhost"
  output_path = File.join($options[:workdir], "#{trial_id}.out")
  puts "************ #{trial_id} ***************"

  # Construct a call to run.rb
  cmd = "./tools/scripts/mosaic/run.rb -5"\
	" -w tpch#{query}/"\
	" -p /local/data/tpch#{sf}g-fpb/"\
	" -s #{switches}"\
	" -n #{nodes}"\
	" --perhost #{perhost}"\
	" --query #{query}"\
	" --dots"\
	" --nmask \".*hd[1-9]$\""\
	" --compile-local"\
	" ../K3-Mosaic/tests/queries/tpch/query#{query}.sql"\
	" 2>&1 | tee #{output_path}"

  # Run and extract time upon success. Time of -1 for failure
  time = -1
  msg = ""
  if system(cmd)
    time = `cat #{output_path} | grep time.*ms.*`.strip.split(" ")[-1].to_i
    msg = "Time: #{time} (ms)"
    if time == 0
      time = -1
      msg = "FAILED!"
    end
  else
    msg = "FAILED!"
  end
  puts "------------ #{msg}  ---------------"
  log_csv(sf, query, switches, nodes, time)
end

# Backup the results of a previous run, if necessary
def backup_old_results()
  path = File.join($options[:workdir], $options[:result_file])
  if not File.exist?(path)
    return
  end

  i = 0
  get_backup_i = lambda {|x| File.join($options[:workdir], "#{$options[:result_file]}.bak#{x}") }
  while File.exist?(get_backup_i[i])
    i += 1
  end
  `mv #{path} #{get_backup_i[i]}`
end

def parse_args()
  usage = "Usage: #{$PROGRAM_NAME} options"
  parser = OptionParser.new do |opts|
    opts.banner = usage
    opts.on("-w", "--workdir [PATH]", "Path in which to create files") {|s| $options[:workdir] = s}
  end
  parser.parse!
end

def main()
  # Initialization
  parse_args()
  if not File.directory?($options[:workdir])
    `mkdir -p #{$options[:workdir]}`
  end
  backup_old_results()
  log_csv("sf", "query", "#switches", "#nodes", "time") # Headers

  # Trials
  for query in $options[:queries]
    for sf in $options[:scale_factors]
      for switch_count in $options[:switch_counts]
        for node_count in $options[:node_counts]
          datanodes_per_host = node_count / $options[:num_machines]
          run_trial(sf, query, switch_count, node_count, datanodes_per_host)
        end
      end
    end
  end
end

main()
