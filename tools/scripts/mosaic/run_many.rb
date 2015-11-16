#!/usr/bin/env ruby
# Experiment driver for mosaic queries

require 'csv'
require 'fileutils'
require 'optparse'

$headers = ["sf", "query", "#switches", "#nodes", "time"]
$options = {
  :dry_run        => false,
  :workdir        => ".",
  :result_file    => "results.csv",
  :num_machines   => 8, #qp-hd[1-9]$
  :queries        => ["1","3","4","6","11a","12","17"],
  :scale_factors  => ["0.1","1", "10"],
  :switch_counts  => [1],
  :node_counts    => [8, 16, 32, 64, 128],
  :correctives    => false,
  :trials         => 1,
  :backfill_files => []
}

# Run a single configuration
def run_trial(sf, query, switches, nodes, perhost)
  # Keep a seperate output file for the trial
  trial_id = "sf-#{sf}-query-#{query}-#{switches}-switch-#{nodes}-nodes-#{perhost}-perhost"
  output_path = File.join($options[:workdir], "#{trial_id}.out")
  puts "************ #{trial_id} ***************"

  # Construct a call to run.rb
  corrective_opt = $options[:correctives] ? "" : "--no-correctives"
  cmd = "./tools/scripts/mosaic/run.rb -5"\
	" -w /local/mosaic/tpch#{query}/"\
	" -p /local/data/tpch#{sf}g-fpb/"\
	" -s #{switches}"\
	" -n #{nodes}"\
	" --perhost #{perhost}"\
	" --query #{query}"\
	" --dots"\
	" --nmask \".*hd[1-9]$\""\
	" --compile-local"\
	" #{corrective_opt}"\
	" --map-overlap 0"\
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
    opts.on("-d", "--dry-run", "Dry Run -- Print options and exit") {|s| $options[:dry_run] = true}
    opts.on("-w", "--workdir [PATH]", "Path in which to create files") {|s| $options[:workdir] = s}
    opts.on("-r", "--result-file [NAME]", "Name of result file to create") {|s| $options[:result_file] = s}
    opts.on("-s", "--sf x,y,z", Array, "List of scale factors to run") {|s| $options[:scale_factors] = s}
    opts.on("-q", "--queries x,y,z", Array, "List of queries to run") {|s| $options[:queries] = s}
    opts.on("-n", "--node-counts x,y,z", Array, "List of node configs to run") {|s| $options[:node_counts] = s.map {|x| x.to_i}}
    opts.on("-t", "--trials [INT]", "Number of trials per configuration") {|s| $options[:trials] = s.to_i}
    opts.on("-b", "--backfill x,y,z",Array, "Backfill mode. Fill gaps in result files.") {|s| $options[:backfill_files] = s}
  end
  parser.parse!
end

# Print options for dry-run mode
def print_opts()
  puts "--- Options --"
  for (key, val) in $options
    puts "\t#{key} => #{val.to_s}"
  end
end

# Setup the workdir
def mk_workdir()
  if not File.directory?($options[:workdir])
    `mkdir -p #{$options[:workdir]}`
  end
end

# Append an entry to the result csv file
def log_csv(sf, query, switches, nodes, time)
  csv_path = File.join($options[:workdir], $options[:result_file])
  CSV.open(csv_path, "a") do |csv|
    csv << [sf, query, switches, nodes, time]
  end
end

# Read result csv files to determine which trials were succesful
def find_successes()
  rows = []
  for path in $options[:backfill_files]
    rows += CSV.read(path, {:headers => true}).map {|x| x.to_h}
  end

  res = rows.select {|x| x["time"] != "-1"}
  puts "Read result files. #Rows: #{rows.length}. #Successes: #{res.length}"
  res.each {|x| x.delete("time")}
  return res
end

def main()
  # Initialization
  parse_args()

  # Materialize an array of desired trial configurations 
  qs = $options[:queries]
  sfs = $options[:scale_factors]
  scs = $options[:switch_counts]
  ncs = $options[:node_counts]
  ts = $options[:trials]
  desired = []
  
  for (query, sf, switch_count, node_count) in qs.product(sfs, scs, ncs)
    for _ in (1..ts)
      desired << $headers[0...-1].zip([sf, query, switch_count, node_count].map {|x| x.to_s}).to_h
    end
  end

  # Use backfill files to determine remaining trials
  completed = find_successes()
  remaining = desired.subtract_once(completed)
  
  if $options[:backfill_files] != []
    puts "--- Backfill --"
    puts "\tDesired: #{desired.length}"
    puts "\tCompleted: #{completed.length}"
    puts "\tRemaining: #{remaining.length}"
  end

  # Print and exit on a dry run
  if $options[:dry_run]
    print_opts()
    puts "Remaining trials:"
    for row in remaining
      puts row.to_s
    end
    return
  end

  # Otherwise, setup and run
  mk_workdir()
  backup_old_results()
  log_csv(*$headers)

  for config in remaining
    datanodes_per_host = node_count / $options[:num_machines]
    run_trial(*config.values, datanodes_per_host)
  end
end

main()

# Lifted from StackOverflow. Duplicate-Aware Array subtraction.
class Array
  # Subtract each passed value once:
  #   %w(1 2 3 1).subtract_once %w(1 1 2) # => ["2", "3"]
  # Time complexity of O(n + m)
  def subtract_once(values)
    counts = values.inject(Hash.new(0)) { |h, v| h[v] += 1; h }
    reject { |e| counts[e] -= 1 unless counts[e].zero? }
  end
end
