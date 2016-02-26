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
  :mach_limit     => [:num_machines, 8], # or :per_host
  :queries        => ["1","3","4","6","11a","12","17"],
  :scale_factors  => ["0.1","1", "10"],
  :switch_counts  => [1, 2, 4, 8, 16, 32, 64],
  :node_counts    => [1, 4, 8, 16, 32, 64],
  :correctives    => false,
  :trials         => 3,
  :rebatch        => nil,
  :sleep_time     => nil,
  :backfill_files => [],
  :use_hms        => false # by default
}

$script_path = File.expand_path(File.dirname(__FILE__))
$k3_path     = File.expand_path(File.join($script_path, "..", "..", ".."))
$common_path = File.expand_path(File.join($k3_path, ".."))

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

# Run a single configuration
def run_trial(sf, query, switches, nodes, perhost, nmask)
  # Keep a seperate output file for the trial
  trial_id = "sf-#{sf}-query-#{query}-#{switches}-switch-#{nodes}-nodes-#{perhost}-perhost"
  output_path = File.join($options[:workdir], "#{trial_id}.out")
  puts "************ #{trial_id} ***************"

  # Construct a call to run.rb
  query_workdir = File.join($options[:workdir], "tpch#{query}")
  corrective_opt = $options[:correctives] ? "--corrective" : ""
  batch_opt = $options[:rebatch] ? "--batch-size #{$options[:rebatch]}" : ""
  sleep_opt = $options[:sleep_time] ? "--msg-delay #{$options[:sleep_time]}" : ""
  cmd = "#{File.join($script_path, "run.rb")} -5"\
  " -w #{query_workdir}/"\
  " -p /local/data/tpch#{sf}g-fpb/"\
  " -s #{switches}"\
  " -n #{nodes}"\
  " --perhost #{perhost}"\
  " --query #{query}"\
  " --nmask #{nmask}"\
  " --compile-local"\
  " #{corrective_opt}"\
  " #{batch_opt}"\
  " #{sleep_opt}"\
  " #{File.join($common_path, "K3-Mosaic/tests/queries/tpch/query#{query}.sql")}"\
  " 2>&1 | tee #{output_path}"

  # Run and extract time upon success. Time of -1 for failure
  time = s_mean = s_std_dev = n_mean = n_std_dev = -1
  msg = "FAILED!"
  if system(cmd)
    # the job will be the highest in the workdir
    job_num = 0
    Dir.glob(File.join(query_workdir, "job_*")) do |dir|
      job = /.*job_(.*)$/.match(dir)[1].to_i
      job_num = job > job_num ? job : job_num
    end
    job_dir = File.join(query_workdir, "job_#{job_num}")
    time_file = File.join(job_dir, "time.txt")
    if File.exist?(time_file)
      s = File.read(time_file)
      regex = /time: (.*)\n.*mean:([,]*),.*std_dev:(.*)\n.*mean:([,]*),.*std_dev:(.*)$/m
      m = regex.match(s)
      time = m[0]; n_mean = m[1]; n_std_dev = m[2]; s_mean = m[3]; s_std_dev = m[4]
      msg = "Time: #{time} (ms)"
    end
  end
  puts "------------ #{msg}  ---------------"
  log_csv(sf, query, switches, nodes, perhost, n_mean, n_std_dev, s_mean, s_std_dev, time)
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
    opts.on("-f", "--sf x,y,z", Array, "List of scale factors to run") {|s| $options[:scale_factors] = s}
    opts.on("-q", "--queries x,y,z", Array, "List of queries to run") {|s| $options[:queries] = s}
    opts.on("-n", "--node-counts x,y,z", Array, "List of node configs to run") {|s| $options[:node_counts] = s.map {|x| x.to_i}}
    opts.on("-s", "--switch-counts x,y,z", Array, "List of switch configs to run") {|s| $options[:switch_counts] = s.map {|x| x.to_i}}
    opts.on("-t", "--trials [INT]", "Number of trials per configuration") {|s| $options[:trials] = s.to_i}
    opts.on("-b", "--backfill x,y,z",Array, "Backfill mode. Fill gaps in result files.") {|s| $options[:backfill_files] = s}
    opts.on("-h", "--use-hms", "Allow usage of HM machines") {$options[:use_hms] = true}
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
def log_csv(sf, query, switches, nodes, perhost, n_mean, n_stdev, s_mean, s_stdev, time)
  csv_path = File.join($options[:workdir], $options[:result_file])
  CSV.open(csv_path, "a") do |csv|
    csv << [sf, query, switches, nodes, perhost, n_mean, n_stdev, s_mean, s_stdev, time]
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
  queries = $options[:queries]
  scale_factors = $options[:scale_factors]
  sw_counts = $options[:switch_counts]
  nd_counts = $options[:node_counts]
  trials = $options[:trials]
  desired = []

  for (query, sf, switch_count, node_count) in queries.product(scale_factors, sw_counts, nd_counts)
    for _ in (1..trials)
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
    per_host = if $options[:mach_limit][0] == :per_host
                            $options[:mach_limit][1]
                         else
                          config["#nodes"].to_i / $options[:mach_limit][1]
                         end
    nodes = config["#nodes"]
    num_mach = nodes / per_host
    nmask = "^(qp-hd([6,7,9]|1[0,2-5]))" # default hd mask
    if num_mach > 8
      if $options[:use_hms]
        nmask += "|(qp-hm.*)" # allow hms as well
      else
        puts "#{num_mach} machines requested, but no HMs available. Skipping test..."
        next
      end
    end
    nmask += ")$"
    run_trial(*config.values, per_host, nmask)
  end
end

main()
