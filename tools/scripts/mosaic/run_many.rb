#!/usr/bin/env ruby
# Experiment driver for mosaic queries

require 'yaml'
require 'fileutils'
require 'optparse'

$options = {
  :workdir        => 'runs',
  :config_file    => 'exp_config.yaml',
  :mach_limit     => [:num_machines, 8], # or :per_host
  :experiments    => %i{scalability latency memory},
  :queries        => %w{3 12 1 17 6 11a 4}, #19,15,13,22a don't terminate
  :scale_factors  => [0.1, 1, 10, 100],
  :switch_counts  => [1, 2, 4, 8, 16], # latency only
  :node_counts    => [1, 2, 4, 8, 16, 31],
  :gc_epochs      => [30 * 1000, 60 * 1000, 5 * 50 * 1000], # memory only
  :delays         => [0],                     # memory only

  # NOTE: most we can do is 2 perhost without disasterous slowdown
  # TODO: correctives currently don't work
  :correctives    => false,
  :trials         => 3,
  :trial_cutoff   => 10,
  :rebatch        => nil,
  :sleep_time     => nil,
  :use_hm         => true, # by default
  :clean          => false
}

$script_path = File.expand_path(File.dirname(__FILE__))
$k3_path     = File.expand_path(File.join($script_path, "..", "..", ".."))
$common_path = File.expand_path(File.join($k3_path, ".."))

# Print options for dry-run mode
def print_opts()
  puts "--- Options --"
  for (key, val) in $options
    puts "\t#{key} => #{val.to_s}"
  end
end

$headers = %i{exp sf nd sw perhost q trial}

def config_path() File.join($workdir, $options[:config_file]) end
def process_path() File.join($workdir, $options[:process_file]) end

def load_config()
  path = config_path()
  File.exists?(path) ? File.open(path, 'r') { |f| YAML.load(f) } : nil
end

def load_process()
  path = process_path()
  File.exists?(path) ? File.open(path, 'r') { |f| YAML.load(f) } : nil
end

def save_file(path, data, backup)
  # backup if needed
  if backup && File.exist?(path)
    max = 0
    Dir.glob("#{path}.*") do |p|
      num = p[/^.+\.(\d+)$/,1].to_i
      max = num if num > max
    end
    FileUtils.cp(path, "#{path}.#{max+1}")
  end
  FileUtils.cp(path, path + ".bak") if File.exist? path
  File.open(path, 'w') {|f| f << data.to_yaml}
end

def save_config(config, backup:false) save_file(config_path(), config, backup) end
def save_process(process, backup:false) save_file(process_path(), process, backup) end

def get_switches(nodes)
  [1,2,4,8,16,32,64].find {|n| n >= nodes}
end

# we're limited in how many nodes we can run with given a scale factor
# NOTE: actually we're currenlty just using HMs
def get_node_counts(sf)
  case sf
  when 100
    $options[:node_counts].reject {|n| n < 8}
  when 10
    $options[:node_counts].reject {|n| n < 2}
  else
    $options[:node_counts]
  end
end

def get_trials(sf) 1..(sf >= $options[:trial_cutoff] ? 2 : $options[:trials]) end

# Scalability: for each experiment, need roundtrip time
# Latency: for each experiment, need do_complete latency distribution
# Memory: for each experiment, need memory series over time
def create_config()
  # Materialize an array of desired trial configurations
  tests = []
  if $options[:experiments].include? :scalability
    $options[:scale_factors].each do |sf|
      get_node_counts(sf).each do |nodes|
        $options[:queries].each do |q|
          get_trials(sf).each do |trial|
            tests << $headers[0..-1].zip([:scalability, sf, nodes, get_switches(nodes), 2, q, trial]).to_h
    end end end end end
  # Latency experiments
  sample_delays = {0.1=>100, 1=>1000, 10=>10000, 100=>100000}
  if $options[:experiments].include? :latency
    [0.1, 1, 10, 100].each do |sf|
      get_node_counts(sf).reject {|nd| nd > 16}.each do |nodes|
        switch_counts = $options[:switch_counts].select {|s| s <= nodes}
        switch_counts.each do |switches|
          ['4', '3'].each do |q|
            get_trials(sf).each do |trial|
              t = $headers[0..-1].zip([:latency, sf, nodes, switches, 2, q, trial]).to_h
              t[:sample_delay] = sample_delays[sf]
              tests << t
  end end end end end end
  # Memory tests
  if $options[:experiments].include? :memory
    [1, 10].each do |sf|
      [1, 4, 8].each do |nodes|
        ['3'].each do |q|
          $options[:gc_epochs].each do |gc_epoch|
            $options[:delays].each do |delay|
              get_trials(sf).each do |trial|
                t = $headers[0..-1].zip([:memory, sf, nodes, get_switches(nodes), 2, q, trial]).to_h
                t[:gc_epoch] = gc_epoch
                t[:delay] = delay
                tests << t
  end end end end end end end
  #puts tests
  config = {
    :options => $options,
    :tests  => tests
  }
  config
end

# Run a single configuration
def run_trial(t)
  perhost = t[:nd] <= t[:perhost] ? 1 : t[:perhost]
  # Keep a seperate output file for the trial
  puts "---- #{t[:exp]}: sf#{t[:sf]} q#{t[:q]} n#{t[:nd]} s#{t[:sw]} #{perhost} perhost trial #{t[:trial]}----"

  # Construct a call to run.rb
  tpch = "tpch#{t[:q]}"
  query_workdir = File.join($workdir, tpch)
  FileUtils.mkdir_p query_workdir if !File.exist? query_workdir
  output_path = File.join(query_workdir, 'out.txt')
  infix = ''
  infix << " --binary #{File.join($options[:bindir], tpch, tpch)}" if $options[:bindir]
  infix << ' --corrective' if $options[:correctives]
  infix << " --batch-size #{$options[:rebatch]}" if $options[:rebatch]
  infix << " --msg-delay #{$options[:sleep_time]}" if $options[:sleep_time]
  infix << " --use-hm" if $options[:use_hm]
  infix << " --profile-latency --process-latency" if t[:exp] == :latency
  infix << " --mem-interval 250 --gc-epoch #{t[:gc_epoch]} --msg-delay#{t[:delay]} --process-memory" if t[:exp] == :memory
  infix << " --perhost #{perhost}"
  infix << " --sample-delay #{t[:sample_delay]}" if t[:sample_delay]

  cmd = "#{File.join($script_path, "run.rb")} -5"\
        " #{File.join($common_path, "K3-Mosaic/tests/queries/tpch/query#{t[:q]}.sql")}"\
        " -w #{query_workdir}/"\
        " -p /local/data/mosaic/#{t[:sf]}f"\
        " -s #{t[:sw]} -n #{t[:nd]}"\
        " --compile-local --create-local #{infix}"\
        " 2>&1 | tee #{output_path}"
  puts cmd

  # Run and extract time upon success.
  msg = "FAILED!"
  system(cmd)
  out_file = File.read(output_path)
  if out_file =~ /.*Mesos job succeeded$/m
    jobid = out_file[/^JOBID = (\d+)$/m, 1].to_i
    t[:jobid] = jobid
    puts "Job #{jobid} succeeded"
    job_dir = File.join(query_workdir, "job_#{jobid}")
    f = File.read(File.join(job_dir, 'time.txt'))
    m = /.*time: (\d+).*mean:(\d+).*max:(\d+).*std_dev:(\d+)\n.*mean:(\d+).*max:(\d+).*std_dev:(\d+)/m.match(f)
    new_t = {
      'time'   => m[1], 'n_mean' => m[2], 'n_max'  => m[3],
      'n_dev'  => m[4], 's_mean' => m[5], 's_max'  => m[6], 's_dev'  => m[7]
    }
    t.merge!(new_t)
    t.delete(:fail) if t[:fail]
    msg = "SUCCESS! Time=#{t['time']}"
  else
    t[:fail] = true
  end
  puts "------------ #{msg}  ---------------"
  t
end

# run processing in-place. Don't modify config file

# Function for actual running of tests
def run_exp(config)
  config = create_config unless config

  # remove unwanted tests
  if $options[:remove_exp] || $options[:remove_q] || $options[:remove_sf]
    config[:tests].reject! do |t|
      ($options[:remove_exp] ? t[:exp] == $options[:remove_exp] : true) &&
      ($options[:remove_q] ? t[:q] == $options[:remove_q] : true) &&
      ($options[:remove_sf] ? t[:sf] == $options[:remove_sf] : true)
    end
  end

  save_config(config, backup:$options[:clean])

  # Otherwise, setup and run
  config2 = config.dup
  config[:tests].each_with_index do |test, i|
    unless test[:jobid] || test[:skip] || (test[:fail] && $options[:skipfails]) # check for empty or skip
      if $options[:skipone] # oneshot
        test[:skip] = true
        $options[:skipone] = false
      else
        test = run_trial(test)
      end
      config2[:tests][i] = test
      save_config config2 # update file
    end
  end
end

# data processing for latency & memory
def run_process(config)
  puts "Processing data"
  if config.nil?
    puts "Must have existing config file"
    exit(1)
  end
  # find any tests we don't have processing data for
  config[:tests].each do |t|
    next if t[:skip] || t[:fail] || t[:exp] != :latency

    # Construct a call to run.rb
    tpch = "tpch#{t[:q]}"
    query_workdir = File.join($workdir, tpch)
    output_path = File.join(query_workdir, 'process.txt')
    job_dir = File.join(query_workdir, "job_#{t[:jobid]}")
    latency_path = File.join(job_dir, 'latencies.txt')
    next if File.exist? latency_path # only do this if no latency file

    puts "---- Processing job #{t[:jobid]}. #{t[:exp]}: sf#{t[:sf]} q#{t[:q]} n#{t[:nd]} s#{t[:sw]} per#{t[:perhost]} trial #{t[:trial]}----"

    infix = ''
    infix << " --process-latency" if t[:exp] == :latency
    infix << " --no-persist" # make sure not to overwrite last jobid
    infix << " --switch-latency" # calculate from switch. doesn't make a big difference
    infix << " --jobid #{t[:jobid]}"

    cmd = "#{File.join($script_path, "run.rb")} -w #{query_workdir}/ #{infix} 2>&1 | tee #{output_path}"
    puts cmd

    # Run and extract time upon success.
    system(cmd)
    if t[:exp] == :latency
      if !File.exist? latency_path
        puts "FAILED to process: no latency file found"
        exit(1)
      else
        puts "SUCCESS!"
      end
    end
  end
end

def main()
  # Initialization
  parse_args()
  $workdir = File.expand_path($options[:workdir])
  FileUtils.mkdir_p $workdir unless File.exist? $workdir

  config = load_config() unless $options[:clean]
  if $options[:do_process]
    run_process(config)
  else
    run_exp(config)
  end
end

def parse_args()
  usage = "Usage: #{$PROGRAM_NAME} options"
  parser = OptionParser.new do |opts|
    opts.banner = usage
    opts.on("-w", "--workdir [PATH]", "Path in which to create files") {|s| $options[:workdir] = s}
    # when a binary is in another directory
    opts.on("-b", "--bindir [PATH]", "Path to binaries") {|s| $options[:bindir] = s}
    opts.on("-c", "--config-file [NAME]", "Name of config file to create") {|s| $options[:config_file] = s}
    opts.on("-p", "--process", "Run processing (not experiments)") {|s| $options[:do_process] = true}
    opts.on("-e", "--experiments x,y,z", Array, "List of experiments to run (s/l/m)") do |a|
      $options[:experiments] = a.map {|s| s == 'm' ? :memory : s == 'l' ? :latency : :scalability}
    end
    opts.on("-f", "--sf x,y,z", Array, "List of scale factors to run") {|s| $options[:scale_factors] = s.map {|x| x == '0.1' ? 0.1 : x.to_i}}
    opts.on("-q", "--queries x,y,z", Array, "List of queries to run") {|s| $options[:queries] = s}
    opts.on("-n", "--node-counts x,y,z", Array, "List of node configs to run") {|s| $options[:node_counts] = s.map {|x| x.to_i}}
    opts.on("-s", "--switch-counts x,y,z", Array, "List of switch configs to run") {|s| $options[:switch_counts] = s.map {|x| x.to_i}}
    opts.on("-t", "--trials [INT]", "Number of trials per configuration") {|s| $options[:trials] = s.to_i}
    opts.on("-h", "--no-use-hm", "Disable usage of HM machines") {$options[:use_hm] = false}
    opts.on("-g", "--gc-epochs x,y,z", "List of gc epochs to use for memory") {|s| $options[:gc_epochs] = s}
    opts.on("-d", "--delays x,y,z", "List of delays to use for memory") {|s| $options[:delays] = s}

    opts.on("--clean", "Clean the experiment file") {$options[:clean] = true}
    opts.on("--remove-q Q", "Remove a query from the queue") {|s| $options[:remove_q] = s}
    opts.on("--remove-exp EXP", "Remove an experiment from the queue") {|s| $options[:remove_exp] = s}
    opts.on("--skipone", "Skip the next test") {$options[:skipone] = true}
    opts.on("--skipfails", "Skip failed tests") {$options[:skipfails] = true}
  end
  parser.parse!
end

main()
