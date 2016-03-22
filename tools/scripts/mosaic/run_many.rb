#!/usr/bin/env ruby
# Experiment driver for mosaic queries

require 'yaml'
require 'fileutils'
require 'optparse'

$options = {
  :dry_run        => false,
  :workdir        => 'runs',
  :config_file    => 'exp_config.yaml',
  :mach_limit     => [:num_machines, 8], # or :per_host
  :experiments    => %w{scalability latency memory},
  :queries        => %w{6 11a 4 19 13 22a 12 1 17 15 3},
  :scale_factors  => [0.1, 1, 10, 100],
  # TODO: switches must currently correspond to node counts
  #:switch_counts  => [1, 2, 4, 8, 16, 32, 64],

  # NOTE: most we can do is 2 perpeer without disasterous slowdown
  :node_counts    => [1, 4, 8, 16, 24, 31],
  # TODO: correctives currently don't work
  :correctives    => false,
  :trials         => 3,
  :rebatch        => nil,
  :sleep_time     => nil,
  :use_hms        => true # by default
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

$headers = %w{exp sf nd sw perhost q trial}

def load_config()
  path = File.join($workdir, $options[:config_file])
  return nil unless File.exists? path
  YAML.load(path)
end

def save_config(h_list)
  path = File.join($workdir, $options[:config_file])
  File.open(path, 'w') do |f|
    f << h_list.to_yaml
  end
end

def get_switches(nodes)
  [1,2,4,8,16,32,64].find {|n| n >= nodes}
end

# Scalability: for each experiment, need roundtrip time
# Latency: for each experiment, need do_complete latency distribution
# Memory: for each experiment, need memory series over time
def create_config()
  # Materialize an array of desired trial configurations
  tests = $options[:scale_factors].product(
                        $options[:experiments],
                        $options[:node_counts],
                        $options[:queries],
                        (1..$options[:trials]).to_a).map do |sf,exp,nodes,query,trial|
    switches = get_switches(nodes)
    $headers[0..-1].zip([exp, sf, nodes, switches, 2, query, trial]).to_h
  end
  #puts tests
  config = {
    :options => $options,
    :tests  => tests
  }
  config
end

# Run a single configuration
def run_trial(t)
  exp = t['exp']
  sf = t['sf']
  nodes = t['nd']
  switches = t['sw']
  perhost = t['perhost']
  query = t['q']
  # Keep a seperate output file for the trial
  puts "---- #{exp}-sf#{sf}-q#{query}-n#{nodes}-s#{switches}-p#{perhost} ----"

  # Construct a call to run.rb
  tpch = "tpch#{query}"
  query_workdir = File.join($workdir, tpch)
  output_path = File.join(query_workdir, 'out.txt')
  infix = ''
  infix << " --binary #{File.join($options[:bindir], tpch, tpch)}" if $options[:bindir]
  infix << ' --corrective' if $options[:correctives]
  infix << " --batch-size #{$options[:rebatch]}" if $options[:rebatch]
  infix << " --msg-delay #{$options[:sleep_time]}" if $options[:sleep_time]
  infix << " --use-hm" if $options[:use_hm]
  #case experiment
  #when 'latency'
  #
  #when 'memory'
  #end
  cmd = "#{File.join($script_path, "run.rb")} -5"\
        " #{File.join($common_path, "K3-Mosaic/tests/queries/tpch/query#{query}.sql")}"\
        " -w #{query_workdir}/"\
        " -p /local/data/mosaic/#{sf}f"\
        " -s #{switches} -n #{nodes}"\
        " --perhost #{perhost} --query #{query}"\
        " --compile-local --create-local #{infix}"\
        " 2>&1 | tee #{output_path}"
  puts cmd

  # Run and extract time upon success.
  msg = "FAILED!"
  system(cmd)
  out_file = File.read(output_path)
  if out_file =~ /.*Mesos job succeeded$/m
    jobid = out_file[/^JOBID = (\d+)$/m, 1].to_i
    t['jobid'] = jobid
    puts "Job #{jobid} succeeded"
    job_dir = File.join(query_workdir, "job_#{jobid}")
    f = File.read(File.join(job_dir, 'time.txt'))
    m = /.*time: (\d+).*mean:(\d+).*max:(\d+).*std_dev:(\d+)\n.*mean:(\d+).*max:(\d+).*std_dev:(\d+)/m.match(f)
    new_t = {
      'time'   => m[1], 'n_mean' => m[2], 'n_max'  => m[3],
      'n_dev'  => m[4], 's_mean' => m[5], 's_max'  => m[6], 's_dev'  => m[7]
    }
    t.merge!(new_t)
    msg = "SUCCESS! Time=#{t['time']}"
  end
  puts "------------ #{msg}  ---------------"
  t
end

def main()
  # Initialization
  parse_args()
  $workdir = $options[:workdir]
  FileUtils.mkdir_p $workdir unless File.exist? $workdir

  config = load_config() unless $options[:clean]
  config = create_config unless config
  save_config(config)

  # Otherwise, setup and run
  config2 = config.dup
  config[:tests].each_with_index do |test, i|
    unless test['jobid'] # check for empty
      test = run_trial(test)
      config2[:tests][i] = test
      save_config config2 # update file
    end
  end
end

def parse_args()
  usage = "Usage: #{$PROGRAM_NAME} options"
  parser = OptionParser.new do |opts|
    opts.banner = usage
    opts.on("-d", "--dry-run", "Dry Run -- Print options and exit") {$options[:dry_run] = true}
    opts.on("-w", "--workdir [PATH]", "Path in which to create files") {|s| $options[:workdir] = s}
    opts.on("-b", "--bindir [PATH]", "Path to binaries") {|s| $options[:bindir] = s}
    opts.on("-r", "--config-file [NAME]", "Name of config file to create") {|s| $options[:config_file] = s}
    opts.on("-e", "--experiments x,y,z", Array, "List of experiments to run (scalability, latency, memory)") {|s| $options[:experiments] = s}
    opts.on("-f", "--sf x,y,z", Array, "List of scale factors to run") {|s| $options[:scale_factors] = s}
    opts.on("-q", "--queries x,y,z", Array, "List of queries to run") {|s| $options[:queries] = s}
    opts.on("-n", "--node-counts x,y,z", Array, "List of node configs to run") {|s| $options[:node_counts] = s.map {|x| x.to_i}}
    #opts.on("-s", "--switch-counts x,y,z", Array, "List of switch configs to run") {|s| $options[:switch_counts] = s.map {|x| x.to_i}}
    opts.on("-t", "--trials [INT]", "Number of trials per configuration") {|s| $options[:trials] = s.to_i}
    opts.on("-h", "--no-use-hms", "Disable usage of HM machines") {$options[:use_hms] = false}
    opts.on("--clean", "Clean the experiment file") {$options[:clean] = true}
  end
  parser.parse!
end

main()
