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
  :queries        => %w{6 11a 4 19 13 22a 12 1 17 15 3},
  :scale_factors  => [0.1, 1, 10, 100],
  :switch_counts  => [1, 2, 4, 8, 16], # latency only
  :node_counts    => [1, 4, 8, 16, 31],
  :gc_epochs      => [5 * 1000, 30 * 1000, 60 * 1000], # memory only
  :delays         => [0, 20, 200],                     # memory only

  # NOTE: most we can do is 2 perhost without disasterous slowdown
  # TODO: correctives currently don't work
  :correctives    => false,
  :trials         => 3,
  :trial_cutoff   => 10,
  :rebatch        => nil,
  :sleep_time     => nil,
  :use_hms        => true, # by default
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

def load_config()
  path = config_path()
  return nil unless File.exists? path
  File.open(path, 'r') { |f| YAML.load(f) }
end

def save_config(config, backup:false)
  path = config_path
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
  File.open(path, 'w') {|f| f << config.to_yaml}
end

def get_switches(nodes)
  [1,2,4,8,16,32,64].find {|n| n >= nodes}
end

# Scalability: for each experiment, need roundtrip time
# Latency: for each experiment, need do_complete latency distribution
# Memory: for each experiment, need memory series over time
def create_config()
  # Materialize an array of desired trial configurations
  tests = []
  if $options[:experiments].include? :scalability
    $options[:scale_factors].each do |sf|
      $options[:node_counts].each do |nodes|
        $options[:queries].each do |q|
          trials = sf >= $options[:trial_cutoff] ? 1 : $options[:trials]
          (1..trials).each do |trial|
            tests << $headers[0..-1].zip([:scalability, sf, nodes, get_switches(nodes), 2, q, trial]).to_h
          end
        end
      end
    end
  end
  if $options[:experiments].include? :latency
    $options[:scale_factors].each do |sf|
      $options[:node_counts].each do |nodes|
        [1, 2, 4].each do |switches|
          $options[:queries].each do |q|
            trials = sf >= $options[:trial_cutoff] ? 1 : $options[:trials]
            (1..trials).each do |trial|
              t = $headers[0..-1].zip([:latency, sf, nodes, switches, 2, q, trial]).to_h
              tests << t
            end
          end
        end
      end
    end
  end
  if $options[:experiments].include? :memory
    $options[:scale_factors].each do |sf|
      $options[:queries].each do |q|
        $options[:node_counts].each do |nodes|
          $options[:gc_epochs].each do |gc_epoch|
            $options[:delays].each do |delay|
              trials = sf >= $options[:trial_cutoff] ? 1 : $options[:trials]
              (1..trials).each do |trial|
                t = $headers[0..-1].zip([:memory, sf, nodes, get_switches(nodes), 2, q, trial]).to_h
                t[:gc_epoch] = gc_epoch
                t[:delay] = delay
                tests << t
              end
            end
          end
        end
      end
    end
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
  exp = t[:exp]
  sf = t[:sf]
  nodes = t[:nd]
  switches = t[:sw]
  perhost = t[:perhost]
  query = t[:q]
  # Keep a seperate output file for the trial
  puts "---- #{exp}-sf#{sf}-q#{query}-n#{nodes}-s#{switches}-p#{perhost} ----"

  # Construct a call to run.rb
  tpch = "tpch#{query}"
  query_workdir = File.join($workdir, tpch)
  FileUtils.mkdir_p query_workdir if !File.exist? query_workdir
  output_path = File.join(query_workdir, 'out.txt')
  infix = ''
  infix << " --binary #{File.join($options[:bindir], tpch, tpch)}" if $options[:bindir]
  infix << ' --corrective' if $options[:correctives]
  infix << " --batch-size #{$options[:rebatch]}" if $options[:rebatch]
  infix << " --msg-delay #{$options[:sleep_time]}" if $options[:sleep_time]
  infix << " --use-hm" if $options[:use_hm]
  infix << " --profile-latency --process-latency" if exp == :latency
  infix << " --mem-interval 250 --gc-epoch #{t[:gc_epoch]} --msg-delay#{t[:delay]} --process-memory" if exp == :memory
  infix << " --perhost #{perhost}" unless nodes < perhost

  cmd = "#{File.join($script_path, "run.rb")} -5"\
        " #{File.join($common_path, "K3-Mosaic/tests/queries/tpch/query#{query}.sql")}"\
        " -w #{query_workdir}/"\
        " -p /local/data/mosaic/#{sf}f"\
        " -s #{switches} -n #{nodes}"\
        " --query #{query}"\
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
  $workdir = File.expand_path($options[:workdir])
  FileUtils.mkdir_p $workdir unless File.exist? $workdir

  config = load_config() unless $options[:clean]
  config = create_config unless config
  save_config(config, backup:$options[:clean])

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
    opts.on("-w", "--workdir [PATH]", "Path in which to create files") {|s| $options[:workdir] = s}
    # when a binary is in another directory
    opts.on("-b", "--bindir [PATH]", "Path to binaries") {|s| $options[:bindir] = s}
    opts.on("-c", "--config-file [NAME]", "Name of config file to create") {|s| $options[:config_file] = s}
    opts.on("-e", "--experiments x,y,z", Array, "List of experiments to run (s/l/m)") do |a|
      $options[:experiments] = a.map {|s| s == 'm' ? :memory : s == 'l' ? :latency : :scalability}
    end
    opts.on("-f", "--sf x,y,z", Array, "List of scale factors to run") {|s| $options[:scale_factors] = s}
    opts.on("-q", "--queries x,y,z", Array, "List of queries to run") {|s| $options[:queries] = s}
    opts.on("-n", "--node-counts x,y,z", Array, "List of node configs to run") {|s| $options[:node_counts] = s.map {|x| x.to_i}}
    opts.on("-s", "--switch-counts x,y,z", Array, "List of switch configs to run") {|s| $options[:switch_counts] = s.map {|x| x.to_i}}
    opts.on("-t", "--trials [INT]", "Number of trials per configuration") {|s| $options[:trials] = s.to_i}
    opts.on("-h", "--no-use-hms", "Disable usage of HM machines") {$options[:use_hms] = false}
    opts.on("-g", "--gc-epochs x,y,z", "List of gc epochs to use for memory") {|s| $options[:gc_epochs] = s}
    opts.on("-d", "--delays x,y,z", "List of delays to use for memory") {|s| $options[:delays] = s}

    opts.on("--clean", "Clean the experiment file") {$options[:clean] = true}
  end
  parser.parse!
end

main()
