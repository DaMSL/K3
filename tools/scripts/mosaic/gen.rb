#!/usr/bin/env ruby
# Run a distributed test and compare to dbtoaster results

require 'optparse'
require 'net/http'
require 'json'

$options = {}

usage = "Usage: #{$PROGRAM_NAME} sql_file [options]"

OptionParser.new do |opts|
  opts.banner = usage
  opts.on("-d", "--data [PATH]", String, "Set the path of the data file") { |s| $options[:data_path] = s }
  opts.on("--debug", "Debug mode") { $options[:debug] = true }
  opts.on("-a", "--all", "All stages") {
    $options[:mosaic]  = true
    $options[:compile] = true
    $options[:deploy]  = true
  }
  opts.on("-1", "--mosaic", "Mosaic stage") { $options[:mosaic] = true }
  opts.on("-2", "--compile", "Compile stage") { $options[:compile] = true }
  opts.on("-3", "--deploy", "Deploy stage") { $options[:deploy] = true }
end.parse!

def run(cmd)
  if $options[:debug] then puts cmd end
  system cmd
end

# get directory of script
script_path = File.expand_path(File.dirname(__FILE__))

unless ARGV.size == 1
  puts usage
  exit
end

# split path components
source      = ARGV[0]
ext         = File.extname(source)
basename    = File.basename(source, ext)
lastpath    = File.split(File.split(source)[0])[1]
source_path = File.expand_path(source)
root_path   = File.join(script_path, "..", "..", "..", "..")
mosaic_path = File.join(root_path, "K3-Mosaic")

start_path = File.expand_path(Dir.pwd)

platform = case RUBY_PLATFORM
            when /darwin/ then :osx
            when /cygwin|mswin|mingw/ then :windows
            else :linux
           end
dbt_plat = case platform
            when :osx then "dbt_osx"
            when :linux then "dbt_linux"
            else fail "windows not yet supported"
           end

test_path    = File.join(mosaic_path, "tests")
dbt_path     = File.join(test_path, dbt_plat) # dbtoaster path
dbt_lib_path = File.join(dbt_path, "lib", "dbt_c++")

nice_name =
  if match = basename.match(/query(.*)/)
    lastpath + match.captures[0]
  else
    basename
  end

k3_name = nice_name + ".k3"
k3_path = File.join("temp", k3_name)

dbt_name = "dbt_" + nice_name
dbt_name_hpp = dbt_name + ".hpp"

### Mosaic stage ###

if $options[:mosaic]

  puts "Creating dbtoaster hpp file"
  Dir.chdir(test_path)
  run("#{File.join(dbt_plat, "dbtoaster")} -d MT --read-agenda -l cpp #{source_path} > #{File.join(start_path, dbt_name_hpp)}")
  Dir.chdir(start_path)

  # if requested, change the data path
  if $options.has_key?(:data_path)
    s = File.read(dbt_name_hpp)
    s.sub!(/agenda.csv/, $options[:data_path])
    File.write(dbt_name_hpp, s)
  end

  puts "Compiling dbtoaster"
  run("g++ #{File.join(dbt_lib_path, "main.cpp")} -std=c++11 -include #{dbt_name_hpp} -o #{dbt_name} -O3 -I#{dbt_lib_path} -L#{dbt_lib_path} -ldbtoaster -lboost_program_options-mt -lboost_serialization-mt -lboost_system-mt -lboost_filesystem-mt -lboost_chrono-mt -lboost_thread-mt -lpthread")

  # create the mosaic files
  puts "Creating mosaic files"
  run("#{File.join(mosaic_path, "tests", "auto_test.py")} --no-interp -d -f #{source}")

  # if requested, change the data path
  if $options.has_key?(:data_path)
    s = File.read(k3_path)
    s.sub!(/= file "[^"]+" psv/, "= file \"#{$options[:data_path]}\" psv")
    File.write(k3_path, s)
  end

  # create cpp file via k3
  puts "Creating k3 cpp file"
  compile_brew = File.join(script_path, "..", "run", "compile_brew.sh")
  run("#{compile_brew} --fstage cexclude=Optimize -1 #{k3_path}")
end

bin_src_file = File.join(root_path, "__build", "A")
bin_file = nice_name

if $options[:compile]
  # compile the k3 cpp file
  puts "Compiling cpp file"
  run("#{compile_brew} -2 #{k3_path}")

  # copy and rename binary file
  FileUtils.copy_file(bin_src_file, bin_file)
end

### Deployment stage ###

role_file = nice_name + ".yaml"

if $options[:deploy]
  deploy_server = "qp1:5000"

  puts "Sending binary to mesos"
  run("curl -i -X POST -H \"Accept: application/json\" -F \"file=@#{bin_file}\" http://#{deploy_server}/apps")

  # generate a yaml file TODO: vary number of nodes/switches
  yaml = `#{File.join(script_path, "gen_yaml.py")} --dist`
  File.write(role_file, yaml)

  puts "Creating new mesos job"
  res = `curl -i -X POST -H \"Accept: application/json\" -F \"file=@#{role_file}\" http://#{deploy_server}/jobs/#{bin_file}`
  i = res =~ /{/
  if i then res = res[i..-1] end

  jobid = JSON::parse(res)['jobId']

  # Run DBToaster to get results (in parallel)
  dbt_xml_out = `dbt_name`

  # Function to get job status
  def get_status(jobid)
    res = `curl -i http://#{deploy_server}/job/#{jobid}`
    if res =~ /Job # \d+ (\w+)/ then $1
    else "FAILED" end
  end

  puts "Waiting for job to finish"
  status = get_status(jobid)
  # loop until we get a result
  while status != "FINISHED" && status != "KILLED"
    sleep(4)
    status = get_status(jobid)
  end

  if status == "KILLED"
    puts "Job has been killed"
    exit(1)
  end

  # Get result data

  # Parse DBToaster results

  # Compare results

end




