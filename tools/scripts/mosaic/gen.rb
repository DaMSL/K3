#!/usr/bin/env ruby
# Run a distributed test and compare to dbtoaster results

require 'optparse'
require 'fileutils'
require 'net/http'
require 'json'
require 'rexml/document'
require 'csv'

def run(cmd, checks=[])
  if $options[:debug] then puts cmd end
  out = `#{cmd} 2>&1`
  if $options[:debug] then puts out end
  res = $?.success?
  # other tests
  checks.each do |err|
    if out =~ err then res = false end
  end
  if !res
    puts "\nERROR\n"
    unless $options[:debug] then puts out end
    exit(1)
  end
  return out
end

def stage(s)
  puts ">> " + s
end

### DBToaster stage ###

def run_dbtoaster(test_path, dbt_plat, dbt_lib_path, dbt_name, dbt_name_hpp, source_path, start_path)

  stage "Creating dbtoaster hpp file"
  Dir.chdir(test_path)
  run("#{File.join(dbt_plat, "dbtoaster")} --read-agenda -l cpp #{source_path} > #{File.join(start_path, dbt_name_hpp)}")
  Dir.chdir(start_path)

  # if requested, change the data path
  if $options.has_key?(:dbt_data_path)
    s = File.read(dbt_name_hpp)
    s.sub!(/agenda.csv/,$options[:dbt_data_path])
    File.write(dbt_name_hpp, s)
  end

  # adjust boost libs for OS
  boost_libs = %w(boost_program_options boost_serialization boost_system boost_filesystem boost_chrono boost_thread)
  mt = dbt_plat == "dbt_osx" ? "-mt" : ""
  boost_libs.map! { |lib| "-l" + lib + mt }

  stage "Compiling dbtoaster"
  run("g++ #{File.join(dbt_lib_path, "main.cpp")} -std=c++11 -include #{dbt_name_hpp} -o #{dbt_name} -O3 -I#{dbt_lib_path} -L#{dbt_lib_path} -ldbtoaster -lpthread #{boost_libs.join ' '}")

  stage "Running DBToaster"
  run("#{File.join(".", dbt_name)} > #{dbt_name}.xml", [/File not found/])
end

### Mosaic stage ###

def run_mosaic(k3_path, mosaic_path, source)
  stage "Creating mosaic files"
  run("#{File.join(mosaic_path, "tests", "auto_test.py")} --no-interp -d -f #{source}")
end

def run_create_k3(k3_path, k3_cpp_path, script_path)
  stage "Creating K3 cpp file"
  compile = File.join(script_path, "..", "run", "compile.sh")
  run("#{compile} --fstage cexclude=Optimize -1 #{k3_path}")

  # change the cpp file to use the dynamic path
  s = File.read(k3_cpp_path)
  s.sub!(/"switch", "[^"]+", "psv"/, '"switch", switch_path, "psv"')
  File.write(k3_cpp_path, s)
end

def run_compile_k3(bin_file, k3_path, root_path, script_path)
  stage "Compiling k3 cpp file"
  brew = $options[:osx_brew] ? "_brew" : ""
  compile = File.join(script_path, "..", "run", "compile#{brew}.sh")
  run("#{compile} -2 #{k3_path}")

  bin_src_file = File.join(root_path, "__build", "A")

  FileUtils.copy_file(bin_src_file, bin_file)
end

### Deployment stage ###

def run_deploy_k3(bin_file, deploy_server, nice_name, script_path)
  role_file = nice_name + ".yaml"

  stage "Sending binary to mesos"
  run("curl -i -X POST -H \"Accept: application/json\" -F \"file=@#{bin_file}\" -F \"jsonfinal=yes\" http://#{deploy_server}/apps")

  # Genereate mesos yaml file"
  cmd = ""
  if $options[:num_switches] then cmd << "--switches " << $options[:num_switches].to_s end
  if $options[:num_nodes]    then cmd << "--nodes "    << $options[:num_nodes].to_s end
  yaml = run("#{File.join(script_path, "gen_yaml.py")} --dist #{cmd}")
  File.write(role_file, yaml)

  stage "Creating new mesos job"
  res = run("curl -i -X POST -H \"Accept: application/json\" -F \"file=@#{role_file}\" http://#{deploy_server}/jobs/#{bin_file}")
  i = res =~ /{/
  if i then res = res[i..-1] end

  stage "Parsing mesos returned jobId"
  jobid = JSON::parse(res)['jobId']

  # Function to get job status
  def get_status(jobid, deploy_server)
    res = run("curl -i http://#{deploy_server}/job/#{jobid}")
    if res =~ /Job # \d+ (\w+)/ then [$1, res]
    else ["FAILED", res] end
  end

  stage "Waiting for Mesos job to finish..."
  status, res = get_status(jobid, deploy_server)
  # loop until we get a result
  while status != "FINISHED" && status != "KILLED"
    sleep(4)
    status, res = get_status(jobid, deploy_server)
    puts status
  end
  if status == "KILLED"
    stage "Mesos job has been killed"
    exit(1)
  end

  stage "Getting result data"
  `rm -rf json`
  file_paths = res.scan(/<a href="([^"]+)">#{bin_file}[^.]+.tar/)
  file_paths.for_each do |path|
    filename = File.split(path)[1]
    res = run("curl -O http://#{deploy_server}#{path}/")
    run("tar xvzf #{filename}")
  end
end

# Parsing stage

def parse_dbt_results(dbt_name)
  stage "Parsing DBToaster results"
  dbt_xml_out = File.read("#{dbt_name}.xml")
  dbt_xml_out.gsub!(/(Could not find insert.+$|Initializing program:|Running program:|Printing final result:)/,'')
  dbt_xml_out.gsub!(/\n\s*/,'')

  r = REXML::Document.new(dbt_xml_out)
  dbt_results = {}
  res2 = []
  r.elements['boost_serialization/snap'].each do |result|
    # complex results
    if result.has_elements?
      result.each do |item|
        res = []
        if item.name == 'item'
          item.each { |e| res << e.text }
        end
      end
      if res.size > 0 then dbt_results[result.name] = res end
    else # simple result
      res2 << result.text
    end
    if res2.size > 0 then dbt_results[result.name] = res2 end
  end
  return dbt_results
end

def parse_k3_results(dbt_results)
  files = []
  Dir.entries("json").each do |f|
    if f =~ /.*Globals.dsv/ then files << f end
  end

  # We assume only final state data
  stage "Processing final map data"
  combined_maps = {}
  files.each do |f|
    str = File.read(f)
    str.each_line do |line|
      csv = CSV.parse(line)
      map_name = csv[2]
      map_data = csv[3]
      unless dbt_results.has_key? map_name then next end
      map_data_j = JSON.parse(map_data)
      max_map = {}
      # check if we're dealing with simple values
      if map_data_j[0].size > 2
        map_data_j.each do |v|
          key = v[1..-2]
          max_vid, _ = max_map[key]
          if !max_vid || ((v[0] <=> max_vid) == 1)
            max_map[key] = [v[0], v[-1]]
          end
        end
        # add the max map to the combined maps
        max_map.each_pair do |key,value|
          combined_maps[map_name][key] = value[1]
        end
      else # simple data type
        max_vid  = nil
        max_data = nil
        map_data_j.each do |v|
          if !max_vid || ((v[0] <=> max_vid) == 1)
            max_vid  = v[0]
            max_data = v[1]
          end
        end
        combined_maps[map_name] = max_data unless !max_vid
      end
    end
  end
  return combined_maps
end

def run_compare(dbt_results, k3_results)
  # Compare results
  dbt_results.each_pair do |k,v1|
    v2 = k3_results[k]
    if !v2 then puts "Mismatch at key #{k}: missing k3 value"; exit 1; end
    v1.sort!
    v2.sort!
    if (v1 <=> v2) != 0
      puts "Mismatch at key #{k}\nv1:#{v1}\nv2:#{v2}"
      exit 1
    end
  end
  stage "Results check...OK"
end

def main()
  $options = {}
  $options[:dbtoaster]  = false
  $options[:mosaic]     = false
  $options[:create_k3]  = false
  $options[:compile_k3] = false
  $options[:deploy_k3]  = false
  $options[:compare]    = false

  usage = "Usage: #{$PROGRAM_NAME} sql_file options"
  parser = OptionParser.new do |opts|
    opts.banner = usage
    opts.on("-d", "--dbtdata [PATH]", String, "Set the path of the dbt data file") { |s| $options[:dbt_data_path] = s }
    opts.on("-k", "--k3data [PATH]", String, "Set the path of the k3 data file") { |s| $options[:k3_data_path] = s }
    opts.on("--debug", "Debug mode") { $options[:debug] = true }
    opts.on("-s", "--switches [NUM]", Integer, "Set the number of switches") { |i| $options[:num_switches] = i }
    opts.on("-n", "--nodes [NUM]", Integer, "Set the number of nodes") { |i| $options[:num_nodes] = i }
    opts.on("--brew", "Use homebrew (OSX)") { $options[:osx_brew] = true }
    # stages
    opts.on("-a", "--all", "All stages") {
      $options[:dbtoaster]  = true
      $options[:mosaic]     = true
      $options[:create_k3]  = true
      $options[:compile_k3] = true
      $options[:deploy_k3]  = true
      $options[:compare]    = true
    }
    opts.on("-1", "--dbtoaster", "DBToaster stage")  { $options[:dbtoaster]  = true }
    opts.on("-2", "--mosaic",    "Mosaic stage")     { $options[:mosaic]     = true }
    opts.on("-3", "--create",    "Create K3 stage")  { $options[:create_k3]  = true }
    opts.on("-4", "--compile",   "Compile K3 stage") { $options[:compile_k3] = true }
    opts.on("-5", "--deploy",    "Deploy stage")     { $options[:deploy_k3]  = true }
    opts.on("-6", "--compare",   "Compare stage")    { $options[:compare]    = true }
  end
  parser.parse!

  # get directory of script
  script_path = File.expand_path(File.dirname(__FILE__))

  unless ARGV.size == 1
    puts parser.help
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

  k3_cpp_path = File.join("__build", "#{nice_name}.cpp")

  dbt_name = "dbt_" + nice_name
  dbt_name_hpp = dbt_name + ".hpp"

  deploy_server = "qp1:5000"

  bin_file = nice_name
  dbt_results = []

  if $options[:dbtoaster]
    run_dbtoaster(test_path, dbt_plat, dbt_lib_path, dbt_name, dbt_name_hpp, source_path, start_path)
  end
  if $options[:mosaic]
    run_mosaic(k3_path, mosaic_path, source)
  end
  if $options[:create_k3]
    run_create_k3(k3_path, k3_cpp_path, script_path)
  end
  if $options[:compile_k3]
    run_compile_k3(bin_file, k3_path, root_path, script_path)
  end
  if $options[:deploy_k3]
    run_deploy_k3(bin_file, deploy_server, nice_name, script_path)
  end

  if $options[:compare]
    dbt_results = parse_dbt_results(dbt_name)
    k3_results  = parse_k3_results(dbt_results)
    run_compare(dbt_results, k3_results)
  end
end

main
