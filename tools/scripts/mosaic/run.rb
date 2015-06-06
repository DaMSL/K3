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

def run_create_k3(k3_path, script_path)
  stage "Creating K3 cpp file"
  compile = File.join(script_path, "..", "run", "compile.sh")
  # for this branch only (for now)
  res = run("time #{compile} -1 #{k3_path} +RTS -N -RTS")
  File.write("k3.log", res)
  #run("#{compile} --fstage cexclude=Optimize -1 #{k3_path}")
end

def run_compile_k3(bin_file, k3_path, k3_cpp_path, k3_root_path, script_path)

  # change the cpp file to use the dynamic path
  s = File.read(k3_cpp_path)
  s.sub!(/"switch", "[^"]+", "psv"/, '"switch", switch_path, "psv"')
  File.write(k3_cpp_path, s)

  stage "Compiling k3 cpp file"
  brew = $options[:osx_brew] ? "_brew" : ""
  compile = File.join(script_path, "..", "run", "compile#{brew}.sh")
  run("#{compile} -2 #{k3_path}")

  bin_src_file = File.join(k3_root_path, "__build", "A")

  FileUtils.copy_file(bin_src_file, bin_file)
end

### Deployment stage ###

def gen_yaml(role_file, script_path)
  # Genereate yaml file"
  cmd = ""
  if $options[:num_switches] then cmd << "--switches "  << $options[:num_switches].to_s << " " end
  if $options[:num_nodes]    then cmd << "--nodes "     << $options[:num_nodes].to_s << " " end
  if $options[:k3_data_path] then cmd << "--file "      << $options[:k3_data_path] << " " end
  if !$options[:local]       then cmd << "--dist " end
  yaml = run("#{File.join(script_path, "gen_yaml.py")} #{cmd}")
  File.write(role_file, yaml)
end

# handle all curl requests through here
def curl(server, url, args:{}, file:"", post:false, json:false, getfile:false)
  cmd = ""
  cmd << if getfile then '-O ' else '-i ' end
  if post then cmd << '-X POST ' end
  if json then cmd << '-H "Accept: application/json" ' end
  if file != "" then cmd << "-F \"file=@#{file}\" " end
  args.each_pair { |k,v| cmd << "-F \"#{k}=#{v}\" " }

  run("curl http://#{server}#{url} #{cmd}")
end

def run_deploy_k3(bin_file, deploy_server, nice_name, script_path)
  role_file = nice_name + ".yaml"

  stage "Sending binary to mesos"
  curl(deploy_server, '/apps', file:bin_file, post:true, json:true)

  # Genereate mesos yaml file"
  gen_yaml(role_file, script_path)

  stage "Creating new mesos job"
  res = curl(deploy_server, "/jobs/#{bin_file}", json:true, post:true, file:role_file, args:{'jsonfinal' => 'yes'})
  i = res =~ /{/
  if i then res = res[i..-1] end

  stage "Parsing mesos-returned job id"
  jobid = JSON::parse(res)['jobId']

  begin
    # Function to get job status
    def get_status(jobid, deploy_server)
      res = curl(deploy_server, "/job/#{jobid}")
      if res =~ /Job # \d+ (\w+)/ then [$1, res]
      else ["FAILED", res] end
    end

    stage "Waiting for Mesos job to finish..."
    status, res = get_status(jobid, deploy_server)
    # loop until we get a result
    while status != "FINISHED" && status != "KILLED" && status != "FAILED"
      sleep(4)
      status, res = get_status(jobid, deploy_server)
      puts status
    end
  rescue StandardError => _
    # kill job in case we Ctrl-C out
    stage "Killing Mesos job"
    curl(deploy_server, "/jobs/#{job_id}/kill", json:true)
    exit! 1
  end

  case status
    when "KILLED"
      stage "Mesos job has been killed"; exit(1)
    when "FAILED"
      stage "Mesos job has failed"; exit(1)
    when "FINISHED"
      stage "Mesos job succeeded"
  end

  stage "Getting result data"
  `rm -rf json`
  file_paths = res.scan(/<a href="([^"]+)">#{bin_file}[^.]+.tar/)
  file_paths.for_each do |path|
    filename = File.split(path)[1]
    curl(deploy_server, path + "/", getfile:true)
    run("tar xvzf #{filename}")
  end
end

# local deployment
def run_deploy_k3_local(bin_file, nice_name, script_path)
  role_file = nice_name + "_local.yaml"
  gen_yaml(role_file, script_path)

  stage "Running K3 executable locally"
  `rm -rf json`
  `mkdir json`
  run("./#{bin_file} -p #{role_file} --json json --json_final_only")
end

# convert a string to the narrowest value
def str_to_val(s)
  case s
    when /^\d+$/     then s.to_i
    when /^\d+\.\d*/ then s.to_f
    when /^true$/    then true
    when /^false$/   then false
    else s
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
  r.elements['boost_serialization/snap'].each do |result|
    # complex results
    if result.has_elements?
      result.each do |item|
        res = []
        if item.name == 'item'
          item.each { |e| res << str_to_val(e.text) }
        end
      end
      if res.size > 0 then dbt_results[result.name] = res end
    # simple result
    else
      dbt_results[result.name] = str_to_val(result.text)
    end
  end
  return dbt_results
end

def parse_k3_results(script_path, dbt_results)
  stage "Parsing K3 results"
  files = []
  Dir.entries("json").each do |f|
    if f =~ /.*Globals.dsv/ then files << File.join("json", f) end
  end

  # Run script to convert json format
  run("#{File.join(script_path, "clean_json.py")} #{files.join(" ")}")

  # We assume only final state data
  combined_maps = {}
  str = File.read('globals.dsv')
  str.each_line do |line|
    csv = line.split('|')
    map_name = csv[2]
    map_data = csv[3]
    unless dbt_results.has_key? map_name then next end
    map_data_j = JSON.parse(map_data)

    # frontier operation
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
        if combined_maps[map_name].has_key?(key)
          combined_maps[map_name][key] += value[1]
        else
          combined_maps[map_name][key] = value[1]
        end
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
      unless !max_vid
        if combined_maps.has_key?(map_name)
          combined_maps[map_name] += max_data
        else
          combined_maps[map_name] = max_data
        end
      end
    end
  end
  puts "combined: #{combined_maps}"
  return combined_maps
end

def run_compare(dbt_results, k3_results)
  # Compare results
  dbt_results.each_pair do |k,v1|
    v2 = k3_results[k]
    if !v2 then puts "Mismatch at key #{k}: missing k3 value"; exit 1; end
    if v1.respond_to?(:sort!) && v2.respond_to?(:sort!)
      v1.sort!
      v2.sort!
    end
    if (v1 <=> v2) != 0
      puts "Mismatch at key #{k}\nv1:#{v1}\nv2:#{v2}"
      exit 1
    end
  end
  stage "Results check...OK"
end

def main()
  $options = {}

  usage = "Usage: #{$PROGRAM_NAME} sql_file options"
  parser = OptionParser.new do |opts|
    opts.banner = usage
    opts.on("-d", "--dbtdata [PATH]", String, "Set the path of the dbt data file") { |s| $options[:dbt_data_path] = s }
    opts.on("-k", "--k3data [PATH]", String, "Set the path of the k3 data file") { |s| $options[:k3_data_path] = s }
    opts.on("--debug", "Debug mode") { $options[:debug] = true }
    opts.on("--json_debug", "Debug queries that won't die") { $options[:json_debug] = true }
    opts.on("-s", "--switches [NUM]", Integer, "Set the number of switches") { |i| $options[:num_switches] = i }
    opts.on("-n", "--nodes [NUM]", Integer, "Set the number of nodes") { |i| $options[:num_nodes] = i }
    opts.on("--brew", "Use homebrew (OSX)") { $options[:osx_brew] = true }
    opts.on("--local", "Run locally") { $options[:local] = true }
    opts.on("-j", "--json [JSON]", String, "JSON file to load options") {|s| $options[:json_file] = s}

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

  unless ARGV.size == 1
    puts parser.help
    exit
  end

  # if only one data file, take that one
  if $options.has_key?(:dbt_data_path) && !$options.has_key?(:k3_data_path)
    $options[:k3_data_path] = $options[:dbt_data_path]
  elsif $options.has_key?(:k3_data_path) && !$options.has_key?(:dbt_data_path)
    $options[:dbt_data_path] = $options[:k3_data_path]
  end

  # handle json options
  if $options.has_key?(:json_file)
    options = JSON.parse($options[:json_file])
    options.each_pair do |k,v|
      unless $options.has_key?(k)
        $options[k] = v
      end
    end
  end

  # get directory of script
  script_path = File.expand_path(File.dirname(__FILE__))

  # split path components
  source       = ARGV[0]
  ext          = File.extname(source)
  basename     = File.basename(source, ext)
  lastpath     = File.split(File.split(source)[0])[1]
  source_path  = File.expand_path(source)
  k3_root_path = File.join(script_path, "..", "..", "..")
  common_path  = File.join(k3_root_path, "..")
  mosaic_path  = File.join(common_path, "K3-Mosaic")

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
    run_create_k3(k3_path, script_path)
  end
  if $options[:compile_k3]
    run_compile_k3(bin_file, k3_path, k3_cpp_path, k3_root_path, script_path)
  end
  if $options[:deploy_k3]
    if $options[:local]
      run_deploy_k3_local(bin_file, nice_name, script_path)
    else
      run_deploy_k3(bin_file, deploy_server, nice_name, script_path)
    end
  end

  if $options[:compare]
    dbt_results = parse_dbt_results(dbt_name)
    k3_results  = parse_k3_results(script_path, dbt_results)
    run_compare(dbt_results, k3_results)
  end
end

main
