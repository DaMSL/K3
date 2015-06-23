#!/usr/bin/env ruby
# Run a distributed test and compare to dbtoaster results

require 'optparse'
require 'fileutils'
require 'net/http'
require 'json'
require 'rexml/document'
require 'csv'

def run(cmd, checks=[])
  puts cmd if $options[:debug]
  out = `#{cmd} 2>&1`
  puts out if $options[:debug]
  res = $?.success?
  # other tests
  checks.each do |err|
    res = false if out =~ err
  end
  if !res
    puts "\nERROR\n"
    puts out unless $options[:debug]
    exit(1)
  end
  return out
end

def stage(s)
  puts ">> " + s
end

### Mosaic stage ###

def run_mosaic(k3_path, mosaic_path, source)
  stage "[1] Creating mosaic files"
  run("#{File.join(mosaic_path, "tests", "auto_test.py")} --workdir #{$workdir} --no-interp -d -f #{source}")

  # change the k3 file to use the dynamic path
  s = File.read(k3_path)
  s.sub!(/file "[^"]+" psv/, "file switch_path psv")
  File.write(k3_path, s)
end


### DBToaster stage ###

def run_dbtoaster(test_path, dbt_platform, dbt_lib_path, dbt_name, dbt_name_hpp, source_path, start_path)
  dbt_name_hpp_path = File.join($workdir, dbt_name_hpp)
  dbt_name_path = File.join($workdir, dbt_name)

  stage "[2] Creating dbtoaster hpp file"
  Dir.chdir(test_path)
  run("#{File.join(dbt_platform, "dbtoaster")} --read-agenda -l cpp #{source_path} > #{dbt_name_hpp_path}")
  Dir.chdir(start_path)

  # if requested, change the data path
  if $options.has_key?(:dbt_data_path)
    s = File.read(dbt_name_hpp_path)
    s.sub!(/agenda.csv/,$options[:dbt_data_path])
    File.write(dbt_name_hpp_path, s)
  end

  # adjust boost libs for OS
  boost_libs = %w(boost_program_options boost_serialization boost_system boost_filesystem boost_chrono boost_thread)
  mt = dbt_platform == "dbt_osx" ? "-mt" : ""
  boost_libs.map! { |lib| "-l" + lib + mt }

  stage "[2] Compiling dbtoaster"
  run("g++ #{File.join(dbt_lib_path, "main.cpp")} -std=c++11 -include #{dbt_name_hpp_path} -o #{dbt_name_path} -O3 -I#{dbt_lib_path} -L#{dbt_lib_path} -ldbtoaster -lpthread #{boost_libs.join ' '}")

  stage "[2] Running DBToaster"
  run("#{File.join(".", dbt_name_path)} > #{dbt_name_path}.xml", [/File not found/])
end

## Create/Compile stage ###

# handle all curl requests through here
def curl(server, url, args:{}, file:"", post:false, json:false, getfile:nil)
  cmd = ""
  # Getfile needs json to remove html, but don't parse as json
  cmd << getfile.nil? ? '-i ' : '-H "Accept: application/json" '
  cmd << '-X POST ' if post
  cmd << '-H "Accept: application/json" ' if json
  cmd << "-F \"file=@#{file}\" " if file != ""
  args.each_pair { |k,v| cmd << "-F \"#{k}=#{v}\" " }

  pipe = getfile.nil? ? '' : '-o ' + File.join($workdir, getfile)
  url2 = !getfile.nil? ? url + getfile + "/" : url

  res = run("curl -s http://#{server}#{url2} #{cmd}#{pipe}")
  # clean up json output
  if json
    i = res =~ /{/
    res = res[i..-1] if i
    res = JSON::parse(res)
  end
end

def curl_status_loop(server_url, url, success_status)
  # get status
  res = curl(server_url, url, json:true)
  last_status = ""
  status = ""
  while status != success_status && status != "KILLED" && status != "FAILED"
    sleep(4)
    res = curl(server_url, url, json:true)
    last_status = status
    status = res['status']
    if status == last_status
      print "."
    else
      puts "Status: " + status + " "
    end
  end
  return status, res
end

def check_status(status, success, process_nm)
  case status
    when "KILLED"
      stage "#{process_nm} has been killed"; exit(1)
    when "FAILED"
      stage "#{process_nm} has failed"; exit(1)
    when success
      stage "#{process_nm} succeeded"
  end
end

# create the k3 cpp file locally
def run_create_k3_local(k3_path, script_path)
  stage "[3] Creating K3 cpp file locally"
  compile = File.join(script_path, "..", "run", "compile.sh")
  res = run("time #{compile} -1 #{k3_path} +RTS -N -RTS")
  File.write(File.join($workdir, "k3.log"), res)
end

def wait_and_fetch_remote_compile(server_url, bin_file, k3_cpp_name, nice_name, uid)

  puts "UID = #{uid}"

  # get status
  status, _ = curl_status_loop(server_url, "/compile/#{uid}", "COMPLETE")

  # get the output file (before exiting on error)
  fs_path = "/fs/build/#{nice_name}-#{uid}/"

  curl(server_url, fs_path, getfile:"output")

  check_status(status, "COMPLETE", "Remote compilation")

  # get the cpp file
  puts k3_cpp_name
  curl(server_url, fs_path, getfile:k3_cpp_name)

  if !bin_file.nil?
    # get the bin file
    curl(server_url, fs_path, getfile:bin_file)
  end
end

# do both creation and compilation remotely (returns uid)
def run_create_compile_k3_remote(server_url, bin_file, k3_cpp_name, k3_path, nice_name)
  stage "[3-4] Remote creating && compiling K3 file to binary"
  res = curl(server_url, "/compile", file: k3_path, post: true, json: true, args:{ "compilestage" => "both"})
  uid = res["uid"]

  wait_and_fetch_remote_compile(server_url, bin_file, k3_cpp_name, nice_name, uid)

  return uid
end

# create the k3 cpp file remotely and copy the cpp locally
def run_create_k3_remote(server_url, k3_cpp_name, k3_path, nice_name)
  stage "[3] Remote creating K3 cpp file."
  res = curl(server_url, "/compile", file: k3_path, post: true, json: true, args:{ "compilestage" => "cpp"})
  uid = res["uid"]

  # get the cpp file
  wait_and_fetch_remote_compile(server_url, nil, k3_cpp_name, nice_name, uid)
end

# compile cpp->bin locally
def run_compile_k3(bin_file, k3_path, k3_cpp_name, k3_cpp_path, k3_root_path, script_path)
  stage "[4] Compiling k3 cpp file"
  brew = $options[:osx_brew] ? "_brew" : ""

  # copy cpp to proper path
  dest_path = File.join(k3_root_path, "__build")
  FileUtils.copy_file(k3_cpp_path, File.join(dest_path, k3_cpp_name))

  compile = File.join(script_path, "..", "run", "compile#{brew}.sh")
  run("#{compile} -2 #{k3_path}")

  bin_src_file = File.join(k3_root_path, "__build", "A")

  FileUtils.copy_file(bin_src_file, bin_file)
end

### Deployment stage ###

def gen_yaml(role_file, script_path)
  # Genereate yaml file"
  cmd = ""
  cmd << "--switches " << $options[:num_switches].to_s << " " if $options[:num_switches]
  cmd << "--nodes " << $options[:num_nodes].to_s << " " if $options[:num_nodes]
  cmd << "--file " << $options[:k3_data_path] << " " if $options[:k3_data_path]
  cmd << "--dist " if !$options[:run_local]
  yaml = run("#{File.join(script_path, "gen_yaml.py")} #{cmd}")
  File.write(role_file, yaml)
end

def wait_and_fetch_results(stage_num, jobid, server_url, nice_name)

  stage "[#{stage_num}] Waiting for Mesos job to finish..."
  status, res = curl_status_loop(server_url, "/job/#{jobid}", "FINISHED")

  check_status(status, "FINISHED", "Mesos job")

  stage "[#{stage_num}] Getting result data"
  `rm -rf json`
  file_paths = []
  res['sandbox'].each do |s|
    if File.extname(s) == '.tar'
      file_paths << s
    end
  end

  file_paths.each do |f|
    curl(server_url, "/fs/jobs/#{nice_name}/#{jobid}/", getfile:f)
    run("tar xvf #{f}")
  end
end

def run_deploy_k3_remote(uid, server_url, bin_file, nice_name, script_path)
  role_path = File.join($workdir, nice_name + ".yaml")

  # we can either have a uid from a previous stage, or send a binary and get a uid now
  if uid.nil?
    stage "[5] Sending binary to mesos"
    res = curl(server_url, '/apps', file:bin_file, post:true, json:true)
    uid = res['uid']
  end

  # Genereate mesos yaml file"
  gen_yaml(role_path, script_path)

  stage "[5] Creating new mesos job"
  res = curl(server_url, "/jobs/#{nice_name}/#{uid}", json:true, post:true, file:role_path, args:{'jsonfinal' => 'yes'})
  jobid = res['jobId']

  # Wait for job to finish and get results
  wait_and_fetch_results(5, jobid, server_url, nice_name)
end

# local deployment
def run_deploy_k3_local(bin_file, nice_name, script_path)
  role_file = File.join($workdir, nice_name + "_local.yaml")
  gen_yaml(role_file, script_path)

  json_dist_path = File.join($workdir, 'json')

  stage "[5] Running K3 executable locally"
  `rm -rf #{json_dist_path}`
  `mkdir -p #{json_dist_path}`
  run("./#{bin_file} -p #{role_file} --json #{json_dist_path} --json_final_only")
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
  dbt_path = File.join($workdir, dbt_name)

  stage "[6] Parsing DBToaster results"
  dbt_xml_out = File.read("#{dbt_path}.xml")
  dbt_xml_out.gsub!(/(Could not find insert.+$|Initializing program:|Running program:|Printing final result:)/,'')
  dbt_xml_out.gsub!(/\n\s*/,'')

  r = REXML::Document.new(dbt_xml_out)
  dbt_results = {}
  r.elements['boost_serialization/snap'].each do |result|
    # complex results
    res = []
    if result.has_elements?
      result.each do |item|
        if item.name == 'item'
          item.each { |e| res << str_to_val(e.text) }
        end
      end
      dbt_results[result.name] = res if res.size > 0
    # simple result
    else
      dbt_results[result.name] = str_to_val(result.text)
    end
  end
  return dbt_results
end

def parse_k3_results(script_path, dbt_results)
  stage "[6] Parsing K3 results"
  files = []
  json_path = File.join($workdir, "json")
  if Dir.exists?(json_path)
    Dir.entries(json_path).each do |f|
      files << File.join(json_path, f) if f =~ /.*Globals.dsv/
    end
  end

  # Run script to convert json format
  unless files.empty?
    run("#{File.join(script_path, "clean_json.py")} --prefix_path #{$workdir} #{files.join(" ")}")
  end

  # We assume only final state data
  combined_maps = {}
  str = File.read(File.join($workdir, 'globals.dsv'))
  stage "[6] Found K3 globals.csv"
  str.each_line do |line|
    csv = line.split('|')
    map_name = csv[2]
    map_data = csv[3]
    # skip irrelevant maps
    next unless dbt_results.has_key? map_name
    map_data_j = JSON.parse(map_data)
    # skip empty maps
    next if map_data_j.empty?
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
        if !combined_maps.has_key?(map_name)
          combined_maps[map_name] = { key => value[1] }
        elsif !combined_maps[map_name].has_key?(key)
          combined_maps[map_name][key] = value[1]
        else
          combined_maps[map_name][key] += value[1]
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
    if !v2
      stage "[6] Mismatch at key #{k}: missing k3 value"; exit 1
    end
    if v1.respond_to?(:sort!) && v2.respond_to?(:sort!)
      v1.sort!
      v2.sort!
    end
    if (v1 <=> v2) != 0
      stage "[6] Mismatch at key #{k}\nv1:#{v1}\nv2:#{v2}"
      exit 1
    end
  end
  stage "[6] Results check...OK"
end

def main()
  $options = {}
  uid = nil

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
    opts.on("--run-local", "Run locally") { $options[:run_local] = true }
    opts.on("--create-local", "Create the cpp file locally") { $options[:create_local] = true }
    opts.on("--compile-local", "Compile locally") { $options[:compile_local] = true }
    opts.on("-j", "--json [JSON]", String, "JSON file to load options") {|s| $options[:json_file] = s}
    opts.on("--uid [UID]", String, "UID of file") {|s| $options[:uid] = s}
    opts.on("--jobid [JOBID]", String, "JOBID of job") {|s| $options[:jobid] = s}
    opts.on("--mosaic-path [PATH]", String, "Path for mosaic") {|s| $options[:mosaic_path] = s}
    opts.on("--fetch-cpp", "Fetch a cpp file after remote creation") { $options[:fetch_cpp] = true}
    opts.on("--fetch-bin", "Fetch bin + cpp files after remote compilation") { $options[:fetch_bin] = true}
    opts.on("--fetch-results", "Fetch results after job") { $options[:fetch_results] = true }
    opts.on("-p", "--path", "Path in which to create files") {|s| $options[:path] = s}

    # stages
    opts.on("-a", "--all", "All stages") {
      $options[:dbtoaster]  = true
      $options[:mosaic]     = true
      $options[:create_k3]  = true
      $options[:compile_k3] = true
      $options[:deploy_k3]  = true
      $options[:compare]    = true
    }
    opts.on("-1", "--mosaic",    "Mosaic stage (creates Mosaic K3 program)")       { $options[:mosaic]     = true }
    opts.on("-2", "--dbtoaster", "DBToaster stage")                                { $options[:dbtoaster]  = true }
    opts.on("-3", "--create",    "Create K3 stage (creates Mosaic CPP program)")   { $options[:create_k3]  = true }
    opts.on("-4", "--compile",   "Compile K3 stage (compiles Mosaic CPP program)") { $options[:compile_k3] = true }
    opts.on("-5", "--deploy",    "Deploy stage")                                   { $options[:deploy_k3]  = true }
    opts.on("-6", "--compare",   "Compare stage")                                  { $options[:compare]    = true }
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
  common_root_path  = File.join(k3_root_path, "..")
  mosaic_path  = File.join(common_root_path, "K3-Mosaic")
  mosaic_path  = $options[:mosaic_path] ? $options[:mosaic_path] : mosaic_path
  $workdir     = $options[:workdir] ? $options[:workdir] : "temp"

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
  k3_path = File.join($workdir, k3_name)

  k3_cpp_name = nice_name + ".cpp"
  k3_cpp_path = File.join($workdir, k3_cpp_name)

  dbt_name = "dbt_" + nice_name
  dbt_name_hpp = dbt_name + ".hpp"

  server_url = "qp2:5000"

  bin_file = File.join($workdir, nice_name)
  dbt_results = []

  if $options[:mosaic]
    run_mosaic(k3_path, mosaic_path, source)
  end

  if $options[:dbtoaster]
    run_dbtoaster(test_path, dbt_plat, dbt_lib_path, dbt_name, dbt_name_hpp, source_path, start_path)
  end
  # either nil or take from command line
  uid = $options[:uid] ? $options[:uid] : nil
  jobid = $options[:jobid] ? $options[:jobid] : nil

  # options to fetch cpp/binary (ie. all) given a uid
  if $options[:fetch_cpp]
    wait_and_fetch_remote_compile(server_url, nil, k3_cpp_name, nice_name, uid)
  elsif $options[:fetch_bin]
    wait_and_fetch_remote_compile(server_url, bin_file, k3_cpp_name, nice_name, uid)
  end

  # check for doing everything remotely
  if !$options[:compile_local] && !$options[:create_local] && ($options[:create_k3] || $options[:compile_k3])
      uid = run_create_compile_k3_remote(server_url, bin_file, k3_cpp_name, k3_path, nice_name)
  else
    if $options[:create_k3]
      if $options[:create_local]
        run_create_k3_local(k3_path, script_path)
      else
        run_create_k3_remote(server_url, k3_cpp_name, k3_path, nice_name)
      end
    end
    # if we're not doing everything remotely, we can only compile locally
    if $options[:compile_k3]
        run_compile_k3(bin_file, k3_path, k3_cpp_name, k3_cpp_path, k3_root_path, script_path)
    end
  end

  if $options[:deploy_k3]
    if $options[:run_local]
      run_deploy_k3_local(bin_file, nice_name, script_path)
    else
      run_deploy_k3_remote(uid, bin_file, server_url, nice_name, script_path)
    end
  end

  # options to fetch to job results
  if $options[:fetch_results]
    wait_and_fetch_results(5, jobid, server_url, nice_name)
  end

  if $options[:compare]
    dbt_results = parse_dbt_results(dbt_name)
    k3_results  = parse_k3_results(script_path, dbt_results)
    run_compare(dbt_results, k3_results)
  end
end

main
