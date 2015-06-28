#!/usr/bin/env ruby
# Run a distributed test and compare to dbtoaster results

require 'optparse'
require 'fileutils'
require 'net/http'
require 'json'
require 'rexml/document'
require 'csv'
require 'open3'

def run(cmd, checks=[])
  puts cmd if $options[:debug]
  out, err, s = Open3.capture3(cmd)
  puts out if $options[:debug]
  puts err if $options[:debug]
  res = s.success?
  # other tests
  checks.each do |check|
    res = false if out =~ check
    res = false if err =~ check
  end
  if !res
    puts "\nERROR\n"
    puts out unless $options[:debug]
    puts err unless $options[:debug]
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

def run_dbtoaster(exec_only, test_path, dbt_data_path, dbt_platform, dbt_lib_path, dbt_name, dbt_name_hpp, source_path, start_path)
  dbt_name_hpp_path = File.join($workdir, dbt_name_hpp)
  dbt_name_path = File.join($workdir, dbt_name)

  if not exec_only
    stage "[2] Creating dbtoaster hpp file"
    Dir.chdir(test_path)
    run("#{File.join(dbt_platform, "dbtoaster")} --read-agenda -l cpp #{source_path} > #{dbt_name_hpp_path}")
    Dir.chdir(start_path)

    # change the data path
    s = File.read(dbt_name_hpp_path)
    s.sub!(/agenda.csv/,dbt_data_path)
    File.write(dbt_name_hpp_path, s)

    # adjust boost libs for OS
    boost_libs = %w(boost_program_options boost_serialization boost_system boost_filesystem boost_chrono boost_thread)
    mt = dbt_platform == "dbt_osx" ? "-mt" : ""
    boost_libs.map! { |lib| "-l" + lib + mt }

    stage "[2] Compiling dbtoaster"
    run("g++ #{File.join(dbt_lib_path, "main.cpp")} -std=c++11 -include #{dbt_name_hpp_path} -o #{dbt_name_path} -O3 -I#{dbt_lib_path} -L#{dbt_lib_path} -ldbtoaster -lpthread #{boost_libs.join ' '}")
  end

  Dir.chdir(start_path)
  stage "[2] Running DBToaster"
  run("#{dbt_name_path} > #{dbt_name_path}.xml", [/File not found/])
end

## Create/Compile stage ###

# handle all curl requests through here
def curl(server, url, args:{}, file:"", post:false, json:false, getfile:nil)
  cmd = ""
  # Getfile needs json to remove html, but don't parse as json
  cmd << if getfile.nil? then '-i ' else '-H "Accept: application/json" ' end
  cmd << '-X POST ' if post
  cmd << '-H "Accept: application/json" ' if json
  cmd << "-F \"file=@#{file}\" " if file != ""
  args.each_pair { |k,v| cmd << "-F \"#{k}=#{v}\" " }

  pipe = getfile.nil? ? '' : '-o ' + File.join($workdir, getfile)
  url2 = !getfile.nil? ? url + getfile + "/" : url

  res = run("curl http://#{server}#{url2} #{cmd}#{pipe}")
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
  check_param(uid, "--uid")

  puts "UID = #{uid}"

  # get status
  status, _ = curl_status_loop(server_url, "/compile/#{uid}", "COMPLETE")

  # get the output file (before exiting on error)
  fs_path = "/fs/build/#{nice_name}-#{uid}/"

  curl(server_url, fs_path, getfile:"output")

  check_status(status, "COMPLETE", "Remote compilation")

  # get the cpp file
  curl(server_url, fs_path, getfile:k3_cpp_name)

  if !bin_file.nil?
    # get the bin file
    curl(server_url, fs_path, getfile:bin_file)
    `chmod +x #{File.join($workdir, bin_file)}`
  end
end

# do both creation and compilation remotely (returns uid)
def run_create_compile_k3_remote(server_url, bin_file, block_on_compile, k3_cpp_name, k3_path, nice_name)
  stage "[3-4] Remote creating && compiling K3 file to binary"
  res = curl(server_url, "/compile", file: k3_path, post: true, json: true,
            args:{ "compilestage" => "both", "workload" => $options[:skew].to_s})
  uid = res["uid"]
  update_options_if_empty(:uid, uid)
  persist_options()

  if block_on_compile
    wait_and_fetch_remote_compile(server_url, bin_file, k3_cpp_name, nice_name, uid)
  end

  return uid
end

# create the k3 cpp file remotely and copy the cpp locally
def run_create_k3_remote(server_url, block_on_compile, k3_cpp_name, k3_path, nice_name)
  stage "[3] Remote creating K3 cpp file."
  res = curl(server_url, "/compile", file: k3_path, post: true, json: true,
            args:{ "compilestage" => "cpp", "workload" => $options[:skew].to_s})
  uid = res["uid"]
  update_options_if_empty(:uid, uid)
  persist_options()

  if block_on_compile
    # get the cpp file
    wait_and_fetch_remote_compile(server_url, nil, k3_cpp_name, nice_name, uid)
  end
end

# compile cpp->bin locally
def run_compile_k3_local(bin_file, k3_path, k3_cpp_name, k3_cpp_path, k3_root_path, script_path)
  stage "[4] Compiling k3 cpp file"
  brew = $options[:osx_brew] ? "_brew" : ""

  # copy cpp to proper path
  dest_path = File.join(k3_root_path, "__build")
  FileUtils.copy_file(k3_cpp_path, File.join(dest_path, k3_cpp_name))

  compile = File.join(script_path, "..", "run", "compile#{brew}.sh")
  run("#{compile} -2 #{k3_path}")

  bin_src_file = File.join(k3_root_path, "__build", "A")

  FileUtils.copy_file(bin_src_file, File.join($workdir, bin_file))
end

### Deployment stage ###

def gen_yaml(k3_data_path, role_file, script_path)
  # Generate yaml file"
  cmd = ""
  cmd << "--switches " << $options[:num_switches].to_s << " " if $options[:num_switches]
  cmd << "--nodes " << $options[:num_nodes].to_s << " " if $options[:num_nodes]
  cmd << "--nmask " << $options[:nmask] << " " if $options[:nmask]
  cmd << "--perhost " << $options[:perhost].to_s << " " if $options[:perhost]
  cmd << "--file " << k3_data_path << " "
  cmd << "--dist " if !$options[:run_local]

  extra_args = []
  extra_args << "ms_gc_interval=" + $options[:gc_epoch] if $options[:gc_epoch]
  extra_args << "sw_driver_sleep=" + $options[:msg_delay] if $options[:msg_delay]
  cmd << "--extra-args " << extra_args.join(',') << " " if extra_args.size > 0

  yaml = run("#{File.join(script_path, "gen_yaml.py")} #{cmd}")
  File.write(role_file, yaml)
end

def wait_and_fetch_results(stage_num, jobid, server_url, nice_name)
  check_param(jobid, "--jobid")

  stage "[#{stage_num}] Waiting for Mesos job to finish..."
  status, res = curl_status_loop(server_url, "/job/#{jobid}", "FINISHED")

  check_status(status, "FINISHED", "Mesos job")

  stage "[#{stage_num}] Getting result data"

  sandbox_path = File.join($workdir, "sandbox_#{jobid}")
  `mkdir -p #{sandbox_path}` unless Dir.exists?(sandbox_path)

  file_paths = []
  res['sandbox'].each do |s|
    puts "Result file: " + s
    if File.extname(s) == '.tar'
      file_paths << s
    end
  end

  file_paths.each do |f|
    curl(server_url, "/fs/jobs/#{nice_name}/#{jobid}/", getfile:f)
    run("tar xvf #{File.join($workdir, f)} -C #{sandbox_path}")
  end
end

def run_deploy_k3_remote(uid, server_url, k3_data_path, bin_path, nice_name, script_path)
  role_path = File.join($workdir, nice_name + ".yaml")

  # we can either have a uid from a previous stage, or send a binary and get a uid now
  if uid.nil?
    stage "[5] Sending binary to mesos"
    res = curl(server_url, '/apps', file:bin_path, post:true, json:true)
    uid = res['uid']
    update_options_if_empty(:uid, uid)
    persist_options()
  end

  # for latest, don't put uid in the command
  uid_s = uid == "latest" ? "" : "/#{uid}"

  # Genereate mesos yaml file"
  gen_yaml(k3_data_path, role_path, script_path)

  stage "[5] Creating new mesos job"
  res = curl(server_url, "/jobs/#{nice_name}#{uid_s}", json:true, post:true, file:role_path, args:{'jsonfinal' => 'yes'})
  jobid = res['jobId']
  update_options_if_empty(:jobid, jobid)
  persist_options()
  puts "JOBID = #{jobid}"

  # Wait for job to finish and get results
  wait_and_fetch_results(5, jobid, server_url, nice_name)
end

# local deployment
def run_deploy_k3_local(bin_path, k3_data_path, nice_name, script_path)
  role_file = File.join($workdir, nice_name + "_local.yaml")
  gen_yaml(k3_data_path, role_file, script_path)

  json_dist_path = File.join($workdir, 'json')

  stage "[5] Running K3 executable locally"
  `rm -rf #{json_dist_path}`
  `mkdir -p #{json_dist_path}`
  run("#{bin_path} -p #{role_file} --json #{json_dist_path} --json_final_only")
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
    # results with keys
    if result.has_elements?
      result.each do |item|
        if item.name == 'item'
          res = []
          item.each { |e| res << str_to_val(e.text) }

          if dbt_results.has_key?(result.name)
            dbt_results[result.name][res[0...-1]] = res[-1]
          else
            dbt_results[result.name] = {res[0...-1] => res[-1]}
          end
        end
      end
    # results without keys
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

    # convert to [k,v] rather than flat [k,v,k,v...]
    vid = nil
    map_data_k = []
    map_data_j.each do |v|
      if vid.nil?
        vid = v
      else
        map_data_k << [vid, v]
        vid = nil
      end
    end

    # frontier operation
    max_map = {}
    # check if we're dealing with maps without keys
    # format of elements: array of [vid, [key, value], vid, [k3y, value]...]
    # check for existence of first element's key (ie. key-less maps)
    if map_data_k[0][1].size > 1
      map_data_k.each do |e|
        vid = e[0]
        key = e[1][0]
        val = e[1][1]
        max_vid, _ = max_map[key]
        # compare vids to see if greater
        if !max_vid || ((vid <=> max_vid) == 1)
          max_map[key] = [vid, val]
        end
      end
      # add the max map to the combined maps and discard vids
      max_map.each_pair do |key,value|
        if !combined_maps.has_key?(map_name)
          combined_maps[map_name] = { key => value[1] }
        elsif !combined_maps[map_name].has_key?(key)
          combined_maps[map_name][key] = value[1]
        else
          # this can happen because each data node has the same maps,
          # and they're zeroed out by default, so adding should work out.
          combined_maps[map_name][key] += value[1]
        end
      end
    else # key-less maps
      max_vid  = nil
      max_data = nil
      map_data_k.each do |e|
        vid = e[0]
        val = e[1]
        if !max_vid || ((vid <=> max_vid) == 1)
          max_vid  = vid
          max_data = val
        end
      end
      unless !max_vid
        if combined_maps.has_key?(map_name)
          # again, this works because there's a 0 value in every data node
          # for this map_name, and adding will take care of that
          combined_maps[map_name] += max_data
        else
          combined_maps[map_name] = max_data
        end
      end
    end
  end
  # puts "combined: #{combined_maps}"
  return combined_maps
end

def run_compare(dbt_results, k3_results)
  # Compare results
  pass = true
  dbt_results.each_pair do |map,v1|
    v2 = k3_results[map]
    if !v2
      stage "[6] Mismatch at map #{map}: missing k3 values"; exit 1
    end
    if v1.respond_to?(:sort!) && v2.respond_to?(:sort!)
      v1.sort!
      v2.sort!
    end
    if (v1 <=> v2) != 0
      stage "[6] Mismatch in map #{map}\nv1:#{v1.to_s}\nv2:#{v2.to_s}"
      pass = false
    else
      stage "[6] Map #{map}: OK"
    end
  end
  if pass
    stage "[6] Results check...OK"
  else
    stage "[6] Results check...FAIL"
  end
end

def check_param(p, nm)
  if p.nil?
    puts "Please provide #{nm} param"
    exit(1)
  end
end

def update_options_if_empty(k, v)
  $options[k] = v unless $options[k]
end

# persist source, data paths, and others
def persist_options()
  def update_if_there(opts, x)
    opts[x] = $options[x] if $options[x]
  end
  options = {}
  update_if_there(options, :source)
  update_if_there(options, :k3_data_path)
  update_if_there(options, :dbt_data_path)
  update_if_there(options, :mosaic_path)
  update_if_there(options, :uid)
  update_if_there(options, :jobid)
  update_if_there(options, :skew)
  File.write($last_path, JSON.dump(options))
end


def main()
  $options = {}
  uid = nil

  usage = "Usage: #{$PROGRAM_NAME} sql_file options"
  parser = OptionParser.new do |opts|
    opts.banner = usage
    opts.on("-w", "--workdir [PATH]", "Path in which to create files") {|s| $options[:workdir] = s}
    opts.on("-d", "--dbtdata [PATH]", String, "Set the path of the dbt data file") { |s| $options[:dbt_data_path] = s }
    opts.on("-k", "--k3data [PATH]", String, "Set the path of the k3 data file") { |s| $options[:k3_data_path] = s }
    opts.on("-s", "--switches [NUM]", Integer, "Set the number of switches") { |i| $options[:num_switches] = i }
    opts.on("-n", "--nodes [NUM]", Integer, "Set the number of nodes") { |i| $options[:num_nodes] = i }
    opts.on("-j", "--json [JSON]", String, "JSON file to load options") {|s| $options[:json_file] = s}
    opts.on("--debug", "Debug mode") { $options[:debug] = true }
    opts.on("--json_debug", "Debug queries that won't die") { $options[:json_debug] = true }
    opts.on("--perhost [NUM]", Integer, "How many peers to run per host") {|i| $options[:perhost] = i}
    opts.on("--nmask [MASK]", String, "Mask for node deployment") {|s| $options[:nmask] = s}
    opts.on("--uid [UID]", String, "UID of file") {|s| $options[:uid] = s}
    opts.on("--jobid [JOBID]", String, "JOBID of job") {|s| $options[:jobid] = s}
    opts.on("--mosaic-path [PATH]", String, "Path for mosaic") {|s| $options[:mosaic_path] = s}
    opts.on("--highmem", "High memory deployment (HM only)") { $options[:nmask] = 'qp-hm.'}
    opts.on("--brew", "Use homebrew (OSX)") { $options[:osx_brew] = true }
    opts.on("--run-local", "Run locally") { $options[:run_local] = true }
    opts.on("--create-local", "Create the cpp file locally") { $options[:create_local] = true }
    opts.on("--compile-local", "Compile locally") { $options[:compile_local] = true }
    opts.on("--dbt-exec-only", "Execute DBToaster only (skipping query build)") { $options[:dbt_exec_only] = true }
    opts.on("--fetch-cpp", "Fetch a cpp file after remote creation") { $options[:fetch_cpp] = true}
    opts.on("--fetch-bin", "Fetch bin + cpp files after remote compilation") { $options[:fetch_bin] = true}
    opts.on("--fetch-results", "Fetch results after job") { $options[:fetch_results] = true }
    opts.on("--latest-uid",  "Use the latest uid on the server") { $options[:latest_uid] = true}
    opts.on("--moderate",  "Query is of moderate skew (and size)") { $options[:skew] = :moderate}
    opts.on("--moderate2",  "Query is of moderate skew (and size), class 2") { $options[:skew] = :moderate2}
    opts.on("--extreme",  "Query is of extreme skew (and size)") { $options[:skew] = :extreme}
    opts.on("--dry-run",  "Dry run for Mosaic deployment (generates K3 YAML topology)") { $options[:dry_run] = true}
    opts.on("--dots", "Get the awesome dots") { $options[:dots] = true }
    opts.on("--gc-epoch [MS]", "Set gc epoch time (ms)") { |i| $options[:gc_epoch] = i }
    opts.on("--msg-delay [MS]", "Set switch message delay (ms)") { |i| $options[:msg_delay] = i }

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

  # get directory of script
  script_path = File.expand_path(File.dirname(__FILE__))

  # a lot can be inferred once we have the workdir
  $workdir     = $options[:workdir] ? $options[:workdir] : "temp"
  puts "WORKDIR = #{$workdir}"
  $workdir     = File.expand_path($workdir)

  `mkdir -p #{$workdir}` unless Dir.exists?($workdir)

  # File for saving settings
  last_file = "last.json"
  $last_path = File.join($workdir, last_file)

  def update_from_json(options)
    options.each_pair do |k,v|
      k2 = k.to_sym
      unless $options.has_key?(k2)
        $options[k2] = v
      end
    end
  end

  # load old options from last run
  if File.exists?($last_path)
    update_from_json(JSON.parse(File.read($last_path)))
  end

  # handle json options
  if $options.has_key?(:json_file)
    update_from_json(JSON.parse($options[:json_file]))
  end

  ### fill in default options (must happen after filling in from json)
  # if only one data file, take that one
  if $options.has_key?(:dbt_data_path) && !$options.has_key?(:k3_data_path)
    $options[:k3_data_path] = $options[:dbt_data_path]
  elsif $options.has_key?(:k3_data_path) && !$options.has_key?(:dbt_data_path)
    $options[:dbt_data_path] = $options[:k3_data_path]
  end
  # skew is balanced if missing
  $options[:skew] = :balanced unless $options[:skew]

  # check that we have a source
  unless ARGV.size == 1 || $options[:source]
    puts parser.help
    exit(1)
  end

  source = ARGV.size == 1 ? ARGV[0] : $options[:source]
  $options[:source] = source

  persist_options()

  # split path components
  ext          = File.extname(source)
  basename     = File.basename(source, ext)
  lastpath     = File.split(File.split(source)[0])[1]
  source_path  = File.expand_path(source)
  k3_root_path = File.join(script_path, "..", "..", "..")
  common_root_path  = File.join(k3_root_path, "..")
  mosaic_path  = File.join(common_root_path, "K3-Mosaic")
  mosaic_path  = $options[:mosaic_path] ? $options[:mosaic_path] : mosaic_path

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

  k3_data_path = $options[:k3_data_path] ? $options[:k3_data_path] : File.join($workdir, nice_name + ".csv")
  dbt_data_path = $options[:dbt_data_path] ? $options[:dbt_data_path] : File.join($workdir, nice_name + ".csv")

  server_url = "qp2:5000"

  bin_file = nice_name
  bin_path = File.join($workdir, bin_file)
  dbt_results = []

  if $options[:mosaic]
    run_mosaic(k3_path, mosaic_path, source)
  end

  if $options[:dbtoaster]
    run_dbtoaster($options[:dbt_exec_only], test_path, dbt_data_path, dbt_plat, dbt_lib_path, dbt_name, dbt_name_hpp, source_path, start_path)
  end
  # either nil or take from command line
  uid = $options[:uid] ? $options[:uid] : $options[:latest_uid] ? "latest" : nil
  jobid = $options[:jobid] ? $options[:jobid] : nil

  # options to fetch cpp/binary (ie. all) given a uid
  if $options[:fetch_cpp]
    wait_and_fetch_remote_compile(server_url, nil, k3_cpp_name, nice_name, uid)
  elsif $options[:fetch_bin]
    wait_and_fetch_remote_compile(server_url, bin_file, k3_cpp_name, nice_name, uid)
  end

  # check for doing everything remotely
  if !$options[:compile_local] && !$options[:create_local] && ($options[:create_k3] || $options[:compile_k3])
      # only block if we need to ie. if we have deployment of some source
      block_on_compile = $options[:deploy_k3] || $options[:run_local] || $options[:dots]
      uid = run_create_compile_k3_remote(server_url, bin_file, block_on_compile, k3_cpp_name, k3_path, nice_name)
  else
    if $options[:create_k3]
      if $options[:create_local]
        run_create_k3_local(k3_path, script_path)
      else
        block_on_compile = $options[:compile_k3] || $options[:deploy_k3] || $options[:dots]
        run_create_k3_remote(server_url, block_on_compile, k3_cpp_name, k3_path, nice_name)
      end
    end
    # if we're not doing everything remotely, we can only compile locally
    if $options[:compile_k3]
        run_compile_k3_local(bin_file, k3_path, k3_cpp_name, k3_cpp_path, k3_root_path, script_path)
    end
  end

  if $options[:deploy_k3]
    if $options[:dry_run]
      role_file = File.join($workdir, nice_name + "_local.yaml")
      gen_yaml(k3_data_path, role_file, script_path)
    elsif $options[:run_local]
      run_deploy_k3_local(bin_path, k3_data_path, nice_name, script_path)
    else
      run_deploy_k3_remote(uid, server_url, k3_data_path, bin_path, nice_name, script_path)
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
