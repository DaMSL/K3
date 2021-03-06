#!/usr/bin/env ruby
require 'csv'
require 'json'
require 'optparse'
require 'pathname'
require 'rest-client'

RC = RestClient

K3 = "/k3/K3"

QUERIES = {
  "tpch" => {
    :roles => {
      "10g" => "tpch_10g.yml",
      "100g" => "tpch_100g.yml",
      "10g_inuse" => "tpch_10g_inuse.yml",
      "10g_accum" => "tpch_10g_accum.yml",
      "100g_inuse" => "tpch_100g_inuse.yml",
      "100g_accum" => "tpch_100g_accum.yml",

      "scalability_256" => "scalability/256.yml",
      "scalability_128" => "scalability/128.yml",
      "scalability_64" => "scalability/64.yml",
      "scalability_32" => "scalability/32.yml",
      "scalability_16" => "scalability/16.yml",

      "10g_selectivity_0.0" => "selectivity/tpch_10g_selectivity_0.0.yml",
      "10g_selectivity_0.5" => "selectivity/tpch_10g_selectivity_0.5.yml",
      "10g_selectivity_1.0" => "selectivity/tpch_10g_selectivity_1.0.yml",

      "100g_selectivity_0.0" => "selectivity/tpch_100g_selectivity_0.0.yml",
      "100g_selectivity_0.5" => "selectivity/tpch_100g_selectivity_0.5.yml",
      "100g_selectivity_1.0" => "selectivity/tpch_100g_selectivity_1.0.yml",
    },
    :queries => {
      "1" => "examples/sql/tpch/queries/k3/q1.k3",
      "3" => "examples/sql/tpch/queries/k3/barrier-queries/q3.k3",
      "5" => "examples/sql/tpch/queries/k3/barrier-queries/q5_bushy_broadcast_broj2.k3",
      "6" => "examples/sql/tpch/queries/k3/q6.k3",
      "18" => "examples/sql/tpch/queries/k3/barrier-queries/q18.k3",
      "22" => "examples/sql/tpch/queries/k3/barrier-queries/q22.k3",

      "1s" => "examples/sql/tpch/queries/k3/selectivity/q1.k3",
      "3s" => "examples/sql/tpch/queries/k3/selectivity/q3.k3",
      "5s" => "examples/sql/tpch/queries/k3/selectivity/q5_bushy_broadcast_broj2.k3",
      "6s" => "examples/sql/tpch/queries/k3/selectivity/q6.k3",
      "18s" => "examples/sql/tpch/queries/k3/selectivity/q18.k3",
      "22s" => "examples/sql/tpch/queries/k3/selectivity/q22.k3",
    }
  },

  "amplab" => {
    :roles => {
      "sf5" => "amplab_sf5.yml",
      "sf5_inuse" => "amplab_sf5_inuse.yml",
      "sf5_accum" => "amplab_sf5_accum.yml",
    },
    :queries => {
      "1" => "examples/distributed/amplab/q1.k3",
      "2" => "examples/distributed/amplab/q2.k3",
      "3" => "examples/distributed/amplab/q3.k3",
    }
  },

  "ml" => {
   :roles => {
    "10g" => "ml_10g.yml",
    "100g" => "ml_100g.yml",
   },
   :queries => {
    "k_means" => "examples/distributed/ml/k_means.k3",
    "sgd" => "examples/distributed/ml/sgd.k3",
   }
  },

  "graph" => {
    :roles => {
      "twitter" => "graph_twitter.yml"
    },
    :queries => {
      "page_rank" => "examples/distributed/graph/page_rank.k3",
    }
  },
}

def slugify(experiment, query)
  return "#{experiment}-#{query}"
end

def select?(experiment, query, role = nil)
  excluded = $options[:excludes].any? do |pattern|
    check_filter(pattern, experiment, query, role)
  end

  included = $options[:includes].any? do |pattern|
    check_filter(pattern, experiment, query, role)
  end

  return !excluded || included
end

def list()
  for experiment, description in QUERIES do
    for query, _ in description[:queries] do
      if select?(experiment, query)
        p [experiment, query]
      end
    end
  end
end

def setup_build_profile(profile)
  profile.fetch("patches", []).each do |p|
    puts "Applying #{p}..."
    `git apply #{$options.fetch(:patch_dir, ".")}/#{p}`
  end

  if profile.has_key? "build_opts"
    ENV["K3_BUILDOPTS"] = profile["build_opts"]
  end

  if profile.has_key? "cxx_opts"
    ENV["K3_CXXFLAGS"] = profile["cxx_opts"]
  end
end

def teardown_build_profile(profile)
  profile.fetch("patches", []).each do |p|
    puts "Reverting #{p}..."
    `git apply -R #{$options.fetch(:patch_dir, ".")}/#{p}`
  end
end

def build(target)
  if $options.has_key?(:build_profile_set) && $options.has_key?(:build_profile)
    profile = JSON.parse(File.read($options[:build_profile_set]))[$options[:build_profile]]
  else
    profile = {}
  end

  setup_build_profile(profile)

  puts "Removing Build Directory"
  system "rm -rf __build"

  for (experiment, description) in QUERIES do
    for query, path in description[:queries] do
      if !select?(experiment, query)
        next
      end
      Dir.chdir(K3) do
        puts "Cleaning build directory..."
        system "tools/scripts/run/clean.sh"
        puts "Compiling K3 -> C++ with options: #{ENV["K3_BUILDOPTS"]}"
        system "mkdir -p #{target}"
        system "tools/scripts/run/compile.sh #{ENV["K3_BUILDOPTS"]} #{path} 2>&1 | tee #{target}/#{slugify(experiment, query)}_build.log"
        puts "Copying artifacts..."
        system "mv __build/#{File.basename(path, ".k3")}.cpp #{target}/#{slugify(experiment, query)}.cpp"
        system "mv __build/A #{target}/#{slugify(experiment, query)}"
        system "cp #{path} #{target}/#{slugify(experiment, query)}.k3"
      end
    end
  end

  teardown_build_profile(profile)
end

def submit(name)
  saved = {}
  for experiment, description in QUERIES do
    for query, _ in description[:queries] do
      if !select?(experiment, query)
        next
      end
      puts "Submitting #{name}/#{slugify(experiment, query)}"
      response = RC.post(
        "http://mddb2:5000/apps",
        {:file => File.new("#{name}/#{slugify(experiment, query)}", 'rb')},
        :accept => :json
      )
      hash = JSON.parse response
      puts(hash)
      saved[hash["name"]] = hash["uid"]
    end
  end
  File.open("#{name}/apps.json", "w") do |f|
    f.write(saved.to_json)
  end
end

def run(name)
  puts "Submitting Run Jobs"
  jobs = {}
  apps = JSON.parse(File.read("#{name}/apps.json"))
  for experiment, description in QUERIES do
    for query, _ in description[:queries] do
      for role, yml in description[:roles] do
        if !select?(experiment, query, role)
          next
        end

        role_prefix = $options.fetch(:role_dir, name)
        for i in 1..($options[:trials]) do
          puts "\tSubmitting #{role} for #{experiment}/#{query} trial #{i}"
          response = RC.post(
            "http://mddb2:5000/jobs/#{slugify(experiment, query)}/#{apps[slugify(experiment, query)]}",
            {:file => File.new("#{role_prefix}/roles/#{yml}")},
            :accept => :json
          )
          json = JSON.parse response
          job_id = json['jobId']
          info = {"role" => role, "name" => slugify(experiment, query), "trial" => i}
          jobs[job_id] = info
        end
      end
    end
  end
  return jobs
end

def statusAll(jobs)
  results = {}
  for (job_id, info) in jobs do
    response = RC.get(
      "http://mddb2:5000/job/#{job_id}",
      :accept => :json
    )
    json = JSON.parse response
    val = {'status' => json['status']}
    val['sandbox'] = json['sandbox']
    val.merge!(info)
    results[job_id] = val
  end
  return results
end

def poll(jobs, message)
  print(message)
  statuses = statusAll(jobs)
  processing = true
  total = statuses.length
  progress = 0
  while processing
    incomplete = 0
    for _, val in statuses
      status = val['status']
      #TODO add "KILLED" to this list after bug is fixed: jobs are reported as KILLED before they run
      if status != "FINISHED" and status != "FAILED"
        incomplete += 1
      end
      done = total - incomplete
      if done > progress
        progress = done
        print "Progress: #{progress}/#{total}\n"
      else
        print "."
      end

      if progress == total
        processing = false
      else
        sleep 4
      end
    end
  end

  return statuses
end

def harvest(statuses, out_folder)
  puts("Harvesting results")
  results = {}
  `mkdir -p #{out_folder}/#{$options[:job_set]}`
  CSV.open("#{out_folder}/#{$options[:job_set]}/raw.csv", "wb") do |rawf|
    for job_id, info in statuses
      run_folder = "#{out_folder}/#{$options[:job_set]}/#{info["role"]}_#{info["name"]}"
      `mkdir -p #{run_folder}`

      if info['status'] == "FINISHED"
        # GET tar from each node
        tars = info['sandbox'].select { |x| x =~ /.*.tar/}
        for tar in tars
          url = "http://mddb2:5000/fs/jobs/#{info["name"]}/#{job_id}/#{tar}"
          name = File.basename(tar, ".tar")
          `mkdir -p #{run_folder}/#{job_id}/#{name}`
          response = RC.get(url)
          file = File.new("#{run_folder}/#{job_id}/#{name}/sandbox.tar", 'w')
          file.write response
          file.close
          `tar -xvf #{run_folder}/#{job_id}/#{name}/sandbox.tar -C #{run_folder}/#{job_id}/#{name}`
        end
        # Find the master tar, for results.csv
        master_tar = tars.select { |x| x =~ /.*\.0_.*/}[0]
        name = File.basename(master_tar, ".tar")
        master_folder = "#{run_folder}/#{job_id}/#{name}/"
        results[job_id] = info
        results[job_id].merge!({"status" => "RAN", "output" => master_folder})
        time_file = "#{master_folder}/time.csv"
        file = File.open(time_file, "rb")
        time_ms = file.read.strip.to_i
        puts "\t#{info} Ran in #{time_ms} ms."
        group_key = {:role => info["role"], :name => info["name"]}
        if not $stats.has_key?(group_key)
          $stats[group_key] = [time_ms]
        else
          $stats[group_key] << time_ms
        end
        _, run_date, variant = out_folder.split("/")
        job_set = $options[:job_set]
        rawf << [job_id, run_date, variant, job_set, info["role"], info["name"], time_ms]
      else
        results[job_id] = { "status" =>  "FAILED" }
        puts "\t#{job_id} FAILED."
      end
    end
  end

  File.open("#{out_folder}/#{$options[:job_set]}/stats.json", "wb") do |outf|
    outf.write($stats.to_json)
  end
  return results
end

def check(folders)
  puts("Checking for correctness")
  ktrace_dir = "#{K3}/tools/ktrace/"
  correct_dir = "/local/correct/correct/"
  diff = "#{ktrace_dir}/csv_diff.py"
  for job_id, info in folders
    if info["status"] == "RAN"
      run_id = "#{info["role"]}-#{info["name"]}"
      correct = "#{correct_dir}/#{run_id}.out"
      actual = "#{info["output"]}/results.csv"
      output = `python2 #{diff} #{correct} #{actual}`
      if $?.to_i == 0
        puts("\t#{job_id} => CORRECT RESULTS.")
      else
        puts("\t#{job_id} => INCORRECT RESULTS")
        puts(output)
      end
    end
  end
end

def postprocess(dir)
  stats_file = "#{dir}/#{$options[:job_set]}/stats.json"
  if File.exists?(stats_file)
    $stats = JSON.parse(File.read(stats_file))
  end
  CSV.open("#{dir}/#{$options[:job_set]}/stats.csv", "wb") do |outf|
    outf << ["variant", "query", "role", "trials", "average", "stddev"]
    puts "Summary"
    for key, val in $stats
      sum = val.reduce(:+)
      cnt = val.size
      avg = 1.0 * sum / cnt
      var = val.map{|x| (x - avg) * (x - avg)}.reduce(:+) / (1.0 * cnt)
      dev = Math.sqrt(var)
      puts "\t#{key} => Successful Trials: #{cnt}/#{$options[:trials]}. Avg: #{avg}. StdDev: #{dev}"
      outf << [dir, key[:name], key[:role], cnt, avg.round(2), dev.round(2)]
    end
  end
end

def check_filter(pattern, experiment, query, role = nil)
  (expected_experiment, expected_query, expected_role) = pattern.split("/")
  return ((Regexp.new expected_experiment) =~ experiment) &&
         ((Regexp.new expected_query) =~  query) &&
         (role.nil? || (Regexp.new expected_role) =~ role)
end

def main()
  STDOUT.sync = true
  $options = { :workdir => ".", :trials => 1, :correctdir => "/local/correct/correct/", :includes => [], :excludes => [] }
  $stats = {}
  usage = "Usage: #{$PROGRAM_NAME} options"

  if ARGF.argv.empty?
    puts usage
  end

  parser = OptionParser.new do |opts|
    opts.banner = usage
    opts.on("-1", "--build", "Build/Submit stage")  { $options[:build] = true }
    opts.on("-2", "--run",   "Run stage")           { $options[:run]   = true }

    opts.on("-x", "--build-profile-set [PATH]", String, "Set of Build Profiles") { |x| $options[:build_profile_set] = x }
    opts.on("-y", "--build-profile [KEY]", String, "Build Profile to use.") { |y| $options[:build_profile] = y }
    opts.on("-z", "--patch-dir [KEY]", String, "Directory to look for patches") { |z| $options[:patch_dir] = z }

    opts.on("-c", "--check", "Check correctness") { $options[:check] = true }
    opts.on("-s", "--submit", "Submit binary") { $options[:submit] = true }
    opts.on("-g", "--gather", "Gather results from remote server") { $options[:gather] = true }
    opts.on("-l", "--list", "List matching workloads") { $options[:list] = true }
    opts.on("-p", "--post-process", "Post-process gathered timing results.") { $options[:post_process] = true }

    opts.on("-w", "--workdir [PATH]",    String,  "Working Directory") { |s| $options[:workdir]      = s    }
    opts.on("-r", "--role-dir [PATH]",   String,  "Role Directory")    { |r| $options[:role_dir]     = r    }
    opts.on("-t", "--trials [NUM]",      Integer, "Number of Trials")  { |i| $options[:trials]       = i    }
    opts.on("-d", "--correctdir [PATH]", Integer, "Number of Trials")  { |s| $options[:correctdir]   = s    }

    opts.on("-i", "--include pat1,pat2,pat3", Array, "Patterns to Include") { |is| $options[:includes] = is }
    opts.on("-e", "--exclude pat1,pat2,pat3", Array, "Patterns to Exclude") { |es| $options[:excludes] = es }

    opts.on("-j", "--job-set [JOBSET]", String, "Name of Current Job Set") { |j| $options[:job_set] = j }
  end
  parser.parse!

  workdir = $options[:workdir]
  if $options[:build]
    build(workdir)
  end

  if $options[:build] or $options[:submit]
    submit(workdir)
  end

  if $options[:job_set]
    job_file = "#{workdir}/jobs_#{$options[:job_set]}.json"
  else
    job_file = "#{workdir}/jobs.json"
  end

  if $options[:run]
    jobs = run(workdir)
    File.open(job_file, "w") do |f|
      f.write(jobs.to_json)
    end
  elsif File.exists?(job_file)
    jobs = JSON.parse(File.read(job_file))
  end

  if $options[:gather]
    statuses = poll(jobs, "Getting status of previously submitted jobs.")
    folders = harvest(statuses, workdir)
  end

  if $options[:post_process]
    postprocess(workdir)
  end

  if $options[:check]
    check(folders)
  end

  if $options[:list]
    list()
  end
end

if __FILE__ == $0
  main
end
