#!/usr/bin/env ruby
require 'rest-client'
require 'optparse'
require 'json'
require 'pathname'

RC = RestClient

XP = Pathname.new(Dir.pwd).realpath
K3 = "/k3/K3"

QUERIES = {
  "tpch" => {
    :roles => {
      "10g" => "tpch_10g.yml",
      "100g" => "tpch_100g.yml",
    },
    :queries => {
      "1" => "examples/sql/tpch/queries/k3/q1.k3",
      "3" => "examples/sql/tpch/queries/k3/barrier-queries/q3.k3",
      "5" => "examples/sql/tpch/queries/k3/barrier-queries/q5_bushy_broadcast_broj2.k3",
      "6" => "examples/sql/tpch/queries/k3/q6.k3",
      "18" => "examples/sql/tpch/queries/k3/barrier-queries/q18.k3",
      "22" => "examples/sql/tpch/queries/k3/barrier-queries/q22.k3",

      "10" => "examples/sql/tpch/queries/k3/q10.k3",
      "11" => "examples/sql/tpch/queries/k3/q11.k3",
    }
  },

  "amplab" => {
    :roles => {
      "sf5" => "amplab_sf5.yml",
    },
    :queries => {
      "1" => "examples/distributed/amplab/compact/q1.k3",
      "2" => "examples/distributed/amplab/compact/q2.k3",
      "3" => "examples/distributed/amplab/compact/q3.k3",
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
    :roles => {},
    :queries => {
      "page_rank" => "examples/distributed/graph/page_rank.k3",
    }
  },

  "conversion" => {
    :roles => {
      "10g" => "convert_10g.yml",
      "100g" => "convert_100g.yml",
    },

    :queries => {
      "convert" => "examples/sql/tpch/convert.k3"
    }
  },

  "test" => {
   :roles => {
     "test" => "test.yml"
   },

   :queries => {
     "test" => "test.k3"
   }
  }
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

def build(name)
  target = "#{XP}/#{name}"
  for (experiment, description) in QUERIES do
    for query, path in description[:queries] do
      if !select?(experiment, query)
        next
      end
      Dir.chdir(K3) do
        puts "Cleaning build directory..."
        `tools/scripts/run/clean.sh`
        puts "Compiling K3 -> C++ ..."
        `tools/scripts/run/compile.sh #{path}`
        puts "Copying artifacts..."
        `mkdir -p #{target}`
        `mv __build/#{File.basename(path, ".k3")}.cpp #{target}/#{slugify(experiment, query)}.cpp`
        `mv __build/A #{target}/#{slugify(experiment, query)}`
        `cp #{path} #{target}/#{slugify(experiment, query)}.k3`
      end
    end
  end
end

def submit(name)
  for experiment, description in QUERIES do
    for query, _ in description[:queries] do
      if !select?(experiment, query)
        next
      end
      puts "Submitting #{name}/#{slugify(experiment, query)}"
      response = RC.post(
        "http://qp2:5000/apps",
        {:file => File.new("#{name}/#{slugify(experiment, query)}", 'rb')},
        :accept => :json
      )
      puts(JSON.parse response)
    end
  end
end

def run(name)
  puts "Submitting Run Jobs"
  jobs = {}
  for experiment, description in QUERIES do
    for query, _ in description[:queries] do
      for role, yml in description[:roles] do
        if !select?(experiment, query, role)
          next
        end
        for i in 1..($options[:trials]) do
          puts "\tSubmitting #{role} for #{experiment}/#{query} trial #{i}"
          response = RC.post(
            "http://qp2:5000/jobs/#{slugify(experiment, query)}",
            {:file => File.new("#{name}/roles/#{yml}")},
            :accept => :json
          )
          json = JSON.parse response
          job_id = json['jobId']
          key = {:role => role, :name => slugify(experiment, query), :trial => i}
          jobs[key] = job_id
        end
      end
    end
  end
  return jobs
end

def statusAll(jobs)
  results = {}
  for (key, job_id) in jobs do
    response = RC.get(
      "http://qp2:5000/job/#{job_id}",
      :accept => :json
    )
    json = JSON.parse response
    val = {'status' => json['status']}
    val['sandbox'] = json['sandbox']
    val['job_id'] = job_id
    results[key] = val
  end
  return results
end

def poll(jobs, message)
  print(message)
  statuses = statusAll(jobs)
  for _, val in statuses
    status = val['status']
    #TODO add "KILLED" to this list after bug is fixed: jobs are reported as KILLED before they run
    if status != "FINISHED" and status != "FAILED"
      sleep(4)
      return poll(jobs, ".")
    end
  end
  puts("")
  return statuses
end

def harvest(statuses, out_folder)
  puts("Harvesting results")
  results = {}
  for key, val in statuses
    if val['status'] == "FINISHED"
      run_folder = "#{out_folder}/#{key[:role]}_#{key[:name]}"
      `mkdir -p #{run_folder}`

      # GET tar from each node
      tars = val['sandbox'].select { |x| x =~ /.*.tar/}
      for tar in tars
        url = "http://qp2:5000/fs/jobs/#{key[:name]}/#{val['job_id']}/#{tar}"
        name = File.basename(tar, ".tar")
        `mkdir -p #{run_folder}/#{name}`
        response = RC.get(url)
        file = File.new("#{run_folder}/#{name}/sandbox.tar", 'w')
        file.write response
        file.close
        `tar -xvf #{run_folder}/#{name}/sandbox.tar -C #{run_folder}/#{name}`
      end
      # Find the master tar, for results.csv
      master_tar = tars.select { |x| x =~ /.*\.0_.*/}[0]
      name = File.basename(master_tar, ".tar")
      master_folder = "#{run_folder}/#{name}/"
      results[key] = {"status" => "RAN", "output" => master_folder}
      time_file = "#{master_folder}/time.csv"
      file = File.open(time_file, "rb")
      time_ms = file.read.strip.to_i
      puts "\t#{key} Ran in #{time_ms} ms."
      group_key = {:role => key[:role], :name => key[:name]}
      if not $stats.has_key?(group_key)
        $stats[group_key] = [time_ms]
      else
        $stats[group_key] << time_ms
      end
    else
      results[key] = {"status" => "FAILED"}
      puts "\t#{key} FAILED."
    end
  end
  return results
end

def check(folders)
  puts("Checking for correctness")
  ktrace_dir = "#{K3}/tools/ktrace/"
  correct_dir = "/local/correct/correct/"
  diff = "#{ktrace_dir}/csv_diff.py"
  for key, val in folders
    if val["status"] == "RAN"
      run_id = "#{key[:role]}-#{key[:name]}"
      correct = "#{correct_dir}/#{run_id}.out"
      actual = "#{val["output"]}/results.csv"
      output = `python2 #{diff} #{correct} #{actual}`
      if $?.to_i == 0
        puts("\t#{key} => CORRECT RESULTS.")
      else
        puts("\t#{key} => INCORRECT RESULTS")
        puts(output)
      end
    end
  end
end

def postprocess()
  puts "Summary"
  for key, val in $stats
    sum = val.reduce(:+)
    cnt = val.size
    avg = 1.0 * sum / cnt
    var = val.map{|x| (x - avg) * (x - avg)}.reduce(:+) / (1.0 * cnt)
    dev = Math.sqrt(var)
    puts "\t#{key} => Succesful Trials: #{cnt}/#{$options[:trials]}. Avg: #{avg}. StdDev: #{dev}"
  end
end

def check_filter(pattern, experiment, query, role = nil)
  (expected_experiment, expected_query, expected_role) = pattern.split("/")
  return ((Regexp.new expected_experiment) =~ experiment) &&
         ((Regexp.new expected_query) =~  query) &&
         ((Regexp.new expected_role) =~ role)
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

    opts.on("-c", "--check",                      "Check correctness") {     $options[:check]        = true }
    opts.on("-s", "--submit",                     "Submit binary")     {     $options[:submit]       = true }
    opts.on("-l", "--list", "List matching workloads") { $options[:list] = true }
    opts.on("-w", "--workdir [PATH]",    String,  "Working Directory") { |s| $options[:workdir]      = s    }
    opts.on("-t", "--trials [NUM]",      Integer, "Number of Trials")  { |i| $options[:trials]       = i    }
    opts.on("-d", "--correctdir [PATH]", Integer, "Number of Trials")  { |s| $options[:correctdir]   = s    }

    opts.on("-i", "--include pat1,pat2,pat3", Array, "Patterns to Include") { |is| $options[:includes] = is }
    opts.on("-e", "--exclude pat1,pat2,pat3", Array, "Patterns to Exclude") { |es| $options[:excludes] = es }
  end
  parser.parse!

  workdir = $options[:workdir]
  if $options[:build]
    build(workdir)
  end

  if $options[:build] or $options[:submit]
    submit(workdir)
  end

  if $options[:run]
    jobs = run(workdir)
    statuses = poll(jobs, "Polling until complete")
    folders = harvest(statuses, workdir)
    postprocess()
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
