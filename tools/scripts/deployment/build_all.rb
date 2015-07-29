#!/usr/bin/env ruby
require 'rest-client'
require 'json'
require 'pathname'

RC = RestClient

XP = Pathname.new(Dir.pwd).realpath
K3 = "/k3/K3"

QUERIES = {
  "tpch" => {
    :roles => {
      "10g" => "tpch_10g.yml",
      #"100g" => "tpch_100g.yml",
    },
    :queries => {
      "q1" => "examples/sql/tpch/queries/k3/q1.k3",
      "q3" => "examples/sql/tpch/queries/k3/barrier-queries/q3.k3",
      "q5" => "examples/sql/tpch/queries/k3/barrier-queries/q5_bushy_broadcast_broj2.k3",
      "q6" => "examples/sql/tpch/queries/k3/q6.k3",
      "q18" => "examples/sql/tpch/queries/k3/barrier-queries/q18.k3",
      "q22" => "examples/sql/tpch/queries/k3/barrier-queries/q22.k3",
    }
  },

  #"amplab" => {
  #  :roles => {
  #    "sf5" => "amplab_sf5.yml",
  #  },
  #  :queries => {
  #    "amplab_q1" => "examples/distributed/amplab/compact/q1.k3",
  #    "amplab_q2" => "examples/distributed/amplab/compact/q2.k3",
  #    "amplab_q3" => "examples/distributed/amplab/compact/q3.k3",
  #  }
  #},

  #"ml" => {
  #  :roles => {
  #   "10g" => "ml_10g.yml",
  #   "100g" => "ml_100g.yml",
  #  },
  #  :queries => {
  #   "k_means" => "examples/distributed/ml/k_means.k3",
  #   "sgd" => "examples/distributed/ml/sgd.k3",
  #  }
  #},

  # "graph" => {
  #   :roles => {},
  #   :queries => {
  #     "page_rank" => "examples/distributed/graph/page_rank.k3",
  #   }
  # }
}

def slugify(experiment, query)
  return "#{experiment}_#{query}"
end

def build(name)
  target = "#{XP}/#{name}"
  for (experiment, description) in QUERIES do
    for query, path in description[:queries] do
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
  jobs = {}
  for experiment, description in QUERIES do
    for query, _ in description[:queries] do
      for role, yml in description[:roles] do
        puts "Submitting #{role} for #{experiment}/#{query}"
        response = RC.post(
          "http://qp2:5000/jobs/#{slugify(experiment, query)}",
          {:file => File.new("#{name}/roles/#{yml}")},
          :accept => :json
        )
        json = JSON.parse response
        job_id = json['jobId']
        key = {'role' => role, 'name' => slugify(experiment, query)}
        jobs[key] = job_id
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
  for key, val in statuses
    status = val['status']
    #TODO add "KILLED" to this list after bug is fixed: jobs are reported as KILLED before they run
    if status != "FINISHED" and status != "FAILED"
      sleep(4)
      return poll(jobs, ".")
    end
  end
  return statuses
end

def harvest(statuses, out_folder)
  puts("Harvesting results")
  results = {}
  for key, val in statuses
    if val['status'] == "FINISHED"
      run_folder = "#{out_folder}/#{key['role']}_#{key['name']}"
      `mkdir -p #{run_folder}`

      # GET tar from each node
      tars = val['sandbox'].select { |x| x =~ /.*.tar/}
      for tar in tars
        url = "http://qp2:5000/fs/jobs/#{key['name']}/#{val['job_id']}/#{tar}"
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
    else
      results[key] = {"status" => "FAILED"}
    end
  end
  return results
end

def check(folders)
  puts("Checking for correctness")
  ktrace_dir = "#{K3}/tools/ktrace/"
  diff = "#{ktrace_dir}/csv_diff.py"
  for key, val in folders
    if val["status"] == "RAN"
      run_id = "#{key['role']}_#{key['name']}"
      correct = "#{ktrace_dir}/correct_results/#{run_id}.csv"
      actual = "#{val["output"]}/results.csv"
      output = `python2 #{diff} #{correct} #{actual}`
      time_file = "#{val["output"]}/time.csv"
      file = File.open(time_file, "rb")
      time_ms = file.read.strip
      if $?.to_i == 0
        puts("#{key} => CORRECT RESULTS. Time: #{time_ms} ms.")
      else
        puts("#{key} => FAILED: INCORRECT RESULTS. Time: #{time_ms} ms.")
        puts(output)
      end
    else
      puts("#{key} => FAILED: DID NOT RUN AND/OR NO OUTPUT")
    end
  end
end

if __FILE__ == $0
  if ARGF.argv.empty?
    puts "usage: #{$0} path/to/build/dir"
    exit
  end
  puts "Running build for #{ARGF.argv[0]}"
  build(ARGF.argv[0])
  submit(ARGF.argv[0])
  jobs = run(ARGF.argv[0])
  statuses = poll(jobs, "Polling until complete")
  puts("")
  folders = harvest(statuses, ARGF.argv[0])
  check(folders)
end
