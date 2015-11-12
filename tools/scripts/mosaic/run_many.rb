require 'csv'

datanode_machine_count = 8 # qp-hd[1-9]$

# Append an entry to the result csv file
def log_csv(sf, query, switches, nodes, time)
  CSV.open("results.csv", "a") do |csv|
    csv << [sf, query, switches, nodes, time]
  end
end

# Run a single configuration
def run_cmd(sf, query, switches, nodes, perhost)
  # Unique run_id for output file
  run_id = "sf-#{sf}-query-#{query}-#{switches}-switch-#{nodes}-nodes-#{perhost}-perhost"
  puts "************ #{run_id} ***************"

  # Build command
  cmd = "./tools/scripts/mosaic/run.rb -w tpch#{query}/ -p /local/data/tpch#{sf}g-fpb/"\
	" -s #{switches} -n #{nodes} --perhost #{perhost} --query #{query} --dots"\
	" --nmask \".*hd[1-9]$\" -5 ../K3-Mosaic/tests/queries/tpch/query#{query}.sql 2>&1"\
	" 2>&1 | tee #{run_id}.out"

  # Run and extract time upon success. -1 for failure
  time = -1
  if system(cmd)
    time = `cat #{run_id}.out | grep time.*ms.*`.strip.split(" ")[-1].to_i
    puts "------------ Time: #{time} (ms) ---------------"
  else
    puts "------------ FAILED!  ---------------"
  end
  log_csv(sf, query, switches, nodes, time)
end

# Log headers
log_csv("sf", "query", "#switches", "#nodes", "time(ms)")

for query in ["1"]
  for sf in ["0.1", "1"]
    for node_count in [8, 32, 128]
      for switch_count in [2]
        datanodes_per_host = node_count / datanode_machine_count
        run_cmd(sf, query, switch_count, node_count, datanodes_per_host)
      end
    end
  end
end
