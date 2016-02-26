#! /usr/bin/ruby
require 'csv'

PROFILE = ARGF.argv.shift
HEAP_INTERVAL = 250

aggregate = Hash.new {}

CSV.foreach(PROFILE, {:headers => true}) do |row|
  key =[[row["query"], row["sf"], row["prof_type"]]]

  unless aggregate.member? key
    aggregate[key] = {}
  end

  job = row["job"]
  unless aggregate[key].member? job
    aggregate[key][job] = {}
  end

  peer_id = row["peer_id"]

  unless aggregate[key][job].member? peer_id
    aggregate[key][job][peer_id] = []
  end

  row["value"] ||= "0.0"

  aggregate[key][job][peer_id] << [row["start_time"].to_i + row["seq_num"].to_i * HEAP_INTERVAL , row["value"].to_f]
end

aggregate.each do |key, jobs|
  jobs.each do |job_id, peers|
    greatest_lower_bound = peers.values.collect(&:min).max.first
    least_upper_bound = peers.values.collect(&:max).min.first
    peers.each do |peer_id, seq|
      seq.reject! do |s|
        s.first < greatest_lower_bound || s.first > least_upper_bound
      end
      seq.collect! do |i|
        i[1]
      end
    end

    head, *tail = peers.values
    sum = head.zip(*tail).map { |z| z.compact.inject(&:+)}
    p (key + [sum.max])
  end
end
