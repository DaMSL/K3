def main()
  if ARGF.argv.empty?
    puts "Usage: #{$PROGRAM_NAME} [list_of_msgcount_logs]"
    exit 1
  end

  global_sum = 0.0
  global_count = 0
  ARGF.argv.each do |path|
    stats = get_stats(path)
    puts "#{path}: #{stats[:sum] / stats[:count]}"
    global_sum += stats[:sum]
    global_count += stats[:count]
  end
  puts "Global Average: #{global_sum / global_count}"
end

def get_stats(path)
  sum = 0.0
  count = 0
  File.readlines(path).each do |line|
    l = line.split(",")[2..4].map {|x| x.strip.to_i }
    node_count = l[0] + l[1]
    if node_count > 1
      empty_count = l[0]
      ratio =  1.0 * empty_count / node_count
      sum += ratio
      count += 1
    end
  end
  return {:sum => sum, :count => count}
end

if __FILE__ == $0
  main
end
