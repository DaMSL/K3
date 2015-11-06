def main()
  if ARGF.argv.empty?
    puts "Usage: #{$PROGRAM_NAME} [list_of_msgcount_logs]"
    exit 1
  end

  global_sum = 0.0
  global_count = 0
  empty_files = 0
  total_files = ARGF.argv.length
  ARGF.argv.each do |path|
    stats = get_stats(path)
    if stats[:count] == 0
      empty_files += 1
    else
      puts "#{path}: #{stats[:sum] / stats[:count]}"
    end
    global_sum += stats[:sum]
    global_count += stats[:count]
  end
  puts "---------------"
  puts "Global Average: #{global_sum / global_count}. Empty files: #{empty_files} / #{total_files}"
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
