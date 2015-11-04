require 'yaml'

if ARGF.argv.length != 3
  puts "Usage: #{$PROGRAM_NAME} <full_mux> <num_partitions> <output_prefix>"
  exit 1
end

mux_file = ARGF.argv[0]
num_partitions = ARGF.argv[1].to_i
prefix = ARGF.argv[2]

# Keep a sequence of tags for each partition
partitioned_mux = {}
for i in (0...num_partitions)
  partitioned_mux[i] = []
end

# Keep a running index for each tag. 
indexes = {}

File.open(mux_file, "r") do |f|
  f.each_line do |tag|
    tag = tag.strip
    # Ignore sentinel tags
    if tag == "0"
      next
    end

    # A tag starts with index 0
    if not indexes.has_key?(tag)
      indexes[tag] = 0
    end
    curr_index = indexes[tag]
   
    # Round robin using index % num_partitions
    partition = curr_index % num_partitions
    if not partitioned_mux.has_key?(partition)
      partitioned_mux[partition] = []
    end
    partitioned_mux[partition] << tag
  
    # Update index for current tag
    indexes[tag] = curr_index + 1
  end
end

# Add a sentinel tag to each partition
for i in (0...num_partitions)
  partitioned_mux[i] << "0"
end

# Dump to files using naming convention
for (key, val) in partitioned_mux
  File.open("#{prefix}_#{key}_#{num_partitions}.csv", "w+") do |f|
      f.puts(val)
  end
end
