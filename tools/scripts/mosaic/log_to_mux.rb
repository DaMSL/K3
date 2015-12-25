require 'yaml'

$tags = {
  'customer' => 1,
  'lineitem' => 2,
  'orders'   => 3,
  'part'     => 4,
  'partsupp' => 5,
  'supplier' => 6
}

def full_path(i)
  return "#{$output_folder}/mux_#{i}_#{$num_partitions}_full.csv"
end

def main()
  if ARGF.argv.length != 3
    puts "#{$PROGRAM_NAME} <log_file> <num_switches> <out_folder>"
    return 1
  end

  logfile = ARGF.argv[0]
  $num_partitions = ARGF.argv[1].to_i
  $output_folder = ARGF.argv[2]

  # Map from partition index to per-table batch counts
  partitions = {}
  for i in (0...$num_partitions)
    partitions[i] = {}
  end

  outfile = "/tmp/file.log"
  puts "Processing: #{logfile}"

  # Filter the input log -- concatenate into single lines for line-at-a-time inspection
  `grep -B 3 "Saved.*batches" #{logfile} | tr '\n' ' ' | sed "s/--/\\n/g" > /tmp/file.log`

  # Process each line, maintaining a seperate count for each table
  File.open(outfile, "r") do |f|
    f.each_line do |line|
      # Parse table, file index and number of batches.
      l = line.strip.split(' ')
      filename = l[0].split('/')[-1]
      tbl = filename[0...-4]
      index = filename[-4..-1].to_i
      count = l[-2].to_i

      # Find the per-table batch counts for this partition
      partition = index % $num_partitions
      mux = partitions[partition]

      if not mux.has_key? tbl
        mux[tbl] = 0
      end
      mux[tbl] += count
   end
  end


  # Display final counts per table
  for (i, mux) in partitions
    # Materialize a list of all the tags in this mux file
    l = []
    for (tbl, count) in mux
        if not $tags.has_key?(tbl)
          puts "INVALID TABLE: #{tbl}"
	  exit 1
	end
      for tag in (1..count).map {|x| $tags[tbl]}
        l << tag
      end
    end

    # Shuffle and write out to a file
    mux_path = full_path(i)
    File.open(mux_path, "w+") do |f|
      f.puts(l.shuffle)
      f.puts("0")
    end

  end

  # Now do the powerset for each partition... bit of a beast
  for t in (1..$tags.length - 1)
    combs = $tags.keys.combination(t)
    for c in combs
      included_tags = c.map {|k| $tags[k] }
      id = c.join("_")
      for (i, _) in partitions
        puts "----- Generating Partition #{i} with #{t} tags: #{c.to_s} ------"
        fp = full_path(i)
        out_path = "#{$output_folder}/mux_#{i}_#{$num_partitions}_#{id}.csv"
        File.open(out_path, "w") do |outf|
          File.open(fp, "r") do |f|
            f.each_line do |line|
              if included_tags.include? (line.to_i)
                outf.puts line
              end
            end
          outf.puts "0"
          end
        end
      end
    end
  end
end

if __FILE__ == $0
  main
end
