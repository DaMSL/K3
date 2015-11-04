def main()
  if ARGF.argv.empty?
    puts "#{$PROGRAM_NAME} <log_file>"
    return 1
  end

  logfile = ARGF.argv[0]
  outfile = "/tmp/file.log"
  puts "Processing: #{logfile}"

  # Filter the input log -- concatenate into single lines for line-at-a-time inspection
  `grep -B 3 "Saved.*batches" #{logfile} | tr '\n' ' ' | sed "s/--/\\n/g" > /tmp/file.log`

  # Process each line, maintaining a seperate count for each table
  mux = {}
  File.open(outfile, "r") do |f|
    f.each_line do |line|
      l = line.strip.split(' ')
      tbl = l[0].split('/')[-1][0...-4]
      cnt = l[-2].to_i

      if not mux.has_key? tbl
        mux[tbl] = 0
      end
      mux[tbl] += cnt
   end
  end

  # Display final counts per table
  puts mux
end

if __FILE__ == $0
  main
end
