#!/usr/bin/env ruby
# Run a distributed test and compare to dbtoaster results

require 'optparse'
require 'fileutils'
require 'pathname'
require 'yaml'
require 'csv'
require 'open3'

$tables = {'sentinel' => 'st',
           'customer' => 'ci',
           'lineitem' => 'li',
           'orders'   => 'or',
           'part'     => 'pt',
           'partsupp' => 'ps',
           'supplier' => 'su'
          }

def create_files()
  # Compile the k3 file
  if $options[:compile]
    puts "Compiling mosaic_tpch_polyfile.k3"
    compile_path = File.join($k3_root_path, "tools", "scripts", "run", "compile.sh")
    clean_path = File.join($k3_root_path, "tools", "scripts", "run", "clean.sh")
    k3_path = File.join($k3_root_path, "examples", "loaders", "mosaic_tpch_polyfile.k3")
    `#{clean_path}`
    `#{compile_path} #{k3_path}`
  end
  out_path = $options[:output_path]

  FileUtils.mkdir_p(out_path) unless Dir.exists?(out_path)

  old_pwd = FileUtils.pwd

  # Change dir for prog output to go to the right place
  FileUtils.cd(out_path)

  num_batches = {}

  main_yaml = {
    'me'       => ["127.0.0.1", 30000],
    'batch_sz' => $options[:batch_size],
  }

  prog_path = File.join($k3_root_path, "__build", "A")

  # Loop over all tables
  $tables.each_key do |table|
    print "Handling table #{table}..."

    files =
      if table == "sentinel" then []
      else
        p = [$options[:input_path]] +
          ($options[:recurse] ? ['**'] : []) +
          ["#{table}*"]
        paths = []
        Dir.glob(File.join(*p)) do |f|
          if File.directory?(f) then next end
          basename = Pathname.new(f).basename.to_s
          # check against regex
          if basename =~ /#{$options[:regex]}/
            paths << f
          end
        end
        paths
      end

    yaml = main_yaml
    yaml['role']  = [{'i' => "go_#{table}"}]
    yaml['files'] = files.map { |s| {'path' => s} }
    yaml = yaml.to_yaml
    File.open('temp.yaml', 'w') { |f| f.puts(yaml) }

    `#{prog_path} -p temp.yaml | tee temp.out`

    out_file = table + ".out"
    FileUtils.mv('p' + out_file, out_file)
    
    # get number of batches
    if table == 'sentinel'
      num_batches[table] = 1
      puts "1 batch"
    else
      s = File.open("temp.out", 'r').read()
      num = s[/^Saved: (\d+) batches.$/m, 1].to_i
      puts "#{num} batch#{num == 1 ? '' : 'es'}"
      if num && num != '0' then
        num_batches[table] = num
      end
    end
  end

  # create order file
  yaml = main_yaml
  yaml['role']  = [{'i' => 'go_exact_mux'}]
  $tables.each do |table, short|
    yaml["num" + short] = num_batches[table]
  end
  yaml = yaml.to_yaml
  File.open('temp_mux.yaml', 'w') { |f| f.puts(yaml) }
  `#{prog_path} -p temp_mux.yaml | tee temp.out`

  # test
  if $options[:test]
    table_list = %w{sentinel customer lineitem orders part partsupp supplier}
    FileUtils.cp('out_order.csv', 'in_order.csv')
    yaml = main_yaml
    yaml['seqfiles'] = table_list.map {|t| {'seq' => [{'path' => "#{t}.out"}]} }
    yaml['role'] = [{'i' => 'loadseq'}]
    yaml = yaml.to_yaml
    File.open('temp_test.yaml', 'w') { |f| f.puts(yaml) }
    `#{prog_path} -p temp_test.yaml | tee temp.out`
  end

  # restore pwd
  FileUtils.cd(old_pwd)
end

$options = {
  :batch_size  => 10000,
  :input_path  => 'tools/ktrace/data/tpch',
  :output_path => './',
  :compile     => false,
  :recurse     => false,  # search recursively for input files
  :regex       => '.+',   # limit input files to specific regex patterns
  :test        => false
}
$script_path  = File.expand_path(File.dirname(__FILE__))
$k3_root_path = File.expand_path(File.join($script_path, "..", "..", ".."))

def main()
  usage = "Usage: #{$PROGRAM_NAME} options input_path"
  parser = OptionParser.new do |opts|
    opts.banner = usage
    opts.on("-b", "--batchsize NUM", "Size of batch") {|s| $options[:batch_size] = s.to_i}
    opts.on("-o", "--output PATH", "Output path") {|s| $options[:output_path] = s}
    opts.on("--compile", "Don't compile K3") { $options[:compile] = true }
    opts.on("-r", "--recurse", "Input path: recursively search for files") { $options[:recurse] = true }
    opts.on("-x", "--regex STR", "Regex string") { |s| $options[:regex] = s }
    opts.on("-t", "--test", "Test data loading") { $options[:test] = true }
  end
  parser.parse!
  unless ARGV.size == 1
    puts "Must have a source path"
    exit(1)
  end
  $options[:input_path] = File.expand_path(ARGV[0])

  create_files()
end

main
