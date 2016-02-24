#!/usr/bin/env ruby
# Create the fpb files from the default dbgen files
#
#
# Stages:
# Note: We assume the dbgen files are already created and split into table.dddd (d=digit)
#   (running dbgen to produce the files takes too long)
#   Since we support at most 128 switches, the files must be divided into 128 chunks each.
# - Split the tbl files from dbgen into 1024 shards each
# - Compile the k3 program necessary to run.
# - For each table, for each shard, run the k3 program, producing an output file
# - Store the number of batches in each (table,shard)
# - Create mux files for the powerset of the tables, for each split of switches up to 128.

require 'optparse'
require 'fileutils'
require 'pathname'
require 'yaml'
require 'csv'
require 'open3'

$script_path  = File.expand_path(File.dirname(__FILE__))
$k3_root_path = File.expand_path(File.join($script_path, "..", "..", ".."))

$tables = {
  :sentinel => 'st',
  :customer => 'ci',
  :lineitem => 'li',
  :orders   => 'or',
  :part     => 'pt',
  :partsupp => 'ps',
  :supplier => 'su'
}

# numbers for table
$tags = {
  :customer => 1,
  :lineitem => 2,
  :orders   => 3,
  :part     => 4,
  :partsupp => 5,
  :supplier => 6
}

# split the original tpch files into 1024 files
def split_files(path)
  pwd = FileUtils.pwd
  puts "Splitting files in #{path}"
  $tables.each_key do |table|
    next if table == :sentinel
    puts "Handling #{table}..."
    table_path = File.join(path, table)
    FileUtils.rm_r(table_path) if File.exist?(table_path)
    FileUtils.mkdir_p(table_path) unless File.exist?(table_path)
    FileUtils.chdir(table_path)
    `split --number=l/128 --numeric-suffixes --suffix-length=4 ../#{table}.tbl #{table}`
  end
  FileUtils.chdir(pwd)
end

# First stage after generating tbl files using dbgen:
# split all the files in a path's subdirectories
def split_all_files(path)
  Dir.glob(File.join(path, '*')).sort.each do |file|
    split_files(file) if File.directory?(file)
  end
end

# Step 2, compiling the k3 file (if requested)
def compile_k3()
  puts "Compiling mosaic_tpch_polyfile.k3"
  compile_path = File.join($k3_root_path, "tools", "scripts", "run", "compile.sh")
  clean_path = File.join($k3_root_path, "tools", "scripts", "run", "clean.sh")
  k3_path = File.join($k3_root_path, "examples", "loaders", "mosaic_tpch_polyfile.k3")
  `#{clean_path}`
  `#{compile_path} #{k3_path}`
end

def make_yaml()
  { 'me' => ["127.0.0.1", 30000] }
end

# create the TPCH FPB files, returning a map of table -> 4-digit num -> batches
def create_data_files(in_path, out_path)
  num_batches = {}
  $tables.each_key { |t| num_batches[t] = {} }

  prog_path = File.join($k3_root_path, "__build", "A")

  # Loop over all tables
  $tables.each_key do |table|
    puts "Converting table #{table}..."

    out_table_path = File.join(out_path, table.to_s)
    FileUtils.mkdir_p(out_table_path) unless Dir.exists?(out_table_path)

    # create yaml
    yaml = make_yaml
    yaml['batch_sz'] = $options[:batch_size]
    yaml['role']  = [{'i' => "go_#{table}"}]
    yaml['files'] = []

    # We operate per file, getting its batch numbers
    if table == :sentinel
      File.open('temp.yaml', 'w') { |f| f.puts(yaml.to_yaml) }
      `#{prog_path} -p temp.yaml | tee temp.out`
      FileUtils.mv('psentinel.out', File.join(out_table_path, 'sentinel.out'))
      puts "1 batch"
    else
      # loop over every data file
      p = [in_path] + ($options[:recurse] ? ['**'] : []) + ["#{table}*"]
      Dir.glob(File.join(*p)).sort.each do |file|
        # check that it's a file
        next if File.directory?(file)
        basename = File.basename(file)
        # check against regex
        next unless basename =~ /#{$options[:regex]}/

        file_num = basename[/\D*(\d+)/, 1]

        yaml['files'] = [file].map { |s| {'path' => s} }
        File.open('temp.yaml', 'w') { |f| f.puts(yaml.to_yaml) }
        `#{prog_path} -p temp.yaml | tee temp.out`
        FileUtils.mv("p#{table}.out", File.join(out_table_path, "#{table}#{file_num}"))

        # get & save number of batches
        str = File.open("temp.out", 'r').read()
        v = str[/^Saved: (\d+) batches.$/m, 1].to_i
        puts "File #{basename}: #{v} batch#{v == 1 ? '' : 'es'}"
        num_batches[table][file_num.to_i] = v
      end
    end
  end
  num_batches
end

def mux_path(out_path, part, num_switches, suffix)
  File.join(out_path, num_switches.to_s, "mux_#{part}_#{num_switches}_#{suffix}.csv")
end

def mux_full_path(out_path, part, num_switches)
  mux_path(out_path, part, num_switches, 'full')
end

# accepts the map from create_data_files to create the order files
# @num_switches: how many partitions we'll actually make use of
def create_order_files(out_path, batch_map, num_switches)
  # partition map for this number of switches
  partitions = {}

  # create subdir
  dir = File.join(out_path, num_switches.to_s)
  FileUtils.mkdir_p(dir) unless File.exist? dir

  (0...num_switches).each {|i| partitions[i] = {}}
  batch_map.each do |table, map|
    map.each do |num, count|
      i = num % num_switches
      partitions[i][table] =
        partitions[i].has_key?(table) ? partitions[i][table] + count : 0
    end
  end

  # linearize and randomize each partition
  partitions.each do |num_part, tables|
    list = []
    tables.each do |table, count|
      list += [$tags[table]] * count
    end
    mux_path = mux_full_path(out_path, num_part, num_switches)
    File.open(mux_path, 'w') do |f|
      f.puts(list.shuffle)
      f.puts('0')
    end
  end

  # Now do the powerset for each partition: filter out the tables we don't need
  # Go over each possible length
  (1..$tags.length - 1).each do |t|
    # create all possible combinations of that length
    $tags.keys.combination(t).each do |combo|
      included_tags = combo.map {|k| $tags[k]}
      id = combo.join('_')
      partitions.each_key do |i|
        puts "----- Generating Partition #{i} with #{t} tags: #{combo.to_s} ------"
        # filter out the lines that don't match our included tags
        File.open(mux_path(out_path, i, num_switches, id), 'w') do |outf|
          File.open(mux_full_path(out_path, i, num_switches), 'r') do |inf|
            inf.each_line do |line|
              outf.puts line if included_tags.include? (line.to_i)
            end
          end
          outf.puts '0' # sentinel
        end
      end
    end
  end
end

# create files for a single scale factor
def create_sf_files(in_path, out_path)
  FileUtils.mkdir_p(out_path) unless File.exists? out_path
  batch_path = File.join(out_path, 'batch.yaml')

  unless $options[:mux_only]
    # create the fpbs
    batch_map = create_data_files(in_path, out_path)

    # save the batch map
    File.open(batch_path, 'w') {|f| f.write(batch_map.to_yaml)}
  end

  # load the batch map from the file
  if !File.exist?(batch_path)
    puts "#{batch_path} doesn't exist"
    exit(1)
  end
  batch_map = YAML.load_file(batch_path)

  # create the mux files
  mux_path = File.join(out_path, 'mux')
  FileUtils.mkdir_p(mux_path) unless File.exists? mux_path
  [1, 2, 4, 8, 16, 32, 64].each do |num_switches|
    create_order_files(mux_path, batch_map, num_switches)
  end
end

# create for all scale factors
def create_all_sf_files(in_path, out_path)
  Dir.glob(File.join(in_path, "*")).sort.each do |f|
    next unless File.directory? f
    basename = File.basename(f)
    create_sf_files(File.join(in_path, basename), File.join(out_path, basename))
  end
end

def run_load_test
  # test
  if $options[:test]
    table_list = %w{sentinel customer lineitem orders part partsupp supplier}
    FileUtils.cp('out_order.csv', 'in_order.csv')
    yaml = make_yaml
    yaml['batch_sz'] = $options[:batch_size]
    yaml['seqfiles'] = table_list.map {|t| {'seq' => [{'path' => "#{t}.out"}]} }
    yaml['role'] = [{'i' => 'loadseq'}]
    yaml = yaml.to_yaml
    File.open('temp_test.yaml', 'w') { |f| f.puts(yaml) }
    prog_path = File.join($k3_root_path, "__build", "A")
    `#{prog_path} -p temp_test.yaml | tee temp_test.out`
  end
end

$options = {
  :batch_size  => 10000,
  :input_path  => 'tools/ktrace/data/tpch',
  :output_path => './',
  :compile     => false,
  :mux_only    => false,
  :recurse     => true,   # search recursively for input files
  :regex       => '^\D+\d\d\d\d$',   # limit input files to specific regex patterns
  :test        => false,
  :split_tpch  => false,
  :action      => :none
}

def main()
  usage = "Usage: #{$PROGRAM_NAME} options input_path"
  parser = OptionParser.new do |opts|
    opts.banner = usage
    opts.on("-b", "--batchsize NUM", "Size of batch") {|s| $options[:batch_size] = s.to_i}
    opts.on("-o", "--output PATH", "Output path") {|s| $options[:output_path] = s}
    opts.on("--split-tpch", "Split TPCH .tbl files") { $options[:split_tpch] = true }
    opts.on("--compile", "Compile K3") { $options[:compile] = true }
    opts.on("--mux-only", "Mux file only") { $options[:mux_only] = true }
    opts.on("-1", '--one', 'Single set of SF files') { $options[:action] = :one }
    opts.on("-a", '--all', 'Many SF files') { $options[:action] = :all }
    opts.on("--no-recurse", "Input path: recursively search for files") { $options[:recurse] = false }
    opts.on("-x", "--regex STR", "Regex test files") { |s| $options[:regex] = s }
    opts.on("-t", "--test", "Test data loading") { $options[:test] = true }
  end
  parser.parse!
  unless ARGV.size == 1
    puts "Must have a source path"
    exit(1)
  end
  $options[:input_path] = File.expand_path(ARGV[0])

  compile_k3 if $options[:compile]

  split_all_files($options[:input_path]) if $options[:split_tpch]

  case $options[:action]
  when :all
    create_all_sf_files($options[:input_path], $options[:output_path])
  when :one
    create_sf_files($options[:input_path], $options[:output_path])
  end

  run_load_test if $options[:test]
end

main if __FILE__ == $0
