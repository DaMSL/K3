#! /usr/bin/ruby

require 'thread'

def get_profile_total(profile_type, binary, profile)
  return `jeprof --text --#{profile_type} #{binary} #{profile} 2>/dev/null`[/^Total: ([0-9.]*)/, 1]
end

GROUP_ROOT = ARGV.shift
OUTFILE_STUB = ARGV.shift

unless GROUP_ROOT && OUTFILE_STUB
  p "usage: #{$0} path/to/experiment/root path/to/outfile_stub"
  exit
end

heap_files = Queue.new

NUM_WORKERS = 8

puts "job,query,sf,prof_type,peer_id,start_time,seq_num,value"
Dir.glob("#{GROUP_ROOT}/*/") do |f|
  next if f == "roles"

  binary = File.basename(f)[/_([^_]*)$/, 1]
  scale_factor = File.basename(f)[/^(.*?)_/, 1]
  profile_type = if f =~ /accum/ then "alloc_space" else "inuse_space" end
  Dir.glob("#{f}*/") do |m|
    host = File.basename(m)[/\.([0-9]*)_(.*)$/, 1]
    job_id = File.basename(m)[/[^_]*_([0-9]*)/, 1]
    Dir.glob("#{m}*.t[0-9]*.heap") do |h|
      sequence_number = h[/\.t([0-9]*)\./, 1]
      start_time = h[/K3\.(-?[0-9]*)/, 1]
      heap_files << [
        profile_type,
        "#{GROUP_ROOT}/#{binary}",
        h,
        "#{job_id},#{binary},#{scale_factor},#{profile_type},#{host},#{start_time},#{sequence_number},"
      ]
    end
  end
end

workers = []

NUM_WORKERS.times.each do |i|
  workers << Thread.new do
    File.open("#{OUTFILE_STUB}_#{i}.csv", "w") do |outf|
      begin
        while entry = heap_files.pop(true)
          pt, hf, h, s  = entry
          total = get_profile_total(pt, hf, h)
          total ||= 0.0
          outf << s + "#{total}\n"
        end
      rescue ThreadError
      end
    end
  end
end

workers.each(&:join)
