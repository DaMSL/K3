#! /usr/bin/ruby

def get_profile_total(profile_type, binary, profile)
  return `jeprof --text --#{profile_type} #{binary} #{profile} 2>/dev/null`[/^Total: ([0-9.]*)/, 1]
end

GROUP_ROOT = ARGV.shift

unless GROUP_ROOT
  p "usage: #{$0} path/to/experiment/root"
  exit
end

Dir.glob("#{GROUP_ROOT}/*/") do |f|
  next if f == "roles"

  binary = File.basename(f)[/_([^_]*)$/, 1]
  scale_factor = File.basename(f)[/^(.*?)_/, 1]
  profile_type = if f =~ /accum/ then "alloc_space" else "inuse_space" end
  job_id = File.basename(f)[/[^_]*_([0-9]*)/, 1]
  puts "job,query,sf,prof_type,peer_id,seq_num,value"
  Dir.glob("#{f}*/") do |m|
    host = File.basename(m)[/\.([0-9]*)_(.*)$/, 1]
    Dir.glob("#{m}*.t[0-9]*.heap") do |h|
      sequence_number = h[/\.t([0-9]*)\./, 1]
      total = get_profile_total(profile_type, "#{GROUP_ROOT}/#{binary}", h)
      puts "#{job_id},#{binary},#{scale_factor},#{profile_type},#{host},#{sequence_number},#{total}"
    end
  end
end
