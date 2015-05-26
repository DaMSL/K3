#!/usr/bin/env ruby
# generate mosaic files and compile them

unless ARGV.size == 1
	puts "Input an argument"
	exit
end

# get directory of script
script_path = File.expand_path(File.dirname(__FILE__))

# split path components
target      = ARGV[0]
ext         = File.extname(target)
basename    = File.basename(target, ext)
lastpath    = File.split(File.split(target)[0])[1]
root_path   = File.join(script_path, "..", "..", "..", "..")
mosaic_path = File.join(root_path, "K3-Mosaic")

k3name =
	if match = basename.match(/query(.*)/)
		"#{lastpath}#{match.captures[0]}.k3"
	else
		"#{basename}.k3"
	end

# create the necessary files
`#{File.join(mosaic_path, "tests", "auto_test.py")} --no-interp -d -f #{target}`

# compile
`#{File.join(script_path, "..", "run", "compile_brew.sh")} --fstage cexclude=Optimize #{File.join("temp", k3name)}`
