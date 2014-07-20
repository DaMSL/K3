import sys

# Constants
addr_prefix = "192.168.1."
uv_prefix = "/local_data/sf5/128l/uservisits_sf5_pp_128l/uservisits_sf5_pp_128l_"
rankings_prefix = "/local_data/sf5/128l/rankings_sf5_128l/rankings_sf5_128l_"
crawl_prefix = "/local_data/sf5/128l/crawl_sf5_128l/crawl_sf5_128l_"

# Configuration
num_files = 128
peers_per_machine = 16
start_addr = 34
start_port = 40000

# Script

def gen_peers(handle, num_machines):
  num_peers = num_machines * peers_per_machine
  curr_addr = start_addr
  curr_port = start_port
  curr_file = 0
  handle.write('  num_peers: "%s"' % str(num_peers))

  handle.write("\n\n")
  handle.write("k3_peers:\n")
  for i in range(num_machines):
    for j in range(peers_per_machine):
      addr = addr_prefix + str(curr_addr)
      port = str(curr_port)
      uv_file = uv_prefix + "%03d" % curr_file
      rankings_file = rankings_prefix + "%03d" % curr_file
      crawl_file = crawl_prefix + "%03d" % curr_file



      ip_line         = '    - ip: "%s"' % addr
      port_line       = '      k3_port: "%s"' % port
      bindings_line   = '      k3_bindings:'
      uv_line       = """        user_visits_file: '"%s"'""" % uv_file
      rankings_line = """        rankings_file: '"%s"'""" % rankings_file
      crawl_line    = """        crawl_file: '"%s"'""" % crawl_file

      handle.write(ip_line + "\n")
      handle.write(port_line + "\n")
      handle.write(bindings_line + "\n")
      handle.write(uv_line + "\n")
      handle.write(rankings_line + "\n")
      handle.write(crawl_line + "\n")
      curr_port = curr_port + 1
      curr_file = curr_file + 1

    curr_addr = curr_addr + 1

if __name__ == "__main__":
  if len(sys.argv) < 3:
    print("usage %s num_machines template_file out_file" % sys.argv[0])

  num_machines = int(sys.argv[1])
  template = sys.argv[2]
  out_path = sys.argv[3]

  with open(template, "r") as in_f:
    with open(out_path, "w") as out_f:
      out_f.write(in_f.read())
      gen_peers(out_f, num_machines)
