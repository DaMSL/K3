HR1 = '\n\n===============================================================================\n'
HR2 = '-------------------------------------------------------------------------------\n'

header = False
endblock = False
with open('flaskweb.py', 'r') as f:
  src = f.read().split('\n')

with open('rest.txt', 'w') as api:
  endpoints = []
  for line in src:
    dowrite = False
    if line.startswith("@"):
      endpoints.append(line)
      continue
    if line.strip().startswith('#-----'):
      if header:
        api.write(HR2)
        for ep in endpoints:
          api.write(ep + '\n')
        endpoints = []
      else:
        api.write(HR1)
      header = not header
      continue
    if header:
      api.write(line.strip()[1:] + '\n')