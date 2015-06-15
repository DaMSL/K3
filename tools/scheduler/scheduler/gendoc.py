HR1 = '\n\n===============================================================================\n'
HR2 = '-------------------------------------------------------------------------------\n'

html_header="""
{% extends "layout/base.html" %}

{% block content %}

<pre>
"""


html_footer="""
</pre>

{% endblock %}

{% block footer %}
  {% include "include/footer.html" %}
{% endblock %}
"""

header = False
endblock = False

with open('flaskweb.py', 'r') as f:
  src = f.read().split('\n')

text = open('static/rest.txt', 'w')
html = open('templates/rest.html', 'w')
endpoints = []

html.write(html_header)

for line in src:
  dowrite = False
  if line.startswith("@"):
    endpoints.append(line)
    continue
  if line.strip().startswith('#-----'):
    if header:
      text.write(HR2)
      html.write(HR2)
      for ep in endpoints:
        text.write(ep + '\n')
        html.write(ep + '\n')
      endpoints = []
    else:
      text.write(HR1)
      html.write(HR1)
    header = not header
    continue
  if header:
    text.write(line.strip()[1:] + '\n')
    html.write(line.strip()[1:] + '\n')

html.write(html_header)

text.close()
html.close()

