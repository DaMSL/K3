# Copy the canonical settings to /etc/default/docker.io
cp /damsl/projects/k3/driver/scripts/docker-net/stubs/default/docker.io /etc/default/docker.io
# Restart the docker service
service docker.io restart
