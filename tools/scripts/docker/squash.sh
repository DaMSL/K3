docker save $1 | sudo ./docker-squash -t squash -verbose | docker load
