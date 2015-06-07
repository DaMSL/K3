K3 Cloud Compiler Service
==========================

A self-contained ansible playbook for launching the K3 compiler service:

1. Edit the `hosts.ini` and `config.yml` files for your cluster.

2. Launch docker containers with:

```ansible-playbook launch-containers.yml -s -K -i hosts.ini```

3. Start the compiler service:

```ansible-playbook launch-service.yml -s -K -i hosts.ini```

4. Submit a compilation job:

```
ansible-playbook submit-job.yml \
     -e "compileargs='--blocksize -1 examples/sql/tpch/queries/k3/q1.k3 \
     -s -K -i hosts.init
```

5. Stop the service:

```ansible-playbook stop-service.yml -s -K -i hosts.init```

6. Stop the containers:

```ansible-playbook stop-containers.yml -s -K -i hosts.init```
