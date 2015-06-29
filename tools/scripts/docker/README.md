K3 Dockerfiles
==========

Dockerfiles are updated for three images:

1. **k3-app** -- (~250MB) light-weight image to run a K3 program. Based on debian:jessie. It contains only the necessary dependency libraries.
2. **k3-compiler** -- (~2 GB) Image containing the GHC and GCC tool chains to compile a K3 program to binary. Based on debian:jessie
3. **k3-dev** (~2.5 GB)  -- Larger container with additional library and application support (e.g. clang, ruby, vim, and others). It is based on debian:sid

To build an image use the following command:

```
docker build -f k3-dev -t damsl/k3-dev:<your_tag> .
```

(Note: Docker build now has the -f option, so you don't have to call all docker files, "Dockerfile")

The image ```damsl/k3-dev:vanilla``` which is pushed to the repo contains the K3 source built with no options. Feel free to pull, use, & re-build K3 with whatever options necessary (and re-push with a new tag if needed).

The other scripts in here are left for legacy purposes.

Build Dependency Versions:
<pre>
  - GHC: 7.10.1
  - GCC: 4.9.2
  - Boost: 1.57
  - Mesos: 0.22.1</pre>
