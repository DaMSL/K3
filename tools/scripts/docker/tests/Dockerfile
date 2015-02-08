FROM damsl/k3-vanilla
RUN apt-get install -y postgresql postgresql-contrib
RUN mkdir /results
RUN mkdir -p /local/data/amplab/1024/rankings/
RUN mkdir -p /local/data/amplab/1024/uservisits/
ADD rankings0000 /local/data/amplab/1024/rankings/
ADD rankings0001 /local/data/amplab/1024/rankings/
ADD uservisits0000 /local/data/amplab/1024/uservisits/
ADD uservisits0001 /local/data/amplab/1024/uservisits/
