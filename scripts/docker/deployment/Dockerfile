FROM damsl/k3-debug
ADD startup.sh /
RUN chmod +x /startup.sh

RUN apt-get install -y ssh
ADD sshd_config /etc/ssh/
RUN ssh-keygen -A
ADD id_rsa /root/.ssh/
ADD id_rsa.pub /root/.ssh/
ADD config /root/.ssh/
RUN cat /root/.ssh/id_rsa.pub > /root/.ssh/authorized_keys

RUN apt-get install -y python-pip
RUN pip install pyyaml

CMD /startup.sh
