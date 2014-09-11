FROM damsl/k3-debug
ADD startup.sh /
RUN chmod +x /startup.sh
ADD sshd_config /etc/ssh/
RUN apt-get install -y ssh
RUN ssh-keygen -A
ADD id_rsa /root/.ssh/
ADD id_rsa.pub /root/.ssh/
ADD config /root/.ssh/
RUN cat /root/.ssh/id_rsa.pub > /root/.ssh/authorized_keys
CMD /startup.sh
