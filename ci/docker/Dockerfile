FROM ceph/daemon:master-6373c6a-luminous-ubuntu-16.04-x86_64

ADD data /data
ADD micro-osd.sh /bin/micro-osd.sh
WORKDIR /root
ENTRYPOINT ["/bin/bash"]
