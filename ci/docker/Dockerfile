FROM ceph/daemon:v3.0.7-stable-3.0-kraken-ubuntu-16.04-x86_64

ADD data /data
ADD micro-osd.sh /bin/micro-osd.sh
WORKDIR /root
ENTRYPOINT ["/bin/bash"]
