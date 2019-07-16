FROM ubuntu:bionic

ENV DEBIAN_FRONTEND=noninteractive

RUN apt install curl lsb-release && \
    curl https://dist.apache.org/repos/dist/dev/arrow/KEYS | apt-key add -

RUN tee /etc/apt/sources.list.d/apache-arrow.list <<APT_LINE
deb [arch=amd64] https://dl.bintray.com/apache/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/ $(lsb_release --codename --short) main
deb-src https://dl.bintray.com/apache/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/ $(lsb_release --codename --short) main
APT_LINE

RUN apt update && \
    apt install -y --no-install-suggests ceph-mon ceph-osd apt-transport-https gnupg libarrow-dev libparquet-dev && \
    rm -rf /var/lib/apt/lists/* /var/tmp/*
