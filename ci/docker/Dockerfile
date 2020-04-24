FROM ubuntu:bionic-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && \
    apt install -y curl gnupg && \
    curl https://dist.apache.org/repos/dist/dev/arrow/KEYS | apt-key add - && \
    printf "deb [arch=amd64] https://dl.bintray.com/apache/arrow/ubuntu/ bionic main\ndeb-src https://dl.bintray.com/apache/arrow/ubuntu/ bionic main\n" | tee /etc/apt/sources.list.d/apache-arrow.list
    apt install -y --no-install-suggests && \
        ceph-mon \
        ceph-osd \
        apt-transport-https \
        gnupg \
        libarrow-dev \
        libparquet-dev && \
    rm -rf /var/lib/apt/lists/* /var/tmp/*

COPY build/bin/cls_tabular build/bin/run-query /usr/bin/
