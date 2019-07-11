FROM ubuntu:bionic

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && \
    apt install -y --no-install-suggests ceph-mon ceph-osd && \
    rm -rf /var/lib/apt/lists/* /var/tmp/*
