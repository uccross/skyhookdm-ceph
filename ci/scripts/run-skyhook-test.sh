#!/bin/bash
set -e

# copy test data
cp -r data /data

# copy cls
mkdir -p /usr/lib/x86_64-linux-gnu/rados-classes/
cp -a build/lib/libcls_tabular.so* /usr/lib/x86_64-linux-gnu/rados-classes/

# launch cluster
ci/scripts/micro-osd.sh test

# copy conf to default location
cp test/ceph.conf .

# run test
build/bin/ceph_test_skyhook_query
