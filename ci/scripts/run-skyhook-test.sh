#!/bin/bash
set -e

# copy test data
cp -r data /data

# launch cluster
cd build/
MON=1 OSD=1 ../src/vstart.sh -d -n -x

# run test
bin/ceph_test_skyhook_query
