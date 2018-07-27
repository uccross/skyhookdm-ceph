#!/bin/bash

set -ex
cp -r ~/data .
docker build -t cephbuilder/ceph:skyhook-test .
rm -r data
