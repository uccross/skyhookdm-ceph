#!/bin/bash
set -e

if [ -d ./data ]; then
  echo "Folder 'data' exists; skipping download task"
  exit 0
fi

mkdir -p data
pushd data

for i in $(seq 0 9); do
  curl -LO https://users.soe.ucsc.edu/~jlefevre/skyhookdb/testdata/obj000000$i.bin
done;
