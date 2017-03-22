#!/bin/bash

set -e

pools="tpc1osd tpc2osd"
nobjs=10001

for pool in $pools; do
  for script in qa qb qc qd qe qf; do
    ./${script}.sh $pool $nobjs 40
    ./${script}.sh $pool $nobjs 40 --use-cls
  done
  ./qd-index.sh $pool $nobjs 40
  ./qf-proj.sh $pool $nobjs 40
done
