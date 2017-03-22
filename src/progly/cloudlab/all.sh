#!/bin/bash

set -e

pools="tpc2osd tpc2osd"
nobjs=150

for pool in $pools; do
  for script in qa qb qc qd qe qf; do
    ./${script}.sh $pool $nobjs 36
    ./${script}.sh $pool $nobjs 36 --use-cls
  done
  ./qd-index.sh $pool $nobjs 36
  ./qf-proj.sh $pool $nobjs 36
done
