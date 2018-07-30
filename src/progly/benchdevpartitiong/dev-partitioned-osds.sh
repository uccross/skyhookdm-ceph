#!/bin/bash
set -e
#set -x

echo `date`

runquery="/usr/bin/run-query"
nosds=$1
objs="5000 7500 10000"
pool=tpc
nthreads="20"
runs="1 2 3 4"
extendedprice=71000
selectivity=10
echo "nosds="$nosds

function executequery() {
  local cmd="$runquery $1"
    for rr in $runs; do
      cachetype="hot"
      if [ $rr -lt 3 ]; then
        cachetype="cold"
        echo "clearing cache on osd"0
        ssh osd0 sync
        ssh osd0 "echo 1 | sudo tee /proc/sys/vm/drop_caches"
      fi
      start=$(date --utc "+%s.%N")
      $cmd
      end=$(date --utc "+%s.%N")
      dur=0$(echo "$end - $start" | bc)
      echo $cmd cache=$cachetype start=$start end=$end run-${rr} $dur
      sleep 10
    done
}


for nthread in ${nthreads}; do
  for nobjs in $objs; do
    args=" --query b --num-objs $nobjs --pool $pool --nthreads $nthread --quiet --extended-price ${extendedprice}"
    executequery "$args "
    executequery "$args --use-cls"
  done
done
echo `date`
