#!/bin/bash
set -e
set -x

PATH=$PWD/bin:$PATH

pool=rbd
valrange=500000
numrows=10000000
objrows=10000
selectivities="0.0 0.1 1.0 5.0 10.0 25.0 50.0"
repeat=1
output=log.txt

reset=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --reset)
      reset=true
      ;;
    --output)
      output=$2
      shift
      ;;
    *)
      echo "invalid option: $1"
      exit 1
      ;;
  esac
  shift
done

# if there aren't any objects, then do datagen
numobjs=$(rados -p $pool ls | wc -l)
if [[ $numobjs -eq 0 ]]; then
  reset=true
fi

# purge the pool and regenerate the input data
if $reset; then
  rados purge $pool --yes-i-really-really-mean-it
  tabular-datagen --range-size $valrange --num-rows $numrows \
    --rows-per-obj $objrows --pool $pool
fi

rm -f $output
echo "name,instance,hotcache,selectivity,duration_ns" > $output

function record_run() {
  local name="$1"
  local instance="$2"
  local hotcache="$3"
  local selectivity="$4"
  IFS=';' read -ra RES <<< "$5"
  local duration_ns=${RES[0]}
  echo "${name},${instance},${hotcache},${selectivity},${duration_ns}" >> $output
}

function clear_cache() {
  ../src/stop.sh || true
  sync
  sync
  echo 3 | sudo tee /proc/sys/vm/drop_caches
  sync
  sync
  ../src/vstart.sh
  while true; do
    if ceph status | tee /dev/tty | grep -q HEALTH_OK; then
      break
    fi
    sleep 1
  done
  sleep 30
  ceph status
}

function run_exprs() {
  hotcache="$1"

  # if we are running with a hot cache, then do a basic run where we read all
  # the data. the osd/buffercache should be populated.
  if $hotcache; then
    tabular-scan --robot --range-size $valrange --num-rows $numrows \
      --rows-per-obj $objrows --pool $pool --selectivity 100.0
  fi

  for selectivity in $selectivities; do
    for instance in `seq 1 $repeat`; do
      # naive: clients reads each object in full and filters locally
      name="client"
      if ! $hotcache; then clear_cache; fi
      out=$(tabular-scan --robot --range-size $valrange --num-rows $numrows \
        --rows-per-obj $objrows --pool $pool --selectivity $selectivity)
      record_run $name $instance $hotcache $selectivity $out

      # object filtering basic: remotely read and filter all object data
      name="cls-all"
      if ! $hotcache; then clear_cache; fi
      out=$(tabular-scan --robot --range-size $valrange --num-rows $numrows \
        --rows-per-obj $objrows --pool $pool --selectivity $selectivity \
        --use-cls)
      record_run $name $instance $hotcache $selectivity $out

      # object filtering with index: avoid unnecessary object reads
      name="cls-index"
      if ! $hotcache; then clear_cache; fi
      out=$(tabular-scan --robot --range-size $valrange --num-rows $numrows \
        --rows-per-obj $objrows --pool $pool --selectivity $selectivity \
        --use-cls --use-index)
      record_run $name $instance $hotcache $selectivity $out

      # pg filtering basic: read and filter all object data at pg level
      name="pg-all"
      if ! $hotcache; then clear_cache; fi
      out=$(tabular-scan --robot --range-size $valrange --num-rows $numrows \
        --rows-per-obj $objrows --pool $pool --selectivity $selectivity \
        --use-pg)
      record_run $name $instance $hotcache $selectivity $out

      # pg filtering basic: read and filter all object data at pg level
      name="pg-index"
      if ! $hotcache; then clear_cache; fi
      out=$(tabular-scan --robot --range-size $valrange --num-rows $numrows \
        --rows-per-obj $objrows --pool $pool --selectivity $selectivity \
        --use-pg --use-index)
      record_run $name $instance $hotcache $selectivity $out
    done
  done
}

while true; do
  if ceph status | tee /dev/tty | grep -q HEALTH_OK; then
    if ! ceph status | tee /dev/tty | grep -q creating; then
      break
    fi
  fi
  sleep 1
done

run_exprs true
run_exprs false
