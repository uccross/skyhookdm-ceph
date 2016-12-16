#!/bin/bash
set -e
set -x

PATH=$PWD/bin:$PATH

pool=rbd
output=log.txt

# 1000 8M objects
valrange=10000000000
numrows=1000000000
objrows=1000000
selectivities="0.0 0.1 1.0 5.0 10.0 25.0 50.0 75.0 100.0"
repeat=1

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
    --rows-per-obj $objrows --pool $pool --range-part
fi

rm -f $output
echo "name,instance,hotcache,selectivity,duration_ns,num_rows,filtered_rows,num_objs,objects_read" > $output

function record_run() {
  local name="$1"
  local instance="$2"
  local hotcache="$3"
  local selectivity="$4"
  IFS=';' read -ra RES <<< "$5"
  local duration_ns=${RES[0]}
  local num_rows=${RES[1]}
  local filtered_rows=${RES[2]}
  local num_objs=${RES[3]}
  local objects_read=${RES[4]}
  echo "${name},${instance},${hotcache},${selectivity},${duration_ns},${num_rows},${filtered_rows},${num_objs},${objects_read}" >> $output
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
      # baseline 1: naive: clients reads each object in full and filters locally
      name="client-vanilla"
      if ! $hotcache; then clear_cache; fi
      out=$(tabular-scan --robot --range-size $valrange --num-rows $numrows \
        --rows-per-obj $objrows --pool $pool --selectivity $selectivity)
      record_run $name $instance $hotcache $selectivity $out

      # baseline 2: same as (1) but bulk read goes through object class
      name="client-cls"
      if ! $hotcache; then clear_cache; fi
      out=$(tabular-scan --robot --range-size $valrange --num-rows $numrows \
        --rows-per-obj $objrows --pool $pool --selectivity $selectivity \
        --use-cls --cls-read)
      record_run $name $instance $hotcache $selectivity $out

      # object filtering basic 1: remotely read and filter all object data
      name="cls-basic"
      if ! $hotcache; then clear_cache; fi
      out=$(tabular-scan --robot --range-size $valrange --num-rows $numrows \
        --rows-per-obj $objrows --pool $pool --selectivity $selectivity \
        --use-cls --cls-naive)
      record_run $name $instance $hotcache $selectivity $out

      # object filtering basic 2: same as 1 but pre-allocates output buffer
      name="cls-basic-prealloc"
      if ! $hotcache; then clear_cache; fi
      out=$(tabular-scan --robot --range-size $valrange --num-rows $numrows \
        --rows-per-obj $objrows --pool $pool --selectivity $selectivity \
        --use-cls --cls-naive --cls-prealloc)
      record_run $name $instance $hotcache $selectivity $out

      # object filtering basic 2: same as 1 but pre-allocates output buffer
      name="cls-staged"
      if ! $hotcache; then clear_cache; fi
      out=$(tabular-scan --robot --range-size $valrange --num-rows $numrows \
        --rows-per-obj $objrows --pool $pool --selectivity $selectivity \
        --use-cls --cls-prealloc)
      record_run $name $instance $hotcache $selectivity $out

      # object filtering with index: avoid unnecessary object reads
      name="cls-index"
      if ! $hotcache; then clear_cache; fi
      out=$(tabular-scan --robot --range-size $valrange --num-rows $numrows \
        --rows-per-obj $objrows --pool $pool --selectivity $selectivity \
        --use-cls --use-index)
      record_run $name $instance $hotcache $selectivity $out

#      # pg filtering basic: read and filter all object data at pg level
#      name="pg-all"
#      if ! $hotcache; then clear_cache; fi
#      out=$(tabular-scan --robot --range-size $valrange --num-rows $numrows \
#        --rows-per-obj $objrows --pool $pool --selectivity $selectivity \
#        --use-pg)
#      record_run $name $instance $hotcache $selectivity $out
#
#      # pg filtering basic: read and filter all object data at pg level
#      name="pg-index"
#      if ! $hotcache; then clear_cache; fi
#      out=$(tabular-scan --robot --range-size $valrange --num-rows $numrows \
#        --rows-per-obj $objrows --pool $pool --selectivity $selectivity \
#        --use-pg --use-index)
#      record_run $name $instance $hotcache $selectivity $out
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
