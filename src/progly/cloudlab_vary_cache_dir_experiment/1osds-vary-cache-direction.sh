#!/bin/bash
set -e
#set -x

# This script will run a hot cache test with differnt object number orders indicated
# by direction as follows.
# --dir fwd|bwd|rnd

echo START at `date`

nosds=$1
queue_depth=$((${nosds}*12))
nobjs=10000
worker_threads=10
pool=tpc

q_ab_extended_prices=(91400.0 71000.0 1.0)
q_ab_selectivities=(1 10 100)

function clear_cache() {
    local n=$1
    for ((j=0; j<n; j++)); do
        echo "clearing cache on osd"$j
        ssh osd${j} sync
        ssh osd${j} "echo 1 | sudo tee /proc/sys/vm/drop_caches"
        ssh osd${j} sync
    done
}

function run_query() {
    local cmd=$1
    local selectivity=$2
    local direction=$3
    local cachetype=$4
    local run=$5
    start=$(date --utc "+%s.%N")
    $cmd
    end=$(date --utc "+%s.%N")
    dur=0$(echo "$end - $start" | bc)
    echo $cmd selectivity-pct=$selectivity cache=$cachetype dir=$direction start=$start end=$end time=$dur run-$run $dur
    sleep 5
}

# desired output line:
# run-query --num-objs 10000 --pool tpc --wthreads 10 --qdepth 12 --quiet --extended-price 91400.0 --query b --use-cls --dir rnd selectivity-pct=1 cache=cold dir=rnd start=1494068181.350015739 end=1494068466.305856274 run-1 0284.955840535

cmdbase="run-query --num-objs ${nobjs} --pool ${pool} --wthreads ${worker_threads} --qdepth ${queue_depth} --quiet"

run_three_times() {
    for i in `seq 1 3`; do

        # client-side
        direction=fwd
        clear_cache "${nosds}"
        run_query "$cmd --dir ${direction} " "${selpct}" "${direction}" "cold" "${i}"
        run_query "$cmd --dir ${direction} " "${selpct}" "${direction}" "hot" "${i}"

        direction=rnd
        clear_cache "${nosds}"
        run_query "$cmd --dir ${direction} " "${selpct}" "${direction}" "cold" "${i}"
        run_query "$cmd --dir ${direction} " "${selpct}" "${direction}" "hot" "${i}"

        direction=fwd
        clear_cache "${nosds}"
        run_query "$cmd --dir ${direction} " "${selpct}" "${direction}" "cold" "${i}"
        direction=bwd
        run_query "$cmd --dir ${direction} " "${selpct}" "${direction}" "hot" "${i}"

        # server-side
        direction=fwd
        clear_cache "${nosds}"
        run_query "$cmd --dir ${direction} --use-cls " "${selpct}" "${direction}" "cold" "${i}"
        run_query "$cmd --dir ${direction} --use-cls " "${selpct}" "${direction}" "hot" "${i}"

        direction=rnd
        clear_cache "${nosds}"
        run_query "$cmd --dir ${direction} --use-cls " "${selpct}" "${direction}" "cold" "${i}"
        run_query "$cmd --dir ${direction} --use-cls " "${selpct}" "${direction}" "hot" "${i}"

        direction=fwd
        clear_cache "${nosds}"
        run_query "$cmd --dir ${direction} --use-cls " "${selpct}" "${direction}" "cold" "${i}"
        direction=bwd
        run_query "$cmd --dir ${direction} --use-cls " "${selpct}" "${direction}" "hot" "${i}"
    done
}

# setup for queries: a, b
#for ((k=0; k<${#q_ab_extended_prices[@]}; k++)); do
#    price=${q_ab_extended_prices[k]}
#    selpct=${q_ab_selectivities[k]}
#    cmd="${cmdbase} --query b --extended-price ${price} "
#    run_three_times "${cmd}" "${selpct}"
#done

# setup for query d, no index needed
selpct="unique"
cmd="${cmdbase} --query d  --order-key 5 --line-number 3 "
run_three_times "${cmd}" "${selpct}"

echo END at `date`
