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
runs="1 2 3 4 5 6"

q_ab_extended_prices=(91400.0 71000.0 1.0)
q_ab_selectivities=(1 10 100)

function run_query() {
    local cmd=$1
    selectivity=$2
    echo "begin function run_query with selectivity percent=" $2
    for rr in $runs; do
    echo "starting run"$rr
        cachetype="hot"
        if [ $rr -lt 4 ]; then
            cachetype="cold"
            for ((j=0; j<${nosds}; j++)); do
                echo "clearing cache on osd"$j
                ssh osd${j} sync
                ssh osd${j} "echo 1 | sudo tee /proc/sys/vm/drop_caches"
                ssh osd${j} sync
            done
        fi
        start=$(date --utc "+%s.%N")
        $cmd
        end=$(date --utc "+%s.%N")
        dur=0$(echo "$end - $start" | bc)      
        echo $cmd selectivity-pct=$selectivity cache=$cachetype start=$start end=$end run-${rr} $dur
        sleep 10
    done
}


cmdbase="run-query --num-objs ${nobjs} --pool ${pool} --wthreads ${worker_threads} --qdepth ${queue_depth} --quiet"

# setup for queries: a, b
for ((i=0; i<${#q_ab_extended_prices[@]}; i++)); do
    price=${q_ab_extended_prices[i]}
    selpct=${q_ab_selectivities[i]}
    cmd="${cmdbase} --extended-price ${price}"
    for qname in b; do
        run_query "$cmd --query ${qname}" "${selpct}"
        run_query "$cmd --query ${qname} --use-cls --dir fwd" "${selpct}"
        run_query "$cmd --query ${qname} --use-cls --dir bwd" "${selpct}"
        run_query "$cmd --query ${qname} --use-cls --dir rnd" "${selpct}"
    done
done

echo END at `date`
