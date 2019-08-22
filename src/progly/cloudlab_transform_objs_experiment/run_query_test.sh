#!/bin/bash
set -e

usage() { echo "Usage: $0 [-n <num_objs>] [-p <pool>] [-r <runs>] [-o <osds>]" 1>&2; exit 1; }

nosds=1
worker_threads=12

while getopts ":n:p:o:r:" opt; do
    case "${opt}" in
        n)
            nobjs=${OPTARG}
            ;;
        p)
            pool=${OPTARG}
            ;;
        o)
            nosds=${OPTARG}
            ;;
        r)
            runs=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

queue_depth=$((${nosds}*12))

if [ -z "${nobjs}" ] || [ -z "${pool}" ] || [ -z "${runs}" ]; then
    usage
fi

function run_query() {
    local cmdbase=$@
    cmd="${cmdbase} --query flatbuf --table-name lineitem"
    echo "Command ran: ${cmd}"
    total_dur=0
    for ((i=0; i<${runs}; i++)); do
        for ((j=0; j<${nosds}; j++)); do
            echo "clearing cache on osd"$j
            ssh osd${j} sync
            ssh osd${j} "echo 1 | sudo tee /proc/sys/vm/drop_caches"
            ssh osd${j} sync
        done
        start=$(date --utc "+%s.%N")
        $cmd
        end=$(date --utc "+%s.%N")
        dur=0$(echo "$end - $start" | bc)      
        echo "run=$i start=$start end=$end duration=$dur"
        total_dur=$(echo "$total_dur + $dur" | bc)
    done
    avg_dur=$(echo "scale=9;$total_dur / $runs" | bc)
    echo "Total dur: ${total_dur}"
    echo "Average Runtime: ${avg_dur}"
}


cmdbase="bin/run-query --num-objs ${nobjs} --pool ${pool} --wthreads ${worker_threads} --qdepth ${queue_depth} --quiet"
run_query ${cmdbase}
