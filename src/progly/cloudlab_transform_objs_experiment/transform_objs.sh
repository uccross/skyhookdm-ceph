#!/bin/bash
set -e

usage() { echo "Usage: $0 [-n <num_objs>] [-f <transform_format>] [-p <pool>] [-o <osds>]" 1>&2; exit 1; }

nosds=1
worker_threads=12

while getopts ":n:f:p:o:" opt; do
    case "${opt}" in
        n)
            nobjs=${OPTARG}
            ;;
        f)
            format=${OPTARG}
            ;;
        p)
            pool=${OPTARG}
            ;;
        o)
            nosds=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

queue_depth=$((${nosds}*12))

if [ -z "${nobjs}" ] || [ -z "${format}" ] || [ -z "${pool}" ]; then
    usage
fi

function transform_objs() {
    local cmdbase=$@
    for ((j=0; j<${nosds}; j++)); do
        echo "clearing cache on osd"$j
        ssh osd${j} sync
        ssh osd${j} "echo 1 | sudo tee /proc/sys/vm/drop_caches"
        ssh osd${j} sync
    done
    cmd="${cmdbase} --query arrow --transform-db --transform-format-type ${format}"
    start=$(date --utc "+%s.%N")
    $cmd
    end=$(date --utc "+%s.%N")
    dur=0$(echo "$end - $start" | bc)      
    echo "Command ran: ${cmd}"
    echo "start=$start end=$end duration=$dur"
}

cmdbase="bin/run-query --num-objs ${nobjs} --pool ${pool} --wthreads ${worker_threads} --qdepth ${queue_depth} --quiet"
transform_objs ${cmdbase}
