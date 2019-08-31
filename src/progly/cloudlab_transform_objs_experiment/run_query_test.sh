#!/bin/bash
set -e

if [ -z "$SKYHOOKBUILD" ];
then
    echo "error: Need to set environment variable SKYHOOKBUILD pointing to abosolute path of skyhook build dir"
    exit 1
fi

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
    cmd="${cmdbase} --conf ${SKYHOOKBUILD}/ceph.conf --query flatbuf --table-name lineitem --num-objs ${nobjs} --pool ${pool} --wthreads ${worker_threads} --qdepth ${queue_depth} --quiet"
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
        eval "$cmd"
        end=$(date --utc "+%s.%N")
        dur=0$(echo "$end - $start" | bc)
        echo "run=$i start=$start end=$end duration=$dur"
        total_dur=$(echo "$total_dur + $dur" | bc)
    done
    avg_dur=$(echo "scale=9;$total_dur / $runs" | bc)
    echo "Total dur: ${total_dur}"
    echo "Average Runtime: ${avg_dur}"
}


cmdbase="${SKYHOOKBUILD}/bin/run-query"
run_query ${cmdbase}

cmdbase="${SKYHOOKBUILD}/bin/run-query --use-cls"
run_query ${cmdbase}

cmdbase="${SKYHOOKBUILD}/bin/run-query --project-cols \"att0\" --data-schema \"0 8 0 0 ATT0 ; 1 12 0 0 ATT1 ; 2 15 0 0 ATT2 ;\""
run_query ${cmdbase}

cmdbase="${SKYHOOKBUILD}/bin/run-query --project-cols \"att0\" --data-schema \"0 8 0 0 ATT0 ; 1 12 0 0 ATT1 ; 2 15 0 0 ATT2 ;\" --use-cls"
run_query ${cmdbase}

cmdbase="${SKYHOOKBUILD}/bin/run-query --project-cols \"att0, att1\" --data-schema \"0 8 0 0 ATT0 ; 1 12 0 0 ATT1 ; 2 15 0 0 ATT2 ;\""
run_query ${cmdbase}

cmdbase="${SKYHOOKBUILD}/bin/run-query --project-cols \"att1, att1\" --data-schema \"0 8 0 0 ATT0 ; 1 12 0 0 ATT1 ; 2 15 0 0 ATT2 ;\" --use-cls"
run_query ${cmdbase}

cmdbase="${SKYHOOKBUILD}/bin/run-query --data-schema \"0 8 0 0 ATT0 ; 1 12 0 0 ATT1 ; 2 15 0 0 ATT2 ;\" --select-preds \"att0,lt,10\""
run_query ${cmdbase}

cmdbase="${SKYHOOKBUILD}/bin/run-query --data-schema \"0 8 0 0 ATT0 ; 1 12 0 0 ATT1 ; 2 15 0 0 ATT2 ;\" --select-preds \"att0,lt,10\" --use-cls"
run_query ${cmdbase}

cmdbase="${SKYHOOKBUILD}/bin/run-query --data-schema \"0 8 0 0 ATT0 ; 1 12 0 0 ATT1 ; 2 15 0 0 ATT2 ;\" --select-preds \"att0,lt,99\""
run_query ${cmdbase}

cmdbase="${SKYHOOKBUILD}/bin/run-query --data-schema \"0 8 0 0 ATT0 ; 1 12 0 0 ATT1 ; 2 15 0 0 ATT2 ;\" --select-preds \"att0,lt,99\" --use-cls"
run_query ${cmdbase}

cmdbase="${SKYHOOKBUILD}/bin/run-query --data-schema \"0 8 0 0 ATT0 ; 1 12 0 0 ATT1 ; 2 15 0 0 ATT2 ;\" --select-preds \"att0,lt,999\""
run_query ${cmdbase}

cmdbase="${SKYHOOKBUILD}/bin/run-query --data-schema \"0 8 0 0 ATT0 ; 1 12 0 0 ATT1 ; 2 15 0 0 ATT2 ;\" --select-preds \"att0,lt,999\" --use-cls"
run_query ${cmdbase}
