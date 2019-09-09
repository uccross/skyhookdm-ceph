#!/bin/bash
trap "kill 0" EXIT
set -e

SKYHOOKBUILD="/mnt/sda4/skyhookdm-ceph/build";
cd $SKYHOOKBUILD;

usage() { echo "Usage: $0 [-n <num_objs>] [-p <pool>] [-r <runs>] [-o <osds>] [-s <{lineitem,ncols100}]" 1>&2; exit 1; }

nosds=1
worker_threads=30

while getopts ":n:p:o:r:s:" opt; do
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
        s)
            schema=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

queue_depth=$((${nosds}*30))

echo $nobjs
echo $pool
echo $runs
echo $nosds
echo $schema

if [ -z "${nobjs}" ] || [ -z "${pool}" ] || [ -z "${runs}" ] || [ -z "${nosds}" ] || [ -z "${schema}" ]; then
    usage
fi

DATA_SCHEMA=""
if [ $schema == "lineitem" ]; then
  echo "using lineitem schema.."
  DATA_SCHEMA="\"0 4 1 0 ORDERKEY; 1 3 0 1 PARTKEY; 2 3 0 1 SUPPKEY;  3 4 1 0 LINENUMBER ; 4 12 0 1 QUANTITY ; 5 13 0 1 EXTENDEDPRICE ;  6 12 0 1 DISCOUNT ; 7 13 0 1 TAX ; 8 9 0 1 RETURNFLAG; 9 9 0 1 LINESTATUS;10 14 0 1 SHIPDATE;11 14 0 1 COMMITDATE ; 12 14 0 1 RECEIPTDATE; 13 15 0 1 SHIPINSTRUCT; 14 15 0 1 SHIPMODE;15 15 0 1 COMMENT\"";
else
  echo "using ncols100 schema.."
  DATA_SCHEMA="\"0 8 1 0 ATT0; 1 8 0 0 ATT1; 2 8 0 0 ATT2; 3 8 0 0 ATT3; 4 8 0 0 ATT4; 5 8 0 0 ATT5; 6 8 0 0 ATT6; 7 8 0 0 ATT7; 8 8 0 0 ATT8; 9 8 0 0 ATT9; 10 8 0 0 ATT10; 11 8 0 0 ATT11; 12 8 0 0 ATT12; 13 8 0 0 ATT13; 14 8 0 0 ATT14; 15 8 0 0 ATT15; 16 8 0 0 ATT16; 17 8 0 0 ATT17; 18 8 0 0 ATT18; 19 8 0 0 ATT19; 20 8 0 0 ATT20; 21 8 0 0 ATT21; 22 8 0 0 ATT22; 23 8 0 0 ATT23; 24 8 0 0 ATT24; 25 8 0 0 ATT25; 26 8 0 0 ATT26; 27 8 0 0 ATT27; 28 8 0 0 ATT28; 29 8 0 0 ATT29; 30 8 0 0 ATT30; 31 8 0 0 ATT31; 32 8 0 0 ATT32; 33 8 0 0 ATT33; 34 8 0 0 ATT34; 35 8 0 0 ATT35; 36 8 0 0 ATT36; 37 8 0 0 ATT37; 38 8 0 0 ATT38; 39 8 0 0 ATT39; 40 8 0 0 ATT40; 41 8 0 0 ATT41; 42 8 0 0 ATT42; 43 8 0 0 ATT43; 44 8 0 0 ATT44; 45 8 0 0 ATT45; 46 8 0 0 ATT46; 47 8 0 0 ATT47; 48 8 0 0 ATT48; 49 8 0 0 ATT49; 50 8 0 0 ATT50; 51 8 0 0 ATT51; 52 8 0 0 ATT52; 53 8 0 0 ATT53; 54 8 0 0 ATT54; 55 8 0 0 ATT55; 56 8 0 0 ATT56; 57 8 0 0 ATT57; 58 8 0 0 ATT58; 59 8 0 0 ATT59; 60 8 0 0 ATT60; 61 8 0 0 ATT61; 62 8 0 0 ATT62; 63 8 0 0 ATT63; 64 8 0 0 ATT64; 65 8 0 0 ATT65; 66 8 0 0 ATT66; 67 8 0 0 ATT67; 68 8 0 0 ATT68; 69 8 0 0 ATT69; 70 8 0 0 ATT70; 71 8 0 0 ATT71; 72 8 0 0 ATT72; 73 8 0 0 ATT73; 74 8 0 0 ATT74; 75 8 0 0 ATT75; 76 8 0 0 ATT76; 77 8 0 0 ATT77; 78 8 0 0 ATT78; 79 8 0 0 ATT79; 80 8 1 0 ATT80; 81 8 0 0 ATT81; 82 8 0 0 ATT82; 83 8 0 0 ATT83; 84 8 0 0 ATT84; 85 8 0 0 ATT85; 86 8 0 0 ATT86; 87 8 0 0 ATT87; 88 8 0 0 ATT88; 89 8 0 0 ATT89; 90 8 0 0 ATT90; 91 8 0 0 ATT91; 92 8 0 0 ATT92; 93 8 0 0 ATT93; 94 8 0 0 ATT94; 95 8 0 0 ATT95; 96 8 0 0 ATT96; 97 8 0 0 ATT97; 98 8 0 0 ATT98; 99 8 0 0 ATT99\"";
fi


function run_query() {
    local cmdbase=$@
    cmd="${cmdbase} --num-objs ${nobjs} --pool ${pool} --wthreads ${worker_threads} --qdepth ${queue_depth} --quiet"
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
        echo "run=$i, start=$start, end=$end, duration=$dur"
        total_dur=$(echo "$total_dur + $dur" | bc)
    done
    avg_dur=$(echo "scale=9;$total_dur / $runs" | bc)
    echo "Total Duration: ${total_dur}"
    echo "Average Runtime: ${avg_dur}"
}


#~ # SELECT * FROM lineitem  100% selectivity
params="--select \"*\"";
cmdbase="${SKYHOOKBUILD}/bin/run-query  ${params} --data-schema ${DATA_SCHEMA}";
run_query ${cmdbase}

# SELECT extendedprice>91350 FROM lineitem 1%
params="--select \"extendedprice,gt,91350\"";
cmdbase="${SKYHOOKBUILD}/bin/run-query  ${params} --data-schema ${DATA_SCHEMA}";
run_query ${cmdbase}

# SELECT extendedprice>91350 FROM lineitem 1%
params="--select \"extendedprice,gt,91350\"";
cmdbase="${SKYHOOKBUILD}/bin/run-query  ${params} --data-schema ${DATA_SCHEMA} --use-cls";
run_query ${cmdbase}

# SELECT extendedprice>91350 FROM lineitem 10%
params="--select \"extendedprice,gt,71000\"";
cmdbase="${SKYHOOKBUILD}/bin/run-query  ${params} --data-schema ${DATA_SCHEMA}";
run_query ${cmdbase}

# SELECT extendedprice>91350 FROM lineitem 10%
params="--select \"extendedprice,gt,71000\"";
cmdbase="${SKYHOOKBUILD}/bin/run-query  ${params} --data-schema ${DATA_SCHEMA} --use-cls";
run_query ${cmdbase}

# SELECT extendedprice>91350 FROM lineitem 100%
params="--select \"extendedprice,gt,0\"";
cmdbase="${SKYHOOKBUILD}/bin/run-query  ${params} --data-schema ${DATA_SCHEMA}";
run_query ${cmdbase}

# SELECT extendedprice>91350 FROM lineitem 100%
params="--select \"extendedprice,gt,0\"";
cmdbase="${SKYHOOKBUILD}/bin/run-query  ${params} --data-schema ${DATA_SCHEMA} --use-cls";
run_query ${cmdbase}




#~ cmdbase="${SKYHOOKBUILD}/bin/run-query --use-cls"
#~ run_query ${cmdbase}

#~ # SELECT att0 FROM lineitem
#~ cmdbase="${SKYHOOKBUILD}/bin/run-query --project-cols \"att0\" --data-schema ${DATA_SCHEMA}"
#~ run_query ${cmdbase}

#~ cmdbase="${SKYHOOKBUILD}/bin/run-query --project-cols \"att0\" --data-schema ${DATA_SCHEMA} --use-cls"
#~ run_query ${cmdbase}

#~ # SELECT att0,att1 FROM lineitem
#~ cmdbase="${SKYHOOKBUILD}/bin/run-query --project-cols \"att0, att1\" --data-schema ${DATA_SCHEMA}"
#~ run_query ${cmdbase}

#~ cmdbase="${SKYHOOKBUILD}/bin/run-query --project-cols \"att0, att1\" --data-schema ${DATA_SCHEMA} --use-cls"
#~ run_query ${cmdbase}

#~ # SELECT * FROM lineitem WHERE att0 < 10 (1% Selectivity)
#~ cmdbase="${SKYHOOKBUILD}/bin/run-query --data-schema ${DATA_SCHEMA} --select-preds \"att0,lt,104\""
#~ run_query ${cmdbase}

#~ cmdbase="${SKYHOOKBUILD}/bin/run-query --data-schema ${DATA_SCHEMA} --select-preds \"att0,lt,104\" --use-cls"
#~ run_query ${cmdbase}

#~ # SELECT * FROM lineitem WHERE att0 < 99 (10% Selectivity)
#~ cmdbase="${SKYHOOKBUILD}/bin/run-query --data-schema ${DATA_SCHEMA} --select-preds \"att0,lt,999\""
#~ run_query ${cmdbase}

#~ cmdbase="${SKYHOOKBUILD}/bin/run-query --data-schema ${DATA_SCHEMA} --select-preds \"att0,lt,999\" --use-cls"
#~ run_query ${cmdbase}

#~ # SELECT * FROM lineitem WHERE att0 < 999 (100% Selectivity)
#~ cmdbase="${SKYHOOKBUILD}/bin/run-query --data-schema ${DATA_SCHEMA} --select-preds \"att0,lt,9999\""
#~ run_query ${cmdbase}

#~ cmdbase="${SKYHOOKBUILD}/bin/run-query --data-schema ${DATA_SCHEMA} --select-preds \"att0,lt,9999\" --use-cls"
#~ run_query ${cmdbase}

lineitem_schema="\"0 4 1 0 ORDERKEY; 1 3 0 1 PARTKEY; 2 3 0 1 SUPPKEY  3 4 1 0 LINENUMBER ; 4 12 0 1 QUANTITY ; 5 13 0 1 EXTENDEDPRICE ;  6 12 0 1 DISCOUNT ; 7 13 0 1 TAX ; 8 9 0 1 RETURNFLAG; 9 9 0 1 LINESTATUS;10 14 0 1 SHIPDATE;11 14 0 1 COMMITDATE ; 12 14 0 1 RECEIPTDATE; 13 15 0 1 SHIPINSTRUCT;14 15 0 1 SHIPMODE;15 15 0 1 COMMENT\""
ncols100_schema="\"0 8 1 0 ATT0; 1 8 0 0 ATT1; 2 8 0 0 ATT2; 3 8 0 0 ATT3; 4 8 0 0 ATT4; 5 8 0 0 ATT5; 6 8 0 0 ATT6; 7 8 0 0 ATT7; 8 8 0 0 ATT8; 9 8 0 0 ATT9; 10 8 0 0 ATT10; 11 8 0 0 ATT11; 12 8 0 0 ATT12; 13 8 0 0 ATT13; 14 8 0 0 ATT14; 15 8 0 0 ATT15; 16 8 0 0 ATT16; 17 8 0 0 ATT17; 18 8 0 0 ATT18; 19 8 0 0 ATT19; 20 8 0 0 ATT20; 21 8 0 0 ATT21; 22 8 0 0 ATT22; 23 8 0 0 ATT23; 24 8 0 0 ATT24; 25 8 0 0 ATT25; 26 8 0 0 ATT26; 27 8 0 0 ATT27; 28 8 0 0 ATT28; 29 8 0 0 ATT29; 30 8 0 0 ATT30; 31 8 0 0 ATT31; 32 8 0 0 ATT32; 33 8 0 0 ATT33; 34 8 0 0 ATT34; 35 8 0 0 ATT35; 36 8 0 0 ATT36; 37 8 0 0 ATT37; 38 8 0 0 ATT38; 39 8 0 0 ATT39; 40 8 0 0 ATT40; 41 8 0 0 ATT41; 42 8 0 0 ATT42; 43 8 0 0 ATT43; 44 8 0 0 ATT44; 45 8 0 0 ATT45; 46 8 0 0 ATT46; 47 8 0 0 ATT47; 48 8 0 0 ATT48; 49 8 0 0 ATT49; 50 8 0 0 ATT50; 51 8 0 0 ATT51; 52 8 0 0 ATT52; 53 8 0 0 ATT53; 54 8 0 0 ATT54; 55 8 0 0 ATT55; 56 8 0 0 ATT56; 57 8 0 0 ATT57; 58 8 0 0 ATT58; 59 8 0 0 ATT59; 60 8 0 0 ATT60; 61 8 0 0 ATT61; 62 8 0 0 ATT62; 63 8 0 0 ATT63; 64 8 0 0 ATT64; 65 8 0 0 ATT65; 66 8 0 0 ATT66; 67 8 0 0 ATT67; 68 8 0 0 ATT68; 69 8 0 0 ATT69; 70 8 0 0 ATT70; 71 8 0 0 ATT71; 72 8 0 0 ATT72; 73 8 0 0 ATT73; 74 8 0 0 ATT74; 75 8 0 0 ATT75; 76 8 0 0 ATT76; 77 8 0 0 ATT77; 78 8 0 0 ATT78; 79 8 0 0 ATT79; 80 8 1 0 ATT80; 81 8 0 0 ATT81; 82 8 0 0 ATT82; 83 8 0 0 ATT83; 84 8 0 0 ATT84; 85 8 0 0 ATT85; 86 8 0 0 ATT86; 87 8 0 0 ATT87; 88 8 0 0 ATT88; 89 8 0 0 ATT89; 90 8 0 0 ATT90; 91 8 0 0 ATT91; 92 8 0 0 ATT92; 93 8 0 0 ATT93; 94 8 0 0 ATT94; 95 8 0 0 ATT95; 96 8 0 0 ATT96; 97 8 0 0 ATT97; 98 8 0 0 ATT98; 99 8 0 0 ATT99\""

wait

