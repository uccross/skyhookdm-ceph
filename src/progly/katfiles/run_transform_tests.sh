#!/bin/bash

nosds=$1
num_objs=$2
poolname=$3
actually_do_it=$4
num_write_groups=$5
worker_threads=24
queue_depth=24
format="arrow"
export PROJECT_COLS_NCOLS100="\"att0\""
export DATA_SCHEMA_NCOLS100="\"0 8 1 0 ATT0; 1 8 0 0 ATT1; 2 8 0 0 ATT2; 3 8 0 0 ATT3; 4 8 0 0 ATT4; 5 8 0 0 ATT5; 6 8 0 0 ATT6; 7 8 0 0 ATT7; 8 8 0 0 ATT8; 9 8 0 0 ATT9; 10 8 0 0 ATT10; 11 8 0 0 ATT11; 12 8 0 0 ATT12; 13 8 0 0 ATT13; 14 8 0 0 ATT14; 15 8 0 0 ATT15; 16 8 0 0 ATT16; 17 8 0 0 ATT17; 18 8 0 0 ATT18; 19 8 0 0 ATT19; 20 8 0 0 ATT20; 21 8 0 0 ATT21; 22 8 0 0 ATT22; 23 8 0 0 ATT23; 24 8 0 0 ATT24; 25 8 0 0 ATT25; 26 8 0 0 ATT26; 27 8 0 0 ATT27; 28 8 0 0 ATT28; 29 8 0 0 ATT29; 30 8 0 0 ATT30; 31 8 0 0 ATT31; 32 8 0 0 ATT32; 33 8 0 0 ATT33; 34 8 0 0 ATT34; 35 8 0 0 ATT35; 36 8 0 0 ATT36; 37 8 0 0 ATT37; 38 8 0 0 ATT38; 39 8 0 0 ATT39; 40 8 0 0 ATT40; 41 8 0 0 ATT41; 42 8 0 0 ATT42; 43 8 0 0 ATT43; 44 8 0 0 ATT44; 45 8 0 0 ATT45; 46 8 0 0 ATT46; 47 8 0 0 ATT47; 48 8 0 0 ATT48; 49 8 0 0 ATT49; 50 8 0 0 ATT50; 51 8 0 0 ATT51; 52 8 0 0 ATT52; 53 8 0 0 ATT53; 54 8 0 0 ATT54; 55 8 0 0 ATT55; 56 8 0 0 ATT56; 57 8 0 0 ATT57; 58 8 0 0 ATT58; 59 8 0 0 ATT59; 60 8 0 0 ATT60; 61 8 0 0 ATT61; 62 8 0 0 ATT62; 63 8 0 0 ATT63; 64 8 0 0 ATT64; 65 8 0 0 ATT65; 66 8 0 0 ATT66; 67 8 0 0 ATT67; 68 8 0 0 ATT68; 69 8 0 0 ATT69; 70 8 0 0 ATT70; 71 8 0 0 ATT71; 72 8 0 0 ATT72; 73 8 0 0 ATT73; 74 8 0 0 ATT74; 75 8 0 0 ATT75; 76 8 0 0 ATT76; 77 8 0 0 ATT77; 78 8 0 0 ATT78; 79 8 0 0 ATT79; 80 8 1 0 ATT80; 81 8 0 0 ATT81; 82 8 0 0 ATT82; 83 8 0 0 ATT83; 84 8 0 0 ATT84; 85 8 0 0 ATT85; 86 8 0 0 ATT86; 87 8 0 0 ATT87; 88 8 0 0 ATT88; 89 8 0 0 ATT89; 90 8 0 0 ATT90; 91 8 0 0 ATT91; 92 8 0 0 ATT92; 93 8 0 0 ATT93; 94 8 0 0 ATT94; 95 8 0 0 ATT95; 96 8 0 0 ATT96; 97 8 0 0 ATT97; 98 8 0 0 ATT98; 99 8 0 0 ATT99\""
export PROJECT_COLS_LINEITEM="\"orderkey\""
export DATA_SCHEMA_LINEITEM="\"0 3 1 0 ORDERKEY; 1 3 0 1 PARTKEY; 2 3 0 1 SUPPKEY; 3 3 1 0 LINENUMBER; 4 12 0 1 QUANTITY; 5 13 0 1 EXTENDEDPRICE; 6 12 0 1 DISCOUNT; 7 13 0 1 TAX; 8 9 0 1 RETURNFLAG; 9 9 0 1 LINESTATUS; 10 14 0 1 SHIPDATE; 11 14 0 1 COMMITDATE; 12 14 0 1 RECEIPTDATE; 13 15 0 1 SHIPINSTRUCT; 14 15 0 1 SHIPMODE; 15 15 0 1 COMMENT\""

# for 100 objects of size 10MB each, use num_merge_objs=25
# bc 256MB max obj size, so merged objs will be about 250MB each.
# for 100 objects of size 100MB each, use num_merge_objs=50
# bc 256MB max obj size, so merged objs will be about 200MB each.
num_merge_objs=$6
num_src_objs_per_merge=$7

test_id=${8} # a string to add to the output files.
test_name=${9}
num_megabytes=${10}

echo "Running run_transform_tests.sh with parameters:"
echo "nosds                  is $nosds"
echo "num_objs               is $num_objs"
echo "poolname               is $poolname"
echo "actually_do_it         is $actually_do_it"
echo "num_write_groups       is $num_write_groups"
echo "num_merge_objs         is $num_merge_objs"
echo "num_src_objs_per_merge is $num_src_objs_per_merge"
echo "test_id                is $test_id"
echo "test_name              is $test_name"
echo "num_megabytes          is $num_megabytes"
sleep 5s;

total_num_objs=$(( num_objs*num_write_groups ))
fbwriter_filename="fbmeta.Skyhook.v2.SFT_FLATBUF_FLEX_ROW.${test_name}_${num_megabytes}MB.0.1-1"

# remove any existing objects
#for i in `rados -p $poolname ls`; do echo $i; rados -p $poolname rm $i; done
rados rmpool $poolname $poolname --yes-i-really-really-mean-it ;
# make the pool
rados mkpool $poolname ;
sleep 10;

# clear the caches
for ((j=0; j<${nosds}; j++)); do
  echo "clearing cache on osd"$j
  ssh osd${j} sync
  ssh osd${j} "echo 1 | sudo tee /proc/sys/vm/drop_caches"
  ssh osd${j} sync
done

start=$(date --utc "+%s.%N")
# ==================================================================== #
# write the fbxrows from disk to ceph

writetoceph1_time_start=$(date --utc "+%s.%N")
for ((group_id=0; group_id<${num_write_groups}; group_id++)); do
  cmd1="sudo python rados_put_parallel.py $num_objs $poolname ./$fbwriter_filename $actually_do_it $group_id"
  eval "$cmd1"
done;
writetoceph1_time_end=$(date --utc "+%s.%N")
writetoceph1_time_dur=$(echo "$writetoceph1_time_end - $writetoceph1_time_start" | bc)
sleep 10;

echo "Command ran: ${cmd1}" >> ${HOME}/writetoceph1_time_results_$test_id.txt
echo "writetoceph1_time_start=$writetoceph1_time_start writetoceph1_time_end=$writetoceph1_time_end writetoceph1_time_duration=$writetoceph1_time_dur" >> ${HOME}/writetoceph1_time_results_$test_id.txt

# ==================================================================== #
# transform the fbxrows into arrow for entire table

if [ $test_name == "ncols100" ]; then
  cmd3="sudo bin/run-query --num-objs ${total_num_objs} --pool ${poolname} --wthreads ${worker_threads} --qdepth ${queue_depth} --transform-db --transform-format-type ${format} --data-schema ${DATA_SCHEMA_NCOLS100}"
else
  cmd3="sudo bin/run-query --num-objs ${total_num_objs} --pool ${poolname} --wthreads ${worker_threads} --qdepth ${queue_depth} --transform-db --transform-format-type ${format} --data-schema ${DATA_SCHEMA_LINEITEM}"
fi
sleep 10;

local_xform_time_start=$(date --utc "+%s.%N")
eval "$cmd3"
local_xform_time_end=$(date --utc "+%s.%N")
local_xform_time_dur=$(echo "$local_xform_time_end - $local_xform_time_start" | bc)
sleep 10;

echo "Command ran: ${cmd3}" >> ${HOME}/local_xform_time_results_$test_id.txt
echo "local_xform_time_start=$local_xform_time_start local_xform_time_end=$local_xform_time_end local_xform_time_duration=$local_xform_time_dur" >> ${HOME}/local_xform_time_results_$test_id.txt

# ==================================================================== #
# prepare for next phase

# ------------------------------------------ #
# remove any existing objects
#for i in `rados -p $poolname ls`; do echo $i; rados -p $poolname rm $i; done
rados rmpool $poolname $poolname --yes-i-really-really-mean-it ;
# make the pool
rados mkpool $poolname ;
sleep 10;

# ------------------------------------------ #
# write the fbxrows from disk to ceph
writetoceph2_time_start=$(date --utc "+%s.%N")
for ((group_id=0; group_id<${num_write_groups}; group_id++)); do
  cmd1="sudo python rados_put_parallel.py $num_objs $poolname ./$fbwriter_filename $actually_do_it $group_id"
  eval "$cmd1"
done;
writetoceph2_time_end=$(date --utc "+%s.%N")
writetoceph2_time_dur=$(echo "$writetoceph2_time_end - $writetoceph2_time_start" | bc)
sleep 10;

echo "Command ran: ${cmd1}" >> ${HOME}/writetoceph2_time_results_$test_id.txt
echo "writetoceph2_time_start=$writetoceph2_time_start writetoceph2_time_end=$writetoceph2_time_end writetoceph2_time_duration=$writetoceph2_time_dur" >> ${HOME}/writetoceph2_time_results_$test_id.txt

# ==================================================================== #
# do transforms with column project

# ------------------------------------------ #
# transform the fbxrows into arrow and project one column (do not time)
if [ $test_name == "ncols100" ]; then
  cmd4="sudo bin/run-query --num-objs ${total_num_objs} --pool ${poolname} --wthreads ${worker_threads} --qdepth ${queue_depth}  --transform-db --transform-format-type ${format} --data-schema ${DATA_SCHEMA_NCOLS100} --project-cols ${PROJECT_COLS_NCOLS100}"
else
  cmd4="sudo bin/run-query --num-objs ${total_num_objs} --pool ${poolname} --wthreads ${worker_threads} --qdepth ${queue_depth}  --transform-db --transform-format-type ${format} --data-schema ${DATA_SCHEMA_LINEITEM} --project-cols ${PROJECT_COLS_LINEITEM}"
fi
eval "$cmd4"
sleep 10;

# ==================================================================== #
# copy from merge
#
# num_merge_objs = number of merge objects to create
# num_src_objs_per_merge = number of source objects per merge object

copyfromappend_merge_time_start=$(date --utc "+%s.%N")
cmd5="python parallel_merges.py ${num_merge_objs} ${poolname} ${num_src_objs_per_merge} \"copyfrom\""
eval "$cmd5"
copyfromappend_merge_time_end=$(date --utc "+%s.%N")
copyfromappend_merge_time_dur=$(echo "$copyfromappend_merge_time_end - $copyfromappend_merge_time_start" | bc)
echo "Command ran: ${cmd5}" >> ${HOME}/copyfromappend_merge_time_results_$test_id.txt
echo "copyfromappend_merge_time_start=$copyfromappend_merge_time_start copyfromappend_merge_time_end=$copyfromappend_merge_time_end copyfromappend_merge_time_duration=$copyfromappend_merge_time_dur" >> ${HOME}/copyfromappend_merge_time_results_$test_id.txt
sleep 10;

# ==================================================================== #
# client merge
#
# num_merge_objs = number of merge objects to create
# num_src_objs_per_merge = number of source objects per merge object

clientmerge_time_start=$(date --utc "+%s.%N")
cmd6="python parallel_merges.py ${num_merge_objs} ${poolname} ${num_src_objs_per_merge} \"client\""
eval "$cmd6"
clientmerge_time_end=$(date --utc "+%s.%N")
clientmerge_time_dur=$(echo "$clientmerge_time_end - $clientmerge_time_start" | bc)
echo "Command ran: ${cmd6}" >> ${HOME}/clientmerge_time_results_$test_id.txt
echo "clientmerge_time_start=$clientmerge_time_start clientmerge_time_end=$clientmerge_time_end clientmerge_time_duration=$clientmerge_time_dur" >> ${HOME}/clientmerge_time_results_$test_id.txt
sleep 10;

# ==================================================================== #
# self-verification

#

# ==================================================================== #
end=$(date --utc "+%s.%N")
dur=0$(echo "$end - $start" | bc)
echo "total runtime:"
echo "start=$start end=$end duration=$dur"
