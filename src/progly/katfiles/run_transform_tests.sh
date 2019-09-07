#!/bin/bash

# 2 osds, 100 groups of 100, written in subgroups of 10
#bash run_transform_tests.sh 2 10000 paper_exps /proj/skyhook-PG0/pdsw19/data/ncols100.10MB.objs.25Krows.csv /proj/skyhook-PG0/pdsw19/data/ncols100.schema.txt True 100 ;
#
# 4 osds, 100 groups of 100, written in subgroups of 10
#bash run_transform_tests.sh 4 10000 paper_exps /proj/skyhook-PG0/pdsw19/data/ncols100.10MB.objs.25Krows.csv /proj/skyhook-PG0/pdsw19/data/ncols100.schema.txt True 100 ;
#
# 8 osds, 100 groups of 100, written in subgroups of 10
#bash run_transform_tests.sh 8 10000 paper_exps /proj/skyhook-PG0/pdsw19/data/ncols100.10MB.objs.25Krows.csv /proj/skyhook-PG0/pdsw19/data/ncols100.schema.txt True 100 ;

nosds=$1
num_objs=$2
poolname=$3
data_path=$4
schema_path=$5
actually_do_it=$6
num_write_groups=$7
fbwriter_filename="fbmeta.Skyhook.v2.SFT_FLATBUF_FLEX_ROW.ncols100_paper_exps.0.1-1"
worker_threads=24
queue_depth=24
format="arrow"
export PROJECT_COLS="\"att0\""
export DATA_SCHEMA="\"0 8 1 0 ATT0; 1 8 0 0 ATT1; 2 8 0 0 ATT2; 3 8 0 0 ATT3; 4 8 0 0 ATT4; 5 8 0 0 ATT5; 6 8 0 0 ATT6; 7 8 0 0 ATT7; 8 8 0 0 ATT8; 9 8 0 0 ATT9; 10 8 0 0 ATT10; 11 8 0 0 ATT11; 12 8 0 0 ATT12; 13 8 0 0 ATT13; 14 8 0 0 ATT14; 15 8 0 0 ATT15; 16 8 0 0 ATT16; 17 8 0 0 ATT17; 18 8 0 0 ATT18; 19 8 0 0 ATT19; 20 8 0 0 ATT20; 21 8 0 0 ATT21; 22 8 0 0 ATT22; 23 8 0 0 ATT23; 24 8 0 0 ATT24; 25 8 0 0 ATT25; 26 8 0 0 ATT26; 27 8 0 0 ATT27; 28 8 0 0 ATT28; 29 8 0 0 ATT29; 30 8 0 0 ATT30; 31 8 0 0 ATT31; 32 8 0 0 ATT32; 33 8 0 0 ATT33; 34 8 0 0 ATT34; 35 8 0 0 ATT35; 36 8 0 0 ATT36; 37 8 0 0 ATT37; 38 8 0 0 ATT38; 39 8 0 0 ATT39; 40 8 0 0 ATT40; 41 8 0 0 ATT41; 42 8 0 0 ATT42; 43 8 0 0 ATT43; 44 8 0 0 ATT44; 45 8 0 0 ATT45; 46 8 0 0 ATT46; 47 8 0 0 ATT47; 48 8 0 0 ATT48; 49 8 0 0 ATT49; 50 8 0 0 ATT50; 51 8 0 0 ATT51; 52 8 0 0 ATT52; 53 8 0 0 ATT53; 54 8 0 0 ATT54; 55 8 0 0 ATT55; 56 8 0 0 ATT56; 57 8 0 0 ATT57; 58 8 0 0 ATT58; 59 8 0 0 ATT59; 60 8 0 0 ATT60; 61 8 0 0 ATT61; 62 8 0 0 ATT62; 63 8 0 0 ATT63; 64 8 0 0 ATT64; 65 8 0 0 ATT65; 66 8 0 0 ATT66; 67 8 0 0 ATT67; 68 8 0 0 ATT68; 69 8 0 0 ATT69; 70 8 0 0 ATT70; 71 8 0 0 ATT71; 72 8 0 0 ATT72; 73 8 0 0 ATT73; 74 8 0 0 ATT74; 75 8 0 0 ATT75; 76 8 0 0 ATT76; 77 8 0 0 ATT77; 78 8 0 0 ATT78; 79 8 0 0 ATT79; 80 8 1 0 ATT80; 81 8 0 0 ATT81; 82 8 0 0 ATT82; 83 8 0 0 ATT83; 84 8 0 0 ATT84; 85 8 0 0 ATT85; 86 8 0 0 ATT86; 87 8 0 0 ATT87; 88 8 0 0 ATT88; 89 8 0 0 ATT89; 90 8 0 0 ATT90; 91 8 0 0 ATT91; 92 8 0 0 ATT92; 93 8 0 0 ATT93; 94 8 0 0 ATT94; 95 8 0 0 ATT95; 96 8 0 0 ATT96; 97 8 0 0 ATT97; 98 8 0 0 ATT98; 99 8 0 0 ATT99\""

# for 100 objects of size 10MB each, use num_merge_objs=25
# bc 256MB max obj size, so merged objs will be about 250MB each.
# for 100 objects of size 100MB each, use num_merge_objs=50
# bc 256MB max obj size, so merged objs will be about 200MB each.
num_merge_objs=$8
num_src_objs_per_merge=`expr $num_objs / $num_merge_objs` #num_objs/num_merge_objs

test_id=$9 # a string to add to the output files.

total_num_objs=`expr $num_objs * num_write_groups`

# make the pool
rados mkpool $poolname ;

# make bins
sudo make -j36 fbwriter run-query run-copyfrom-merge run-client-merge ;

# remove any existing objects
for i in `rados -p $poolname ls`; do echo $i; rados -p $poolname rm $i; done

# clear the caches
for ((j=0; j<${nosds}; j++)); do
  echo "clearing cache on osd"$j
  ssh osd${j} sync
  ssh osd${j} "echo 1 | sudo tee /proc/sys/vm/drop_caches"
  ssh osd${j} sync
done

start=$(date --utc "+%s.%N")
# ==================================================================== #
# write out data to ceph in groups of 10

# ------------------------------------------ #
# write the csv to fbxrows
cmd0="sudo bin/fbwriter --file_name $data_path --schema_file_name  $schema_path --num_objs 1 --flush_rows 25000 --read_rows 25000 --csv_delim \"|\" --use_hashing false --rid_start_value 1 --table_name ncols100_paper_exps --input_oid 0 --obj_type SFT_FLATBUF_FLEX_ROW ;"
  eval "$cmd0"

# ------------------------------------------ #
# write the fbxrows from disk to ceph
for ((group_id=0; group_id<${num_write_groups}; group_id++)); do
  cmd1="sudo python rados_put_parallel.py $num_objs $poolname ./$fbwriter_filename $actually_do_it $group_id"
  eval "$cmd1"
done

# ------------------------------------------ #
# remove the fbxrows disk file to conserve space
cmd2="sudo rm $fbwriter_filename"
eval "$cmd2"

# ==================================================================== #
# transform the fbxrows into arrow for entire table

cmd3="sudo bin/run-query --num-objs ${total_num_objs} --pool ${poolname} --wthreads ${worker_threads} --qdepth ${queue_depth} --project-cols ${PROJECT_COLS} --transform-db --transform-format-type ${format} --data-schema ${DATA_SCHEMA}"

local_xform_time_start=$(date --utc "+%s.%N")
eval "$cmd3"
local_xform_time_end=$(date --utc "+%s.%N")
local_xform_time_dur=0$(echo "$local_xform_time_end - $local_xform_time_start" | bc)

echo "Command ran: ${cmd3}" >> ${HOME}/local_xform_time_results_$test_id.txt
echo "local_xform_time_start=$local_xform_time_start local_xform_time_end=$local_xform_time_end local_xform_time_duration=$local_xform_time_dur" >> ${HOME}/local_xform_time_results_$test_id.txt

# ==================================================================== #
# prepare for next phase

# ------------------------------------------ #
# remove any existing objects
for i in `rados -p $poolname ls`; do echo $i; rados -p $poolname rm $i; done

# ------------------------------------------ #
# write the csv to fbxrows
cmd0="sudo bin/fbwriter --file_name $data_path --schema_file_name  $schema_path --num_objs 1 --flush_rows 25000 --read_rows 25000 --csv_delim \"|\" --use_hashing false --rid_start_value 1 --table_name ncols100_paper_exps --input_oid 0 --obj_type SFT_FLATBUF_FLEX_ROW ;"
eval "$cmd0"

# ------------------------------------------ #
# write the fbxrows from disk to ceph
for ((group_id=0; group_id<${num_write_groups}; group_id++)); do
  cmd1="sudo python rados_put_parallel.py $num_objs $poolname ./$fbwriter_filename $actually_do_it $group_id"
  eval "$cmd1"
done

# ------------------------------------------ #
# remove the fbxrows disk file to conserve space
cmd2="sudo rm $fbwriter_filename"
eval "$cmd2"

# ==================================================================== #
# do transforms with column project

# ------------------------------------------ #
# transform the fbxrows into arrow and project one column (do not time)
cmd4="sudo bin/run-query --num-objs ${total_num_objs} --pool ${poolname} --wthreads ${worker_threads} --qdepth ${queue_depth}  --transform-db --transform-format-type ${format} --data-schema ${DATA_SCHEMA} --project-cols ${PROJECT_COLS}"
eval "$cmd4"

# ==================================================================== #
# merge using copy from append
#
# num_merge_objs = number of merge objects to create
# num_src_objs_per_merge = number of source objects per merge object

# ------------------------------------------ #
copyfromappend_merge_time_start=$(date --utc "+%s.%N")
cmd5="python parallel_merges.py ${num_merge_objs} ${poolname} ${num_src_objs_per_merge} \"copyfrom\""
for ((group_id=0; group_id<${num_write_groups}; group_id++)); do
  eval "$cmd5"
done
copyfromappend_merge_time_end=$(date --utc "+%s.%N")
copyfromappend_merge_time_dur=0$(echo "$copyfromappend_merge_time_end - $copyfromappend_merge_time_start" | bc)
echo "Command ran: ${cmd5}" >> ${HOME}/copyfromappend_merge_time_results_$test_id.txt
echo "copyfromappend_merge_time_start=$copyfromappend_merge_time_start copyfromappend_merge_time_end=$copyfromappend_merge_time_end copyfromappend_merge_time_duration=$copyfromappend_merge_time_dur" >> ${HOME}/copyfromappend_merge_time_results_$test_id.txt

# ==================================================================== #
# merge using client
#
# num_merge_objs = number of merge objects to create
# num_src_objs_per_merge = number of source objects per merge object

# ------------------------------------------ #
clientmerge_time_start=$(date --utc "+%s.%N")
cmd6="python parallel_merges.py ${num_merge_objs} ${poolname} ${num_src_objs_per_merge} \"client\""
for ((group_id=0; group_id<${num_write_groups}; group_id++)); do
  eval "$cmd6"
done
clientmerge_time_end=$(date --utc "+%s.%N")
clientmerge_time_dur=0$(echo "$clientmerge_time_end - $clientmerge_time_start" | bc)
echo "Command ran: ${cmd6}" >> ${HOME}/clientmerge_time_results_$test_id.txt
echo "clientmerge_time_start=$clientmerge_time_start clientmerge_time_end=$clientmerge_time_end clientmerge_time_duration=$clientmerge_time_dur" >> ${HOME}/clientmerge_time_results_$test_id.txt

# ==================================================================== #
end=$(date --utc "+%s.%N")
dur=0$(echo "$end - $start" | bc)
echo "total runtime:"
echo "start=$start end=$end duration=$dur"
