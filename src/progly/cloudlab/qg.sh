set -e

pool=$1
nobjs=$2
nthreads=$3
cls=$4
echo "usage: ./qf.sh <pool> <nobjs> <nthreads> [--use-cls]"
echo "--pool=$pool"
echo "--num-objs=$nobjs"
echo "--nthreads=$nthreads"
echo "--use-cls=$cls"
#
## ========
# QUERY: g
# regex text search
# ========
#
# selectivity = 100%
# select * from lineitem
q="run-query --num-objs $nobjs --pool $pool --nthreads $nthreads --query g --quiet $cls"
for i in `seq 1 2`;
do
    echo $q
    t1=`date --utc "+%s.%N"`
    $q
    t2=`date --utc "+%s.%N"`
    res=0$(echo "$t2 - $t1"|bc); 
    echo "qg selectivity=100% pool=$pool nthreads=$nthreads cls=$cls run$i=$res"
done 
#
# 
# select * from lineitem
# with projection
q="run-query --num-objs $nobjs --pool $pool --nthreads $nthreads --query g --projection --quiet $cls"
for i in `seq 1 2`;
do
    echo $q
    t1=`date --utc "+%s.%N"`
    $q
    t2=`date --utc "+%s.%N"`
    res=0$(echo "$t2 - $t1"|bc); 
    echo "qg selectivity=100%--projection pool=$pool nthreads=$nthreads cls=$cls run$i=$res"
done 
#

