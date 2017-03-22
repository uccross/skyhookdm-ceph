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
# QUERY: f
# regex text search
# ========
#
# selectivity 10801/1000000 = 1%
# select * from lineitem1m where l_comment ilike '%ave%';
q="run-query --num-objs $nobjs --pool $pool --nthreads $nthreads --query f --comment_regex ave --quiet $cls"
for i in `seq 1 2`;
do
    echo $q
    t1=`date --utc "+%s.%N"`
    $q
    t2=`date --utc "+%s.%N"`
    res=0$(echo "0$t2 - $t1"|bc); 
    echo "qf selectivity=1% pool=$pool nthreads=$nthreads cls=$cls run$i=$res"
done 
#
# selectivity 100810/1000000 = 10%
# select * from lineitem1m where l_comment ilike '%uriously%';
q="run-query --num-objs $nobjs --pool $pool --nthreads $nthreads --query f --comment_regex uriously --quiet $cls"
for i in `seq 1 2`;
do
    echo $q
    t1=`date --utc "+%s.%N"`
    $q
    t2=`date --utc "+%s.%N"`
    res=0$(echo "$t2 - $t1"|bc); 
    echo "qf selectivity=10% pool=$pool nthreads=$nthreads cls=$cls run$i=$res"
done 
#

