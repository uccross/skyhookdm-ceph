pool=$1
nobjs=$2
nthreads=$3
cls=$4
echo "usage: ./qc.sh <pool> <nobjs> <nthreads> [--use-cls]"
echo "--pool=$pool"
echo "--num-objs=$nobjs"
echo "--nthreads=$nthreads"
echo "--use-cls=$cls"
#
# ========
# QUERY: c
# equality predicate
# ========
#
# selectivity 2/1000000 <= 0.001%
# select * from lineitem1m where l_extendedprice =21168.23;
q="run-query --num-objs $nobjs --pool $pool --nthreads $nthreads --query c --extended-price 21168.23 --quiet $cls"
for i in `seq 1 3`;
do
    echo $q
    t1=`date --utc "+%s.%N"`
    $q
    t2=`date --utc "+%s.%N"`
    res=$(echo "$t2 - $t1"|bc); 
    echo "qc selectivity=0.0001% run$i=$res"
done 
#
#

