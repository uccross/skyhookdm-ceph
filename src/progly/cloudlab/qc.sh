set -e

pool=$1
nobjs=$2
nthreads=$3
nruns=$4
cls=$5
echo "usage: ./qc.sh <pool> <nobjs> <nthreads> <nruns> [--use-cls]"
echo "--pool=$pool"
echo "--num-objs=$nobjs"
echo "--nthreads=$nthreads"
echo "--nruns=$nruns"
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
for i in `seq 1 $nruns`;
do
    echo $q
    t1=`date --utc "+%s.%N"`
    $q
    t2=`date --utc "+%s.%N"`
    res=0$(echo "$t2 - $t1"|bc);
    echo "qc selectivity=0.001% pool=$pool nthreads=$nthreads cls=$cls run$i=$res"
done
#
#
