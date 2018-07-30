set -e

pool=$1
nobjs=$2
nthreads=$3
nruns=$4
cls=$5
echo "usage: ./qb-select10.sh <pool> <nobjs> <nthreads> <nruns> [--use-cls]"
echo "--pool=$pool"
echo "--num-objs=$nobjs"
echo "--nthreads=$nthreads"
echo "--nruns=$nruns"
echo "--use-cls=$cls"
#
# ========
# QUERY: a
# count with range
# ========
#
# selectivity 100452/1000000 = 10%
# select * from lineitem1m where l_extendedprice > 71000.0;
q="run-query --num-objs $nobjs --pool $pool --nthreads $nthreads --query b --extended-price 71000.0 --quiet $cls"
for i in `seq 1 $nruns`;
do
    echo $q
    t1=`date --utc "+%s.%N"`
    $q
    t2=`date --utc "+%s.%N"`
    res=0$(echo "$t2 - $t1"|bc);
    echo "qb selectivity=10% pool=$pool nthreads=$nthreads cls=$cls run$i=$res"
done

