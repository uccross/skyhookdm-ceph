set -e

pool=$1
nobjs=$2
nthreads=$3
cls=$4
echo "usage: ./qd.sh <pool> <nobjs> <nthreads> [--use-cls]"
echo "--pool=$pool"
echo "--num-objs=$nobjs"
echo "--nthreads=$nthreads"
echo "--use-cls=$cls"
#
# ========
# QUERY: d
# equality predicate on 2 cols + compound index on those cols 
# (i.e. the primary key index for lineitem table).
# ========
#
# selectivity 1/1000000 = this should be a unique row
# select * from lineitem1m where l_orderkey=5 and l_linenumber=3;
#
q="run-query --num-objs $nobjs --pool $pool --nthreads $nthreads --query d --order-key 5 --line-number 3 --quiet $cls"
for i in `seq 1 2`;
do
    echo $q
    t1=`date --utc "+%s.%N"`
    $q
    t2=`date --utc "+%s.%N"`
    res=0$(echo "$t2 - $t1"|bc); 
    echo "qd selectivity=unique pool=$pool nthreads=$nthreads cls=$cls run$i=$res"
done 
#
#
