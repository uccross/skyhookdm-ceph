set -e

pool=$1
nobjs=$2
nthreads=$3
nruns=$4
cls=$5
echo "usage: ./qe.sh <pool> <nobjs> <nthreads> <nruns> [--use-cls]"
echo "--pool=$pool"
echo "--num-objs=$nobjs"
echo "--nthreads=$nthreads"
echo "--nruns=$nruns"
echo "--use-cls=$cls"
#
# ========
# QUERY: e
# several range predicates
# ========
#
# selectivity 100525/1000000 = 10%
# select * from lineitem1m where l_shipdate >= '01-04-1994' and l_shipdate < '09-25-1994' and l_discount> 0.001 and l_discount < 5 and l_quantity < 100;
q="run-query --num-objs $nobjs --pool $pool --nthreads $nthreads --query e --ship-date-low 757641600 --ship-date-high 780451200 --discount-low 0.001 --discount-high 5 --quantity 100 --quiet $cls"
for i in `seq 1 $nruns`;
do
    echo $q
    t1=`date --utc "+%s.%N"`
    $q
    t2=`date --utc "+%s.%N"`
    res=0$(echo "$t2 - $t1"|bc);
    echo "qe selectivity=10% pool=$pool nthreads=$nthreads cls=$cls run$i=$res"
done
#
