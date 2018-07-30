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
#
# selectivity 1098/1000000 = 0.1%
# select * from lineitem1m where l_shipdate >= '01-04-1994' and l_shipdate < '01-07-1994' and l_discount> 0.001 and l_discount < 5 and l_quantity < 100;
q="run-query --num-objs $nobjs --pool $pool --nthreads $nthreads --query e --ship-date-low 757641600 --ship-date-high 757900800 --discount-low 0.001 --discount-high 5 --quantity 100 --quiet $cls"
for i in `seq 1 $nruns`;
do
    echo $q
    t1=`date --utc "+%s.%N"`
    $q
    t2=`date --utc "+%s.%N"`
    res=0$(echo "$t2 - $t1"|bc);
    echo "qe selectivity=0.1% pool=$pool nthreads=$nthreads cls=$cls run$i=$res"
done
#
#
# selectivity 10197/1000000 = 1%
# select * from lineitem1m where l_shipdate >= '01-04-1994' and l_shipdate < '01-31-1994' and l_discount> 0.001 and l_discount < 5 and l_quantity < 100;
q="run-query --num-objs $nobjs --pool $pool --nthreads $nthreads --query e --ship-date-low 757641600 --ship-date-high 759974400 --discount-low 0.001 --discount-high 5 --quantity 100 --quiet $cls"
for i in `seq 1 $nruns`;
do
    echo $q
    t1=`date --utc "+%s.%N"`
    $q
    t2=`date --utc "+%s.%N"`
    res=0$(echo "$t2 - $t1"|bc);
    echo "qe selectivity=1% pool=$pool nthreads=$nthreads cls=$cls run$i=$res"
done
#
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
#
# selectivity 1000000/1000000 = 100%
# select * from lineitem1m where l_shipdate >= '12-01-1991' and l_shipdate < '12-01-2000' and l_discount> -1.0 and l_discount < 1.0 and l_quantity < 100;
q="run-query --num-objs $nobjs --pool $pool --nthreads $nthreads --query e --ship-date-low 691545600 --ship-date-high 975628800 --discount-low -0.1 --discount-high 1.0 --quantity 100 --quiet"
for i in `seq 1 $nruns`;
do
    echo $q
    t1=`date --utc "+%s.%N"`
    $q
    t2=`date --utc "+%s.%N"`
    res=0$(echo "$t2 - $t1"|bc);
    echo "qe selectivity=100% pool=$pool nthreads=$nthreads cls=$cls run$i=$res"
done
#
