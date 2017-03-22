pool=$1
nobjs=$2
nthreads=$3
cls=$4
echo "usage: ./qe.sh <pool> <nobjs> <nthreads> [--use-cls]"
echo "--pool=$pool"
echo "--num-objs=$nobjs"
echo "--nthreads=$nthreads"
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
for i in `seq 1 3`;
do
    echo $q
    t1=`date --utc "+%s.%N"`
    $q
    t2=`date --utc "+%s.%N"`
    res=$(echo "$t2 - $t1"|bc); 
    echo "qe selectivity=0.1% run$i=$res"
done 
#
#
# selectivity 10197/1000000 = 1%
# select * from lineitem1m where l_shipdate >= '01-04-1994' and l_shipdate < '01-31-1994' and l_discount> 0.001 and l_discount < 5 and l_quantity < 100;
q="run-query --num-objs $nobjs --pool $pool --nthreads $nthreads --query e --ship-date-low 757641600 --ship-date-high 759974400 --discount-low 0.001 --discount-high 5 --quantity 100 --quiet $cls"
for i in `seq 1 3`;
do
    echo $q
    t1=`date --utc "+%s.%N"`
    $q
    t2=`date --utc "+%s.%N"`
    res=$(echo "$t2 - $t1"|bc); 
    echo "qe selectivity=1% run$i=$res"
done 
#
#
# selectivity 50278/1000000 = 5%
# select * from lineitem1m where l_shipdate >= '01-04-1994' and l_shipdate < '05-16-1994' and l_discount> 0.001 and l_discount < 5 and l_quantity < 100;
q="run-query --num-objs $nobjs --pool $pool --nthreads $nthreads --query e --ship-date-low 757641600 --ship-date-high 769046400 --discount-low 0.001 --discount-high 5 --quantity 100 --quiet $cls"
for i in `seq 1 3`;
do
    echo $q
    t1=`date --utc "+%s.%N"`
    $q
    t2=`date --utc "+%s.%N"`
    res=$(echo "$t2 - $t1"|bc); 
    echo "qd selectivity=5% run$i=$res"
done 
#
#
# selectivity 100525/1000000 = 10%
# select * from lineitem1m where l_shipdate >= '01-04-1994' and l_shipdate < '09-25-1994' and l_discount> 0.001 and l_discount < 5 and l_quantity < 100;
q="run-query --num-objs $nobjs --pool $pool --nthreads $nthreads --query e --ship-date-low 757641600 --ship-date-high 780451200 --discount-low 0.001 --discount-high 5 --quantity 100 --quiet $cls"
for i in `seq 1 3`;
do
    echo $q
    t1=`date --utc "+%s.%N"`
    $q
    t2=`date --utc "+%s.%N"`
    res=$(echo "$t2 - $t1"|bc); 
    echo "qe selectivity=10% run$i=$res"
done 
#
#
# selectivity 499985/1000000 = 50%
# select * from lineitem1m where l_shipdate >= '01-04-1994' and l_shipdate < '08-16-1997' and l_discount> 0.001 and l_discount < 5 and l_quantity < 100;
q="run-query --num-objs $nobjs --pool $pool --nthreads $nthreads --query e --ship-date-low 757641600 --ship-date-high 871689600 --discount-low 0.001 --discount-high 5 --quantity 100 --quiet $cls"
for i in `seq 1 3`;
do
    echo $q
    t1=`date --utc "+%s.%N"`
    $q
    t2=`date --utc "+%s.%N"`
    res=$(echo "$t2 - $t1"|bc); 
    echo "qe selectivity=50% run$i=$res"
done 
#
