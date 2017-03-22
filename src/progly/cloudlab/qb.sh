pool=$1
nobjs=$2
nthreads=$3
cls=$4
echo "usage: ./qb.sh <pool> <nobjs> <nthreads> [--use-cls]"
echo "--pool=$pool"
echo "--num-objs=$nobjs"
echo "--nthreads=$nthreads"
echo "--use-cls=$cls"
#
# ========
# QUERY: b
# range
# ========
#
# selectivity 1105/1000000 = 0.1%
# select * from lineitem1m where l_extendedprice > 99000.0;
q="run-query --num-objs $nobjs --pool $pool --nthreads $nthreads --query b --extended-price 99000.0 --quiet $cls"
for i in `seq 1 3`;
do
    echo $q
    t1=`date --utc "+%s.%N"`
    $q
    t2=`date --utc "+%s.%N"`
    res=$(echo "$t2 - $t1"|bc); 
    echo "qb selectivity=0.1% run$i=$res"
done 
#
# selectivity 10058/1000000 = 1%
# select * from lineitem1m where l_extendedprice > 91400.0;
q="run-query --num-objs $nobjs --pool $pool --nthreads $nthreads --query b --extended-price 91400.0 --quiet $cls"
for i in `seq 1 3`;
do
    echo $q
    t1=`date --utc "+%s.%N"`
    $q
    t2=`date --utc "+%s.%N"`
    res=$(echo "$t2 - $t1"|bc); 
    echo "qb selectivity=1% run$i=$res"
done 
#
# selectivity 100452/1000000 = 10%
# select * from lineitem1m where l_extendedprice > 71000.0;
q="run-query --num-objs $nobjs --pool $pool --nthreads $nthreads --query b --extended-price 71000.0 --quiet $cls"
for i in `seq 1 3`;
do
    echo $q
    t1=`date --utc "+%s.%N"`
    $q
    t2=`date --utc "+%s.%N"`
    res=$(echo "$t2 - $t1"|bc); 
    echo "qb selectivity=10% run$i=$res"
done 
#
# selectivity 503747/1000000 = 50%
# select * from lineitem1m where l_extendedprice > 36500.0;
q="run-query --num-objs $nobjs --pool $pool --nthreads $nthreads --query b --extended-price 36500.0 --quiet $cls"
for i in `seq 1 3`;
do
    echo $q
    t1=`date --utc "+%s.%N"`
    $q
    t2=`date --utc "+%s.%N"`
    res=$(echo "$t2 - $t1"|bc); 
    echo "qb selectivity=50% run$i=$res"
done 
#
# selectivity 1000000/1000000 = 100%
# select * from lineitem1m where l_extendedprice > 1.0;
q="run-query --num-objs $nobjs --pool $pool --nthreads $nthreads --query b --extended-price 1.0 --quiet $cls"
for i in `seq 1 3`;
do
    echo $q
    t1=`date --utc "+%s.%N"`
    $q
    t2=`date --utc "+%s.%N"`
    res=$(echo "$t2 - $t1"|bc); 
    echo "qb selectivity=100% run$i=$res"
done 

