#!/bin/bash
set -e

date
t=$1
for q in qa.10pct.int.sql  qa.10pct.sql qb.10pct.1col.sql qb.10pct.3cols.sql qb.10pct.sql qd.sql qf.10pct.1col.sql qf.10pct.count.sql qf.10pct.sql; do
    ./pg-bench.sh $t $q | tee -a pgresult-$t-$q.log
done

t=lineitem1bssdpk
./pg-bench.sh $t $q | tee -a pgresult-$t-$q.log
date


