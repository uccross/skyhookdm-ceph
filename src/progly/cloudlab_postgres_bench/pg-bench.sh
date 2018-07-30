#!/bin/bash
set -e

table=$1
query=$2

echo "starting 3 run query bench for table=$table query=$query"
echo "query text file:"
cat $query

function clear_cache() {
    sudo sync;
    echo 1 | sudo tee /proc/sys/vm/drop_caches;
    sudo sync;
    sudo systemctl restart postgresql@9.6-main;
    sleep 5;
    echo "CACHE CLEARED AND Postgres restarted...READY!!!";
}

for i in seq 1 to 3; do
    clear_cache
    start=$(date --utc "+%s.%N")
    sudo -u postgres psql -d d -f $query -v tname=$table --echo-all
    end=$(date --utc "+%s.%N")
    dur=0$(echo "$end - $start" | bc)
    echo "result: table=$table query=$query time=0$dur"
done;
