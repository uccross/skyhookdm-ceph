echo "Usage: ./this <pool> <num_pgs> <num_replicas>; note: with 4 osds repl=1:1024 PGs repl=3:256 PGs"
pool=$1
npgs=$2
nrepl=$3
ceph osd pool delete $pool $pool --yes-i-really-really-mean-it; 
ceph osd pool create $pool $npgs $npgs replicated; 
ceph osd pool set $pool size $nrepl; 
sleep 10;


