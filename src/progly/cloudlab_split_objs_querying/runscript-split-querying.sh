
./recreate-replicated-pool.sh tpc 256 3
./rados-store-from-infile.sh tpc 10K-25pct-split.txt
date; 
time ./4osds-split-objs-querying.sh 4 12500 | tee 4osds.split.objs.querying.10Kobjs.25pct.split.log-1;
time ./4osds-split-objs-querying.sh 4 12500 | tee 4osds.split.objs.querying.10Kobjs.25pct.split.log-2;
date
echo "next split size"
./recreate-replicated-pool.sh tpc 256 3
./rados-store-from-infile.sh tpc 10K-50pct-split.txt
date; 
time ./4osds-split-objs-querying.sh 4 15000 | tee 4osds.split.objs.querying.10Kobjs.50pct.split.log-1;
time ./4osds-split-objs-querying.sh 4 15000 | tee 4osds.split.objs.querying.10Kobjs.50pct.split.log-2;
date
echo "next split size"
./recreate-replicated-pool.sh tpc 256 3
./rados-store-from-infile.sh tpc 10K-75pct-split.txt
date; 
time ./4osds-split-objs-querying.sh 4 17500 | tee 4osds.split.objs.querying.10Kobjs.75pct.split.log-1;
time ./4osds-split-objs-querying.sh 4 17500 | tee 4osds.split.objs.querying.10Kobjs.75pct.split.log-2;
date
echo "next split size"
./recreate-replicated-pool.sh tpc 256 3
./rados-store-from-infile.sh tpc 10K-100pct-split.txt
date; 
time ./4osds-split-objs-querying.sh 4 20000 | tee 4osds.split.objs.querying.10Kobjs.100pct.split.log-1;
time ./4osds-split-objs-querying.sh 4 20000 | tee 4osds.split.objs.querying.10Kobjs.100pct.split.log-2;
date
###############
###############
./recreate-replicated-pool.sh tpc 256 3
./rados-store-from-infile.sh tpc 20K-0pct-split.txt
date; 
time ./4osds-split-objs-querying.sh 4 20000 | tee 4osds.split.objs.querying.20Kobjs.0pct.split.log-1;
time ./4osds-split-objs-querying.sh 4 20000 | tee 4osds.split.objs.querying.20Kobjs.0pct.split.log-2;
date
echo "next split size"
./recreate-replicated-pool.sh tpc 256 3
./rados-store-from-infile.sh tpc 20K-25pct-split.txt
date; 
time ./4osds-split-objs-querying.sh 4 25000 | tee 4osds.split.objs.querying.20Kobjs.25pct.split.log-1;
time ./4osds-split-objs-querying.sh 4 25000 | tee 4osds.split.objs.querying.20Kobjs.25pct.split.log-2;
date
echo "next split size"
./recreate-replicated-pool.sh tpc 256 3
./rados-store-from-infile.sh tpc 20K-50pct-split.txt
date; 
time ./4osds-split-objs-querying.sh 4 30000 | tee 4osds.split.objs.querying.20Kobjs.50pct.split.log-1;
time ./4osds-split-objs-querying.sh 4 30000 | tee 4osds.split.objs.querying.20Kobjs.50pct.split.log-2;
date
echo "next split size"
./recreate-replicated-pool.sh tpc 256 3
./rados-store-from-infile.sh tpc 20K-75pct-split.txt
date; 
time ./4osds-split-objs-querying.sh 4 35000 | tee 4osds.split.objs.querying.20Kobjs.75pct.split.log-1;
time ./4osds-split-objs-querying.sh 4 35000 | tee 4osds.split.objs.querying.20Kobjs.75pct.split.log-2;
date
echo "next split size"
./recreate-replicated-pool.sh tpc 256 3
./rados-store-from-infile.sh tpc 20K-100pct-split.txt
date; 
time ./4osds-split-objs-querying.sh 4 40000 | tee 4osds.split.objs.querying.20Kobjs.100pct.split.log-1;
time ./4osds-split-objs-querying.sh 4 40000 | tee 4osds.split.objs.querying.20Kobjs.100pct.split.log-2;
date

