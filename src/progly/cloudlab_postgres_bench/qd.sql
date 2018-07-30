\timing
SHOW force_parallel_mode;   SHOW max_parallel_workers_per_gather; SHOW max_worker_processes;
EXPLAIN VERBOSE SELECT  l_orderkey, l_linenumber, l_extendedprice FROM :tname WHERE l_orderkey=5 and l_linenumber=3;
\o '/dev/null'
SELECT  l_orderkey, l_linenumber, l_extendedprice FROM :tname WHERE l_orderkey=5 and l_linenumber=3;
