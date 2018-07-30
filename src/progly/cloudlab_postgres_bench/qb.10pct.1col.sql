\timing
SHOW force_parallel_mode;   SHOW max_parallel_workers_per_gather; SHOW max_worker_processes;
EXPLAIN VERBOSE SELECT l_orderkey FROM :tname WHERE l_extendedprice > 71000;
\o '/dev/null'
SELECT l_orderkey FROM :tname WHERE l_extendedprice > 71000;
