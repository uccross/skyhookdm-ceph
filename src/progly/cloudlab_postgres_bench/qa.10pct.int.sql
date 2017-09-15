\timing
SHOW force_parallel_mode;   SHOW max_parallel_workers_per_gather; SHOW max_worker_processes;
EXPLAIN VERBOSE SELECT count(*) FROM :tname WHERE l_orderkey > 900000000;
\o '/dev/null'
SELECT count(*) FROM :tname WHERE l_orderkey > 900000000;

