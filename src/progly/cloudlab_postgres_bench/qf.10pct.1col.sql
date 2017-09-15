\timing
SHOW force_parallel_mode;   SHOW max_parallel_workers_per_gather; SHOW max_worker_processes ;
EXPLAIN VERBOSE SELECT l_linenumber FROM :tname WHERE l_comment ilike '%uriously%';
\o '/dev/null'
SELECT l_linenumber FROM :tname WHERE l_comment ilike '%uriously%';


