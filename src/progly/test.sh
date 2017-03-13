#!/bin/bash

set -e

THIS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

output=$(mktemp)
trap "rm -f $output" EXIT

for nthreads in 1 2 3 4 5; do
  for q in a b c d e f; do
    local_cmd=$(cat ${THIS_DIR}/test/q${q}.txt)
    $local_cmd --nthreads $nthreads | sort > $output
    diff $output ${THIS_DIR}/test/q${q}.expected_result.sorted.txt
    $local_cmd --nthreads $nthreads --projection | sort > $output
    diff $output ${THIS_DIR}/test/q${q}.projection.expected_result.sorted.txt

    remote_cmd=$(cat ${THIS_DIR}/test/q${q}.cls.txt)
    $remote_cmd --nthreads $nthreads | sort > $output
    diff $output ${THIS_DIR}/test/q${q}.cls.expected_result.sorted.txt
    $remote_cmd --nthreads $nthreads --projection | sort > $output
    diff $output ${THIS_DIR}/test/q${q}.cls.projection.expected_result.sorted.txt
  done

  # test --use-index. must run --build-index first manually
  # bin/run-query --num-objs 10 --pool rbd --build-index
  qd_cmd=$(cat ${THIS_DIR}/test/qd.cls.txt)
  $qd_cmd --nthreads $nthreads --use-index | sort > $output
  diff $output ${THIS_DIR}/test/qd.cls.expected_result.sorted.txt
  $qd_cmd --nthreads $nthreads --use-index --projection | sort > $output
  diff $output ${THIS_DIR}/test/qd.cls.projection.expected_result.sorted.txt
done
