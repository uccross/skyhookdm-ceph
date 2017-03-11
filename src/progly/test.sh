#!/bin/bash

set -e

THIS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

output=$(mktemp)
trap "rm -f $output" EXIT

for nthreads in 1 2 3 4; do
  for q in a b c d e f; do
    local_cmd=$(cat ${THIS_DIR}/test/q${q}.txt)
    $local_cmd --nthreads $nthreads | sort > $output
    diff $output ${THIS_DIR}/test/q${q}.expected_result.sorted.txt

    remote_cmd=$(cat ${THIS_DIR}/test/q${q}.cls.txt)
    $remote_cmd --nthreads $nthreads | sort > $output
    diff $output ${THIS_DIR}/test/q${q}.cls.expected_result.sorted.txt
  done
done
