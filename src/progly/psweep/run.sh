
#!/bin/bash
set -e
#set -x

nosds=3
nobjs=10000
pool=tpc
nthreads="10 20 40 60"
runs="1 2"

q_ab_extended_prices=(91400.0 71000.0 1.0)
q_ab_selectivities=(1 10 100)

q_f_regex=("ave" "uriously" "[i|\s*|a]")
q_f_selectivities=(1 10 100)

function run_query() {
  local cmd=$1
  for rr in $runs; do
    for ((j=0; j<${nosds}; j++)); do
      ssh osd${j} sync
      ssh osd${j} "echo 1 | sudo tee /proc/sys/vm/drop_caches"
    done
    start=$(date --utc "+%s.%N")
    $cmd
    end=$(date --utc "+%s.%N")
    dur=0$(echo "$end - $start" | bc)
    echo $cmd run-${rr} $dur
  done
}

for nthread in ${nthreads}; do
  cmdbase="run-query --num-objs $nobjs --pool $pool --nthreads $nthread --quiet"

  # setup for queries: a, b
  for ((i=0; i<${#q_ab_extended_prices[@]}; i++)); do
    price=${q_ab_extended_prices[i]}
    selpct=${q_ab_selectivities[i]}
    cmd="${cmdbase} --extended-price ${price}"
    for qname in a b; do
      run_query "$cmd --query ${qname}"
      run_query "$cmd --query ${qname} --use-cls"
    done
  done

  # query: d
  cmd_q_f="${cmdbase} --query d --order-key 5 --line-number 3"
  run_query "${cmd_q_f}"
  run_query "${cmd_q_f} --use-cls"
  run_query "${cmd_q_f} --use-cls --use-index"

  # query: f
  if [ $nthread -gt 2 ]; then
    for ((i=0; i<${#q_f_regex[@]}; i++)); do
      regex=${q_f_regex[i]}
      selpct=${q_f_selectivities[i]}
      cmd="${cmdbase} --query f --comment_regex \"${regex}\""
      run_query "$cmd"
      run_query "$cmd --use-cls"
    done
  fi
done
