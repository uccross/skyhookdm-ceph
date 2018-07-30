#!/bin/bash
set -e
#set -x

query=b
pool=tpc
nobjs=10000
nthreads="1 10"
runs="1 2 3"
extended_prices=(150000.0 91400.0 71000.0 1.0)
selectivies=(0 1 10 100)

for nthread in ${nthreads}; do
  for ((i=0; i<${#extended_prices[@]}; i++)); do
    price=${extended_prices[i]}
    pct=${selectivies[i]}
    for run in ${runs}; do
      name="no-${nobjs}_q-${query}_nt-${nthread}_ep-${price}_sel-${pct}_run-${run}"
      logfile="${name}.objcosts.log"
      cmd="run-query --quiet --num-objs ${nobjs} --pool ${pool} \
          --query ${query} --nthreads ${nthread} --extended-price ${price} \
          --use-cls --log-file ${logfile}"
      if [ $run -lt 3 ]; then
        ssh osd0 sync
        ssh osd0 "echo 1 | sudo tee /proc/sys/vm/drop_caches"
        name="${name}_coldcache-1"
      else
        name="${name}_coldcache-0"
      fi

      start=$(date --utc "+%s.%N")
      $cmd
      end=$(date --utc "+%s.%N")
      dur=0$(echo "$end - $start" | bc)

      echo "${name}_dur-${dur}"
    done 
  done
done
