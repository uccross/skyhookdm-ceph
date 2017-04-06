#!/bin/bash
set -e
set -x

query=b
pool=tpc
nobjs=10000
nthreads="1 2 10 20 40 60"
runs="1 2"
nosds=2
extended_prices=(1.0)
selectivies=(100)

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
        for ((j=0; j<${nosds}; j++)); do
          ssh osd${j} sync
          ssh osd${j} "echo 1 | sudo tee /proc/sys/vm/drop_caches"
        done
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
