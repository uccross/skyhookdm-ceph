#!/bin/bash
# katgen.sh

#set -x

numobjs=$1
nrows=$2
arity=$3
start_row=$4
end_row=$5
outfile_path=$6
for ((i=0;i<$numobjs;i++)); do
  echo "katgen : i = $i" ;
  python pdsw19.py $nrows $arity $i ;
done
