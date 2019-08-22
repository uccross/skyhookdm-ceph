#!/bin/bash
# katgen.sh

set -x

numobjs=$1
nrows=$2
arity=$3
splitter=$4
for ((i=0;i<$numobjs;i++)); do
  echo "katgen : i = $i" ;
  python pdsw19.py $nrows $arity $i $splitter ;
done
