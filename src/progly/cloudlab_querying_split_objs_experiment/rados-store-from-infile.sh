#!/bin/bash
# reads each line of the given filename and puts them into ceph as an object

set -e

if [[ $# < 1 ]]; then
  echo "usage: <pool> <filename> ;where each line in filename is a path to binary tpc object file."
  exit 1
fi

pool=$1
groupsize=24
counter=0

while IFS='' read -r line || [[ -n "$line" ]]; do
    echo "Text read from file: $line"
    ofile=${line}
    oname=obj.${counter}
    rados -p $pool rm $oname || true
    echo "putting $oname now..." 
    rados -p $pool put $oname $ofile &
    #sleep 2 &    
    if ! ((counter % groupsize)); then 
        wait  # wait until all objs in this group are stored.
    fi
    counter=$((counter+1))
done < "$2"

