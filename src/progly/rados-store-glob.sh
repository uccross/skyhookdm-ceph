#!/bin/bash

set -e

if [[ $# < 1 ]]; then
  echo "usage: <pool> <glob>"
  exit 1
fi

groupsize=5

pool=$1
shift
objects=($@)

echo "Loading ${#objects[@]} into pool $pool..."
echo ${objects[@]} | fold  -w 80 -s
read -p "Continue? (y/n) " -n 1 -r
if [[ $REPLY =~ ^[Yy]$ ]]; then
  count=0
  for ((i=0; i < ${#objects[@]}; i+=groupsize)); do
    objgroup=("${objects[@]:i:groupsize}")
    for objfile in ${objgroup[*]}; do
      objname="obj.${count}"
      rados -p $pool rm $objname || true
      echo "writing $objfile into $pool/$objname"
      rados -p $pool put $objname $objfile &
      count=$((count+1))
    done
    wait
  done
fi
