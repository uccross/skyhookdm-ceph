#!/bin/bash

set -e

if [[ $# < 1 ]]; then
  echo "usage: <pool> <glob>"
  exit 1
fi

pool=$1
shift
objects=($@)

echo "Loading ${#objects[@]} into pool $pool..."
echo ${objects[@]} | fold  -w 80 -s
read -p "Continue? (y/n) " -n 1 -r
if [[ $REPLY =~ ^[Yy]$ ]]; then
  count=0
  for objfile in ${objects[@]}; do
    objname="obj.${count}"
    rados -p $pool rm $objname || true
    echo "writing $objfile into $pool/$objname"
    rados -p $pool put $objname $objfile
    count=$((count+1))
  done
fi
