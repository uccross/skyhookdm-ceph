#!/bin/bash

set -e

echo "Usage:"
echo "./rados-store-glob.sh <oid-prefix> <table-name>"
echo "Example:"
echo "./rados-store-glob.sh public lineitem"

groupsize=15

pool=$1
objprefix=$2
tablename=$3
shift 3
objects=($@)


echo "Loading ${#objects[@]} into pool $pool..."
echo ${objects[@]} | fold  -w 80 -s
read -p "Continue? (y/n) " -n 1 -r

if [[ $REPLY =~ ^[Yy]$ ]]; then
    count=0

    for ((i=0; i < ${#objects[@]}; i+=groupsize)); do
        objgroup=("${objects[@]:i:groupsize}")

        for objfile in ${objgroup[*]}; do
            objname="${objprefix}.${tablename}.${count}"

            rados -p $pool rm $objname || true
            echo "writing $objfile into $pool/$objname"
            rados -p $pool put $objname $objfile &

            count=$((count+1))
        done

        wait
    done
fi
