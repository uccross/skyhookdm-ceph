#!/bin/bash
set -e
pool=$1
objfile=$2
nosds=$3
nobjs=$4


echo "pool=${pool}";
echo "objfile=${objfile}";
echo "nosds=${nosds}";
echo "nobjs=${nobjs}";


cd $HOME
rados rmpool $pool $pool --yes-i-really-really-mean-it
rados mkpool $pool

# magic number, seems work well at all data sizes 10MB/100MB and cluster scales 1,2,4
groupsize_multiplier=20
groupsize=$(($groupsize_multiplier*$nosds))
echo "groupsize=${groupsize}";
groupsize=$(( groupsize < nobjs ? groupsize : nobjs ))
echo "groupsize=${groupsize}";
echo "nobjs=${nobjs}";
for ((i=0; i<nobjs; i+=groupsize)); do
    for ((j=0; j<groupsize; j++)); do
        oid=$(($i+$j))
        if [ "${oid}" -ge "${nobjs}" ]
        then
            break
        fi
        objname="obj.${oid}"
        echo "writing $objfile into $pool/$objname"  &
        rados -p $pool put $objname $objfile  &
    done
    wait
done
rados -p $pool df

