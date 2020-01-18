#!/bin/bash

echo "This script will copy the skyhookdm extensions shared library (libcls_tabular.so) to all nodes in a cluster.  Assumes hostnames available as client0, osd0, osd1, ..., osdn"

if [[ -z $1 && -z $2 ]]; then
  echo "Usage: ./copylibs.sh </path/to/libcls_tabular.so.1.0.0> <nosds> <ubuntu|centos>"
  echo "Ex:    ./copylibs.sh ~/skyhookdm-ceph/build/lib/libcls_tabular.so.1.0.0 2 ubuntu"
  echo "forcing program exit..."
  exit 1
fi

so_path=$1
nosds=$2
os=$3

if [ $os = "ubuntu" ]
    then echo "ubuntu confirmed";
    LIB_CLS_DIR=/usr/lib/x86_64-linux-gnu/rados-classes/
fi
if [ $os = "centos" ]
    then echo "centos confirmed";
    LIB_CLS_DIR=/usr/lib64/rados-classes/
fi

echo "Copying our libcls so file to each host, move into the correct dir, and set symlinks";

for ((i = 0 ; i <= $nosds ; i++)); do
    hostname=""
    if [ $i -eq $nosds ] ; then hostname="client0"; else hostname="osd$i"; fi;
    echo $hostname
    scp $so_path  $hostname:/tmp/;
    ssh $hostname "sudo cp /tmp/libcls_tabular.so.1.0.0 ${LIB_CLS_DIR};";
    ssh $hostname "cd ${LIB_CLS_DIR}; if test -f libcls_tabular.so; then sudo unlink libcls_tabular.so; fi";
    ssh $hostname "cd ${LIB_CLS_DIR}; if test -f libcls_tabular.so.1; then sudo  unlink libcls_tabular.so.1; fi";
    ssh $hostname "cd ${LIB_CLS_DIR}; sudo ln -s libcls_tabular.so.1.0.0 libcls_tabular.so.1;";
    ssh $hostname "cd ${LIB_CLS_DIR}; sudo ln -s libcls_tabular.so.1 libcls_tabular.so;";
done;
