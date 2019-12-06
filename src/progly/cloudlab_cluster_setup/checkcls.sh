#!/bin/bash
if [[ -z $1 ]]; then
        echo "Please provide the number of Ceph OSDs".
exit
fi

nosds=$1
echo "Checking each OSD has the the libcls_tabular.so file and soft links.";
echo "on each osd, verify that libcls_tabular.so is present here: /usr/lib/x86_64-linux-gnu/rados-classes/"

for ((i=0;i<nosds;i++)); do
    echo "checking for libcls_tabular on osd${i}";
    ssh osd$i "ls -alh /usr/lib/x86_64-linux-gnu/rados-classes/ | grep libcls_tabular";
done;

