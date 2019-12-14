#!/bin/bash

# usage: ./copycls <path-to-new-tabular.so-file> <nosds>

if [[ -z $1 && -z $2 ]]; then
	echo "Please provide path to your latest libcls_tabular.so.1.0.0 followed by number of Ceph OSDs to copy and link library."
	echo "Assumes OSD hostnames are of the form `osd$n` ".
exit
fi

#libclsfile=skyhookdm-ceph/build/lib/libcls_tabular.so.1.0.0;

libclsfile=$1
nosds=$2
echo "Copying our libcls so file to each osd, to the correct dir, and set symlinks";
sudo chmod a-x  $libclsfile 
for ((i=0;i<nosds;i++)); do
   echo "$i";
    echo osd$i;
    scp $libclsfile  osd$i:/tmp/;
    ssh  osd$i "sudo cp /tmp/libcls_tabular.so.1.0.0 /usr/lib/x86_64-linux-gnu/rados-classes/;";
    ssh  osd$i "cd /usr/lib/x86_64-linux-gnu/rados-classes/; if test -f libcls_tabular.so; then sudo unlink libcls_tabular.so; fi";
    ssh  osd$i "cd /usr/lib/x86_64-linux-gnu/rados-classes/; if test -f libcls_tabular.so.1; then sudo unlink libcls_tabular.so.1; fi";
    ssh  osd$i "cd /usr/lib/x86_64-linux-gnu/rados-classes/; if test -f libcls_tabular.so; then sudo unlink libcls_tabular.so; fi";
    ssh  osd$i "cd /usr/lib/x86_64-linux-gnu/rados-classes/; if test -f libcls_tabular.so.1; then sudo unlink libcls_tabular.so.1; fi";
    ssh  osd$i "cd /usr/lib/x86_64-linux-gnu/rados-classes/;sudo ln -s libcls_tabular.so.1.0.0 libcls_tabular.so.1;";
    ssh  osd$i "cd /usr/lib/x86_64-linux-gnu/rados-classes/;sudo ln -s libcls_tabular.so.1 libcls_tabular.so;";
done;




