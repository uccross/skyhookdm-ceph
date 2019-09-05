#!/bin/bash
set  -e

if [ $# -le 1 ]
then
  echo "Usage:"
  echo "./this <number-of-osds> <path-to-your-id_rsa>"
  echo "forcing program exit..."
  exit 1
else
  echo "using nosds as '${1}'"
  echo "using sshkeypath as '${2}'"
fi
nosds=$1
sshkeypath=$2
max_osd=`expr ${nosds} - 1`;

echo "START:"
echo `date`

# hardcoded vars for now.
pdsw_branch="skyhook-luminous";
repo_dir="/mnt/sda4/"
ansible_dir="${HOME}/skyhook-ansible/ansible/"
echo "clear out prev data dirs and scripts."
scripts_dir="${HOME}/scripts/"
data_dir="${HOME}/data/"
rm -rf $scripts_dir/
rm -rf $data_dir/
mkdir -p $scripts_dir
mkdir -p $data_dir
touch nodes.txt
rm nodes.txt
touch postreqs.sh
rm postreqs.sh
touch cluster_setup_copy_ssh_keys.sh
rm cluster_setup_copy_ssh_keys.sh
touch mount-sdX.sh
rm mount-sdX.sh
touch format-sdX.sh
rm format-sdX.sh



# setup common ssh key for all client/osd nodes.
# this should be provided by the user,
# it is the key they use to ssh into their cloudlab profile machines.
echo "setup ssh keys..."
touch  $HOME/.ssh/id_rsa
cp $HOME/.ssh/id_rsa ~/.ssh/id_rsa.bak
cp $sshkeypath  ~/.ssh/id_rsa
chmod 0600 $HOME/.ssh/id_rsa;


# TODO:
# 1. US: put this scripts dir into a public repo: /proj/skyhook-PG0/pdsw19/scripts/
# 2. US: put this data dir into a public repo: /proj/skyhook-PG0/pdsw19/data/


# CHEATING HERE UNTIL ABOVE DONE
# git clone that public repo with scripts here onto client0
echo "retrieving scripts and sample datasets"
cp /proj/skyhook-PG0/pdsw19/scripts/* $scripts_dir
cp /proj/skyhook-PG0/pdsw19/data/*  $data_dir

# setup keys access everywhere
echo "copying cluster_setup_copy_ssh_keys.sh to here."
cp $scripts_dir/cluster_setup_copy_ssh_keys.sh . ;

# create nodes.txt file for ssh key copy/setup
echo $max_osd;
echo "client0" >> nodes.txt;
for i in  $(seq 0  ${max_osd}); do
  echo "osd${i}" >> nodes.txt;
done;

echo "Setting up ssh keyless between all machines..."
sh cluster_setup_copy_ssh_keys.sh;

echo "copy scripts to all nodes"
for n in  `cat nodes.txt`; do
  echo "copy scripts dir to node"$n
  scp -r $scripts_dir/*  $n:~/
done;

echo "setup ansible via script, and install other small stuff locally"
for n in `cat nodes.txt`; do
  echo "executing bash ./postreqs.sh on node"$n
  ssh $n  "bash ./postreqs.sh ;"
done;

echo "specify correct num osds for this cluster in ansible hosts file"
for i in  $(seq 1  ${max_osd}); do
  echo "osd${i}" >> $ansible_dir/hosts;
done;
cp $ansible_dir/hosts $ansible_dir/lib/ceph-deploy-ansible/ansible/hosts

echo "VERIFY osds all there"
cat $ansible_dir/lib/ceph-deploy-ansible/ansible/hosts

echo "update the max_osd here /vars/extra_vars.yml  as well."
head -n 4 $ansible_dir/lib/ceph-deploy-ansible/ansible/vars/extra_vars.yml >>   $ansible_dir/lib/ceph-deploy-ansible/ansible/vars/tmp.yml;
echo "num_osds: " `expr ${max_osd} + 1` >>$ansible_dir/lib/ceph-deploy-ansible/ansible/vars/tmp.yml;
mv $ansible_dir/lib/ceph-deploy-ansible/ansible/vars/tmp.yml $ansible_dir/lib/ceph-deploy-ansible/ansible/vars/extra_vars.yml;
rm -rf $ansible_dir/lib/ceph-deploy-ansible/ansible/vars/tmp.yml;

echo "Format and mount all the devs for skyhookdm-ceph cloned repo building on each client+osd nodes.."
for n in `cat nodes.txt`; do
  scp $scripts_dir/format-sdX.sh $n:~/
  scp $scripts_dir/mount-sdX.sh $n:~/
  echo $n
  ssh $n "if test -f /mnt/sda4/skyhookdm-ceph/README; then sudo rm -rf /mnt/sda4/skyhookdm-ceph/; fi;";
  ssh $n "sudo umount /mnt/sda4;"
  ssh $n "./format-sdX.sh sda4; ./mount-sdX.sh sda4;" &
done;
echo "waiting next.."
wait;
echo "done waiting..."
echo " "
echo "sleeping 1m next ... just to ensure formating all finished on all nodes"

echo "visually  verify all sda4 mounted at /mnt/sda4."
for n in `cat nodes.txt`; do
  echo $n
  ssh $n "df -h | grep sda4;"
done;
echo "All done";
sleep 5;


echo "Client0: apt-get update, install basic stuff, clone skyhookdm-ceph..."
cd $repo_dir
sudo apt-get update;
sudo apt-get install wget cmake git gnupg -y;
git clone https://github.com/uccross/skyhookdm-ceph.git;
cd skyhookdm-ceph;
git checkout $pdsw_branch;
echo "Client0: submodule update, ./install-deps.sh; ./do_cmake.sh; make -j36 cls_tabular run-query fbwriter .."
export DEBIAN_FRONTEND="noninteractive";
git submodule update --init --recursive;
sudo ./install-deps.sh;
./do_cmake.sh;
cd build;
make -j36 cls_tabular run-query fbwriter;

echo `date`;
echo "run ansible playbook to install vanilla ceph luminous on cluster!";
cd $ansible_dir;
time ansible-playbook -K -b setup_playbook.yml -vvvv ;
echo `date`;
echo "ansible playbook done.";

# do not parallelize
echo `date`;
echo "On each osd, clone skyhook-ceph since we need to install deps one each node.";
cd $repo_dir
for i in  $(seq 0  ${max_osd}); do
    echo osd$i;
    ssh osd$i "sudo apt-get update && sudo apt-get install git wget cmake git gnupg -y  > apt-get-install.out 2>&1;"
done;


# note this typically takes 1 min per node
echo `date`;
echo "ssh to each osd, clone skyhook repo...";
for i in  $(seq 0  ${max_osd}); do
    echo osd$i;
    ssh osd$i  "cd /mnt/sda4/; sudo rm -rf skyhookdm-ceph/; git clone https://github.com/uccross/skyhookdm-ceph.git > clone-repo.out 2>&1;" &
done;
echo "Waiting... clone-repo.sh";
wait;
echo "";
echo `date`;
sleep 2s;


# note this takes about 1 min per node as well.
echo `date`;
echo "ssh to each osd, run install deps...";
for i in  $(seq 0  ${max_osd}); do
    echo osd$i;
    ssh osd$i  "cd /mnt/sda4/skyhookdm-ceph;  export DEBIAN_FRONTEND=noninteractive; sudo ./install-deps.sh  > install-deps.out 2>&1 "  &
done;
echo "Waiting... install-deps.sh";
wait;
echo "";
echo `date`;
sleep 2s;


echo "Copying our libcls so file to each osd, to the correct dir, and set symlinks";
sudo chmod a-x   $repo_dir/skyhookdm-ceph/build/lib/libcls_tabular.so.1.0.0
for i in  $(seq 0  ${max_osd}); do
    echo osd$i;
    scp $repo_dir/skyhookdm-ceph/build/lib/libcls_tabular.so.1.0.0  osd$i:/tmp/;
    ssh  osd$i "sudo cp /tmp/libcls_tabular.so.1.0.0 /usr/lib/x86_64-linux-gnu/rados-classes/;";
    ssh  osd$i "cd /usr/lib/x86_64-linux-gnu/rados-classes/; if test -f libcls_tabular.so.1; then sudo  unlink libcls_tabular.so.1; fi";
    ssh  osd$i "cd /usr/lib/x86_64-linux-gnu/rados-classes/; if test -f libcls_tabular.so; then sudo unlink libcls_tabular.so; fi";
    ssh  osd$i "cd /usr/lib/x86_64-linux-gnu/rados-classes/;sudo ln -s libcls_tabular.so.1.0.0 libcls_tabular.so.1;";
    ssh  osd$i "cd /usr/lib/x86_64-linux-gnu/rados-classes/;sudo ln -s libcls_tabular.so.1 libcls_tabular.so;";
done;


echo "on each osd, verify that libcls_tabular.so is present here: /usr/lib/x86_64-linux-gnu/rados-classes/"
for i in  $(seq 0  ${max_osd}); do
echo "checking for libcls_tabular on osd${i}";
ssh osd$i "ls -alh /usr/lib/x86_64-linux-gnu/rados-classes/ | grep libcls_tabular";
done;


echo "All done skyhook ansible setup.";
echo `date`;

# now create some data objects
cd $repo_dir/skyhookdm-ceph/build/;

# get input data schema  and  csv files.
cp $data_dir/*.txt  .;
cp $data_dir/*.csv  .;



#############################################################
#####  create 10MB  and 100MB data for lineitem dataset and ncols100 dataset
#############################################################

# create lineitem obj data for 10 and 100 MB sizes and rename obj to simple name.
lineitem_10MB_fname_base="lineitem.10MB.75Krows.obj.0";
lineitem_100MB_fname_base="lineitem.100MB.750Krows.obj.0";
./bin/fbwriter --file_name lineitem.10MB.objs.75Krows.csv   --schema_file_name lineitem-schema.txt  --num_objs 1 --flush_rows 75000  --read_rows 75000  --csv_delim "|" --use_hashing true --rid_start_value 1 --table_name lineitem --input_oid 0  --obj_type SFT_FLATBUF_FLEX_ROW;
mv fbmeta.Skyhook.v2.SFT_FLATBUF_FLEX_ROW.lineitem.0.1-1  "fbx.${lineitem_10MB_fname_base}";

./bin/fbwriter --file_name lineitem.100MB.objs.750Krows.csv --schema_file_name lineitem-schema.txt  --num_objs 1 --flush_rows 750000 --read_rows 750000 --csv_delim "|" --use_hashing true --rid_start_value 1 --table_name lineitem --input_oid 0  --obj_type SFT_FLATBUF_FLEX_ROW;
mv fbmeta.Skyhook.v2.SFT_FLATBUF_FLEX_ROW.lineitem.0.1-1 "fbx.${lineitem_100MB_fname_base}";


# create ncols100 obj data for 10 and 100 MB sizes and rename obj to simple name.
ncols100_10MB_fname_base="ncols100.10MB.25Krows.obj.0";
ncols100_100MB_fname_base="ncols100.100MB.250Krows.obj.0";
./bin/fbwriter --file_name ncols100.10MB.objs.25Krows.csv   --schema_file_name ncols100.schema.txt  --num_objs 1 --flush_rows 25000  --read_rows 25000  --csv_delim "|" --use_hashing true --rid_start_value 1 --table_name ncols100 --input_oid 0  --obj_type SFT_FLATBUF_FLEX_ROW;
mv fbmeta.Skyhook.v2.SFT_FLATBUF_FLEX_ROW.ncols100.0.1-1 "fbx.${ncols100_10MB_fname_base}";

./bin/fbwriter --file_name ncols100.100MB.objs.250Krows.csv   --schema_file_name ncols100.schema.txt  --num_objs 1 --flush_rows 250000  --read_rows 250000  --csv_delim "|" --use_hashing true --rid_start_value 1 --table_name ncols100 --input_oid 0  --obj_type SFT_FLATBUF_FLEX_ROW;
mv fbmeta.Skyhook.v2.SFT_FLATBUF_FLEX_ROW.ncols100.0.1-1 "fbx.${ncols100_100MB_fname_base}";



#############################################################
#####  lineitem: convert fbx obj 10MB to ARROW, retrieve arrow obj.
#############################################################
set -e
objname="fbx.${lineitem_10MB_fname_base}"
pool="tpchdata"
groupsize=15
nobjs=1
groupsize=$(( groupsize < nobjs ? groupsize : nobjs ))
for ((i=0; i < $nobjs; i+=groupsize)); do
    for ((j=0; j < groupsize; j++)); do
        oid=$(($i+$j))
        if [ "${oid}" -ge "${nobjs}" ]
        then
            break
        fi
        objname="obj.${oid}"
        rados -p $pool rm $objname || true
        echo "writing $objfile into $pool/$objname" &
        rados -p $pool put $objname $objfile &
    done
    wait
done
## DO XFORM to ARROW HERE
cd $repo_dir/skyhookdm-ceph/build/;
/bin/run-query --num-objs 1 --pool ${pool} --wthreads 1 --qdepth 1 --quiet --transform-db --transform-format-type arrow
rados -p $pool get obj.0 "arrow.${lineitem_10MB_fname_base}"
#############################################################
#############################################################



#############################################################
#####  lineitem: convert fbx obj 100MB to ARROW, retrieve arrow obj.
#############################################################
set -e
objname="fbx.${lineitem_100MB_fname_base}"
pool="tpchdata"
groupsize=15
nobjs=1
groupsize=$(( groupsize < nobjs ? groupsize : nobjs ))
for ((i=0; i < $nobjs; i+=groupsize)); do
    for ((j=0; j < groupsize; j++)); do
        oid=$(($i+$j))
        if [ "${oid}" -ge "${nobjs}" ]
        then
            break
        fi
        objname="obj.${oid}"
        rados -p $pool rm $objname || true
        echo "writing $objfile into $pool/$objname" &
        rados -p $pool put $objname $objfile &
    done
    wait
done
## DO XFORM to ARROW HERE
cd $repo_dir/skyhookdm-ceph/build/;
/bin/run-query --num-objs 1 --pool ${pool} --wthreads 1 --qdepth 1 --quiet --transform-db --transform-format-type arrow
rados -p $pool get obj.0 "arrow.${lineitem_100MB_fname_base}"
#############################################################
#############################################################





#############################################################
#####  ncols100: convert fbx obj 10MB to ARROW, retrieve arrow obj.
#############################################################
set -e
objname="fbx.${ncols100_10MB_fname_base}"
pool="tpchdata"
groupsize=15
nobjs=1
groupsize=$(( groupsize < nobjs ? groupsize : nobjs ))
for ((i=0; i < $nobjs; i+=groupsize)); do
    for ((j=0; j < groupsize; j++)); do
        oid=$(($i+$j))
        if [ "${oid}" -ge "${nobjs}" ]
        then
            break
        fi
        objname="obj.${oid}"
        rados -p $pool rm $objname || true
        echo "writing $objfile into $pool/$objname" &
        rados -p $pool put $objname $objfile &
    done
    wait
done
## DO XFORM to ARROW HERE
cd $repo_dir/skyhookdm-ceph/build/;
/bin/run-query --num-objs 1 --pool ${pool} --wthreads 1 --qdepth 1 --quiet --transform-db --transform-format-type arrow
rados -p $pool get obj.0 "arrow.${ncols100_10MB_fname_base}"
#############################################################
#############################################################


#############################################################
#####  ncols100: convert fbx obj 100MB to ARROW, retrieve arrow obj.
#############################################################
set -e
objname="fbx.${ncols100_100MB_fname_base}"
pool="tpchdata"
groupsize=15
nobjs=1
groupsize=$(( groupsize < nobjs ? groupsize : nobjs ))
for ((i=0; i < $nobjs; i+=groupsize)); do
    for ((j=0; j < groupsize; j++)); do
        oid=$(($i+$j))
        if [ "${oid}" -ge "${nobjs}" ]
        then
            break
        fi
        objname="obj.${oid}"
        rados -p $pool rm $objname || true
        echo "writing $objfile into $pool/$objname" &
        rados -p $pool put $objname $objfile &
    done
    wait
done
## DO XFORM to ARROW HERE
cd $repo_dir/skyhookdm-ceph/build/;
/bin/run-query --num-objs 1 --pool ${pool} --wthreads 1 --qdepth 1 --quiet --transform-db --transform-format-type arrow
rados -p $pool get obj.0 "arrow.${ncols100_100MB_fname_base}"
#############################################################
#############################################################



### PARALLELIZE Ansible
# see parallel

#~ for i in  $(seq 0  ${max_osd}); do
  #~ echo "osd${i}: calling->ansible-playbook site.yml --limit osds[$i]"
#~ done;
#~ ansible-playbook site.yml --limit client
#~ ansible-playbook site.yml --limit osds[0]
#~ ansible-playbook site.yml --limit osds[1-2]
#~ ansible-playbook site.yml --limit osds[2-3]


