#!/bin/bash
# set  -e

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
max_osd=$((nosds-1))

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
cd $HOME
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
cd $HOME
cp /proj/skyhook-PG0/pdsw19/scripts/* $scripts_dir
cp /proj/skyhook-PG0/pdsw19/data/*  $data_dir

echo "create nodes.txt file for ssh key copy/setup"
cd $HOME
echo "client0" > nodes.txt;
for ((i = 0 ; i < $nosds ; i++)); do
  echo "osd${i}" >> nodes.txt;
done;

echo "Setting up ssh keyless between all machines..."
cp $scripts_dir/cluster_setup_copy_ssh_keys.sh . ;
sh cluster_setup_copy_ssh_keys.sh;

echo "copy scripts to all nodes"
cd $HOME
for n in  `cat nodes.txt`; do
  echo "copy scripts dir to node ${n}";
  scp -r ${scripts_dir}/*  ${n}:~/ ;
done;

echo "setup ansible prereqs via script, and install other small stuff locally...";
bash postreqs.sh;


echo "specify correct num osds for this cluster in ansible hosts file";
cd $HOME
for ((i = 0 ; i < $nosds ; i++)); do
  echo "osd${i}" >> $ansible_dir/hosts;
done;
cp $ansible_dir/hosts $ansible_dir/lib/ceph-deploy-ansible/ansible/hosts;

echo "VERIFY osds all there"
cat $ansible_dir/lib/ceph-deploy-ansible/ansible/hosts;

echo "update the max_osd here /vars/extra_vars.yml  as well."
head -n 4 $ansible_dir/lib/ceph-deploy-ansible/ansible/vars/extra_vars.yml >>   $ansible_dir/lib/ceph-deploy-ansible/ansible/vars/tmp.yml;
echo "num_osds: ${nosds}" >>$ansible_dir/lib/ceph-deploy-ansible/ansible/vars/tmp.yml;
mv $ansible_dir/lib/ceph-deploy-ansible/ansible/vars/tmp.yml $ansible_dir/lib/ceph-deploy-ansible/ansible/vars/extra_vars.yml;
rm -rf $ansible_dir/lib/ceph-deploy-ansible/ansible/vars/tmp.yml;

echo "Format and mount all the devs for skyhookdm-ceph cloned repo building on each client+osd nodes..";
cd $HOME
for n in `cat nodes.txt`; do
  echo $n
  scp $scripts_dir/format-sdX.sh $n:~/
  scp $scripts_dir/mount-sdX.sh $n:~/
  #ssh $n "if test -s /mnt/sda4/skyhookdm-ceph/README; then sudo rm -rf  /mnt/sda4/skyhookdm-ceph/; sudo umount /mnt/sda4; fi;";
  ssh $n "if df -h | grep -q sda4; then echo \"mounted, unmounting\"; sudo umount /mnt/sda4; fi;";
  ssh $n "./format-sdX.sh sda4; ./mount-sdX.sh sda4;" &
done;
echo "Waiting..../format-sdX.sh sda4; ./mount-sdX.sh sda4";
wait;
echo "";
sleep 2s;

echo "visually  verify all sda4 mounted at /mnt/sda4.";
cd $HOME
for n in `cat nodes.txt`; do
  echo $n
  ssh $n "df -h | grep sda4;";
done;
sleep 3s;

############### INSTALL OUR SKYHOOK DEPS ON ALL NODES
# do not parallelize
echo "on all machines, run apt-get update and install pre-reqs";
cd $HOME
for n in  `cat nodes.txt`; do
    echo $n;
    ssh $n "sudo apt-get update && sudo apt-get install git wget cmake git gnupg -y  > apt-get-install.out 2>&1;" &
done;
echo "Waiting...apt-get update and install pre-reqs";
wait;
echo "";
sleep 2s;

# note this typically takes 1 min per node
echo "on all machines, clone skyhook repo...";
echo `date`;
cd $HOME
for n in  `cat nodes.txt`; do
    echo $n;
    ssh $n "cd /mnt/sda4/; sudo rm -rf skyhookdm-ceph/; git clone https://github.com/uccross/skyhookdm-ceph.git > clone-repo.out 2>&1;" &
done;
echo "Waiting... clone-repo.sh";
wait;
echo "";
sleep 2s;


# note this takes about 1 min per node as well.
echo "on all machines, , run install deps...";
echo `date`;
cd $HOME
for n in  `cat nodes.txt`; do
    echo $n;
    ssh $n  "cd /mnt/sda4/skyhookdm-ceph;  export DEBIAN_FRONTEND=noninteractive; git checkout $pdsw_branch > git-checkout.out 2>&1; git submodule update --init --recursive  > submodule-init.out 2>&1; sudo ./install-deps.sh  > install-deps.out 2>&1 "  &
done;
echo "Waiting... install-deps.sh on all nodes";
wait;
echo "";
sleep 2s;


###MAKE SKYHOOK SPECIFIC BINARIES BEFORE INSTALLING VANILLA CEPH LUMINOUS
#~ echo "Client0: apt-get update, install basic stuff, clone skyhookdm-ceph...";
#~ if test -s $repo_dir/skyhookdm-ceph/README; then echo "removing ${repo_dir}/skyhookdm-ceph"; sudo rm -rf $repo_dir/skyhookdm-ceph/; fi
echo "Client0: building skyhook specific binaries:  ./do_cmake.sh; make -j36 cls_tabular run-query fbwriter...";
echo `date`;
cd $repo_dir/skyhookdm-ceph;
git checkout $pdsw_branch;
export DEBIAN_FRONTEND="noninteractive";
./do_cmake.sh;
cd build;
# make -j36  ceph-osd librados cls_tabular run-query fbwriter;
make -j36 cls_tabular run-query fbwriter;




# INSTALLING VANILLA CEPH LUMINOUS on all nodes client+osds
echo `date`;
echo "run ansible playbook to install vanilla ceph luminous on cluster!";
cd $ansible_dir;
time ansible-playbook setup_playbook.yml -vvvv ;
echo `date`;
echo "ansible playbook done.";


#~ echo "Copying our libcls so file to each osd, to the correct dir, and set symlinks";
#~ sudo chmod a-x   $repo_dir/skyhookdm-ceph/build/lib/libcls_tabular.so.1.0.0;
#~ for ((i = 0 ; i < $nosds ; i++)); do
    #~ echo osd$i;
    #~ scp $repo_dir/skyhookdm-ceph/build/lib/libcls_tabular.so.1.0.0  osd$i:/tmp/;
    #~ ssh  osd$i "sudo cp /tmp/libcls_tabular.so.1.0.0 /usr/lib/x86_64-linux-gnu/rados-classes/;";
    #~ ssh  osd$i "cd /usr/lib/x86_64-linux-gnu/rados-classes/; if test -f libcls_tabular.so.1; then sudo  unlink libcls_tabular.so.1; fi";
    #~ ssh  osd$i "cd /usr/lib/x86_64-linux-gnu/rados-classes/; if test -f libcls_tabular.so; then sudo unlink libcls_tabular.so; fi";
    #~ ssh  osd$i "cd /usr/lib/x86_64-linux-gnu/rados-classes/;sudo ln -s libcls_tabular.so.1.0.0 libcls_tabular.so.1;";
    #~ ssh  osd$i "cd /usr/lib/x86_64-linux-gnu/rados-classes/;sudo ln -s libcls_tabular.so.1 libcls_tabular.so;";
#~ done;


echo "on each osd, verify that libcls_tabular.so is present here: /usr/lib/x86_64-linux-gnu/rados-classes/"
for ((i = 0 ; i < $nosds ; i++)); do
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
lineitem_10MB_objfilename_base="lineitem.10MB.75Krows.obj.0";
lineitem_100MB_objfilename_base="lineitem.100MB.750Krows.obj.0";
lineitem_75Kcsv="lineitem.10MB.objs.75Krows.csv";
lineitem_750Kcsv="lineitem.100MB.objs.750Krows.csv";
touch $lineitem_750Kcsv;
echo "lineitem: generating 750K rows csv by 10x concat of our 75K row csv..."
for ((i = 0 ; i < 10 ; i++)); do
    cat $lineitem_75Kcsv >> $lineitem_750Kcsv;
done;


./bin/fbwriter --file_name $lineitem_75Kcsv  --schema_file_name lineitem.schema.txt  --num_objs 1 --flush_rows 75000  --read_rows 75000  --csv_delim "|" --use_hashing true --rid_start_value 1 --table_name lineitem --input_oid 0  --obj_type SFT_FLATBUF_FLEX_ROW;
mv fbmeta.Skyhook.v2.SFT_FLATBUF_FLEX_ROW.lineitem.0.1-1  "fbx.${lineitem_10MB_objfilename_base}";


./bin/fbwriter --file_name $lineitem_750Kcsv --schema_file_name lineitem.schema.txt  --num_objs 1 --flush_rows 750000 --read_rows 750000 --csv_delim "|" --use_hashing true --rid_start_value 1 --table_name lineitem --input_oid 0  --obj_type SFT_FLATBUF_FLEX_ROW;
mv fbmeta.Skyhook.v2.SFT_FLATBUF_FLEX_ROW.lineitem.0.1-1 "fbx.${lineitem_100MB_objfilename_base}";


# create ncols100 obj data for 10 and 100 MB sizes and rename obj to simple name.
ncols100_10MB_objfilename_base="ncols100.10MB.25Krows.obj.0";
ncols100_100MB_objfilename_base="ncols100.100MB.250Krows.obj.0";
ncols100_25Kcsv="ncols100.10MB.objs.25Krows.csv";
ncols100_250Kcsv="ncols100.100MB.objs.250Krows.csv";
touch $ncols100_250Kcsv;
echo "nocls100: generating 250K rows csv by 10x concat of our 25K row csv..."
for ((i = 0 ; i < 10 ; i++)); do
    cat $ncols100_25Kcsv >> $ncols100_250Kcsv;
done;


./bin/fbwriter --file_name ncols100.10MB.objs.25Krows.csv   --schema_file_name ncols100.schema.txt  --num_objs 1 --flush_rows 25000  --read_rows 25000  --csv_delim "|" --use_hashing true --rid_start_value 1 --table_name ncols100 --input_oid 0  --obj_type SFT_FLATBUF_FLEX_ROW;
mv fbmeta.Skyhook.v2.SFT_FLATBUF_FLEX_ROW.ncols100.0.1-1 "fbx.${ncols100_10MB_objfilename_base}";

./bin/fbwriter --file_name ncols100.100MB.objs.250Krows.csv   --schema_file_name ncols100.schema.txt  --num_objs 1 --flush_rows 250000  --read_rows 250000  --csv_delim "|" --use_hashing true --rid_start_value 1 --table_name ncols100 --input_oid 0  --obj_type SFT_FLATBUF_FLEX_ROW;
mv fbmeta.Skyhook.v2.SFT_FLATBUF_FLEX_ROW.ncols100.0.1-1 "fbx.${ncols100_100MB_objfilename_base}";


pool="tpchdata"
rados mkpool $pool
#############################################################
#####  lineitem: convert fbx obj 10MB to ARROW, retrieve arrow obj.
#############################################################
echo "lineitem: convert fbx obj 10MB to ARROW, retrieve arrow obj."
#set -e
objfile="fbx.${lineitem_10MB_objfilename_base}"
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
cd $repo_dir/skyhookdm-ceph/build/;
###############
###############  TRANSFORM  LINEITEM fbx obj to arrow obj
./bin/run-query --num-objs 1 --pool  $pool   --wthreads 1 --qdepth 1  --transform-db --transform-format-type arrow --data-schema "0 3 1 0 ORDERKEY ; 1 3 0 1 PARTKEY ; 2 3 0 1 SUPPKEY ; 3 3 1 0 LINENUMBER ; 4 12 0 1 QUANTITY ; 5 13 0 1 EXTENDEDPRICE ; 6 12 0 1 DISCOUNT ; 7 13 0 1 TAX ; 8 9 0 1 RETURNFLAG ; 9 9 0 1 LINESTATUS ; 10 14 0 1 SHIPDATE ; 11 14 0 1 COMMITDATE ; 12 14 0 1 RECEIPTDATE ; 13 15 0 1 SHIPINSTRUCT ; 14 15 0 1 SHIPMODE ; 15 15 0 1 COMMENT" --use-cls  --quiet
###############
###############  RETRIEVE LINEITEM arrow  obj
objfile="arrow.${lineitem_10MB_objfilename_base}"
rados get --pool $pool obj.0  $objfile
#############################################################
#############################################################


#############################################################
#####  lineitem: convert fbx obj 100MB to ARROW, retrieve arrow obj.
#############################################################
echo "lineitem: convert fbx obj 100MB to ARROW, retrieve arrow obj."
#set -e
objfile="fbx.${lineitem_100MB_objfilename_base}"
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
cd $repo_dir/skyhookdm-ceph/build/;
###############
###############  TRANSFORM  LINEITEM fbx obj to arrow obj
./bin/run-query --num-objs 1 --pool  $pool   --wthreads 1 --qdepth 1  --transform-db --transform-format-type arrow --data-schema "0 3 1 0 ORDERKEY ; 1 3 0 1 PARTKEY ; 2 3 0 1 SUPPKEY ; 3 3 1 0 LINENUMBER ; 4 12 0 1 QUANTITY ; 5 13 0 1 EXTENDEDPRICE ; 6 12 0 1 DISCOUNT ; 7 13 0 1 TAX ; 8 9 0 1 RETURNFLAG ; 9 9 0 1 LINESTATUS ; 10 14 0 1 SHIPDATE ; 11 14 0 1 COMMITDATE ; 12 14 0 1 RECEIPTDATE ; 13 15 0 1 SHIPINSTRUCT ; 14 15 0 1 SHIPMODE ; 15 15 0 1 COMMENT" --use-cls  --quiet
###############
###############  RETRIEVE LINEITEM arrow  obj
objfile="arrow.${lineitem_100MB_objfilename_base}"
rados get --pool $pool obj.0  $objfile
#############################################################
#############################################################





pool="ncols100"
rados mkpool $pool
#############################################################
#####  ncols100: convert fbx obj 10MB to ARROW, retrieve arrow obj.
#############################################################
echo " ncols100: convert fbx obj 10MB to ARROW, retrieve arrow obj."
# set -e
objfile="fbx.${ncols100_10MB_objfilename_base}"
pool="ncols100"
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
###############
###############  TRANSFORM  NCOLS100 fbx obj to arrow obj
cd $repo_dir/skyhookdm-ceph/build/;
./bin/run-query --num-objs 1 --pool $pool   --wthreads 1 --qdepth 1  --transform-db --transform-format-type arrow --data-schema "0 8 1 0 ATT0; 1 8 0 0 ATT1; 2 8 0 0 ATT2; 3 8 0 0 ATT3; 4 8 0 0 ATT4; 5 8 0 0 ATT5; 6 8 0 0 ATT6; 7 8 0 0 ATT7; 8 8 0 0 ATT8; 9 8 0 0 ATT9; 10 8 0 0 ATT10; 11 8 0 0 ATT11; 12 8 0 0 ATT12; 13 8 0 0 ATT13; 14 8 0 0 ATT14; 15 8 0 0 ATT15; 16 8 0 0 ATT16; 17 8 0 0 ATT17; 18 8 0 0 ATT18; 19 8 0 0 ATT19; 20 8 0 0 ATT20; 21 8 0 0 ATT21; 22 8 0 0 ATT22; 23 8 0 0 ATT23; 24 8 0 0 ATT24; 25 8 0 0 ATT25; 26 8 0 0 ATT26; 27 8 0 0 ATT27; 28 8 0 0 ATT28; 29 8 0 0 ATT29; 30 8 0 0 ATT30; 31 8 0 0 ATT31; 32 8 0 0 ATT32; 33 8 0 0 ATT33; 34 8 0 0 ATT34; 35 8 0 0 ATT35; 36 8 0 0 ATT36; 37 8 0 0 ATT37; 38 8 0 0 ATT38; 39 8 0 0 ATT39; 40 8 0 0 ATT40; 41 8 0 0 ATT41; 42 8 0 0 ATT42; 43 8 0 0 ATT43; 44 8 0 0 ATT44; 45 8 0 0 ATT45; 46 8 0 0 ATT46; 47 8 0 0 ATT47; 48 8 0 0 ATT48; 49 8 0 0 ATT49; 50 8 0 0 ATT50; 51 8 0 0 ATT51; 52 8 0 0 ATT52; 53 8 0 0 ATT53; 54 8 0 0 ATT54; 55 8 0 0 ATT55; 56 8 0 0 ATT56; 57 8 0 0 ATT57; 58 8 0 0 ATT58; 59 8 0 0 ATT59; 60 8 0 0 ATT60; 61 8 0 0 ATT61; 62 8 0 0 ATT62; 63 8 0 0 ATT63; 64 8 0 0 ATT64; 65 8 0 0 ATT65; 66 8 0 0 ATT66; 67 8 0 0 ATT67; 68 8 0 0 ATT68; 69 8 0 0 ATT69; 70 8 0 0 ATT70; 71 8 0 0 ATT71; 72 8 0 0 ATT72; 73 8 0 0 ATT73; 74 8 0 0 ATT74; 75 8 0 0 ATT75; 76 8 0 0 ATT76; 77 8 0 0 ATT77; 78 8 0 0 ATT78; 79 8 0 0 ATT79; 80 8 1 0 ATT80; 81 8 0 0 ATT81; 82 8 0 0 ATT82; 83 8 0 0 ATT83; 84 8 0 0 ATT84; 85 8 0 0 ATT85; 86 8 0 0 ATT86; 87 8 0 0 ATT87; 88 8 0 0 ATT88; 89 8 0 0 ATT89; 90 8 0 0 ATT90; 91 8 0 0 ATT91; 92 8 0 0 ATT92; 93 8 0 0 ATT93; 94 8 0 0 ATT94; 95 8 0 0 ATT95; 96 8 0 0 ATT96; 97 8 0 0 ATT97; 98 8 0 0 ATT98; 99 8 0 0 ATT99;" --use-cls
###############
###############  RETRIEVE NCOLS100  arrow  obj
objfile="arrow.${ncols100_10MB_objfilename_base}"
rados get --pool $pool obj.0  $objfile
#############################################################
#############################################################


#############################################################
#####  ncols100: convert fbx obj 100MB to ARROW, retrieve arrow obj.
#############################################################
echo " ncols100: convert fbx obj 100MB to ARROW, retrieve arrow obj."
# set -e
objfile="fbx.${ncols100_100MB_objfilename_base}"
pool="ncols100"
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
###############
###############  TRANSFORM  NCOLS100 fbx obj to arrow obj
cd $repo_dir/skyhookdm-ceph/build/;
./bin/run-query --num-objs 1 --pool $pool   --wthreads 1 --qdepth 1  --transform-db --transform-format-type arrow --data-schema "0 8 1 0 ATT0; 1 8 0 0 ATT1; 2 8 0 0 ATT2; 3 8 0 0 ATT3; 4 8 0 0 ATT4; 5 8 0 0 ATT5; 6 8 0 0 ATT6; 7 8 0 0 ATT7; 8 8 0 0 ATT8; 9 8 0 0 ATT9; 10 8 0 0 ATT10; 11 8 0 0 ATT11; 12 8 0 0 ATT12; 13 8 0 0 ATT13; 14 8 0 0 ATT14; 15 8 0 0 ATT15; 16 8 0 0 ATT16; 17 8 0 0 ATT17; 18 8 0 0 ATT18; 19 8 0 0 ATT19; 20 8 0 0 ATT20; 21 8 0 0 ATT21; 22 8 0 0 ATT22; 23 8 0 0 ATT23; 24 8 0 0 ATT24; 25 8 0 0 ATT25; 26 8 0 0 ATT26; 27 8 0 0 ATT27; 28 8 0 0 ATT28; 29 8 0 0 ATT29; 30 8 0 0 ATT30; 31 8 0 0 ATT31; 32 8 0 0 ATT32; 33 8 0 0 ATT33; 34 8 0 0 ATT34; 35 8 0 0 ATT35; 36 8 0 0 ATT36; 37 8 0 0 ATT37; 38 8 0 0 ATT38; 39 8 0 0 ATT39; 40 8 0 0 ATT40; 41 8 0 0 ATT41; 42 8 0 0 ATT42; 43 8 0 0 ATT43; 44 8 0 0 ATT44; 45 8 0 0 ATT45; 46 8 0 0 ATT46; 47 8 0 0 ATT47; 48 8 0 0 ATT48; 49 8 0 0 ATT49; 50 8 0 0 ATT50; 51 8 0 0 ATT51; 52 8 0 0 ATT52; 53 8 0 0 ATT53; 54 8 0 0 ATT54; 55 8 0 0 ATT55; 56 8 0 0 ATT56; 57 8 0 0 ATT57; 58 8 0 0 ATT58; 59 8 0 0 ATT59; 60 8 0 0 ATT60; 61 8 0 0 ATT61; 62 8 0 0 ATT62; 63 8 0 0 ATT63; 64 8 0 0 ATT64; 65 8 0 0 ATT65; 66 8 0 0 ATT66; 67 8 0 0 ATT67; 68 8 0 0 ATT68; 69 8 0 0 ATT69; 70 8 0 0 ATT70; 71 8 0 0 ATT71; 72 8 0 0 ATT72; 73 8 0 0 ATT73; 74 8 0 0 ATT74; 75 8 0 0 ATT75; 76 8 0 0 ATT76; 77 8 0 0 ATT77; 78 8 0 0 ATT78; 79 8 0 0 ATT79; 80 8 1 0 ATT80; 81 8 0 0 ATT81; 82 8 0 0 ATT82; 83 8 0 0 ATT83; 84 8 0 0 ATT84; 85 8 0 0 ATT85; 86 8 0 0 ATT86; 87 8 0 0 ATT87; 88 8 0 0 ATT88; 89 8 0 0 ATT89; 90 8 0 0 ATT90; 91 8 0 0 ATT91; 92 8 0 0 ATT92; 93 8 0 0 ATT93; 94 8 0 0 ATT94; 95 8 0 0 ATT95; 96 8 0 0 ATT96; 97 8 0 0 ATT97; 98 8 0 0 ATT98; 99 8 0 0 ATT99;" --use-cls
###############
###############  RETRIEVE NCOLS100  arrow  obj
objfile="arrow.${ncols100_100MB_objfilename_base}"
rados get --pool $pool obj.0  $objfile
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


