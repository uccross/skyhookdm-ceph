# TODO:  fix common ssh key as id_rsa issue.
# a priv key should exist on the client0 already,
# that allows ssh to all other nodes.
## TODO cp /proj/skyhook-PG0/keys/ceph_key ~/.ssh/id_rsa;
# example:
# jlefevre@client0:~/.ssh$ nano id_rsa
#jlefevre@client0:~/.ssh$ chmod 0600 id_rsa


if [ -z "$1" ]
then
  echo "Usage:"
  echo "\$1 is empty"
  echo "please pass num osds in argument \$1"
  echo "forcing program exit..."
  exit 1
else
  echo "\$1 is NOT empty"
  echo "using nosds as '${1}'"
fi


pdsw_branch="skyhook-luminous";
repo_dir="/mnt/sda4/"
ansible_dir="${HOME}/skyhook-ansible/ansible/"

### ${HOME}/pdsw19-reprod/
scripts_dir="${HOME}/scripts/"
data_dir="${HOME}/data/"
rm -rf $scripts_dir/
rm -rf $data_dir/
mkdir -p $scripts_dir
mkdir -p $data_dir

# TODO: number_osds=$1;
number_osds=4

# TODO:
# 1. US: put this scripts dir into a public repo: /proj/skyhook-PG0/pdsw19/scripts/
# 2. US: put this data dir into a public repo: /proj/skyhook-PG0/pdsw19/data/
# 3. git clone that public repo here onto client0


# CHEATING HERE UNTIL ABOVE DONE
cp /proj/skyhook-PG0/pdsw19/scripts/* $scripts_dir
cp /proj/skyhook-PG0/pdsw19/data/*  $data_dir

# setup keys access everywhere
cp $scripts_dir/cluster_setup_copy_ssh_keys.sh . ;

rm nodes.txt
nosds=`expr ${number_osds} - 1`;
echo $nosds;
echo "client0" >> nodes.txt;
for i in  $(seq 0  ${nosds}); do
  echo "osd${i}" >> nodes.txt;
done;
sh cluster_setup_copy_ssh_keys.sh;

# copy scripts to all nodes
for n in  `cat nodes.txt`; do
  echo $n
  scp -r $scripts_dir/*  $n:~/
done;


# setup ansible via script, and install other small stuff locally
for n in `cat nodes.txt`; do
  echo $n
  ssh $n  "bash ./postreqs.sh ;"
done;

# specify correct num osds for this cluster
for i in  $(seq 1  ${nosds}); do
  echo "osd${i}" >> $ansible_dir/hosts;
done;
cp $ansible_dir/hosts $ansible_dir/lib/ceph-deploy-ansible/ansible/hosts


# update the nosds here as well.
head -n 4 $ansible_dir/lib/ceph-deploy-ansible/ansible/vars/extra_vars.yml >>   $ansible_dir/lib/ceph-deploy-ansible/ansible/vars/tmp.yml;
echo "num_osds: " `expr ${nosds} + 1` >>$ansible_dir/lib/ceph-deploy-ansible/ansible/vars/tmp.yml;
mv $ansible_dir/lib/ceph-deploy-ansible/ansible/vars/tmp.yml $ansible_dir/lib/ceph-deploy-ansible/ansible/vars/extra_vars.yml;
rm -rf $ansible_dir/lib/ceph-deploy-ansible/ansible/vars/tmp.yml;

# TODO: parallelize
# create the dir to clone skyhook ceph repo
for n in `cat nodes.txt`; do
  scp $scripts_dir/format-sdX.sh $n:~/
  scp $scripts_dir/mount-sdX.sh $n:~/
  echo $n
  ssh $n "sudo umount /mnt/sda4;"
  ssh $n "./format-sdX.sh sda4; ./mount-sdX.sh sda4;"
done;
#wait


# CLIENT0:
cd $repo_dir
sudo apt-get update;
sudo apt-get install wget cmake git gnupg -y;
git clone https://github.com/uccross/skyhookdm-ceph.git;
cd skyhookdm-ceph;
git checkout $pdsw_branch;

export DEBIAN_FRONTEND="noninteractive";
git submodule update --init --recursive;
sudo ./install-deps.sh;
./do_cmake.sh;
cd build;
make -j36 cls_tabular run-query fbwriter;


# install skyhook ceph on cluster!
cd $ansible_dir

# TODO: modify this:
# 1. comment out building skyhook
time ansible-playbook -K -b setup_playbook.yml -vvvv ;
date;


# on each osd, clone skyhook-ceph since we need to install deps one each node.
cd $repo_dir
for i in  $(seq 0  ${nosds}); do
    scp -r skyhookdm-ceph $n:/mnt/sda4/
    ssh $n  "export DEBIAN_FRONTEND="noninteractive"; cd /mnt/sda4/skyhookdm-ceph; sudo ./install_deps.sh;"
done
#wait

# to each osd, copy our libcls so file to the correct places, and set symlinks
# TODO: fix the hardcoded sda4 device
for i in  $(seq 0  ${nosds}); do
  scp $repo_dir/skyhookdm-ceph/build/lib/libcls_tabular.so.1.0.0 $n: /usr/lib/x86_64-linux-gnu/rados-classes/
  ssh $n "cd /usr/lib/x86_64-linux-gnu/rados-classes/ ; sudo ln -s libcls_tabular.so.1.0.0 libcls_tabular.so.1 ; sudo ln -s libcls_tabular.so.1 libcls_tabular.so ;"
done

# now create some data objects
cd $repo_dir/skyhookdm-ceph/build/;

# get input data schema files
cp /proj/skyhook-PG0/pdsw19/data/*.txt  .;

# get input data csv files.
cp /proj/skyhook-PG0/pdsw19/data/*.csv  .;

# create obj data for 10 and 100 MB sizes and rename obj to simple name.
./bin/fbwriter --file_name lineitem.10MB.objs.75Krows.csv   --schema_file_name lineitem-schema.txt  --num_objs 1 --flush_rows 75000  --read_rows 75000  --csv_delim "|" --use_hashing true --rid_start_value 1 --table_name lineitem --input_oid 0  --obj_type SFT_FLATBUF_FLEX_ROW;
mv fbmeta.Skyhook.v2.SFT_FLATBUF_FLEX_ROW.lineitem.0.1-1 fbx.lineitem.10MB.obj.0
./bin/fbwriter --file_name lineitem.100MB.objs.750Krows.csv --schema_file_name lineitem-schema.txt  --num_objs 1 --flush_rows 750000 --read_rows 750000 --csv_delim "|" --use_hashing true --rid_start_value 1 --table_name lineitem --input_oid 0  --obj_type SFT_FLATBUF_FLEX_ROW;
mv fbmeta.Skyhook.v2.SFT_FLATBUF_FLEX_ROW.lineitem.0.1-1 fbx.lineitem.100MB.obj.0

#############################################################
#####  convert fbx obj 10MB to ARROW, retrieve arrow obj.
#############################################################
objfile="fbx.lineitem.10MB.obj.0"
set -e
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

rados -p $pool get obj.0 arrow.lineitem.10MB.obj.0
#############################################################
#############################################################



#############################################################
#####  convert fbx obj 100MB to ARROW, retrieve arrow obj.
#############################################################
objfile="fbx.lineitem.100MB.obj.0"
set -e
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
rados -p $pool get obj.0 arrow.lineitem.100MB.obj.0
#############################################################
#############################################################

#~ for i in  $(seq 0  ${nosds}); do
  #~ echo "osd${i}: calling->ansible-playbook site.yml --limit osds[$i]"
#~ done;
#~ ansible-playbook site.yml --limit client
#~ ansible-playbook site.yml --limit osds[0]
#~ ansible-playbook site.yml --limit osds[1-2]
#~ ansible-playbook site.yml --limit osds[2-3]


