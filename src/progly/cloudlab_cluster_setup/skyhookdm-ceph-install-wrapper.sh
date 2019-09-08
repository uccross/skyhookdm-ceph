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

cd $HOME
# get all the scripts and sample data from public repo.
echo "also assumes your cluster ssh generic privkey is avail at absolute path provided as arg (for node to node ssh)."
sleep 5s

# hardcoded vars for now.
pdsw_branch="skyhook-luminous";
repo_dir="/mnt/sda4/"
ansible_dir="${HOME}/skyhook-ansible/ansible/"
echo "clear out prev data dirs and scripts."
scripts_dir="${HOME}/pdsw19-reprod/scripts/"
data_dir="${HOME}/pdsw19-reprod/data/"
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
echo "Client0: building skyhook specific binaries:  ./do_cmake.sh; then make -j38  ceph-osd librados cls_tabular run-query fbwriter...";
echo `date`;
cd $repo_dir/skyhookdm-ceph;
git checkout $pdsw_branch;
export DEBIAN_FRONTEND="noninteractive";
./do_cmake.sh;
cd build;
make -j36  ceph-osd librados cls_tabular run-query fbwriter;


# INSTALLING VANILLA CEPH LUMINOUS on all nodes client+osds
echo `date`;
echo "run ansible playbook to install vanilla ceph luminous on cluster!";
cd $ansible_dir;
time ansible-playbook setup_playbook.yml -vvvv ;
echo `date`;
echo "ansible playbook done.";


echo "Copying our libcls so file to each osd, to the correct dir, and set symlinks";
sudo chmod a-x   $repo_dir/skyhookdm-ceph/build/lib/libcls_tabular.so.1.0.0;
for ((i = 0 ; i < $nosds ; i++)); do
    echo osd$i;
    scp $repo_dir/skyhookdm-ceph/build/lib/libcls_tabular.so.1.0.0  osd$i:/tmp/;
    ssh  osd$i "sudo cp /tmp/libcls_tabular.so.1.0.0 /usr/lib/x86_64-linux-gnu/rados-classes/;";
    ssh  osd$i "cd /usr/lib/x86_64-linux-gnu/rados-classes/; if test -f libcls_tabular.so.1; then sudo  unlink libcls_tabular.so.1; fi";
    ssh  osd$i "cd /usr/lib/x86_64-linux-gnu/rados-classes/; if test -f libcls_tabular.so; then sudo unlink libcls_tabular.so; fi";
    ssh  osd$i "cd /usr/lib/x86_64-linux-gnu/rados-classes/;sudo ln -s libcls_tabular.so.1.0.0 libcls_tabular.so.1;";
    ssh  osd$i "cd /usr/lib/x86_64-linux-gnu/rados-classes/;sudo ln -s libcls_tabular.so.1 libcls_tabular.so;";
done;


echo "on each osd, verify that libcls_tabular.so is present here: /usr/lib/x86_64-linux-gnu/rados-classes/"
for ((i = 0 ; i < $nosds ; i++)); do
    echo "checking for libcls_tabular on osd${i}";
    ssh osd$i "ls -alh /usr/lib/x86_64-linux-gnu/rados-classes/ | grep libcls_tabular";
done;


echo "All done skyhook ansible setup.";
echo `date`;

#~ ### PARALLELIZE Ansible
#~ # see parallel

#~ for i in  $(seq 0  ${max_osd}); do
  #~ echo "osd${i}: calling->ansible-playbook site.yml --limit osds[$i]"
#~ done;
#~ ansible-playbook site.yml --limit client
#~ ansible-playbook site.yml --limit osds[0]
#~ ansible-playbook site.yml --limit osds[1-2]
#~ ansible-playbook site.yml --limit osds[2-3]
