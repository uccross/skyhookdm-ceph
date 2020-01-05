#!/bin/bash
# set  -e

if [ $# -le 1 ]
then
  echo "Usage:"
  echo "./this <number-of-osds> <path-to-your-id_rsa> <ubuntu|centos>"
  echo "forcing program exit..."
  exit 1
else
  echo "using nosds as '${1}'"
  echo "using sshkeypath as '${2}'"
  if [ -z $3 ]
        then os="ubuntu"
    else
        os=$3
  fi
fi

if [ $os != "ubuntu" ]
    then
        if [ $os != "centos" ]
            then
                echo "invalid OS.  select 'ubuntu' or 'centos' default=ubuntu"
                exit 1
        else
            os="centos"
        fi
fi
echo "using OS as '${os}'"
nosds=$1
sshkeypath=$2
max_osd=$((nosds-1))
pkgs="git x11-apps screen curl python nano scite vim x11-apps tmux dstat wget cmake ccache gnupg python-pip python3 python-virtualenv gcc g++"
ANSIBLE_VER="2.5.1"
REPO_DISK="sda4"

# prep for postgres installation later.
sudo su -c "useradd postgres" ;

# set OS specific vars
if [ $os = "ubuntu" ]
    then echo "ubuntu confirmed"
    pkgmgr="apt-get"
    INSTALLBUILDTOOLS="${pkgmgr} install build-essential"
    LIB_CLS_DIR=/usr/lib/x86_64-linux-gnu/rados-classes/

    # postgres OS-specific
    sudo su -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ bionic-pgdg main" >  /etc/apt/sources.list.d/pgdg.list' ;
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
    pkgs=${pkgs}" postgresql-10 postgresql-client-10 postgresql-server-dev-10 postgresql-contrib "
fi
if [ $os = "centos" ]
    then echo "centos confirmed"
    pkgmgr="yum"
    INSTALLBUILDTOOLS="${pkgmgr} group install \"Development Tools\""
    LIB_CLS_DIR=/usr/lib64/rados-classes/

    # postgres OS-specific
    wget https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm
    sudo yum install -y pgdg-redhat-repo-latest.noarch.rpm
    pkgs=${pkgs}" postgresql10.x86_64 postgresql10-server postgresql10-contrib postgresql10-libs"
fi

echo `date`
echo "START:"
echo "OS=${os}"
echo "pkgmgr=${pkgmgr}"
echo "INSTALLBUILDTOOLS=${INSTALLBUILDTOOLS}"
echo "LIB_CLS_DIR=${LIB_CLS_DIR}"
echo "pkgs=${pkgs}"
echo "ANSIBLE_VER=${ANSIBLE_VER}"
echo "ANSIBLE USES ceph-deploy-1.5.38 for ceph-luminous, i.e., env/bin/pip install ceph-deploy==1.5.38 and env/bin/ceph-deploy install --release={{ release_name }} {{ item }}"

cd $HOME
# get all the scripts and sample data from public repo.
echo "also assumes your cluster ssh generic privkey is avail at absolute path provided as arg (for node to node ssh)."
sleep 5s

# hardcoded vars for now.
pdsw_branch="skyhook-luminous";
repo_dir="/mnt/${REPO_DISK}/"
ansible_dir="${HOME}/skyhook-ansible/ansible/"
echo "clear out prev data dirs and scripts."
scripts_dir="${HOME}/"
touch nodes.txt
rm nodes.txt

FILE1="${HOME}/copy_ssh_keys.sh"
if test -f "$FILE1"; then
    echo "$FILE1 present, OK."
else
    echo "$FILE1 not present but required."
    exit 1
fi

FILE2="${HOME}/format-sdx.sh"
if test -f "$FILE2"; then
    echo "$FILE2 present, OK."
else
    echo "$FILE2 not present but required."
    exit 1
fi

FILE3="${HOME}/mount-sdx.sh"
if test -f "$FILE3"; then
    echo "$FILE3 present, OK."
else
    echo "$FILE3 not present but required."
    exit 1
fi

# setup common ssh key for all client/osd nodes.
# this should be provided by the user,
# it is the key they use to ssh into their cloudlab profile machines.
echo "setup ssh keys..."
cd $HOME
touch  $HOME/.ssh/id_rsa
cp $HOME/.ssh/id_rsa ~/.ssh/id_rsa.bak
cp $sshkeypath  ~/.ssh/id_rsa
chmod 0600 $HOME/.ssh/id_rsa;

echo "create nodes.txt file for ssh key copy/setup"
cd $HOME
echo "client0" > nodes.txt;
for ((i = 0 ; i < $nosds ; i++)); do
  echo "osd${i}" >> nodes.txt;
done;

echo "Setting up ssh keyless between all machines..."
cp $scripts_dir/copy_ssh_keys.sh . ;
sh copy_ssh_keys.sh;

echo "copy scripts to all nodes"
cd $HOME
for n in  `cat nodes.txt`; do
  echo "copy scripts dir to node ${n}";
  scp -r ${scripts_dir}/*  ${n}:~/ ;
done;

echo "on all machines, run ${pkgmgr} update and install pkgs -- note: yum update takes 30 min on cloudlab...why?";

# INSTALL BUILD TOOLS
cd $HOME
for n in  `cat nodes.txt`; do
    echo $n;
    ssh $n "sudo ${pkgmgr} update -y && sudo ${INSTALLBUILDTOOLS} -y > ${pkgmgr}-INSTALLBUILDTOOLS.out 2>&1;" &
done;
echo "Waiting... ${pkgmgr} update and install INSTALLBUILDTOOLS";
wait;
echo "";
sleep 2s;

# INSTALL PKGS
cd $HOME
for n in  `cat nodes.txt`; do
    echo $n;
    ssh $n "sudo ${pkgmgr} update && sudo ${pkgmgr} install ${pkgs} -y > ${pkgmgr}-install-pkgs.out 2>&1;" &
done;
echo "Waiting... ${pkgmgr} update and install pkgs";
wait;
echo "";
sleep 2s;

#~ echo | sudo apt-add-repository ppa:ansible/ansible ;
#~ sudo apt-get update ;
#~ sudo apt-get install ansible=2.5.1+dfsg-1ubuntu0.1 -y ;

echo "install ansible on the controller host/client0 node only."
echo "remove previous ansible verisons if needed present ..."
sudo ${pkgmgr} remove ansible -y;
sudo pip uninstall -y ansible;
sudo -H pip install ansible==${ANSIBLE_VER}
echo "done sudo pip install ansible==${ANSIBLE_VER} ..."

# pip on ubuntu may install ansible in /usr/local/bin, so link to /usr/bin/
if test -f /usr/local/bin/ansible;
    then  sudo ln -s /usr/local/bin/ansible /usr/bin/ansible;
fi

cd $HOME
git clone https://github.com/uccross/skyhook-ansible.git
cd skyhook-ansible
git checkout pdsw19
git submodule update --init

echo "specify correct num osds for this cluster in ansible hosts file"
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

echo "Format all the repo devs for skyhookdm-ceph cloned repo building on each client+osd nodes..";
cd $HOME
for n in `cat nodes.txt`; do
  echo $n
  scp $scripts_dir/format-sdx.sh $n:~/
  ssh $n "if df -h | grep -q ${REPO_DISK}; then echo \"mounted, unmounting\"; sudo umount /mnt/${REPO_DISK}; fi;";
  ssh $n "./format-sdx.sh ${REPO_DISK};" &
done;
echo "Waiting...  ./format-sdx.sh ${REPO_DISK};";
wait;
echo "";
sleep 2s;

echo "Mount all the repo devs for skyhookdm-ceph cloned repo building on each client+osd nodes..";
cd $HOME
for n in `cat nodes.txt`; do
  echo $n
  scp $scripts_dir/mount-sdx.sh $n:~/
  ssh $n "if df -h | grep -q ${REPO_DISK}; then echo \"already mounted\"; else ./mount-sdx.sh ${REPO_DISK};fi;" &
done;
echo "Waiting...  ./mount-sdx.sh ${REPO_DISK}";
wait;
echo "";
sleep 2s;

echo "visually  verify all devices ${REPO_DISK} mounted at /mnt/${REPO_DISK}";
cd $HOME
for n in `cat nodes.txt`; do
  echo $n
  ssh $n "df -h | grep ${REPO_DISK};";
done;
sleep 3s;

# note this typically takes 1 min per node
echo "on all machines, clone skyhook repo...";
echo `date`;
cd $HOME
for n in  `cat nodes.txt`; do
    echo $n;
    ssh $n "cd ${repo_dir}; sudo rm -rf skyhookdm-ceph/; git clone https://github.com/uccross/skyhookdm-ceph.git > clone-repo.out 2>&1;" &
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
    ssh $n  "cd ${repo_dir}/skyhookdm-ceph;  export DEBIAN_FRONTEND=noninteractive; git checkout $pdsw_branch > git-checkout.out 2>&1; git submodule update --init --recursive  > submodule-init.out 2>&1; sudo ./install-deps.sh  > install-deps.out 2>&1 "  &
done;
echo "Waiting... install-deps.sh on all nodes";
wait;
echo "";
sleep 2s;


###MAKE SKYHOOK SPECIFIC BINARIES BEFORE INSTALLING VANILLA CEPH LUMINOUS
echo "Client0: building skyhook specific binaries:  ./do_cmake.sh; then make cls_tabular run-query sky_tabular_flatflex_writer";
echo `date`;
cd $repo_dir/skyhookdm-ceph;
git checkout $pdsw_branch;
export DEBIAN_FRONTEND="noninteractive";
git submodule update --init --recursive;
sudo ./install-deps.sh;
./do_cmake.sh;
cd build;
make -j40 cls_tabular  run-query  sky_tabular_flatflex_writer;
make -j40 cls_tabular  run-query  sky_tabular_flatflex_writer;

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
    ssh  osd$i "sudo cp /tmp/libcls_tabular.so.1.0.0 ${LIB_CLS_DIR};";
    ssh  osd$i "cd ${LIB_CLS_DIR}; if test -f libcls_tabular.so.1; then sudo  unlink libcls_tabular.so.1; fi";
    ssh  osd$i "cd ${LIB_CLS_DIR}; if test -f libcls_tabular.so; then sudo unlink libcls_tabular.so; fi";
    ssh  osd$i "cd ${LIB_CLS_DIR}; sudo ln -s libcls_tabular.so.1.0.0 libcls_tabular.so.1;";
    ssh  osd$i "cd ${LIB_CLS_DIR}; sudo ln -s libcls_tabular.so.1 libcls_tabular.so;";
done;

echo "on each osd, verify that libcls_tabular.so is present at ${LIB_CLS_DIR}"
for ((i = 0 ; i < $nosds ; i++)); do
    echo "checking for libcls_tabular on osd${i}";
    ssh osd$i "ls -alh ${LIB_CLS_DIR} | grep -i libcls_tabular";
done;

echo "All done skyhook ansible setup.";
echo `date`;

#~ ### PARALLELIZE Ansible
#~ for i in  $(seq 0  ${max_osd}); do
  #~ echo "osd${i}: calling->ansible-playbook site.yml --limit osds[$i]"
#~ done;
#~ ansible-playbook site.yml --limit client
#~ ansible-playbook site.yml --limit osds[0]
#~ ansible-playbook site.yml --limit osds[1-2]
#~ ansible-playbook site.yml --limit osds[2-3]
