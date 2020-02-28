#!/bin/bash
# set  -e

# This script will install Ceph luminous onto the current node (but no osd storage here)
# And onto each of the other nodes that will serve as OSDs with disk storage.


if [ $# -le 1 ]
then
  echo "Usage:"
  echo "./this <number-of-osds> <yourclusterkey.id_rsa> <ubuntu|centos> <storage-device>"
  echo "Ex:"
  echo "./this 8 clusterkey.id_rsa ubuntu sdb"
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
pkgs="git x11-apps screen curl python nano scite vim x11-apps tmux dstat wget cmake ccache gnupg python-pip python3 python-virtualenv gcc g++ bzip2 libzip"
CEPH_VER="luminous"
STORAGE_DEVICE=$4
REPO_DIR="/mnt/${STORAGE_DEVICE}/"

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
echo "Using ceph-deploy-1.5.38 for ceph-luminous, i.e., env/bin/pip install ceph-deploy==1.5.38 and env/bin/ceph-deploy install --release=${CEPH_VER} hostname"

cd ${HOME}
# get all the scripts and sample data from public repo.
echo "Requires <4GB of space in your HOME dir to build some Ceph libs locally."
echo "Installing Ceph ${CEPH_VER}, note assumes your cluster ssh generic privkey is avail in current dir, will replace your .ssh/id.rsa with this key, and move your current id.rsa to id.rsa.bak. "
echo "!!WARNING!!"
echo "!!WARNING!!"
echo "!!WARNING!!: this will clobber this entire device: ${STORAGE_DEVICE} on each machine.  Please cancel now if needed..."
echo "!!WARNING!!"
echo "!!WARNING!!"
sleep 20s

# hardcoded vars for now.
BRANCH="skyhook-luminous";
echo "clear out prev skyhookdm repo dir and nodes.txt file."
scripts_dir="${HOME}/"
touch nodes.txt
rm nodes.txt

# setup common ssh key for all client/osd nodes.
# this should be provided by the user,
# it is the key they use to ssh into their other machines.
echo "setup ssh keys..."
cd ${HOME}
touch  ${HOME}/.ssh/id_rsa
cp ${HOME}/.ssh/id_rsa ~/.ssh/id_rsa.bak
cp ${sshkeypath}  ~/.ssh/id_rsa
chmod 0600 ${HOME}/.ssh/id_rsa;

echo "create nodes.txt file for ssh key copy/setup"
cd ${HOME}
echo "client0" > nodes.txt;
for ((i = 0 ; i < ${nosds} ; i++)); do
  echo "osd${i}" >> nodes.txt;
done;

echo "Setting up ssh keyless between all machines..."
echo "assumes there is a local file 'nodes.txt' with the correct node names of all machines:clientX through osdX"
echo "assumes this node has its ~/.ssh/id_rsa key present and permissions are 0600"
echo "will copy ssh keys and known host signatures to the following nodes in 10 seconds:"
cat nodes.txt
sleep 10

for node in `cat nodes.txt`; do
  ssh-keyscan -H ${node} >> ~/.ssh/known_hosts
done;

for node in `cat nodes.txt`; do
  ssh ${node} 'hostname'
  scp -p  ~/.ssh/id_rsa ${node}:~/.ssh/
  scp -p  ~/.ssh/known_hosts ${node}:~/.ssh/

done;

# INSTALL BUILD TOOLS
cd ${HOME}
for node in  `cat nodes.txt`; do
    echo ${node};
    ssh ${node} "sudo ${pkgmgr} update -y && sudo ${INSTALLBUILDTOOLS} -y > ${pkgmgr}-INSTALLBUILDTOOLS.out 2>&1;" &
done;
echo "Waiting... ${pkgmgr} update and install INSTALLBUILDTOOLS";
wait;
echo "";
sleep 2s;

# INSTALL PKGS
cd ${HOME}
for node in  `cat nodes.txt`; do
    echo ${node};
    ssh ${node} "sudo ${pkgmgr} update && sudo ${pkgmgr} install ${pkgs} -y > ${pkgmgr}-install-pkgs.out 2>&1;" &
done;
echo "Waiting... ${pkgmgr} update and install pkgs";
wait;
echo "";
sleep 2s;

# format storage device on all nodes, including client0
echo "on all machines, format storage device and create repo dir...";
echo `date`;
cd ${HOME}
for node in  `cat nodes.txt`; do
    echo ${node};
    ssh ${node} "if df -h | grep -q ${STORAGE_DEVICE}; then echo \"mounted, unmounting\"; sudo umount /mnt/${STORAGE_DEVICE}; fi;";
    ssh ${node}  "yes | sudo mkfs -t ext4 /dev/${STORAGE_DEVICE}; sudo mkdir -p ${REPO_DIR}; " &
done;
echo "Waiting... formatting storage device for repository dir";
wait;
echo "";
sleep 2s;


# note this typically takes 1 min per node
echo "on all machines, clone skyhook repo...";
echo `date`;
cd ${HOME}
for node in  `cat nodes.txt`; do
    echo ${node};
    ssh ${node} "sudo mount /dev/${STORAGE_DEVICE} ${REPO_DIR}; sudo chown -R ${USER} ${REPO_DIR};"
    ssh ${node} "cd ${REPO_DIR}; sudo rm -rf skyhookdm-ceph/; git clone https://github.com/uccross/skyhookdm-ceph.git > clone-repo.out 2>&1;" &
done;
echo "Waiting... clone-repo.sh";
wait;
echo "";
sleep 2s;

# note this takes about 1 min per node as well.
echo "on all machines, run install deps...";
echo `date`;
cd ${HOME}
for node in  `cat nodes.txt`; do
    echo ${node};
    ssh ${node}  "cd ${REPO_DIR}/skyhookdm-ceph; export DEBIAN_FRONTEND=noninteractive; git checkout ${BRANCH} > git-checkout.out 2>&1; git submodule update --init --recursive  > submodule-init.out 2>&1; sudo ./install-deps.sh  > install-deps.out 2>&1 "  &
done;
echo "Waiting... install-deps.sh on all nodes";
wait;
echo "";
sleep 2s;

echo "INSTALL CEPH LUMINOUS on all nodes VIA CEPH-DEPLOY STEPS"
mkdir -p ${HOME}/cluster
cd ${HOME}/cluster
virtualenv env
env/bin/pip  install ceph-deploy==1.5.38
for node in  `cat ${HOME}/nodes.txt`; do
    echo ${node};
    env/bin/ceph-deploy install --release=${CEPH_VER} ${node}
done;

# CREATE A NEW CLUSTER default name=ceph; also this creates ceph.conf locally
cd ${HOME}/cluster
env/bin/ceph-deploy new client0
sudo chmod 0777 ceph.conf
# append this to /etc/ceph/ceph.conf
echo "
osd pool default size = 3
osd pool default min size = 1
osd crush chooseleaf type = 0
osd pool default pg num = 32
osd pool default pgp num = 32
mon_allow_pool_delete = true
osd_class_load_list = *
osd_class_default_list = *
objecter_inflight_op_bytes = 2147483648
enable experimental unrecoverable data corrupting features = *

[osd]
osd max write size = 250 #The maximum size of a write in megabytes.
osd max object size = 256000000 #The maximum size of a RADOS object in bytes.
debug ms = 1
debug osd = 25
debug objecter = 20
debug monc = 20
debug mgrc = 20
debug journal = 20
debug filestore = 20
debug bluestore = 30
debug bluefs = 20
debug rocksdb = 10
debug bdev = 20
debug rgw = 20
debug reserver = 10
debug objclass = 20
" >> ceph.conf

cd ${HOME}/cluster
env/bin/ceph-deploy gatherkeys client0
sudo chmod 0766 *.keyring
sudo cp *.keyring /etc/ceph/
env/bin/ceph-deploy mon create-initial
env/bin/ceph-deploy mgr create client0
env/bin/ceph-deploy admin client0

echo "Unmounting the data storage devs for ceph on osd nodes only..";
cd ${HOME}
for ((i = 0 ; i < ${nosds} ; i++)); do
    echo osd${i};
    ssh osd${i} "if df -h | grep -q ${STORAGE_DEVICE}; then echo \"mounted, unmounting\"; sudo umount /mnt/${STORAGE_DEVICE}; fi;";
   ## no need to mkfs since we are now zapping ods
   ##  ssh osd${i} "yes | sudo mkfs -t ext4 /dev/${STORAGE_DEVICE};" &
   # clear out any residue from first ceph partition (typically first 100MB-2GB)
   # problem seems unique to 12.2 luminous
   # https://tracker.ceph.com/issues/22354
   ssh osd${i} "sudo dd if=/dev/zero of=${STORAGE_DEVICE} bs=1M count=2048;"
done;
echo "";
sleep 2s;

# CREATE CEPH OSDS
cd ${HOME}/cluster
for ((i = 0 ; i < ${nosds} ; i++)); do
    echo osd${i};
    env/bin/ceph-deploy disk zap osd${i}:/dev/${STORAGE_DEVICE}
    env/bin/ceph-deploy osd create osd${i}:/dev/${STORAGE_DEVICE}
done;

# force push any new config changes
cd ${HOME}/cluster
for ((i = 0 ; i < ${nosds}; i++)); do
    echo osd${i};
    env/bin/ceph-deploy --overwrite-conf  config push osd${i}
    env/bin/ceph-deploy osd list osd${i}
done;

# Get keys and set perms to allow other than root to connect to cluster.  Do these gather into the local dir or /etc/ceph?
# rm *.keyring
cd ${HOME}/cluster
#env/bin/ceph-deploy forgetkeys
env/bin/ceph-deploy gatherkeys client0
sudo cp *.keyring /etc/ceph/
sudo chmod 0766 /etc/ceph/*.keyring

###MAKE SKYHOOK SPECIFIC BINARIES BEFORE INSTALLING VANILLA CEPH LUMINOUS
echo "Client0: building skyhook specific binaries:  ./do_cmake.sh; then make cls_tabular run-query";
echo `date`;
cd ${REPO_DIR}/skyhookdm-ceph/;
git checkout ${BRANCH};
export DEBIAN_FRONTEND="noninteractive";
git submodule update --init --recursive;
sudo ./install-deps.sh;
./do_cmake.sh;
cd build;
make -j2 cls_tabular;
make -j10 run-query;
make -j10 sky_tabular_flatflex_writer;
echo `date`

echo "Copying our libcls so file to each osd, to the correct dir, and set symlinks";
for node in  `cat ${HOME}/nodes.txt`; do
    echo ""
    echo ${node};
    scp ${REPO_DIR}/skyhookdm-ceph/build/lib/libcls_tabular.so.1.0.0  ${node}:/tmp/;
    ssh ${node} "sudo cp /tmp/libcls_tabular.so.1.0.0 ${LIB_CLS_DIR};";
    ssh ${node} "cd ${LIB_CLS_DIR}; if test -f libcls_tabular.so; then sudo unlink libcls_tabular.so; fi";
    ssh ${node} "cd ${LIB_CLS_DIR}; if test -f libcls_tabular.so.1; then sudo  unlink libcls_tabular.so.1; fi";
    ssh ${node} "cd ${LIB_CLS_DIR}; sudo ln -s libcls_tabular.so.1.0.0 libcls_tabular.so.1;";
    ssh ${node} "cd ${LIB_CLS_DIR}; sudo ln -s libcls_tabular.so.1 libcls_tabular.so;";
done;

echo "on each osd, verify that libcls_tabular.so is present at ${LIB_CLS_DIR}"
for node in  `cat ${HOME}/nodes.txt`; do
    echo ${node};
    echo "checking for libcls_tabular:";
    ssh ${node} "ls -alh ${LIB_CLS_DIR} | grep -i libcls_tabular";
done;

echo "All done skyhook ansible setup.";
echo `date`;

