echo "Updating. Installing python-pip3 and sqlparse module"
# sudo apt-get update 
apt-get install python-pip3 -y
pip3 install sqlparse

echo "Creating storage pool"
ceph osd pool create tpchdata 128 128 replicated;

echo "Getting FLATBUF_FLEX_ROW test data"
OBJ_TYPE=SFT_FLATBUF_FLEX_ROW; 
OBJ_BASE_NAME=skyhook.${OBJ_TYPE}.lineitem;
for i in {0..1}; do 
    rm -rf ${OBJ_BASE_NAME}.$i;   # remove the old test objects
    wget https://users.soe.ucsc.edu/~jlefevre/skyhookdb/testdata/${OBJ_BASE_NAME}.$i;
done;

echo "Storing test data into Ceph objects"
yes | ../src/progly/rados-store-glob.sh tpchdata ${OBJ_BASE_NAME}.*;

echo "Verifying data"
bin/rados df; 

echo "Run startup.sh to execute SkyhookDM-Ceph python sql client script"
