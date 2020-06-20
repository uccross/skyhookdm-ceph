echo "Change directory to /build"
cd ../../../build/

echo "Creating storage pool"
bin/rados mkpool tpchdata;

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

echo "Back into sql_interface"
cd ../src/progly/sql_interface

echo "Run startup.sh to execute SkyhookDM-Ceph python sql client script"
