pdsw_branch="skyhook-luminous";
repo_dir="/mnt/sda4/"
ansible_dir="${HOME}/skyhook-ansible/ansible/"
echo "clear out prev data dirs and scripts."
scripts_dir="${HOME}/pdsw19-reprod/scripts/"
data_dir="${HOME}/pdsw19-reprod/data/"


# now create some data objects
cd $repo_dir/skyhookdm-ceph/build/;

# get input data schema  and  csv files.
cp $data_dir/*.txt  .;
cp $data_dir/*.csv  .;


#############################################################
#############################################################
## LINEITEM DATASET
#############################################################
#############################################################

# create lineitem obj data for 10 and 100 MB sizes and rename obj to simple name.
lineitem_10MB_objfilename_base="lineitem.10MB.75Krows.obj.0";
lineitem_100MB_objfilename_base="lineitem.100MB.750Krows.obj.0";
lineitem_75Kcsv="lineitem.10MB.objs.75Krows.csv";
lineitem_750Kcsv="lineitem.100MB.objs.750Krows.csv";
touch $lineitem_750Kcsv;
rm  $lineitem_750Kcsv;
touch $lineitem_750Kcsv;
echo "lineitem: generating 750K rows csv by 10x concat of our 75K row csv..."
for ((i = 0 ; i < 10 ; i++)); do
    cat $lineitem_75Kcsv >> $lineitem_750Kcsv;
done;


./bin/fbwriter --file_name $lineitem_75Kcsv  --schema_file_name lineitem.schema.txt  --num_objs 1 --flush_rows 75000  --read_rows 75000  --csv_delim "|" --use_hashing true --rid_start_value 1 --table_name lineitem --input_oid 0  --obj_type SFT_FLATBUF_FLEX_ROW;
mv fbmeta.Skyhook.v2.SFT_FLATBUF_FLEX_ROW.lineitem.0.1-1  "fbx.${lineitem_10MB_objfilename_base}";


./bin/fbwriter --file_name $lineitem_750Kcsv --schema_file_name lineitem.schema.txt  --num_objs 1 --flush_rows 750000 --read_rows 750000 --csv_delim "|" --use_hashing true --rid_start_value 1 --table_name lineitem --input_oid 0  --obj_type SFT_FLATBUF_FLEX_ROW;
mv fbmeta.Skyhook.v2.SFT_FLATBUF_FLEX_ROW.lineitem.0.1-1 "fbx.${lineitem_100MB_objfilename_base}";



## DO TRANSFORMS....
cd $repo_dir/skyhookdm-ceph/build/;
pool="tpchdata"
rados rmpool $pool $pool --yes-i-really-really-mean-it
rados mkpool $pool
#############################################################
#####  lineitem: convert fbx obj 10MB to ARROW, retrieve arrow obj.
#############################################################
echo "lineitem: convert fbx obj 10MB to ARROW, retrieve arrow obj, current pool ${pool} status:"
rados -p $pool df
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
        echo "removing object ${objname} from pool ${pool}..."
        rados -p $pool rm $objname || true
        echo "writing $objfile into $pool/$objname"  &
        rados -p $pool put $objname $objfile  &
    done
    wait
done
sleep 10s
echo "done putting obj to pool ${pool}, tranforming next...current pool ${pool} status:"
rados -p $pool df
###############
###############  TRANSFORM  LINEITEM fbx obj to arrow obj
echo "TRANSFORM  LINEITEM fbx obj to arrow obj.."
./bin/run-query --num-objs 1 --pool  $pool   --wthreads 1 --qdepth 1  --transform-db --transform-format-type arrow --data-schema "0 3 1 0 ORDERKEY ; 1 3 0 1 PARTKEY ; 2 3 0 1 SUPPKEY ; 3 3 1 0 LINENUMBER ; 4 12 0 1 QUANTITY ; 5 13 0 1 EXTENDEDPRICE ; 6 12 0 1 DISCOUNT ; 7 13 0 1 TAX ; 8 9 0 1 RETURNFLAG ; 9 9 0 1 LINESTATUS ; 10 14 0 1 SHIPDATE ; 11 14 0 1 COMMITDATE ; 12 14 0 1 RECEIPTDATE ; 13 15 0 1 SHIPINSTRUCT ; 14 15 0 1 SHIPMODE ; 15 15 0 1 COMMENT" --use-cls  --quiet
###############
###############  RETRIEVE LINEITEM arrow  obj
echo "done transforming obj to arrow."
sleep 10s
echo "current pool ${pool} status:"
rados -p $pool df
echo " RETRIEVE LINEITEM arrow  obj"
objfile="arrow.${lineitem_10MB_objfilename_base}"
rados get --pool $pool obj.0  $objfile
echo "done getting arrow obj as local file ${objfile}.  current pool ${pool} status:"
rados -p $pool df
#############################################################
#####  lineitem: convert fbx obj 100MB to ARROW, retrieve arrow obj.
#############################################################
echo "lineitem: convert fbx obj 100MB to ARROW, retrieve arrow obj, current pool ${pool} status:"
rados -p $pool df
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
        echo "removing object ${objname} from pool ${pool}..."
        rados -p $pool rm $objname || true
        echo "writing $objfile into $pool/$objname"  &
        rados -p $pool put $objname $objfile  &
    done
    wait
done
sleep 10s
echo "done putting obj to pool ${pool}, tranforming next...current pool ${pool} status:"
rados -p $pool df
###############
###############  TRANSFORM  LINEITEM fbx obj to arrow obj
echo "TRANSFORM  LINEITEM fbx obj to arrow obj.."
./bin/run-query --num-objs 1 --pool  $pool   --wthreads 1 --qdepth 1  --transform-db --transform-format-type arrow --data-schema "0 3 1 0 ORDERKEY ; 1 3 0 1 PARTKEY ; 2 3 0 1 SUPPKEY ; 3 3 1 0 LINENUMBER ; 4 12 0 1 QUANTITY ; 5 13 0 1 EXTENDEDPRICE ; 6 12 0 1 DISCOUNT ; 7 13 0 1 TAX ; 8 9 0 1 RETURNFLAG ; 9 9 0 1 LINESTATUS ; 10 14 0 1 SHIPDATE ; 11 14 0 1 COMMITDATE ; 12 14 0 1 RECEIPTDATE ; 13 15 0 1 SHIPINSTRUCT ; 14 15 0 1 SHIPMODE ; 15 15 0 1 COMMENT" --use-cls  --quiet
###############
###############  RETRIEVE LINEITEM arrow  obj
echo "done transforming obj to arrow."
sleep 10s
echo "current pool ${pool} status:"
rados -p $pool df
echo " RETRIEVE LINEITEM arrow  obj"
objfile="arrow.${lineitem_100MB_objfilename_base}"
rados get --pool $pool obj.0  $objfile
echo "done getting arrow obj as local file ${objfile}.  current pool ${pool} status:"
rados -p $pool df
#############################################################
#############################################################
# CLEANUP DATA
rados -p $pool rm "obj.0";


#############################################################
#############################################################
## NCOLS100 DATASET
#############################################################
#############################################################

# create ncols100 obj data for 10 and 100 MB sizes and rename obj to simple name.
ncols100_10MB_objfilename_base="ncols100.10MB.25Krows.obj.0";
ncols100_100MB_objfilename_base="ncols100.100MB.250Krows.obj.0";
ncols100_25Kcsv="ncols100.10MB.objs.25Krows.csv";
ncols100_250Kcsv="ncols100.100MB.objs.250Krows.csv";
touch $ncols100_250Kcsv;
rm $ncols100_250Kcsv;
touch $ncols100_250Kcsv;
echo "nocls100: generating 250K rows csv by 10x concat of our 25K row csv..."
for ((i = 0 ; i < 10 ; i++)); do
    cat $ncols100_25Kcsv >> $ncols100_250Kcsv;
done;

./bin/fbwriter --file_name $ncols100_25Kcsv   --schema_file_name ncols100.schema.txt  --num_objs 1 --flush_rows 25000  --read_rows 25000  --csv_delim "|" --use_hashing true --rid_start_value 1 --table_name ncols100 --input_oid 0  --obj_type SFT_FLATBUF_FLEX_ROW;
mv fbmeta.Skyhook.v2.SFT_FLATBUF_FLEX_ROW.ncols100.0.1-1 "fbx.${ncols100_10MB_objfilename_base}";

./bin/fbwriter --file_name $ncols100_250Kcsv   --schema_file_name ncols100.schema.txt  --num_objs 1 --flush_rows 250000  --read_rows 250000  --csv_delim "|" --use_hashing true --rid_start_value 1 --table_name ncols100 --input_oid 0  --obj_type SFT_FLATBUF_FLEX_ROW;
mv fbmeta.Skyhook.v2.SFT_FLATBUF_FLEX_ROW.ncols100.0.1-1 "fbx.${ncols100_100MB_objfilename_base}";


## DO TRANSFORMS....
cd $repo_dir/skyhookdm-ceph/build/;
echo "begin tranform of ncols100 dataset..."
pool="ncols100"
rados rmpool $pool $pool --yes-i-really-really-mean-it
rados mkpool $pool
#############################################################
#####  ncols100: convert fbx obj 10MB to ARROW, retrieve arrow obj.
#############################################################
echo "ncols100: convert fbx obj 10MB to ARROW, retrieve arrow obj, current pool ${pool} status:"
rados -p $pool df
objfile="fbx.${ncols100_10MB_objfilename_base}"
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
        echo "removing object ${objname} from pool ${pool}..."
        rados -p $pool rm $objname || true
        echo "writing $objfile into $pool/$objname"  &
        rados -p $pool put $objname $objfile  &
    done
    wait
done
sleep 10s
echo "done putting obj to pool ${pool}, tranforming next...current pool ${pool} status:"
rados -p $pool df
###############
###############  TRANSFORM  ncols100 fbx obj to arrow obj
echo "TRANSFORM  ncols100 fbx obj to arrow obj.."
./bin/run-query --num-objs 1 --pool $pool   --wthreads 1 --qdepth 1  --transform-db --transform-format-type arrow --data-schema "0 8 1 0 ATT0; 1 8 0 0 ATT1; 2 8 0 0 ATT2; 3 8 0 0 ATT3; 4 8 0 0 ATT4; 5 8 0 0 ATT5; 6 8 0 0 ATT6; 7 8 0 0 ATT7; 8 8 0 0 ATT8; 9 8 0 0 ATT9; 10 8 0 0 ATT10; 11 8 0 0 ATT11; 12 8 0 0 ATT12; 13 8 0 0 ATT13; 14 8 0 0 ATT14; 15 8 0 0 ATT15; 16 8 0 0 ATT16; 17 8 0 0 ATT17; 18 8 0 0 ATT18; 19 8 0 0 ATT19; 20 8 0 0 ATT20; 21 8 0 0 ATT21; 22 8 0 0 ATT22; 23 8 0 0 ATT23; 24 8 0 0 ATT24; 25 8 0 0 ATT25; 26 8 0 0 ATT26; 27 8 0 0 ATT27; 28 8 0 0 ATT28; 29 8 0 0 ATT29; 30 8 0 0 ATT30; 31 8 0 0 ATT31; 32 8 0 0 ATT32; 33 8 0 0 ATT33; 34 8 0 0 ATT34; 35 8 0 0 ATT35; 36 8 0 0 ATT36; 37 8 0 0 ATT37; 38 8 0 0 ATT38; 39 8 0 0 ATT39; 40 8 0 0 ATT40; 41 8 0 0 ATT41; 42 8 0 0 ATT42; 43 8 0 0 ATT43; 44 8 0 0 ATT44; 45 8 0 0 ATT45; 46 8 0 0 ATT46; 47 8 0 0 ATT47; 48 8 0 0 ATT48; 49 8 0 0 ATT49; 50 8 0 0 ATT50; 51 8 0 0 ATT51; 52 8 0 0 ATT52; 53 8 0 0 ATT53; 54 8 0 0 ATT54; 55 8 0 0 ATT55; 56 8 0 0 ATT56; 57 8 0 0 ATT57; 58 8 0 0 ATT58; 59 8 0 0 ATT59; 60 8 0 0 ATT60; 61 8 0 0 ATT61; 62 8 0 0 ATT62; 63 8 0 0 ATT63; 64 8 0 0 ATT64; 65 8 0 0 ATT65; 66 8 0 0 ATT66; 67 8 0 0 ATT67; 68 8 0 0 ATT68; 69 8 0 0 ATT69; 70 8 0 0 ATT70; 71 8 0 0 ATT71; 72 8 0 0 ATT72; 73 8 0 0 ATT73; 74 8 0 0 ATT74; 75 8 0 0 ATT75; 76 8 0 0 ATT76; 77 8 0 0 ATT77; 78 8 0 0 ATT78; 79 8 0 0 ATT79; 80 8 1 0 ATT80; 81 8 0 0 ATT81; 82 8 0 0 ATT82; 83 8 0 0 ATT83; 84 8 0 0 ATT84; 85 8 0 0 ATT85; 86 8 0 0 ATT86; 87 8 0 0 ATT87; 88 8 0 0 ATT88; 89 8 0 0 ATT89; 90 8 0 0 ATT90; 91 8 0 0 ATT91; 92 8 0 0 ATT92; 93 8 0 0 ATT93; 94 8 0 0 ATT94; 95 8 0 0 ATT95; 96 8 0 0 ATT96; 97 8 0 0 ATT97; 98 8 0 0 ATT98; 99 8 0 0 ATT99;" --use-cls --quiet
###############
###############  RETRIEVE ncols100 arrow  obj
echo "done transforming obj to arrow."
sleep 10s
echo "current pool ${pool} status:"
rados -p $pool df
echo " RETRIEVE ncols100 arrow  obj"
objfile="arrow.${ncols100_10MB_objfilename_base}"
rados get --pool $pool obj.0  $objfile
echo "done getting arrow obj as local file ${objfile}.  current pool ${pool} status:"
rados -p $pool df
#############################################################
#############################################################


#############################################################
#####  ncols100: convert fbx obj 100MB to ARROW, retrieve arrow obj.
#############################################################
echo "ncols100: convert fbx obj 100MB to ARROW, retrieve arrow obj, current pool ${pool} status:"
rados -p $pool df
objfile="fbx.${ncols100_100MB_objfilename_base}"
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
        echo "removing object ${objname} from pool ${pool}..."
        rados -p $pool rm $objname || true
        echo "writing $objfile into $pool/$objname"  &
        rados -p $pool put $objname $objfile  &
    done
    wait
done
sleep 10s
echo "done putting obj to pool ${pool}, tranforming next...current pool ${pool} status:"
rados -p $pool df
###############
###############  TRANSFORM  ncols100 fbx obj to arrow obj
echo "TRANSFORM  ncols100 fbx obj to arrow obj.."
./bin/run-query --num-objs 1 --pool $pool   --wthreads 1 --qdepth 1  --transform-db --transform-format-type arrow --data-schema "0 8 1 0 ATT0; 1 8 0 0 ATT1; 2 8 0 0 ATT2; 3 8 0 0 ATT3; 4 8 0 0 ATT4; 5 8 0 0 ATT5; 6 8 0 0 ATT6; 7 8 0 0 ATT7; 8 8 0 0 ATT8; 9 8 0 0 ATT9; 10 8 0 0 ATT10; 11 8 0 0 ATT11; 12 8 0 0 ATT12; 13 8 0 0 ATT13; 14 8 0 0 ATT14; 15 8 0 0 ATT15; 16 8 0 0 ATT16; 17 8 0 0 ATT17; 18 8 0 0 ATT18; 19 8 0 0 ATT19; 20 8 0 0 ATT20; 21 8 0 0 ATT21; 22 8 0 0 ATT22; 23 8 0 0 ATT23; 24 8 0 0 ATT24; 25 8 0 0 ATT25; 26 8 0 0 ATT26; 27 8 0 0 ATT27; 28 8 0 0 ATT28; 29 8 0 0 ATT29; 30 8 0 0 ATT30; 31 8 0 0 ATT31; 32 8 0 0 ATT32; 33 8 0 0 ATT33; 34 8 0 0 ATT34; 35 8 0 0 ATT35; 36 8 0 0 ATT36; 37 8 0 0 ATT37; 38 8 0 0 ATT38; 39 8 0 0 ATT39; 40 8 0 0 ATT40; 41 8 0 0 ATT41; 42 8 0 0 ATT42; 43 8 0 0 ATT43; 44 8 0 0 ATT44; 45 8 0 0 ATT45; 46 8 0 0 ATT46; 47 8 0 0 ATT47; 48 8 0 0 ATT48; 49 8 0 0 ATT49; 50 8 0 0 ATT50; 51 8 0 0 ATT51; 52 8 0 0 ATT52; 53 8 0 0 ATT53; 54 8 0 0 ATT54; 55 8 0 0 ATT55; 56 8 0 0 ATT56; 57 8 0 0 ATT57; 58 8 0 0 ATT58; 59 8 0 0 ATT59; 60 8 0 0 ATT60; 61 8 0 0 ATT61; 62 8 0 0 ATT62; 63 8 0 0 ATT63; 64 8 0 0 ATT64; 65 8 0 0 ATT65; 66 8 0 0 ATT66; 67 8 0 0 ATT67; 68 8 0 0 ATT68; 69 8 0 0 ATT69; 70 8 0 0 ATT70; 71 8 0 0 ATT71; 72 8 0 0 ATT72; 73 8 0 0 ATT73; 74 8 0 0 ATT74; 75 8 0 0 ATT75; 76 8 0 0 ATT76; 77 8 0 0 ATT77; 78 8 0 0 ATT78; 79 8 0 0 ATT79; 80 8 1 0 ATT80; 81 8 0 0 ATT81; 82 8 0 0 ATT82; 83 8 0 0 ATT83; 84 8 0 0 ATT84; 85 8 0 0 ATT85; 86 8 0 0 ATT86; 87 8 0 0 ATT87; 88 8 0 0 ATT88; 89 8 0 0 ATT89; 90 8 0 0 ATT90; 91 8 0 0 ATT91; 92 8 0 0 ATT92; 93 8 0 0 ATT93; 94 8 0 0 ATT94; 95 8 0 0 ATT95; 96 8 0 0 ATT96; 97 8 0 0 ATT97; 98 8 0 0 ATT98; 99 8 0 0 ATT99;" --use-cls --quiet
###############
###############  RETRIEVE ncols100 arrow  obj
echo "done transforming obj to arrow."
sleep 10s
echo "current pool ${pool} status:"
rados -p $pool df
echo " RETRIEVE ncols100 arrow  obj"
objfile="arrow.${ncols100_100MB_objfilename_base}"
rados get --pool $pool obj.0  $objfile
echo "done getting arrow obj as local file ${objfile}.  current pool ${pool} status:"
rados -p $pool df
#############################################################
#############################################################

# CLEANUP DATA
rados -p $pool rm "obj.0";

# COPY OBJ DATA TO HOME DIR FOR LOAD SCRIPTS
cp *.obj* $HOME

