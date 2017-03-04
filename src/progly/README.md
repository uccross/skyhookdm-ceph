# Loading TPC-H Data

1. Use `convert-tpch-tables.py` to build the binary objects to be loaded into
   RADOS. For example, `convert-tpch-tables.py -i data.tbl -r 1000` will
   produce binary files with 1000 rows each named `objNNNNN.bin`.

2. Load them into RADOS. The shell script `rados-store-glob.sh` will handle
   this. Run `rados-store-glob.sh <pool> obj*.bin` to load the objects into
   the RADOS pool `pool`.
