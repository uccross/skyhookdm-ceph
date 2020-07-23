# SkyhookDM SQL Interface

A python frontend for interfacing with SkyhookDM that generates query commands via SQL statements. It is asssumed that 
you have already completed the build steps to make SkyhookDM-Ceph and are ready to run test queries. 

## How to run 

* Run `setup.sh` to install depedencies and create ceph pool. Currently it is assumed that the object type of the data will be `SFT_FLATBUF_FLEX_ROW` with pool name `tpchdata` and number of objects at `2`.

* Run `startup.sh` to run the client 

* For custom options do not run `startup.sh`, and instead run `python3 -m interface.client -h` to show options. 
