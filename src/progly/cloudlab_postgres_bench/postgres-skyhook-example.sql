-- ## end to end example for Postgres 10 file_fdw programmatic input
-- currently skyhook only supports binary fstream postgres output from
-- skyhook.SFT_FLATBUF_FLEX_ROW.* object formats
-- log into postgres as the user postgres
-- then create a new dabase, register the foreign server, and create a table
-- modify paths in the program arg where appropriate to call skyhook query
-- create some views or CTAS and select from those
-- "postgresql-10 - object-relational SQL database, version 10 server"
-- apt-get install postgresql-10
-- assumes ceph cluster (or vstart) is running at the specified path and
-- postgres user is the owner or has write permissions to that path/cluster
-- IO error 2 indicates permissions problem, since ceph writes some logging info
--- *****************************************************************************
--- *****************************************************************************
--- *****************************************************************************
--- *****************************************************************************
--- *****************************************************************************
CREATE EXTENSION file_fdw;
CREATE SERVER skyhookcephserver FOREIGN DATA WRAPPER file_fdw;
--- *****************************************************************************
drop foreign table small cascade;
create foreign table small (
  LINENUMBER int,
  QUANTITY double precision,
  SHIPDATE date)
SERVER skyhookcephserver
OPTIONS (format 'binary',  program 'cd /ceph/path; ./bin/run-query --num-objs 2 --pool tpchdata --wthreads 2 --qdepth 10 --output-format sft_pg_binary --project-cols linenumber,quantity,shipdate --use-cls' );
select count(*) from small;
select * from small;

-- with create table as
create table newtable as select * from small limit 10;
select * from newtable;

-- with a view
create view smallview as select * from small;
select count(*) from smallview;
select * from smallview;
--- *****************************************************************************
--- *****************************************************************************
drop foreign table lineitem cascade;
create foreign table lineitem (
  ORDERKEY int,
  PARTKEY int,
  SUPPKEY int,
  LINENUMBER int,
  QUANTITY float,
  EXTENDEDPRICE float,
  DISCOUNT float,
  TAX float,
  RETURNFLAG varchar,
  LINESTATUS varchar,
  SHIPDATE date,
  COMMITDATE date,
  RECEIPTDATE date,
  SHIPINSTRUCT varchar,
  SHIPMODE varchar,
  COMMENT varchar)
  SERVER skyhookcephserver
OPTIONS (format 'binary',  program 'cd /ceph/path; ./bin/run-query --num-objs 2 --pool tpchdata --wthreads 2 --qdepth 10 --output-format sft_pg_binary --use-cls' );
select count(*) from lineitem;
select * from lineitem;

-- with create table as select
create table newtable as select * from lineitem limit 10;
select * from newtable;

-- with a view
create view lineitemview as select * from lineitem group by orderkey;
select count(*) from lineitemview;
select * from lineitemview;

