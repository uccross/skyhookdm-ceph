-- usage:  psql -d <dbname> -f <this.ddl.sql>
-- This script will create 2 tablespaces one each on ssd and hdd on our 
-- standard cloudlab machine paths below which are:
-- hdd: /mnt/sda4/ssdpgtablespace
-- ssd: /mnt/sdc/ssdpgtablespace
-- 
-- It assumes postgres 9.6 is installed with the PostreSQL user 'postgres'
-- and the above dirs are owned by the OS user 'postgres'
-- i.e.,
-- sudo mkdir /mnt/sdc/ssdpgtablespace; sudo chown postgres /mnt/sdc/ssdpgtablespace
-- sudo mkdir /mnt/sda4/hddpgtablespace; sudo chown postgres /mnt/sda4/hddpgtablespace
-- 
-- It will then load 10M and 1B versions of our tpch dataset, it assumes files of the below
-- numbers of rows, each of lineitem table, that were created with the tpc datagen
-- and using the pipe | delimiter
-- and the filepaths are such as:
-- /mnt/sda4/data/lineitem-10M-rows.tbl
-- /mnt/sda4/data/lineitem-1B-rows.tbl
--
-- example create statement for indexes and table data onto a tablespace.
-- create table t (a int, b int, c char(10), primary key (a,b) using index tablespace ssd) tablespace ssd;
-- 
-- NOTE SOME DB SERVER SETTINGS FOR PARALLEL QUERY
-- EDIT The below settings in /etc/postgresql/9.6/main/postgresql.conf
-- SET VAR TO X/Y/DEFAULT
-- SHOW VAR
SET force_parallel_mode TO DEFAULT;  --default is off. -- on forces always parallel
SET Max_parallel_workers_per_gather TO 30;  --cloudlab machine has 40 cores
SET max_worker_processes TO 32;  --cloudlab machine has 40 cores
SHOW force_parallel_mode;   SHOW max_parallel_workers_per_gather; SHOW max_worker_processes ;  
-- RESTART DB 
-- change settings via command line:
-- postgres -c log_connections=yes -c log_destination='syslog'

DROP TABLE IF EXISTS LINEITEM10MSSD CASCADE;     
DROP TABLE IF EXISTS LINEITEM10MHDD CASCADE;
DROP TABLE IF EXISTS LINEITEM10MSSDPK CASCADE;     
DROP TABLE IF EXISTS LINEITEM10MHDDPK CASCADE;

DROP TABLE IF EXISTS LINEITEM100MSSD CASCADE;     
DROP TABLE IF EXISTS LINEITEM100MHDD CASCADE;
DROP TABLE IF EXISTS LINEITEM100MSSDPK CASCADE;     
DROP TABLE IF EXISTS LINEITEM100MHDDPK CASCADE;

DROP TABLE IF EXISTS LINEITEM1BSSD CASCADE;     
DROP TABLE IF EXISTS LINEITEM1BHDD CASCADE;
DROP TABLE IF EXISTS LINEITEM1BSSDPK CASCADE;     
DROP TABLE IF EXISTS LINEITEM1BHDDPK CASCADE;

drop tablespace if exists ssd;
drop tablespace if exists hdd;

\connect postgres
drop database if exists d;
create database d;
\connect d

create tablespace ssd owner postgres location '/mnt/sdc/ssdpgtablespace';
create tablespace hdd owner postgres location '/mnt/sda4/hddpgtablespace';

\timing

CREATE TABLE LINEITEM10MSSD(
         L_ORDERKEY    INTEGER NOT NULL, 
         L_PARTKEY     INTEGER NOT NULL,
         L_SUPPKEY     INTEGER NOT NULL, 
         L_LINENUMBER  INTEGER NOT NULL, 
         L_QUANTITY    DECIMAL(15,2) NOT NULL, 
         L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL, 
         L_DISCOUNT    DECIMAL(15,2) NOT NULL, 
         L_TAX         DECIMAL(15,2) NOT NULL, 
         L_RETURNFLAG  CHAR(1) NOT NULL, 
         L_LINESTATUS  CHAR(1) NOT NULL, 
         L_SHIPDATE    DATE NOT NULL, 
         L_COMMITDATE  DATE NOT NULL, 
         L_RECEIPTDATE DATE NOT NULL, 
         L_SHIPINSTRUCT CHAR(25) NOT NULL,  
         L_SHIPMODE     CHAR(10) NOT NULL, 
         L_COMMENT      VARCHAR(44) NOT NULL,
         L_EMPTY CHAR(1))
         TABLESPACE ssd; 
         
         
CREATE TABLE LINEITEM10MHDD(
         L_ORDERKEY    INTEGER NOT NULL, 
         L_PARTKEY     INTEGER NOT NULL,
         L_SUPPKEY     INTEGER NOT NULL, 
         L_LINENUMBER  INTEGER NOT NULL, 
         L_QUANTITY    DECIMAL(15,2) NOT NULL, 
         L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL, 
         L_DISCOUNT    DECIMAL(15,2) NOT NULL, 
         L_TAX         DECIMAL(15,2) NOT NULL, 
         L_RETURNFLAG  CHAR(1) NOT NULL, 
         L_LINESTATUS  CHAR(1) NOT NULL, 
         L_SHIPDATE    DATE NOT NULL, 
         L_COMMITDATE  DATE NOT NULL, 
         L_RECEIPTDATE DATE NOT NULL, 
         L_SHIPINSTRUCT CHAR(25) NOT NULL,  
         L_SHIPMODE     CHAR(10) NOT NULL, 
         L_COMMENT      VARCHAR(44) NOT NULL,
         L_EMPTY CHAR(1))
         TABLESPACE hdd;  



CREATE TABLE LINEITEM100MSSD(
         L_ORDERKEY    INTEGER NOT NULL, 
         L_PARTKEY     INTEGER NOT NULL,
         L_SUPPKEY     INTEGER NOT NULL, 
         L_LINENUMBER  INTEGER NOT NULL, 
         L_QUANTITY    DECIMAL(15,2) NOT NULL, 
         L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL, 
         L_DISCOUNT    DECIMAL(15,2) NOT NULL, 
         L_TAX         DECIMAL(15,2) NOT NULL, 
         L_RETURNFLAG  CHAR(1) NOT NULL, 
         L_LINESTATUS  CHAR(1) NOT NULL, 
         L_SHIPDATE    DATE NOT NULL, 
         L_COMMITDATE  DATE NOT NULL, 
         L_RECEIPTDATE DATE NOT NULL, 
         L_SHIPINSTRUCT CHAR(25) NOT NULL,  
         L_SHIPMODE     CHAR(10) NOT NULL, 
         L_COMMENT      VARCHAR(44) NOT NULL,
         L_EMPTY CHAR(1))
         TABLESPACE ssd; 
         
         
CREATE TABLE LINEITEM100MHDD(
         L_ORDERKEY    INTEGER NOT NULL, 
         L_PARTKEY     INTEGER NOT NULL,
         L_SUPPKEY     INTEGER NOT NULL, 
         L_LINENUMBER  INTEGER NOT NULL, 
         L_QUANTITY    DECIMAL(15,2) NOT NULL, 
         L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL, 
         L_DISCOUNT    DECIMAL(15,2) NOT NULL, 
         L_TAX         DECIMAL(15,2) NOT NULL, 
         L_RETURNFLAG  CHAR(1) NOT NULL, 
         L_LINESTATUS  CHAR(1) NOT NULL, 
         L_SHIPDATE    DATE NOT NULL, 
         L_COMMITDATE  DATE NOT NULL, 
         L_RECEIPTDATE DATE NOT NULL, 
         L_SHIPINSTRUCT CHAR(25) NOT NULL,  
         L_SHIPMODE     CHAR(10) NOT NULL, 
         L_COMMENT      VARCHAR(44) NOT NULL,
         L_EMPTY CHAR(1))
         TABLESPACE hdd;  


CREATE TABLE LINEITEM100MSSDPK(
         L_ORDERKEY    INTEGER NOT NULL, 
         L_PARTKEY     INTEGER NOT NULL,
         L_SUPPKEY     INTEGER NOT NULL, 
         L_LINENUMBER  INTEGER NOT NULL, 
         L_QUANTITY    DECIMAL(15,2) NOT NULL, 
         L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL, 
         L_DISCOUNT    DECIMAL(15,2) NOT NULL, 
         L_TAX         DECIMAL(15,2) NOT NULL, 
         L_RETURNFLAG  CHAR(1) NOT NULL, 
         L_LINESTATUS  CHAR(1) NOT NULL, 
         L_SHIPDATE    DATE NOT NULL, 
         L_COMMITDATE  DATE NOT NULL, 
         L_RECEIPTDATE DATE NOT NULL, 
         L_SHIPINSTRUCT CHAR(25) NOT NULL,  
         L_SHIPMODE     CHAR(10) NOT NULL, 
         L_COMMENT      VARCHAR(44) NOT NULL,
         L_EMPTY CHAR(1),
         PRIMARY KEY (L_ORDERKEY,L_LINENUMBER) USING INDEX TABLESPACE ssd)
         TABLESPACE ssd; 
         
         
CREATE TABLE LINEITEM100MHDDPK(
         L_ORDERKEY    INTEGER NOT NULL, 
         L_PARTKEY     INTEGER NOT NULL,
         L_SUPPKEY     INTEGER NOT NULL, 
         L_LINENUMBER  INTEGER NOT NULL, 
         L_QUANTITY    DECIMAL(15,2) NOT NULL, 
         L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL, 
         L_DISCOUNT    DECIMAL(15,2) NOT NULL, 
         L_TAX         DECIMAL(15,2) NOT NULL, 
         L_RETURNFLAG  CHAR(1) NOT NULL, 
         L_LINESTATUS  CHAR(1) NOT NULL, 
         L_SHIPDATE    DATE NOT NULL, 
         L_COMMITDATE  DATE NOT NULL, 
         L_RECEIPTDATE DATE NOT NULL, 
         L_SHIPINSTRUCT CHAR(25) NOT NULL,  
         L_SHIPMODE     CHAR(10) NOT NULL, 
         L_COMMENT      VARCHAR(44) NOT NULL,
         L_EMPTY CHAR(1),
         PRIMARY KEY (L_ORDERKEY,L_LINENUMBER) USING INDEX TABLESPACE hdd)
         TABLESPACE hdd; 


CREATE TABLE LINEITEM1BSSD(
         L_ORDERKEY    INTEGER NOT NULL, 
         L_PARTKEY     INTEGER NOT NULL,
         L_SUPPKEY     INTEGER NOT NULL, 
         L_LINENUMBER  INTEGER NOT NULL, 
         L_QUANTITY    DECIMAL(15,2) NOT NULL, 
         L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL, 
         L_DISCOUNT    DECIMAL(15,2) NOT NULL, 
         L_TAX         DECIMAL(15,2) NOT NULL, 
         L_RETURNFLAG  CHAR(1) NOT NULL, 
         L_LINESTATUS  CHAR(1) NOT NULL, 
         L_SHIPDATE    DATE NOT NULL, 
         L_COMMITDATE  DATE NOT NULL, 
         L_RECEIPTDATE DATE NOT NULL, 
         L_SHIPINSTRUCT CHAR(25) NOT NULL,  
         L_SHIPMODE     CHAR(10) NOT NULL, 
         L_COMMENT      VARCHAR(44) NOT NULL,
         L_EMPTY CHAR(1))
         TABLESPACE ssd; 
         
         
CREATE TABLE LINEITEM1BHDD(
         L_ORDERKEY    INTEGER NOT NULL, 
         L_PARTKEY     INTEGER NOT NULL,
         L_SUPPKEY     INTEGER NOT NULL, 
         L_LINENUMBER  INTEGER NOT NULL, 
         L_QUANTITY    DECIMAL(15,2) NOT NULL, 
         L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL, 
         L_DISCOUNT    DECIMAL(15,2) NOT NULL, 
         L_TAX         DECIMAL(15,2) NOT NULL, 
         L_RETURNFLAG  CHAR(1) NOT NULL, 
         L_LINESTATUS  CHAR(1) NOT NULL, 
         L_SHIPDATE    DATE NOT NULL, 
         L_COMMITDATE  DATE NOT NULL, 
         L_RECEIPTDATE DATE NOT NULL, 
         L_SHIPINSTRUCT CHAR(25) NOT NULL,  
         L_SHIPMODE     CHAR(10) NOT NULL, 
         L_COMMENT      VARCHAR(44) NOT NULL,
         L_EMPTY CHAR(1))
         TABLESPACE hdd;  


CREATE TABLE LINEITEM1BSSDPK(
         L_ORDERKEY    INTEGER NOT NULL, 
         L_PARTKEY     INTEGER NOT NULL,
         L_SUPPKEY     INTEGER NOT NULL, 
         L_LINENUMBER  INTEGER NOT NULL, 
         L_QUANTITY    DECIMAL(15,2) NOT NULL, 
         L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL, 
         L_DISCOUNT    DECIMAL(15,2) NOT NULL, 
         L_TAX         DECIMAL(15,2) NOT NULL, 
         L_RETURNFLAG  CHAR(1) NOT NULL, 
         L_LINESTATUS  CHAR(1) NOT NULL, 
         L_SHIPDATE    DATE NOT NULL, 
         L_COMMITDATE  DATE NOT NULL, 
         L_RECEIPTDATE DATE NOT NULL, 
         L_SHIPINSTRUCT CHAR(25) NOT NULL,  
         L_SHIPMODE     CHAR(10) NOT NULL, 
         L_COMMENT      VARCHAR(44) NOT NULL,
         L_EMPTY CHAR(1),
         PRIMARY KEY (L_ORDERKEY,L_LINENUMBER) USING INDEX TABLESPACE ssd)
         TABLESPACE ssd; 
         
         
CREATE TABLE LINEITEM1BHDDPK(
         L_ORDERKEY    INTEGER NOT NULL, 
         L_PARTKEY     INTEGER NOT NULL,
         L_SUPPKEY     INTEGER NOT NULL, 
         L_LINENUMBER  INTEGER NOT NULL, 
         L_QUANTITY    DECIMAL(15,2) NOT NULL, 
         L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL, 
         L_DISCOUNT    DECIMAL(15,2) NOT NULL, 
         L_TAX         DECIMAL(15,2) NOT NULL, 
         L_RETURNFLAG  CHAR(1) NOT NULL, 
         L_LINESTATUS  CHAR(1) NOT NULL, 
         L_SHIPDATE    DATE NOT NULL, 
         L_COMMITDATE  DATE NOT NULL, 
         L_RECEIPTDATE DATE NOT NULL, 
         L_SHIPINSTRUCT CHAR(25) NOT NULL,  
         L_SHIPMODE     CHAR(10) NOT NULL, 
         L_COMMENT      VARCHAR(44) NOT NULL,
         L_EMPTY CHAR(1),
         PRIMARY KEY (L_ORDERKEY,L_LINENUMBER) USING INDEX TABLESPACE hdd)
         TABLESPACE hdd; 

COMMIT WORK;

-- LOAD THE DATA 

-- 10M rows
COPY lineitem10MHDD from '/mnt/sda4/data/lineitem-10M-rows.tbl' DELIMITER '|' CSV;
COMMIT WORK;
COPY lineitem10MSSD from '/mnt/sda4/data/lineitem-10M-rows.tbl' DELIMITER '|' CSV;
COMMIT WORK;
COPY lineitem10MHDDPK from '/mnt/sda4/data/lineitem-10M-rows.tbl' DELIMITER '|' CSV;
COMMIT WORK;
COPY lineitem10MSSDPK from '/mnt/sda4/data/lineitem-10M-rows.tbl' DELIMITER '|' CSV;
COMMIT WORK;

-- 100M rows
COPY lineitem100MHDD from '/mnt/sda4/data/lineitem-100M-rows.tbl' DELIMITER '|' CSV;
COMMIT WORK;
COPY lineitem100MSSD from '/mnt/sda4/data/lineitem-100M-rows.tbl' DELIMITER '|' CSV;
COMMIT WORK;
COPY lineitem100MHDDPK from '/mnt/sda4/data/lineitem-100M-rows.tbl' DELIMITER '|' CSV;
COMMIT WORK;
COPY lineitem100MSSDPK from '/mnt/sda4/data/lineitem-100M-rows.tbl' DELIMITER '|' CSV;
COMMIT WORK;

-- 1B rows
COPY lineitem1BHDD from '/mnt/sda4/data/lineitem-1B-rows.tbl' DELIMITER '|' CSV;
COMMIT WORK;
COPY lineitem1BSSD from '/mnt/sda4/data/lineitem-1B-rows.tbl' DELIMITER '|' CSV;
COMMIT WORK;
COPY lineitem1BHDDPK from '/mnt/sda4/data/lineitem-1B-rows.tbl' DELIMITER '|' CSV;
COMMIT WORK;
COPY lineitem1BSSDPK from '/mnt/sda4/data/lineitem-1B-rows.tbl' DELIMITER '|' CSV;
COMMIT WORK;

-- no arg will analyze all tables.
ANALYZE;  
COMMIT WORK;


