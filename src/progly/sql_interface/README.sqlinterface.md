# SkyhookDM SQL Interface (Experimental) 

A python frontend for interfacing with SkyhookDM that generates query commands via SQL statements. It is asssumed that 
you have already completed the necessary build steps to make SkyhookDM-Ceph and are ready to run test queries. 

## Setup
Run setup.sh to perform the following steps: 
1. Install pip3 (if not already installed).
2. Install SQLParse dependency for the project.
3. Create a storage pool.
4. Get and Store test data. 

Steps 3. and 4. are the same as found in the Run Test Queries wiki page.
 
## Startup  
Run startup.sh to start python shell for SQL interface.

## Current Support
This is an experimental project at this time. As such, only a small factor of SQL syntax is support as of now. 

Currently, SELECT-FROM query statements are supported. 

### Examples from Run Test Queries Wiki Page 
* `SELECT * FROM lineitem;`
* `SELECT orderkey,tax,comment,linenumber,returnflag FROM lineitem;`
* `SELECT linenumber,returnflag FROM lineitem`
* `SELECT returnflag FROM lineitem;` 

## Upcoming Updates
* `WHERE` and `LIKE` clause support
* Subquery support in `WHERE` clauses. 
* Options for --use-cls, --quiet. 
* Basic aggregation queries.
* Fix issue joins in `FROM` clauses to be comma separate strings when parsed.
* Support for SQL files as an alternative to python shell.
