## Current Support
We support a small factor of SQL syntax at this time. Currently, we generate Skyhook queries and pushdown projections via a SQL statement such as: 

* SELECT a,b FROM t; 

### Features Supported

* Projection queries (`SELECT ... FROM ...`)
* Setting options at execution time (`python3 client.py [OPTIONS]`)
* Showing data schema (`DESCRIBE <tablename>`)

### Upcoming Updates

* Compound `WHERE` (`AND`/`OR`)
* `LIKE` support
* Aggregation queries
* SQL file input
* Subqueries 
* Updating options 