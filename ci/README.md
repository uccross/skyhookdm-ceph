# CI workflows

The [smoke test workflow](workflows/smoke.workflow) builds the skyhook 
fork and RADOS class, and subsequently executes the test (upload, 
index and query data). To run it:

```bash
cd skyhook-ceph/
popper run --recursive
```
