# Loading TPC-H Data

1. Use `convert-tpch-tables.py` to build the binary objects to be loaded into
   RADOS. For example, `convert-tpch-tables.py -i data.tbl -r 1000` will
   produce binary files with 1000 rows each named `objNNNNN.bin`.

2. Load them into RADOS. The shell script `rados-store-glob.sh` will handle
   this. Run `rados-store-glob.sh <pool> obj*.bin` to load the objects into
   the RADOS pool `pool`.

# regex scan example

load some data

```bash
../src/progly/rados-store-glob.sh rbd ../src/progly/*.bin
```

print out some of the tpch row comments

```bash
nwatkins@pl1:~/ceph-progly/build$ bin/regex-scan --pool rbd --num-objs 10 --row-size 141 --field-offset 97 --field-length 44 | head -n 10
egular courts above the
ly final dependencies: slyly bold
riously. regular, express dep
lites. fluffily even de
pending foxes. slyly re
arefully slyly ex
ven requests. deposits breach a
ongside of the furiously brave acco
unusual accounts. eve
nal foxes wake.
```

print out just the rows with `foxes` in the comment. this does the filtering
on in the client, so all the data is returned from rados.

```bash
nwatkins@pl1:~/ceph-progly/build$ bin/regex-scan --pool rbd --num-objs 10 --row-size 141 --field-offset 97 --field-length 44 --regex foxes | head -n 10
pending foxes. slyly re
nal foxes wake.
p furiously special foxes
ar foxes sleep
heodolites sleep silently pending foxes. ac
uests. foxes cajole slyly after the ca
ggle fluffily foxes. fluffily ironic ex
sts maintain foxes. furiously regular p
ending foxes acr
y unusual foxes
```

Add `--use-cls` to perform the filtering in the object class and only return
the rows that match. To sanity check this the client re-filters the rows and
prints out how many matched out of the total number of rows. Here is an
example trace

```bash
nwatkins@pl1:~/ceph-progly/build$ bin/regex-scan --pool rbd --num-objs 10 --row-size 141 --field-offset 97 --field-length 44 --regex foxes | grep filtered
obj.0: filtered 47 / 1000
obj.1: filtered 41 / 1000
obj.2: filtered 50 / 1000
obj.3: filtered 33 / 1000
obj.4: filtered 34 / 1000
obj.5: filtered 47 / 1000
obj.6: filtered 45 / 1000
obj.7: filtered 38 / 1000
obj.8: filtered 36 / 1000
obj.9: filtered 39 / 1000
nwatkins@pl1:~/ceph-progly/build$ bin/regex-scan --pool rbd --num-objs 10 --row-size 141 --field-offset 97 --field-length 44 --regex foxes --use-cls | grep filtered
obj.0: filtered 47 / 47
obj.1: filtered 41 / 41
obj.2: filtered 50 / 50
obj.3: filtered 33 / 33
obj.4: filtered 34 / 34
obj.5: filtered 47 / 47
obj.6: filtered 45 / 45
obj.7: filtered 38 / 38
obj.8: filtered 36 / 36
obj.9: filtered 39 / 39
```
