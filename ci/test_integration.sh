#!/bin/bash
set -ex

export PATH=$PATH:/ceph/build/bin
export PATH=$PATH:/ceph/src/progly

docker_exec () {
  time docker exec deploy /bin/bash -c "$1"
}

# build plugin and query runner
docker run -di --name test -v /home/travis/build/uccross/skyhookdb-ceph:/ceph cephbuilder/ceph:${RELEASE}
docker exec test /bin/bash -c "mkdir build && cd build && cmake .." >> /dev/null 2>&1
docker exec test /bin/bash -c "cd build && make -j2 cls_tabular run-query"

# run integration tests
docker run -dit --name deploy -v /home/travis/build/uccross/skyhookdb-ceph:/ceph cephbuilder/ceph:skyhook-test
docker_exec "micro-osd.sh test"
docker_exec "cp test/ceph.conf /etc/ceph/ceph.conf"
docker_exec "cp -a /ceph/build/lib/libcls_tabular.so* /usr/lib/rados-classes/"
docker_exec "ceph -c test/ceph.conf -s"
docker_exec "rados -c test/ceph.conf mkpool tpch"
docker_exec "rados -c test/ceph.conf lspools"
docker_exec "ls /data"
docker_exec "echo y | /ceph/src/progly/rados-store-glob.sh tpch /data/obj*.bin"
docker_exec "/ceph/build/bin/run-query --num-objs 10 --pool tpch --wthreads 10 --qdepth 10 --quiet --extended-price 91400.0 --query b"
docker_exec "/ceph/build/bin/run-query --num-objs 10 --pool tpch --wthreads 10 --qdepth 10 --quiet --extended-price 1.0 --query b"
docker_exec "/ceph/build/bin/run-query --num-objs 10 --pool tpch --wthreads 10 --qdepth 10 --quiet --query d --order-key 5 --line-number 3"
docker_exec "/ceph/build/bin/run-query --num-objs 10 --pool tpch --wthreads 10 --qdepth 10 --quiet --extended-price 91400.0 --query b --use-cls"
docker_exec "/ceph/build/bin/run-query --num-objs 10 --pool tpch --wthreads 10 --qdepth 10 --quiet --query d --order-key 5 --line-number 3 --use-cls"
docker_exec "/ceph/build/bin/run-query --num-objs 10 --pool tpch --wthreads 10 --qdepth 10 --quiet --query d --order-key 5 --line-number 3 --use-cls --use-index"
docker_exec "/ceph/build/bin/run-query --num-objs 10 --pool tpch --wthreads 10 --qdepth 10 --quiet --query d --build-index"
docker_exec "/ceph/build/bin/run-query --num-objs 10 --pool tpch --wthreads 10 --qdepth 10 --quiet --query fastpath --use-cls"
echo "DONE!"

