workflow "smoke tests" {
  resolves = "run test"
}

# NOTE:
# - the skyhook-test-data image has nothing but the data
# - the ceph-builder image is a pre-built github action
#   that is available at https://github.com/popperized/ceph

action "build ceph" {
  uses = "docker://popperized/ceph-builder:nautilus"
  args = "vstart"
  env = {
    CEPH_SRC_DIR = "./"
    CEPH_CMAKE_FLAGS = "-DCMAKE_BUILD_TYPE=MinSizeRel -DWITH_RBD=OFF -DWITH_CEPHFS=OFF -DWITH_RADOSGW=OFF -DWITH_LEVELDB=OFF -DWITH_MANPAGE=OFF -DWITH_RDMA=OFF -DWITH_OPENLDAP=OFF -DWITH_FUSE=OFF -DWITH_LIBCEPHFS=OFF -DWITH_KRBD=OFF -DWITH_LTTNG=OFF -DWITH_BABELTRACE=OFF -DWITH_MGR_DASHBOARD_FRONTEND=OFF -DWITH_SYSTEMD=OFF -DWITH_SPDK=OFF"
  }
}

action "build skyhook" {
  needs = "build ceph"
  uses = "docker://popperized/ceph-builder:nautilus"
  runs = [
    "sh", "-c", "cd build && make -j2 cls_tabular ceph_test_skyhook_query"
  ]
}

action "download test data" {
  needs = "build skyhook"
  uses = "docker://popperized/skyhook-test-data"
  runs = ["sh", "-c", "cp -r /data data"]
}

action "run test" {
  needs = "download test data"
  uses = "docker://popperized/ceph-builder:nautilus"
  runs = [
    "sh", "-c", "ci/scripts/run-skyhook-test"
  ]
}
