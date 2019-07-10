workflow "smoke tests" {
  resolves = "run test"
}

action "build ceph and skyhook cls" {
  uses = "popperized/cmake@ubuntu-18.04"
  args = "vstart cls_tabular run-query ceph_test_skyhook_query"
  env = {
    CMAKE_PROJECT_DIR = "./"
    CMAKE_INSTALL_DEPS_SCRIPT = "install-deps.sh"
    CMAKE_FLAGS = "-DCMAKE_BUILD_TYPE=MinSizeRel -DWITH_RBD=OFF -DWITH_CEPHFS=OFF -DWITH_RADOSGW=OFF -DWITH_LEVELDB=OFF -DWITH_MANPAGE=OFF -DWITH_RDMA=OFF -DWITH_OPENLDAP=OFF -DWITH_FUSE=OFF -DWITH_LIBCEPHFS=OFF -DWITH_KRBD=OFF -DWITH_LTTNG=OFF -DWITH_BABELTRACE=OFF -DWITH_MGR_DASHBOARD_FRONTEND=OFF -DWITH_SYSTEMD=OFF -DWITH_SPDK=OFF -DWITH_CCACHE=ON -DBOOST_J=12"
    CMAKE_BUILD_THREADS = "12"
  }
}

action "download test data" {
  needs = "build ceph and skyhook cls"
  uses = "actions/bin/curl@master"
  runs = ["sh", "-c", "ci/scripts/download-test-data.sh"]
}

action "run test" {
  needs = "download test data"
  uses = "docker://popperized/ceph-builder:nautilus"
  runs = [
    "sh", "-c", "ci/scripts/run-skyhook-test.sh"
  ]
}
