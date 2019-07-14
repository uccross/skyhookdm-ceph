workflow "smoke tests" {
  resolves = "run tests"
}

action "build skyhook cls" {
  uses = "popperized/cmake@ubuntu-18.04"
  args = "cls_tabular run-query ceph_test_skyhook_query"
  env = {
    CMAKE_PROJECT_DIR = "./"
    CMAKE_INSTALL_DEPS_SCRIPT = "install-deps.sh"
    CMAKE_FLAGS = "-DCMAKE_BUILD_TYPE=MinSizeRel -DWITH_RBD=OFF -DWITH_CEPHFS=OFF -DWITH_RADOSGW=OFF -DWITH_LEVELDB=OFF -DWITH_MANPAGE=OFF -DWITH_RDMA=OFF -DWITH_OPENLDAP=OFF -DWITH_FUSE=OFF -DWITH_LIBCEPHFS=OFF -DWITH_KRBD=OFF -DWITH_LTTNG=OFF -DWITH_BABELTRACE=OFF -DWITH_SYSTEMD=OFF -DWITH_SPDK=OFF -DWITH_CCACHE=ON -DBOOST_J=2"
    CMAKE_BUILD_THREADS = "2"
  }
}

action "download test data" {
  needs = "build skyhook cls"
  uses = "actions/bin/curl@master"
  runs = ["sh", "-c", "ci/scripts/download-test-data.sh"]
}

# NOTE: done offline to speedup travis build time
#
# action "build ceph image" {
#   needs = "download test data"
#   uses = "actions/docker/cli@master"
#   args = "build -t popperized/ceph:luminous ci/docker"
# }
#
# action "registry login" {
#   needs = "build ceph image"
#   uses = "actions/docker/login@master"
#   secrets = ["DOCKER_USERNAME", "DOCKER_PASSWORD"]
# }
#
# action "push image" {
#   needs = "registry login"
#   uses = "actions/docker/cli@master"
#   args = "push popperized/ceph:luminous"
# }

action "run tests" {
#  needs = "push image"
  needs = "download test data"
  uses = "docker://popperized/ceph:luminous"
  runs = [
    "sh", "-c", "ci/scripts/run-skyhook-test.sh"
  ]
}
