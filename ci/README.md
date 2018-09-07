docker run -it --entrypoint=/bin/bash -v $PWD/skyhook/ci:/ci -v $PWD/skyhookdb-ceph:/ceph ceph/daemon:v3.0.7-stable-3.0-kraken-ubuntu-16.04-x86_64
./micro-osd deploy
docker run -it --rm --name test -v $PWD/skyhookdb-ceph:/ceph cephbuilder/ceph:kraken
make run-query

TODO: pull build container and compile run-query and cls, pull ceph/daemon:kraken + raw data in container, run micro-osd, and run containers
 package raw data into container, pull container

