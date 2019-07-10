# CI workflows

This folder contains continuous integration (github action) workflows. 
The workflows available are:

  * [`**smoke**`](workflows/smoke.workflow). Builds the skyhook fork 
    and RADOS class, and subsequently executes the test (upload, index 
    and query data).

These workflows run in a container runtime ([Docker][docker], 
[Singularity][singularity], [Podman][podman], etc.) and can be 
executed locally on your machine with the [Popper CLI tool][popper]. 
The following executes all the workflows:

```bash
cd skyhook-ceph/
popper run --recursive
```

To execute a single workflow:

```bash
popper run --wfile ci/workflows/smoke.workflow
```

[docker]: https://get.docker.com
[popper]: https://github.com/systemslab/popper
[singularity]: https://github.com/sylabs/singularity
[podman]: https://podman.io
