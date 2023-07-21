Informix FDW testing infrastructure
===================================

Images
----------

This directory contains definitions to build and run the Informix Foreign Data Wrapper extension
out of the box within podman or eventually docker. The latter wasn't tested so far.

The containers here contains of two images, which you can build as follows to setup the
infrastructure. First, the `informix_fdw_test` image can run a container with a specific
PostgreSQL major version to build and test. For this, you have to specify the `PG_MAJOR_VERSION`
build argument. This build argument currently defaults to `14`, so if omitted the image
will be build with PostgreSQL 14. The PostgreSQL performs a standard RPM install from
`https://yum.postgresql.org` and additionally installs `debuginfo` packages for debug symbols and
better debugging along with other development tools.

The Informix CSDK is *not* included and must be copied alongside the `Dockerfile` to build the
`informix_fdw_test` image correctly. To this, put the name of the downloaded CSDK package into
the build argument `CSDK_PACKAGE`, like the following example shows (which builds against
PostgreSQL 15):

```shell
podman build --build-arg CSDK_PACKAGE=ibm.csdk.4.50.FC8.LNX.tar --build-arg PG_MAJOR_VERSION=15 -t informix_fdw_test:dev -f Dockerfile
```

This directory also provides a `Dockerfile-informix` to build an image based on
`https://hub.docker.com/r/ibmcom/informix-developer-database`. The database instance is
automatically initialized with the `regression` and `regression_dml` regression databases to run
the extension regression tests.

```shell
podman build -t informix_fdw_server:dev -f Dockerfile-informix
```

podman-compose
--------------

To ease all this more, this directory also contains a `podman-compose.yaml` definition for
`podman-compose`. For example, to run everything you need with your own `CSDK_PACKAGE` and
PostgreSQL 15, you just execute:

```shell
PG_MAJOR_VERSION=15 podman-compose build --build-arg CSDK_PACKAGE=ibm.csdk.4.50.FC8.LNX.tar \
--build-arg PG_MAJOR_VERSION=15
```

Start containers:

```shell
PG_MAJOR_VERSION=15 podman-compose up
```

It's possible to have multiple Postgres major versions around and running, just pass the correct
`PG_MAJOR_VERSION` to the commands above. Note that we currently have to provide the PG major
versions twice, as a build arg and environment variable.

When started, there are two containers running, `informix` with a running Informix Developer
Edition and a Postgres container with the specified PG version (e.g. `pg-fdw-15` for PG15). Building
and executing regression tests can be done in the following way:

```shell
podman exec -it pg-fdw-15 bash build.sh
```

Editing and compiling can also be done within the container, all required development file are
installed.

Using testing containers
------------------------

`podman-compose-pgdg-testing.yaml` provides a service `pg-fdw-pgdg-testing` especially for builds
against the PGDG testing repositories. All other definition are the same as in podman-compose.yaml.

The `pg-fdw` container there is in contrast initialized with `pgdg${PG_MAJOR_VERSION}-updates-testing`
repositories, which also allows to test with a new upcoming major release as soon as PGDG upstream
provides those packages. To initiate testing, issue the following commands for example:

```shell
 PG_MAJOR_VERSION=16 podman-compose -f podman-compose-pgdg-testing.yaml up -d  informix-regression pg-fdw-pgdg-testing
```
