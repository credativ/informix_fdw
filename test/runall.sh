#!/bin/bash

TEST_VERSIONS=${1:-18}

podman-compose build informix-regression
podman-compose up -d informix-regression

for i in ${TEST_VERSIONS}; do

    PG_MAJOR_VERSION="${i}" podman-compose build --build-arg CSDK_PACKAGE=ibm.csdk.4.50.FC8.LNX.tar \
                        --build-arg PG_MAJOR_VERSION="${i}" pg-fdw
    PG_MAJOR_VERSION="${i}" podman-compose up -d pg-fdw
    podman exec -it pg-fdw-"${i}" bash build.sh ${2}

done

