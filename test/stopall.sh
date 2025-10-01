#/bin/bash

TEST_VERSIONS=${1-18}

for i in ${TEST_VERSIONS}; do
    PG_MAJOR_VERSION=${i} podman-compose down pg-fdw
done
