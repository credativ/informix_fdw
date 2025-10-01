#/bin/bash

set -e

# Check for given major version of PG and Informix env, use default PG17
: ${PG_MAJOR_VERSION:=17}
: ${INFORMIXSERVER:=informix}
: ${INFORMIXSQLHOSTS:=/tmp/csdk/etc/sqlhosts}
: ${INFORMIXDIR:=/tmp/csdk}
export PGUSER=postgres
export PATH=$PATH:/tmp/csdk/bin:/usr/pgsql-${PG_MAJOR_VERSION}/bin/

## Check if a specific git hash was provided as the first argument. If true
## we have to check it out below from the repo.
[ ! -z "$1" ] && : ${IFX_FDW_COMMIT:=$1}

# Show pg_config version
echo "Testing for $(pg_config --version)"

if [ ! -f /.initialized ]; then

  # Setup PostgreSQL instance
  su - postgres -c "/usr/pgsql-${PG_MAJOR_VERSION}/bin/initdb -D /var/lib/pgsql/${PG_MAJOR_VERSION}/data"
  su - postgres -c "/usr/pgsql-${PG_MAJOR_VERSION}/bin/pg_ctl -D /var/lib/pgsql/${PG_MAJOR_VERSION}/data start"

  touch /.initialized

  echo "Waiting 30s for Informix database (setup delay)"
  sleep 30

  git clone https://github.com/credativ/informix_fdw.git

fi

# Build informix foreign data wrapper extension and install it

pushd informix_fdw

# In case a specific commit was specified, check it out
if [ ! -z "${IFX_FDW_COMMIT}" ]; then
  echo "checking out specific commit hash ${IFX_FDW_COMMIT}"
  git checkout ${IFX_FDW_COMMIT}
fi

make
CFLAGS="-g" make install
ln -sf /regression_variables.dml sql/regression_variables
REGRESS=informix_fdw_tx make installcheck
popd
