FROM rockylinux/rockylinux:8 AS informix_fdw_test

ARG CSDK_PACKAGE
ARG PG_MAJOR_VERSION=14
ENV PGUSER=postgres
ENV PG_MAJOR_VERSION=${PG_MAJOR_VERSION}
ENV CSDK_PACKAGE=${CSDK_PACKAGE}

COPY ${CSDK_PACKAGE} /tmp
COPY build.sh /
COPY regression_variables.dml /
COPY sqlhosts /
COPY emacs-rc /root/.emacs

RUN dnf install -y redhat-rpm-config gcc java-1.8.0-openjdk findutils git tar ncurses-compat-libs \
     wget make bzip2 diffutils gdb emacs-nox vim && \
    wget --quiet https://download.postgresql.org/pub/repos/yum/RPM-GPG-KEY-PGDG && \
    rpm --import RPM-GPG-KEY-PGDG && \
    dnf module -y disable postgresql && \
    dnf install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-8-x86_64/pgdg-redhat-repo-latest.noarch.rpm && \
    dnf install -y postgresql${PG_MAJOR_VERSION}-server postgresql${PG_MAJOR_VERSION}-devel && \
    dnf --enablerepo pgdg${PG_MAJOR_VERSION}-debuginfo install -y \
postgresql${PG_MAJOR_VERSION}-server-debuginfo  postgresql${PG_MAJOR_VERSION}-devel-debuginfo && \
    cd /tmp && \
    ls -la && \
    tar -xf ${CSDK_PACKAGE} && \
    ls -la && \
    useradd -d /home/informix informix && \
    echo "export PATH=$PATH:/tmp/csdk/bin:/usr/pgsql-${PG_MAJOR_VERSION}/bin/" >> /home/informix/.bash_profile && \
    echo "export INFORMIXDIR=/tmp/csdk" >> /home/informix/.bash_profile && \
    echo "export INFORMIXSERVER=informix" >> /home/informix/.bash_profile && \
    echo "export CLIENT_LOCALE=en_US.utf8" >> /home/informix/.bash_profile && \
    echo "export DB_LOCALE=en_US.819" >> /home/informix/.bash_profile && \
    echo "export INFORMIXSQLHOSTS=/tmp/csdk/etc/sqlhosts" >> /home/informix/.bash_profile && \
    bash /tmp/installclientsdk -i silent -DLICENSE_ACCEPTED=TRUE -DUSER_INSTALL_DIR=/tmp/csdk && \
    echo /tmp/csdk/lib >> /etc/ld.so.conf.d/informix.conf && \
    echo /tmp/csdk/lib/esql >> /etc/ld.so.conf.d/informix.conf && \
    ln -sf /sqlhosts /tmp/csdk/etc/sqlhosts && \
    ldconfig
