MODULE_big=ifx_fdw
OBJS=ifx_connection.o ifx_conncache.o ifx_utils.o ifx_conv.o ifx_fdw.o
ESQL=esql

##
## Which ESQL/C libs to link.
##
## Currently we link statically against all required libraries
## needed by ESQL/C.
ESQL_LIBS=$(shell $(ESQL) -libs -static)

SHLIB_LINK += -L$(INFORMIXDIR)/lib/ -L$(INFORMIXDIR)/lib/esql
EXTENSION = informix_fdw
DATA = informix_fdw--1.0.sql
PG_CPPFLAGS += -I$(INFORMIXDIR)/incl/esql
LDFLAGS_SL=$(ESQL_LIBS)

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

ifx_connection.c: ifx_connection.ec
	@echo "Preprocessing Informix ESQL/C sources"
	## Only preprocessing, compilation will be performed later
	$(ESQL) -c $<

maintainer-clean:
	rm -f ifx_connection.c
	rm -rf *.*~
	rm -rf *~
