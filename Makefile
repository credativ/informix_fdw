MODULE_big=ifx_fdw
OBJS=ifx_connection.o ifx_conncache.o ifx_utils.o ifx_conv.o ifx_fdw.o
ESQL=esql

ifndef PG_CONFIG
PG_CONFIG=pg_config
endif

##
## Which ESQL/C libs to link.
##
ESQL_LIBS=$(shell $(ESQL) -libs)

SHLIB_LINK += -L$(INFORMIXDIR)/lib/ -L$(INFORMIXDIR)/lib/esql
EXTENSION = informix_fdw
DATA = informix_fdw--1.0.sql
PG_CPPFLAGS += -I$(INFORMIXDIR)/incl/esql

## GNU/Linux
ifeq (--as-needed, $(findstring --as-needed, $(shell $(PG_CONFIG) --ldflags)))
LDFLAGS_SL=-Wl,--no-as-needed $(ESQL_LIBS) -Wl,--as-needed
endif

## OSX
ifeq (-dead_strip_dylibs, $(findstring -dead_strip_dylibs, $(shell $(PG_CONFIG) --ldflags)))
## Currently we link statically on OSX.
ESQL_LIBS=$(shell $(ESQL) -libs -static)
LDFLAGS_SL = $(ESQL_LIBS)
endif

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
