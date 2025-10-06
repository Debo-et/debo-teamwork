CC = gcc
CFLAGS = -g -Wall -Wextra -O2 -I.
LDFLAGS =
LDLIBS = -lresolv
PREFIX ?= /usr/local
DESTDIR ?=
bindir = $(DESTDIR)$(PREFIX)/bin

# Source files and object groups
SRC1 = utiles.c install.c action.c uninstall.c report.c metrics.c configuration.c atalas_conf.c flink_conf.c hbase_conf.c hdfs_conf.c hive_conf.c kafka_conf.c livy_conf.c pig_conf.c presto_conf.c ranger_conf.c solar_conf.c spark_conf.c storm_conf.c tez_conf.c zeppelin_conf.c zookeeper_conf.c
SRC2 = comm.c format.c be-secure-gssapi.c be-gssapi-common.c stringinfo.c ip.c ifaddr.c gsignal.c latch.c debo.c
SRC = $(SRC1) $(SRC2)
OBJ1 = $(SRC1:.c=.o)
OBJ2 = $(SRC2:.c=.o)
OBJ = $(OBJ1) $(OBJ2)

# Kerberos configuration
KRB5_CFLAGS = $(shell krb5-config --cflags)
KRB5_LDFLAGS = $(shell krb5-config --libs gssapi)

# Library checks and flags
LIBSSH_EXISTS := $(shell pkg-config --exists libssh && echo "yes")
LIBXML2_EXISTS := $(shell pkg-config --exists libxml-2.0 && echo "yes")
GSSAPI_EXISTS := $(shell echo "int main(){}" | $(CC) -x c - $(KRB5_LDFLAGS) -o /dev/null 2>/dev/null && echo "yes")

ifeq ($(LIBSSH_EXISTS),)
$(error "libssh is not installed. Please install libssh before proceeding.")
endif

ifeq ($(LIBXML2_EXISTS),)
$(error "libxml2 is not installed. Please install libxml2 before proceeding.")
endif

ifeq ($(GSSAPI_EXISTS),)
$(error "Kerberos GSSAPI (libgssapi_krb5) is missing. Please install libkrb5-dev.")
endif

LIBSSH_CFLAGS = $(shell pkg-config --cflags libssh)
LIBSSH_LDFLAGS = $(shell pkg-config --libs libssh)
LIBXML2_CFLAGS = $(shell pkg-config --cflags libxml-2.0)
LIBXML2_LDFLAGS = $(shell pkg-config --libs libxml-2.0)

# Combine all flags
ALL_CFLAGS = $(CFLAGS) $(KRB5_CFLAGS) $(LIBSSH_CFLAGS) $(LIBXML2_CFLAGS)
ALL_LDFLAGS = $(LDFLAGS) $(LIBSSH_LDFLAGS) $(LIBXML2_LDFLAGS) $(LDLIBS) $(KRB5_LDFLAGS)

# Build targets
all: deboAgent
	@echo "✅ Build completed successfully: deboAgent"

deboAgent: $(OBJ)
	$(CC) $(ALL_CFLAGS) -o $@ $^ $(ALL_LDFLAGS)

# Pattern rules for object files
$(OBJ1): %.o: %.c utiles.h configuration.h action.h uninstall.h report.h
	$(CC) $(ALL_CFLAGS) -c $< -o $@

$(OBJ2): %.o: %.c
	$(CC) $(ALL_CFLAGS) -c $< -o $@

# Installation targets
installdirs:
	mkdir -p $(bindir)

install: all installdirs
	install -m 755 deboAgent $(bindir)
	@echo "✅ Installed deboAgent to $(bindir)"

uninstall:
	rm -f $(bindir)/deboAgent
	@echo "🗑️ Uninstalled deboAgent from $(bindir)"

clean:
	rm -f $(OBJ) deboAgent
	@echo "🧹 Cleaned up build files and object files"

.PHONY: all install uninstall clean installdirs

