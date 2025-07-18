CC = gcc
CFLAGS = -g -Wall -Wextra -O0 -I. -pthread
CFLAGS += -Wno-format-truncation

LDLIBS = -lresolv -pthread

# Separate main application objects from library objects
MAIN_OBJ = apache.o
LIB_OBJ = getopt_long.o utiles.o install.o action.o uninstall.o report.o connect.o misc.o expbuffer.o fe-secure-gssapi.o fe-gssapi-common.o configuration.o atalas_conf.o flink_conf.o hbase_conf.o hdfs_conf.o hive_conf.o kafka_conf.o livy_conf.o pig_conf.o presto_conf.o ranger_conf.o solar_conf.o spark_conf.o storm_conf.o tez_conf.o zeppelin_conf.o zookeeper_conf.o
OBJ = $(MAIN_OBJ) $(LIB_OBJ)
TARGET = debo

PREFIX ?= /usr/local
DESTDIR ?=
bindir = $(DESTDIR)$(PREFIX)/bin

# Check for dependencies
LIBSSH_EXISTS := $(shell pkg-config --exists libssh && echo "yes")
LIBXML2_EXISTS := $(shell pkg-config --exists libxml-2.0 && echo "yes")

ifeq ($(LIBSSH_EXISTS),)
$(error "libssh is not installed. Please install libssh before proceeding.")
endif

ifeq ($(LIBXML2_EXISTS),)
$(error "libxml2 is not installed. Please install libxml2 before proceeding.")
endif

# Manually check for GSSAPI presence (libgssapi_krb5)
GSSAPI_INSTALLED := $(shell ld -lgssapi_krb5 -o /dev/null 2>/dev/null && echo yes)

ifeq ($(GSSAPI_INSTALLED),)
$(error "Kerberos GSSAPI (libgssapi_krb5) is not found by the linker. Ensure that libkrb5-dev is installed and that the library path is correct. This client requires GSSAPI support to connect to the 'debo' Kerberos service.")
endif


# Extract flags
LIBSSH_CFLAGS = $(shell pkg-config --cflags libssh)
LIBSSH_LDFLAGS = $(shell pkg-config --libs libssh)
LIBXML2_CFLAGS = $(shell pkg-config --cflags libxml-2.0)
LIBXML2_LDFLAGS = $(shell pkg-config --libs libxml-2.0)

# Add GSSAPI manually since pkg-config doesn't work
LDLIBS += -lgssapi_krb5

COMMON_HEADERS = getopt_long.h utiles.h configuration.h action.h uninstall.h report.h

all: $(TARGET)

$(TARGET): $(OBJ)
	$(CC) $(CFLAGS) $(LIBSSH_CFLAGS) $(LIBXML2_CFLAGS) -o $@ $^ \
		$(LIBSSH_LDFLAGS) $(LIBXML2_LDFLAGS) $(LDLIBS)

# Rules for compiling source files
%.o: %.c $(COMMON_HEADERS)
	$(CC) $(CFLAGS) $(LIBSSH_CFLAGS) $(LIBXML2_CFLAGS) -c $< -o $@

connect.o misc.o expbuffer.o: %.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

installdirs:
	mkdir -p $(bindir)

install: all installdirs
	install -m 755 $(TARGET) $(bindir)
	@echo "Installed $(TARGET) to $(bindir)"

uninstall:
	rm -f $(bindir)/$(TARGET)
	@echo "Uninstalled $(TARGET)"

clean:
	rm -f $(OBJ) $(TARGET) test_debo test_debo.o test_debo_remote test_debo_remote.o
	@echo "Cleaned up build files and test artifacts"

# Test compilation and execution
TEST_TARGET = test_debo
TEST_OBJ = test_debo.o

$(TEST_TARGET): $(TEST_OBJ) $(LIB_OBJ)
	$

