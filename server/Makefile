CC = gcc
CFLAGS = -g -Wall -Wextra -O0 -I.
CFLAGS += -Wno-format-truncation

LDLIBS = -lresolv

# Separate main application objects from library objects
MAIN_OBJ = apache.o
LIB_OBJ = getopt_long.o utiles.o install.o action.o uninstall.o report.o connect.o misc.o expbuffer.o atalas_conf.o flink_conf.o hbase_conf.o hdfs_conf.o hive_conf.o kafka_conf.o livy_conf.o pig_conf.o presto_conf.o ranger_conf.o solar_conf.o spark_conf.o storm_conf.o tez_conf.o zeppelin_conf.o zookeeper_conf.o
OBJ = $(MAIN_OBJ) $(LIB_OBJ)
TARGET = debo

PREFIX ?= /usr/local
DESTDIR ?=
bindir = $(DESTDIR)$(PREFIX)/bin

LIBSSH_EXISTS := $(shell pkg-config --exists libssh && echo "yes")
LIBXML2_EXISTS := $(shell pkg-config --exists libxml-2.0 && echo "yes")

ifeq ($(LIBSSH_EXISTS),)
$(error "libssh is not installed. Please install libssh before proceeding.")
endif

ifeq ($(LIBXML2_EXISTS),)
$(error "libxml2 is not installed. Please install libxml2 before proceeding.")
endif

LIBSSH_CFLAGS = $(shell pkg-config --cflags libssh)
LIBSSH_LDFLAGS = $(shell pkg-config --libs libssh)
LIBXML2_CFLAGS = $(shell pkg-config --cflags libxml-2.0)
LIBXML2_LDFLAGS = $(shell pkg-config --libs libxml-2.0)

COMMON_HEADERS = getopt_long.h utiles.h configuration.h action.h uninstall.h report.h

all: $(TARGET)

$(TARGET): $(OBJ)
	$(CC) $(CFLAGS) $(LIBSSH_CFLAGS) $(LIBXML2_CFLAGS) -o $@ $^ $(LIBSSH_LDFLAGS) $(LIBXML2_LDFLAGS) $(LDLIBS)

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
	$(CC) $(CFLAGS) $(LIBSSH_CFLAGS) $(LIBXML2_CFLAGS) -o $@ $^ \
		$(LIBSSH_LDFLAGS) $(LIBXML2_LDFLAGS) $(LDLIBS)

test_debo.o: test_debo.c $(COMMON_HEADERS)
	$(CC) $(CFLAGS) $(LIBSSH_CFLAGS) $(LIBXML2_CFLAGS) -c $< -o $@

check: $(TEST_TARGET)
	@echo "Running test suite $(TEST_TARGET)..."
	@./$(TEST_TARGET) || (echo "Test failed with exit code $$?"; exit 1)
	@echo "All tests passed successfully"

# Remote test targets
TEST_REMOTE_TARGET = test_debo_remote
TEST_REMOTE_OBJ = test_debo_remote.o

$(TEST_REMOTE_TARGET): $(TEST_REMOTE_OBJ) $(LIB_OBJ)
	$(CC) $(CFLAGS) $(LIBSSH_CFLAGS) $(LIBXML2_CFLAGS) -o $@ $^ \
		$(LIBSSH_LDFLAGS) $(LIBXML2_LDFLAGS) $(LDLIBS)

test_debo_remote.o: test_debo_remote.c $(COMMON_HEADERS)
	$(CC) $(CFLAGS) $(LIBSSH_CFLAGS) $(LIBXML2_CFLAGS) -c $< -o $@

check-remote: $(TEST_REMOTE_TARGET)
	@echo "Running REMOTE test suite $(TEST_REMOTE_TARGET)..."
	@./$(TEST_REMOTE_TARGET) || (echo "Remote tests failed with exit code $$?"; exit 1)
	@echo "All REMOTE tests passed successfully"

.PHONY: all install uninstall clean installdirs check check-remote
