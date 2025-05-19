CC = gcc
CFLAGS = -g -Wall -Wextra -O0 -I.
CFLAGS += -Wno-format-truncation

LDLIBS = -lresolv

OBJ1 = getopt_long.o utiles.o apache.o configuration.o install.o action.o uninstall.o report.o
OBJ2 = connect.o misc.o expbuffer.o
OBJ = $(OBJ1) $(OBJ2)
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

$(OBJ1): %.o: %.c $(COMMON_HEADERS)
	$(CC) $(CFLAGS) $(LIBSSH_CFLAGS) $(LIBXML2_CFLAGS) -c $< -o $@

$(OBJ2): %.o: %.c
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
	rm -f $(OBJ) $(TARGET)
	@echo "Cleaned up build files"

.PHONY: all install uninstall clean installdirs
