
deboAgent – Distributed Node Agent
Overview

deboAgent is a lightweight agent designed to be deployed on each node of a distributed system. It facilitates remote management by listening for incoming instructions from a central server, executing operations locally on the node, and reporting the results back to the server. This enables centralized monitoring, control, and automation across multiple nodes in a distributed environment.

This agent is ideal for systems where distributed nodes need to be managed efficiently through a central orchestration mechanism.
Features

    Listens for and handles management instructions from a central server

    Executes tasks locally on the node

    Sends execution results back to the server

    Modular source structure for easy extensibility

    Clean and portable C codebase

Directory Structure

agent/
├── action.c / .h         # Execution logic for actions
├── comm.c                # Communication functions
├── configuration.c / .h  # Agent configuration handling
├── debo.c                # Main entry point
├── format.c              # Formatting utilities
├── getopt_long.c / .h    # Command-line argument parsing
├── ifaddr.c              # Interface address utilities
├── install.c             # Installation logic
├── ip.c                  # IP utility functions
├── latch.c               # Synchronization primitives
├── report.c / .h         # Reporting to server
├── stringinfo.c          # String manipulation helpers
├── uninstall.c / .h      # Uninstallation logic
├── utiles.c / .h         # General utilities
├── gsignal.c             # Signal handling
├── Makefile              # Build instructions
└── README.md             # This file

Prerequisites

Before building deboAgent, ensure the following libraries are installed:

    libssh – for secure communication

    libxml2 – for configuration and message parsing

You can install them using:

sudo apt-get install libssh-dev libxml2-dev

Build Instructions
To build the agent:

make

This compiles the code and produces the deboAgent binary.
To clean build artifacts:

make clean

Installation

You can install the compiled agent binary system-wide using:

sudo make install

By default, this installs the binary into /usr/local/bin. You can override the install prefix if needed:

sudo make PREFIX=/custom/path install

Uninstallation

To remove the installed agent:

sudo make uninstall

Usage

After installation, the deboAgent can be executed on each node. It typically runs as a background service listening for instructions. Further documentation on configuration and protocol definitions is forthcoming.
