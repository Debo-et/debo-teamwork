# debo - Hadoop Ecosystem Component Manager

`debo` is a command-line tool designed to manage, monitor, and automate actions on components of the Hadoop ecosystem. It supports operations like start, stop, restart, install, configure, and uninstall for services such as HDFS, Spark, Kafka, and more.

---

## 🚀 Features

- Control major Hadoop ecosystem components
- Perform operations: start, stop, restart, install, upgrade, uninstall, configure
- Intuitive command-line interface
- Remote host targeting (`--host`, `--port`)
- Built-in help (`--help`) and version info (`--version`)

---

## 📦 Supported Components

- HDFS
- HBase
- Spark
- Kafka
- ZooKeeper
- Flink
- Storm
- Hive
- Pig
- Presto
- Tez
- Atlas
- Ranger
- Zeppelin
- Livy
- Phoenix
- Solr

---

## 🛠️ Usage

### Command Syntax
```sh
./debo [ACTION] [COMPONENT] [OPTIONS...]
./debo [OPTIONS...]
Actions
Option	Description
--start	Start the component
--stop	Stop the component
--restart	Restart the component
--install	Install the component
--upgrade	Upgrade the component
--uninstall	Uninstall the component
--configure	Apply configuration changes
Components
Option	Description
--all	Apply action to all components
--hdfs	HDFS distributed filesystem
--hbase	HBase database
--spark	Spark processing engine
--kafka	Kafka messaging system
--zookeeper	ZooKeeper coordination
...	(See full list in source code)
General Options
Short Option	Long Option	Description
-h	--host=HOST	Target server hostname
-p	--port=PORT	Connection port number
-V	--version	Show component version
	--help	Show help message
🧪 Examples
sh

# Check HDFS status
./debo --hdfs

# Start ZooKeeper
./debo --start --zookeeper

# Restart all components
./debo --restart --all

# Install Kafka on a remote host
./debo --install --kafka --host hadoop-cluster --port 8080

# Show version info
./debo --version

🧰 Build Instructions
🧱 Dependencies

Ensure the following libraries are installed:

    libssh

    libxml2

    libresolv (typically part of libc)

Install dependencies on Debian/Ubuntu:

sudo apt install libssh-dev libxml2-dev

🏗️ Build

make            # Compile the project
sudo make install   # Install system-wide (/usr/local/bin by default)
sudo make uninstall # Uninstall
make clean      # Clean build files

📁 Custom Installation Path

sudo make PREFIX=/opt/debo install  # Install to /opt/debo/bin/debo

