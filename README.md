# Debo — Hadoop Cluster Management Made Simple

**Debo** is an emerging cluster management platform tailored for the Hadoop ecosystem. It currently includes two complementary products:

- **Debo CLI** — A lightweight, terminal-based tool for managing and monitoring your Hadoop cluster. **Free forever**.
- **Debo GUI** — A graphical interface (coming soon) designed to simplify operations even further.

---

### 📘 CLI User Manual

To get started with the CLI, we recommend reading the official user manual:

- [Online Document Version (Google Docs)](https://docs.google.com/document/d/1f6aAdDYIo8IV4-KOuE9T5wYHQrArYjkqLgzj4ATo9Tg/edit?tab=t.0)
- [Downloadable PDF Version (PDF)](https://drive.google.com/file/d/1c8SzU6iZ3oulhuR5pnM5Kb-Toi0KhXSx/view)

The guide includes setup instructions, usage examples, and available commands.

# Integrating Debo with Existing Hadoop Ecosystem Deployments

This README provides a comprehensive overview of how **Debo** can be seamlessly integrated into systems where various Hadoop ecosystem components are already installed and actively in use. Debo is designed to work alongside existing infrastructure — whether managed manually or through external cluster managers — without requiring reinstallation or reconfiguration of the underlying services.

## ✅ Compatibility with Pre-Installed Hadoop Components

Debo can be deployed in environments that already include a wide range of Hadoop-related tools. As long as the environment variables for each component — particularly those pointing to their home directories — are correctly configured, Debo will detect and interact with them reliably.

These environment variables (e.g., `SPARK_HOME`, `HADOOP_HOME`, `HIVE_HOME`, etc.) must be set in the system or user shell where Debo is executed. They allow Debo to locate binaries, configuration files, and runtime dependencies for each component without requiring hardcoded paths or manual overrides.

## 🧩 Supported Components

Debo is compatible with the following Hadoop ecosystem components:

* Apache Flink
* Hadoop Distributed File System (HDFS)
* Apache HBase
* Apache Hive
* Apache Kafka
* Apache Livy
* Apache Phoenix
* Apache Storm
* Apache Pig
* Apache Oozie
* Presto
* Apache Atlas
* Apache Ranger
* Apache Solr
* Apache Spark
* Apache Tez
* Apache Zeppelin
* Apache Zookeeper

## 🔧 Configuration Requirements

To ensure proper integration, users must verify that the relevant environment variables are correctly set. Below is a sample snippet showing how to export common environment variables in a bash-compatible shell. Adjust paths to match your installation locations.

```bash
# Example environment variables (adjust to your installation paths)
export HADOOP_HOME=/opt/hadoop
export SPARK_HOME=/opt/spark
export HIVE_HOME=/opt/hive
export KAFKA_HOME=/opt/kafka
export HBASE_HOME=/opt/hbase
export FLINK_HOME=/opt/flink
export ZOOKEEPER_HOME=/opt/zookeeper

# Add binaries to PATH (optional but recommended)
export PATH="$HADOOP_HOME/bin:$SPARK_HOME/bin:$KAFKA_HOME/bin:$PATH"
```

> Tip: If you manage your cluster with systemd, environment files, or container orchestration, ensure Debo's runtime environment either inherits these variables or that you provide equivalent configuration to the Debo process.

---

For more, stay tuned to this repository as we continue to improve and expand Debo.

