Debo Hadoop Manager – Lightweight Tool for Hadoop Clusters

🚀 Introduction

Debo Hadoop Manager is a powerful utility designed to simplify Hadoop cluster management with both command-line (CLI) and graphical user interface (GUI) options. Whether you prefer terminal efficiency or visual interaction, Debo provides flexible access to control and monitor your Hadoop ecosystem.

The CLI is optimized for performance, automation, and scripting, while the GUI offers a user-friendly interface for those more comfortable with graphical tools. This dual-interface approach ensures accessibility and convenience for both technically-inclined users and those preferring visual management, with both interfaces maintained independently for streamlined development and usage.

🔑 Key Features

    Dual Interface: CLI + GUI

        Use the command-line for speed and automation, or switch to the GUI for intuitive, visual management.

    Command-Line Hadoop Cluster Management

        Lightweight and scriptable terminal commands.

    Graphical User Interface

        Modern, responsive GUI for easier navigation and control (maintained separately from CLI).

    Control 10+ Hadoop Ecosystem Components

        Manage HDFS, YARN, HBase, Spark, Kafka, ZooKeeper, Hive, and more.

    Real-Time System Health Monitoring

        Instantly check service statuses and cluster metrics.

    Automated Configuration Deployment

        Deploy and update configurations across the cluster with minimal effort.

    Task Automation

        Script and schedule routine operations with CLI-friendly output.
        

🗂 Directory Structure & Architecture

From an architectural perspective, the Debo project’s codebase is logically divided into two main directories, enabling modularity and scalability in distributed environments:

    server/ – Central Orchestrator

        Acts as a standalone service running on the control node.

        Manages and monitors Hadoop-related components across the cluster.

        Issues commands to agents and processes their status reports.

        Hosts both CLI and GUI interfaces, maintained independently.

    agent/ – Distributed Executors

        Contains the code for the agent software deployed on each cluster node.

        Listens for management instructions from the server.

        Executes local operations (start/stop services, collect metrics, etc.).

        Sends execution results back to the server for centralized reporting.

Together, these components form a centralized control / distributed execution architecture, making Debo a powerful and extensible solution for administrators of Hadoop-based or similarly structured cluster environments.
