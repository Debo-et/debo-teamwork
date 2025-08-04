/*
 * Copyright 2025 Surafel Temesgen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#include <libxml/parser.h>
#include <libxml/tree.h>
#include <libxml/xpath.h>

#include "utiles.h"

typedef struct {
    const char* canonical_name;
    const char* config_file;
} PredefinedHiveParameter;

ConfigResult* process_hive_parameter(const char* param_name, const char* param_value) {
    PredefinedHiveParameter hive_predefined_params[] = {
        // Execution Engine & Runtime Behavior
        { "hive.execution.engine", "hive-site.xml" },                  // mr/tez/spark
        { "hive.exec.parallel", "hive-site.xml" },
        { "hive.exec.parallel.thread.number", "hive-site.xml" },
        { "hive.fetch.task.conversion", "hive-site.xml" },              // query result fetching
        { "hive.exec.mode.local.auto", "hive-site.xml" },               // auto-local mode

        // Metastore Configuration
        { "hive.metastore.uris", "hive-metastore-site.xml" },           // remote metastore URIs
        { "javax.jdo.option.ConnectionURL", "hive-metastore-site.xml" },// Embedded metastore JDBC URL
        { "javax.jdo.option.ConnectionDriverName", "hive-metastore-site.xml" },
        { "hive.metastore.warehouse.dir", "hive-metastore-site.xml" },
        { "hive.metastore.schema.verification", "hive-metastore-site.xml" },
        { "hive.metastore.thrift.port", "hive-metastore-site.xml" },
        { "hive.metastore.sasl.enabled", "hive-metastore-site.xml" },   // Metastore security

        // Security & Authorization
        { "hive.security.authorization.enabled", "hive-site.xml" },
        { "hive.security.authorization.manager", "hive-site.xml" },     // SQLStd/Ranger
        { "hive.server2.authentication", "hive-site.xml" },             // KERBEROS/LDAP/etc
        { "hive.server2.xsrf.filter.enabled", "hive-site.xml" },
        { "hive.server2.enable.doAs", "hive-site.xml" },                // impersonation
        { "hive.users.in.admin.role", "hive-site.xml" },
        { "hive.security.authorization.ranger.url", "hive-site.xml" },  // Ranger integration

        // Transactions & Concurrency
        { "hive.support.concurrency", "hive-site.xml" },               // enable concurrency
        { "hive.txn.manager", "hive-site.xml" },                        // DbTxnManager
        { "hive.compactor.worker.threads", "hive-site.xml" },
        { "hive.lock.numretries", "hive-site.xml" },
        { "hive.lock.sleep.between.retries", "hive-site.xml" },

        // Query Optimization
        { "hive.auto.convert.join", "hive-site.xml" },
        { "hive.optimize.bucketmapjoin", "hive-site.xml" },
        { "hive.cbo.enable", "hive-site.xml" },                         // Cost-based optimization
        { "hive.vectorized.execution.enabled", "hive-site.xml" },
        { "hive.optimize.ppd", "hive-site.xml" },                       // predicate pushdown
        { "hive.optimize.skewjoin", "hive-site.xml" },
        { "hive.merge.mapfiles", "hive-site.xml" },                     // small file merging

        // Storage & Serialization
        { "hive.default.fileformat", "hive-site.xml" },                 // ORC/Parquet/Text
        { "hive.exec.compress.output", "hive-site.xml" },
        { "hive.exec.compress.intermediate", "hive-site.xml" },
        { "hive.orc.compute.splits.num.threads", "hive-site.xml" },
        { "hive.parquet.compression", "hive-site.xml" },

        // Tez/Spark Engine Configuration
        { "hive.tez.container.size", "hive-site.xml" },                 // Tez container sizing
        { "hive.tez.java.opts", "hive-site.xml" },
        { "hive.execution.spark.client.timeout", "hive-site.xml" },
        { "hive.spark.client.server.connect.timeout", "hive-site.xml" },

        // LLAP Configuration
        { "hive.llap.io.enabled", "hive-site.xml" },
        { "hive.llap.daemon.service.hosts", "hive-site.xml" },

        // Dynamic Partitioning
        { "hive.exec.dynamic.partition.mode", "hive-site.xml" },
        { "hive.exec.max.dynamic.partitions", "hive-site.xml" },
        { "hive.exec.max.dynamic.partitions.pernode", "hive-site.xml" },

        // Statistics & Metadata
        { "hive.stats.autogather", "hive-site.xml" },
        { "hive.stats.fetch.column.stats", "hive-site.xml" },

        // HDFS Integration
        { "hive.exec.stagingdir", "hive-site.xml" },                     // temp directory
        { "hive.blobstore.use.blobstore.as.scratchdir", "hive-site.xml" }, // S3/Cloud integration

        // Server Configuration
        { "hive.server2.thrift.port", "hive-site.xml" },
        { "hive.server2.idle.operation.timeout", "hive-site.xml" },
        { "hive.server2.thrift.max.worker.threads", "hive-site.xml" },

        // Legacy & Compatibility
        { "hive.mapred.mode", "hive-site.xml" },                        // strict/nonstrict
        { "hive.support.sql11.reserved.keywords", "hive-site.xml" },
        // Global audit parameters
        { "xasecure.audit.is.enabled", "ranger-hive-audit.xml" },

        // HDFS audit parameters :cite[2]:cite[3]
        { "xasecure.audit.hdfs.is.enabled", "ranger-hive-audit.xml" },
        { "xasecure.audit.hdfs.is.async", "ranger-hive-audit.xml" },
        { "xasecure.audit.hdfs.async.max.queue.size", "ranger-hive-audit.xml" },
        { "xasecure.audit.hdfs.async.max.flush.interval.ms", "ranger-hive-audit.xml" },
        { "xasecure.audit.hdfs.config.encoding", "ranger-hive-audit.xml" },
        { "xasecure.audit.hdfs.config.destination.directory", "ranger-hive-audit.xml" },
        { "xasecure.audit.hdfs.config.destination.file", "ranger-hive-audit.xml" },
        { "xasecure.audit.hdfs.config.destination.flush.interval.seconds", "ranger-hive-audit.xml" },
        { "xasecure.audit.hdfs.config.destination.rollover.interval.seconds", "ranger-hive-audit.xml" },
        { "xasecure.audit.hdfs.config.destination.open.retry.interval.seconds", "ranger-hive-audit.xml" },
        { "xasecure.audit.hdfs.config.local.buffer.directory", "ranger-hive-audit.xml" },
        { "xasecure.audit.hdfs.config.local.buffer.file", "ranger-hive-audit.xml" },
        { "xasecure.audit.hdfs.config.local.buffer.file.buffer.size.bytes", "ranger-hive-audit.xml" },
        { "xasecure.audit.hdfs.config.local.buffer.flush.interval.seconds", "ranger-hive-audit.xml" },
        { "xasecure.audit.hdfs.config.local.buffer.rollover.interval.seconds", "ranger-hive-audit.xml" },
        { "xasecure.audit.hdfs.config.local.archive.directory", "ranger-hive-audit.xml" },
        { "xasecure.audit.hdfs.config.local.archive.max.file.count", "ranger-hive-audit.xml" },

        // Log4j audit parameters
        { "xasecure.audit.log4j.is.enabled", "ranger-hive-audit.xml" },
        { "xasecure.audit.log4j.is.async", "ranger-hive-audit.xml" },
        { "xasecure.audit.log4j.async.max.queue.size", "ranger-hive-audit.xml" },
        { "xasecure.audit.log4j.async.max.flush.interval.ms", "ranger-hive-audit.xml" },

        // Kafka audit parameters :cite[3]
        { "xasecure.audit.kafka.is.enabled", "ranger-hive-audit.xml" },
        { "xasecure.audit.kafka.async.max.queue.size", "ranger-hive-audit.xml" },
        { "xasecure.audit.kafka.async.max.flush.interval.ms", "ranger-hive-audit.xml" },
        { "xasecure.audit.kafka.broker_list", "ranger-hive-audit.xml" },
        { "xasecure.audit.kafka.topic_name", "ranger-hive-audit.xml" },

        // Solr audit parameters :cite[1]:cite[3]:cite[6]
        { "xasecure.audit.solr.is.enabled", "ranger-hive-audit.xml" },
        { "xasecure.audit.solr.async.max.queue.size", "ranger-hive-audit.xml" },
        { "xasecure.audit.solr.async.max.flush.interval.ms", "ranger-hive-audit.xml" },
        { "xasecure.audit.solr.solr_url", "ranger-hive-audit.xml" },

        // Ranger security core parameters
        { "ranger.plugin.hive.service.name", "ranger-hive-security.xml" },          // Ranger service name (e.g., hivedev)
        { "ranger.plugin.hive.policy.source.impl", "ranger-hive-security.xml" },    // Policy retrieval class
        { "ranger.plugin.hive.policy.rest.url", "ranger-hive-security.xml" },       // URL to Ranger Admin (critical for policy sync)
        { "ranger.plugin.hive.policy.rest.ssl.config.file", "ranger-hive-security.xml" }, // SSL config path
        { "ranger.plugin.hive.policy.pollIntervalMs", "ranger-hive-security.xml" }, // Policy refresh interval (default: 30s)
        { "ranger.plugin.hive.policy.cache.dir", "ranger-hive-security.xml" },      // Policy cache directory

        // Policy synchronization controls
        { "xasecure.hive.update.xapolicies.on.grant.revoke", "ranger-hive-security.xml" }, // Sync Ranger policies on GRANT/REVOKE :cite[5]
        { "xasecure.hive.uri.permission.coarse.check", "ranger-hive-security.xml" }, // Skip recursive URI checks (optimization)

        // Connection tuning
        { "ranger.plugin.hive.policy.rest.client.connection.timeoutMs", "ranger-hive-security.xml" }, // REST client timeout
        { "ranger.plugin.hive.policy.rest.client.read.timeoutMs", "ranger-hive-security.xml" },       // REST read timeout

        // Ranger audit parameters (from previous integration)
        { "xasecure.audit.is.enabled", "ranger-hive-audit.xml" },
        { "xasecure.audit.solr.is.enabled", "ranger-hive-audit.xml" },
        // SSL/TLS Configuration (ranger-hive-policymgr-ssl.xml)
        { "xasecure.policymgr.clientssl.keystore", "ranger-hive-policymgr-ssl.xml" },             // Keystore file path
        { "xasecure.policymgr.clientssl.truststore", "ranger-hive-policymgr-ssl.xml" },           // Truststore file path
        { "xasecure.policymgr.clientssl.keystore.credential.file", "ranger-hive-policymgr-ssl.xml" },  // Keystore credentials
        { "xasecure.policymgr.clientssl.truststore.credential.file", "ranger-hive-policymgr-ssl.xml" },  // Truststore credentials
                                                                                                         // Beeline Log4j2 Configuration Parameters
        { "status", "beeline-log4j2.properties" },
        { "name", "beeline-log4j2.properties" },
        { "packages", "beeline-log4j2.properties" },
        { "property.hive.log.level", "beeline-log4j2.properties" },
        { "property.hive.root.logger", "beeline-log4j2.properties" },
        { "appenders", "beeline-log4j2.properties" },
        { "appender.console.type", "beeline-log4j2.properties" },
        { "appender.console.name", "beeline-log4j2.properties" },
        { "appender.console.target", "beeline-log4j2.properties" },
        { "appender.console.layout.type", "beeline-log4j2.properties" },
        { "appender.console.layout.pattern", "beeline-log4j2.properties" },
        { "loggers", "beeline-log4j2.properties" },
        { "logger.HiveConnection.name", "beeline-log4j2.properties" },
        { "logger.HiveConnection.level", "beeline-log4j2.properties" },
        { "logger.HiveJDBC.name", "beeline-log4j2.properties" },
        { "logger.HiveJDBC.level", "beeline-log4j2.properties" },
        { "rootLogger.level", "beeline-log4j2.properties" },
        { "rootLogger.appenderRefs", "beeline-log4j2.properties" },
        { "rootLogger.appenderRef.root.ref", "beeline-log4j2.properties" },
        // Log4j2 Configuration Parameters
        { "status", "hive-exec-log4j2.properties" },
        { "name", "hive-exec-log4j2.properties" },
        { "packages", "hive-exec-log4j2.properties" },
        { "property.hive.log.level", "hive-exec-log4j2.properties" },
        { "property.hive.root.logger", "hive-exec-log4j2.properties" },
        { "property.hive.query.id", "hive-exec-log4j2.properties" },
        { "property.hive.log.dir", "hive-exec-log4j2.properties" },
        { "property.hive.log.file", "hive-exec-log4j2.properties" },
        { "appenders", "hive-exec-log4j2.properties" },
        { "appender.console.type", "hive-exec-log4j2.properties" },
        { "appender.console.name", "hive-exec-log4j2.properties" },
        { "appender.console.target", "hive-exec-log4j2.properties" },
        { "appender.console.layout.type", "hive-exec-log4j2.properties" },
        { "appender.console.layout.pattern", "hive-exec-log4j2.properties" },
        { "appender.FA.type", "hive-exec-log4j2.properties" },
        { "appender.FA.name", "hive-exec-log4j2.properties" },
        { "appender.FA.fileName", "hive-exec-log4j2.properties" },
        { "appender.FA.layout.type", "hive-exec-log4j2.properties" },
        { "appender.FA.layout.pattern", "hive-exec-log4j2.properties" },
        { "loggers", "hive-exec-log4j2.properties" },
        { "logger.NIOServerCnxn.name", "hive-exec-log4j2.properties" },
        { "logger.NIOServerCnxn.level", "hive-exec-log4j2.properties" },
        { "logger.ClientCnxnSocketNIO.name", "hive-exec-log4j2.properties" },
        { "logger.ClientCnxnSocketNIO.level", "hive-exec-log4j2.properties" },
        { "logger.DataNucleus.name", "hive-exec-log4j2.properties" },
        { "logger.DataNucleus.level", "hive-exec-log4j2.properties" },
        { "logger.Datastore.name", "hive-exec-log4j2.properties" },
        { "logger.Datastore.level", "hive-exec-log4j2.properties" },
        { "logger.JPOX.name", "hive-exec-log4j2.properties" },
        { "logger.JPOX.level", "hive-exec-log4j2.properties" },
        { "rootLogger.level", "hive-exec-log4j2.properties" },
        { "rootLogger.appenderRefs", "hive-exec-log4j2.properties" },
        { "rootLogger.appenderRef.root.ref", "hive-exec-log4j2.properties" },
        // hive-log4j2.properties parameters
        { "name", "hive-log4j2.properties" },
        { "property.hive.log.level", "hive-log4j2.properties" },
        { "property.hive.root.logger", "hive-log4j2.properties" },
        { "property.hive.log.dir", "hive-log4j2.properties" },
        { "property.hive.log.file", "hive-log4j2.properties" },
        { "property.hive.test.console.log.level", "hive-log4j2.properties" },
        { "appender.console.type", "hive-log4j2.properties" },
        { "appender.console.name", "hive-log4j2.properties" },
        { "appender.console.target", "hive-log4j2.properties" },
        { "appender.console.layout.type", "hive-log4j2.properties" },
        { "appender.console.layout.pattern", "hive-log4j2.properties" },
        { "appender.DRFA.type", "hive-log4j2.properties" },
        { "appender.DRFA.name", "hive-log4j2.properties" },
        { "appender.DRFA.fileName", "hive-log4j2.properties" },
        { "appender.DRFA.filePattern", "hive-log4j2.properties" },
        { "appender.DRFA.layout.type", "hive-log4j2.properties" },
        { "appender.DRFA.layout.pattern", "hive-log4j2.properties" },
        { "appender.DRFA.policies.type", "hive-log4j2.properties" },
        { "appender.DRFA.policies.time.type", "hive-log4j2.properties" },
        { "appender.DRFA.policies.time.interval", "hive-log4j2.properties" },
        { "appender.DRFA.policies.time.modulate", "hive-log4j2.properties" },
        { "appender.DRFA.strategy.type", "hive-log4j2.properties" },
        { "appender.DRFA.strategy.max", "hive-log4j2.properties" },
        { "logger.HadoopIPC.name", "hive-log4j2.properties" },
        { "logger.HadoopIPC.level", "hive-log4j2.properties" },
        { "logger.HadoopSecurity.name", "hive-log4j2.properties" },
        { "logger.HadoopSecurity.level", "hive-log4j2.properties" },
        { "logger.Hdfs.name", "hive-log4j2.properties" },
        { "logger.Hdfs.level", "hive-log4j2.properties" },
        { "logger.HdfsServer.name", "hive-log4j2.properties" },
        { "logger.HdfsServer.level", "hive-log4j2.properties" },
        { "logger.HadoopMetrics2.name", "hive-log4j2.properties" },
        { "logger.HadoopMetrics2.level", "hive-log4j2.properties" },
        { "logger.Mortbay.name", "hive-log4j2.properties" },
        { "logger.Mortbay.level", "hive-log4j2.properties" },
        { "logger.Yarn.name", "hive-log4j2.properties" },
        { "logger.Yarn.level", "hive-log4j2.properties" },
        { "logger.YarnServer.name", "hive-log4j2.properties" },
        { "logger.YarnServer.level", "hive-log4j2.properties" },
        { "logger.Tez.name", "hive-log4j2.properties" },
        { "logger.Tez.level", "hive-log4j2.properties" },
        { "logger.HadoopConf.name", "hive-log4j2.properties" },
        { "logger.HadoopConf.level", "hive-log4j2.properties" },
        { "logger.Zookeeper.name", "hive-log4j2.properties" },
        { "logger.Zookeeper.level", "hive-log4j2.properties" },
        { "logger.ServerCnxn.name", "hive-log4j2.properties" },
        { "logger.ServerCnxn.level", "hive-log4j2.properties" },
        { "logger.NIOServerCnxn.name", "hive-log4j2.properties" },
        { "logger.NIOServerCnxn.level", "hive-log4j2.properties" },
        { "logger.ClientCnxn.name", "hive-log4j2.properties" },
        { "logger.ClientCnxn.level", "hive-log4j2.properties" },
        { "logger.ClientCnxnSocket.name", "hive-log4j2.properties" },
        { "logger.ClientCnxnSocket.level", "hive-log4j2.properties" },
        { "logger.ClientCnxnSocketNIO.name", "hive-log4j2.properties" },
        { "logger.ClientCnxnSocketNIO.level", "hive-log4j2.properties" },
        { "logger.DataNucleus.name", "hive-log4j2.properties" },
        { "logger.DataNucleus.level", "hive-log4j2.properties" },
        { "logger.Datastore.name", "hive-log4j2.properties" },
        { "logger.Datastore.level", "hive-log4j2.properties" },
        { "logger.JPOX.name", "hive-log4j2.properties" },
        { "logger.JPOX.level", "hive-log4j2.properties" },
        { "logger.Operator.name", "hive-log4j2.properties" },
        { "logger.Operator.level", "hive-log4j2.properties" },
        { "logger.Serde2Lazy.name", "hive-log4j2.properties" },
        { "logger.Serde2Lazy.level", "hive-log4j2.properties" },
        { "logger.ObjectStore.name", "hive-log4j2.properties" },
        { "logger.ObjectStore.level", "hive-log4j2.properties" },
        { "logger.CalcitePlanner.name", "hive-log4j2.properties" },
        { "logger.CalcitePlanner.level", "hive-log4j2.properties" },
        { "logger.CBORuleLogger.name", "hive-log4j2.properties" },
        { "logger.CBORuleLogger.level", "hive-log4j2.properties" },
        { "logger.CBORuleLogger.filter.marker.type", "hive-log4j2.properties" },
        { "logger.CBORuleLogger.filter.marker.marker", "hive-log4j2.properties" },
        { "logger.CBORuleLogger.filter.marker.onMatch", "hive-log4j2.properties" },
        { "logger.CBORuleLogger.filter.marker.onMismatch", "hive-log4j2.properties" },
        { "logger.AmazonAws.name", "hive-log4j2.properties" },
        { "logger.AmazonAws.level", "hive-log4j2.properties" },
        { "logger.ApacheHttp.name", "hive-log4j2.properties" },
        { "logger.ApacheHttp.level", "hive-log4j2.properties" },
        { "logger.Thrift.name", "hive-log4j2.properties" },
        { "logger.Thrift.level", "hive-log4j2.properties" },
        { "logger.Jetty.name", "hive-log4j2.properties" },
        { "logger.Jetty.level", "hive-log4j2.properties" },
        { "logger.BlockStateChange.name", "hive-log4j2.properties" },
        { "logger.BlockStateChange.level", "hive-log4j2.properties" },
        { "rootLogger.level", "hive-log4j2.properties" },
        { "rootLogger.appenderRefs", "hive-log4j2.properties" },
        { "rootLogger.appenderRef.root.ref", "hive-log4j2.properties" },
        { "rootLogger.appenderRef.console.ref", "hive-log4j2.properties" },
        { "rootLogger.appenderRef.console.level", "hive-log4j2.properties" },
        { "logger.swo.name", "hive-log4j2.properties" },
        { "logger.swo.level", "hive-log4j2.properties" },
        // Parquet Logging Configuration (from parquet-logging.properties)
        { "org.apache.parquet.handlers", "parquet-logging.properties" },
        { ".level", "parquet-logging.properties" },
        { "java.util.logging.ConsoleHandler.level", "parquet-logging.properties" },
        { "java.util.logging.ConsoleHandler.formatter", "parquet-logging.properties" },
        { "java.util.logging.SimpleFormatter.format", "parquet-logging.properties" },
        { "java.util.logging.FileHandler.level", "parquet-logging.properties" },
        { "java.util.logging.FileHandler.pattern", "parquet-logging.properties" },
        { "java.util.logging.FileHandler.limit", "parquet-logging.properties" },
        { "java.util.logging.FileHandler.count", "parquet-logging.properties" },
        { "java.util.logging.FileHandler.formatter", "parquet-logging.properties" },
        // Configuration Parameters from llap-cli-log4j2.properties
        { "status", "llap-cli-log4j2.properties" },
        { "name", "llap-cli-log4j2.properties" },
        { "packages", "llap-cli-log4j2.properties" },
        { "property.hive.log.level", "llap-cli-log4j2.properties" },
        { "property.hive.root.logger", "llap-cli-log4j2.properties" },
        { "property.hive.log.dir", "llap-cli-log4j2.properties" },
        { "property.hive.log.file", "llap-cli-log4j2.properties" },
        { "property.hive.llapstatus.consolelogger.level", "llap-cli-log4j2.properties" },
        { "appenders", "llap-cli-log4j2.properties" },
        { "appender.console.type", "llap-cli-log4j2.properties" },
        { "appender.console.name", "llap-cli-log4j2.properties" },
        { "appender.console.target", "llap-cli-log4j2.properties" },
        { "appender.console.layout.type", "llap-cli-log4j2.properties" },
        { "appender.console.layout.pattern", "llap-cli-log4j2.properties" },
        { "appender.llapstatusconsole.type", "llap-cli-log4j2.properties" },
        { "appender.llapstatusconsole.name", "llap-cli-log4j2.properties" },
        { "appender.llapstatusconsole.target", "llap-cli-log4j2.properties" },
        { "appender.llapstatusconsole.layout.type", "llap-cli-log4j2.properties" },
        { "appender.llapstatusconsole.layout.pattern", "llap-cli-log4j2.properties" },
        { "appender.DRFA.type", "llap-cli-log4j2.properties" },
        { "appender.DRFA.name", "llap-cli-log4j2.properties" },
        { "appender.DRFA.fileName", "llap-cli-log4j2.properties" },
        { "appender.DRFA.filePattern", "llap-cli-log4j2.properties" },
        { "appender.DRFA.layout.type", "llap-cli-log4j2.properties" },
        { "appender.DRFA.layout.pattern", "llap-cli-log4j2.properties" },
        { "appender.DRFA.policies.type", "llap-cli-log4j2.properties" },
        { "appender.DRFA.policies.time.type", "llap-cli-log4j2.properties" },
        { "appender.DRFA.policies.time.interval", "llap-cli-log4j2.properties" },
        { "appender.DRFA.policies.time.modulate", "llap-cli-log4j2.properties" },
        { "appender.DRFA.strategy.type", "llap-cli-log4j2.properties" },
        { "appender.DRFA.strategy.max", "llap-cli-log4j2.properties" },
        { "appender.DRFA.policies.fsize.type", "llap-cli-log4j2.properties" },
        { "appender.DRFA.policies.fsize.size", "llap-cli-log4j2.properties" },
        { "loggers", "llap-cli-log4j2.properties" },
        { "logger.ZooKeeper.name", "llap-cli-log4j2.properties" },
        { "logger.ZooKeeper.level", "llap-cli-log4j2.properties" },
        { "logger.DataNucleus.name", "llap-cli-log4j2.properties" },
        { "logger.DataNucleus.level", "llap-cli-log4j2.properties" },
        { "logger.Datastore.name", "llap-cli-log4j2.properties" },
        { "logger.Datastore.level", "llap-cli-log4j2.properties" },
        { "logger.JPOX.name", "llap-cli-log4j2.properties" },
        { "logger.JPOX.level", "llap-cli-log4j2.properties" },
        { "logger.HadoopConf.name", "llap-cli-log4j2.properties" },
        { "logger.HadoopConf.level", "llap-cli-log4j2.properties" },
        { "logger.LlapStatusServiceDriverConsole.name", "llap-cli-log4j2.properties" },
        { "logger.LlapStatusServiceDriverConsole.additivity", "llap-cli-log4j2.properties" },
        { "logger.LlapStatusServiceDriverConsole.level", "llap-cli-log4j2.properties" },
        { "rootLogger.level", "llap-cli-log4j2.properties" },
        { "rootLogger.appenderRefs", "llap-cli-log4j2.properties" },
        { "rootLogger.appenderRef.root.ref", "llap-cli-log4j2.properties" },
        { "rootLogger.appenderRef.DRFA.ref", "llap-cli-log4j2.properties" },
        { "logger.LlapStatusServiceDriverConsole.appenderRefs", "llap-cli-log4j2.properties" },
        { "logger.LlapStatusServiceDriverConsole.appenderRef.llapstatusconsole.ref", "llap-cli-log4j2.properties" },
        { "logger.LlapStatusServiceDriverConsole.appenderRef.DRFA.ref", "llap-cli-log4j2.properties" },
        // Extracted from llap-daemon-log4j.properties
        { "llap.daemon.log.level", "llap-daemon-log4j.properties" },     // Root log level (INFO)
        { "llap.daemon.root.logger", "llap-daemon-log4j.properties" },   // Default appender (console)
        { "llap.daemon.log.dir", "llap-daemon-log4j.properties" },       // Log directory (.)
        { "llap.daemon.log.file", "llap-daemon-log4j.properties" },      // Main log filename (llapdaemon.log)
        { "llap.daemon.historylog.file", "llap-daemon-log4j.properties" }, // History log filename
        { "llap.daemon.log.maxfilesize", "llap-daemon-log4j.properties" }, // Max log file size (256MB)
        { "llap.daemon.log.maxbackupindex", "llap-daemon-log4j.properties" } ,
    };

    for (size_t i = 0; i < sizeof(hive_predefined_params)/sizeof(hive_predefined_params[0]); i++) {
        const char* canonical_name = hive_predefined_params[i].canonical_name;
        const char* config_file = hive_predefined_params[i].config_file;

        char* regex_pattern = generate_regex_pattern(canonical_name);
        if (!regex_pattern) continue;

        regex_t regex;
        int reti = regcomp(&regex, regex_pattern, REG_EXTENDED | REG_ICASE | REG_NOSUB);
        free(regex_pattern);

        if (reti) {
            continue;
        }

        reti = regexec(&regex, param_name, 0, NULL, 0);
        regfree(&regex);

        if (reti == 0) {
            ConfigResult* result = malloc(sizeof(ConfigResult));
            if (!result) return NULL;

            result->canonical_name = strdup(canonical_name);
            result->value = strdup(param_value);
            result->config_file = strdup(config_file);

            if (!result->canonical_name || !result->value || !result->config_file) {
                free(result->canonical_name);
                free(result->value);
                free(result->config_file);
                free(result);
                return NULL;
            }

            return result;
        }
    }

    return NULL;
}

ValidationResult validateHiveConfigParam(const char *param_name, const char *value) {
    // Check if parameter exists
    bool param_exists = true;
    if (!param_exists) return ERROR_PARAM_NOT_FOUND;

    // Check value presence
    if (!value || strlen(value) == 0) return ERROR_VALUE_EMPTY;
    if (strstr(param_name, "xasecure.audit") == param_name) {
        // Boolean audit switches
        if (strstr(param_name, ".is.enabled") || strstr(param_name, ".is.async")) {
            return isValidBoolean(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
        }
        // Numeric parameters
        if (strstr(param_name, ".max.queue.size") ||
            strstr(param_name, ".max.flush.interval") ||
            strstr(param_name, ".flush.interval") ||
            strstr(param_name, ".rollover.interval") ||
            strstr(param_name, ".open.retry.interval") ||
            strstr(param_name, ".buffer.size.bytes") ||
            strstr(param_name, ".max.file.count")) {
            return isPositiveInteger(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
        }
        // Path/directory parameters
        if (strstr(param_name, ".directory") ||
            strstr(param_name, ".keystore") ||
            strstr(param_name, ".truststore")) {
            return isValidPath(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
        }
        // Special cases
        if (strcmp(param_name, "xasecure.audit.kafka.broker_list") == 0) {
            char *copy = strdup(value);
            char *token = strtok(copy, ",");
            while (token) {
                if (!isHostPortPair(token)) {
                    free(copy);
                    return ERROR_INVALID_FORMAT;
                }
                token = strtok(NULL, ",");
            }
            free(copy);
        }
        if (strcmp(param_name, "xasecure.audit.solr.solr_url") == 0) {
            return isURL(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
        }
    }

    // Ranger Security Parameters
    if (strstr(param_name, "ranger.plugin.hive") == param_name) {
        // Numeric parameters
        if (strstr(param_name, ".pollIntervalMs") ||
            strstr(param_name, ".timeoutMs")) {
            return isPositiveInteger(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
        }
        // URL parameters
        if (strcmp(param_name, "ranger.plugin.hive.policy.rest.url") == 0) {
            return isURL(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
        }
        // Path parameters
        if (strcmp(param_name, "ranger.plugin.hive.policy.rest.ssl.config.file") == 0 ||
            strcmp(param_name, "ranger.plugin.hive.policy.cache.dir") == 0) {
            return isValidPath(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
        }
        // Service name
        if (strcmp(param_name, "ranger.plugin.hive.service.name") == 0) {
            return (strlen(value) > 0) ? VALIDATION_OK : ERROR_VALUE_EMPTY;

        }
    }

    // SSL Configuration Parameters
    if (strstr(param_name, "xasecure.policymgr.clientssl") == param_name) {
        // Credential paths
        if (strstr(param_name, ".credential.file")) {
            return isJCEKSPath(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
        }
        // Keystore/truststore files
        if (strstr(param_name, ".keystore") || strstr(param_name, ".truststore")) {
            return (strlen(value) > 0) ? VALIDATION_OK : ERROR_VALUE_EMPTY;
        }
    }

    // Security Booleans
    if (strcmp(param_name, "xasecure.hive.update.xapolicies.on.grant.revoke") == 0 ||
        strcmp(param_name, "xasecure.hive.uri.permission.coarse.check") == 0) {
        return isValidBoolean(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    // Parameter-specific validation
    if (strcmp(param_name, "hive.execution.engine") == 0) {
        return (strcmp(value, "mr") == 0 || strcmp(value, "tez") == 0 || strcmp(value, "spark") == 0)
            ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "hive.metastore.uris") == 0) {
        char *copy = strdup(value);
        char *token = strtok(copy, ",");
        while (token) {
            if (strstr(token, "thrift://") == NULL || !isHostPortPair(token + 8)) {
                free(copy);
                return ERROR_INVALID_FORMAT;
            }
            token = strtok(NULL, ",");
        }
        free(copy);
    }
    else if (strcmp(param_name, "javax.jdo.option.ConnectionURL") == 0) {
        return isJDBCURL(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "hive.metastore.thrift.port") == 0 ||
             strcmp(param_name, "hive.server2.thrift.port") == 0) {
        return isValidPort(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "hive.exec.parallel") == 0 ||
             strcmp(param_name, "hive.exec.mode.local.auto") == 0 ||
             strcmp(param_name, "hive.security.authorization.enabled") == 0 ||
             strcmp(param_name, "hive.support.concurrency") == 0 ||
             strcmp(param_name, "hive.auto.convert.join") == 0) {
        return isValidBoolean(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "hive.exec.parallel.thread.number") == 0 ||
             strcmp(param_name, "hive.compactor.worker.threads") == 0 ||
             strcmp(param_name, "hive.lock.numretries") == 0) {
        return isPositiveInteger(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "hive.tez.container.size") == 0) {
        return isMemorySize(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "hive.default.fileformat") == 0) {
        const char *valid[] = {"ORC", "Parquet", "TextFile", "SequenceFile", NULL};
        for (int i = 0; valid[i]; i++)
            if (strcmp(value, valid[i]) == 0) return VALIDATION_OK;
        return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "hive.parquet.compression") == 0) {
        return isValidCompressionCodec(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "hive.server2.authentication") == 0) {
        const char *valid[] = {"NONE", "KERBEROS", "LDAP", "PAM", "CUSTOM", NULL};
        for (int i = 0; valid[i]; i++)
            if (strcmp(value, valid[i]) == 0) return VALIDATION_OK;
        return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "hive.txn.manager") == 0) {
        return (strstr(value, "DbTxnManager") != NULL) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "hive.exec.max.dynamic.partitions") == 0 ||
             strcmp(param_name, "hive.exec.max.dynamic.partitions.pernode") == 0) {
        return isPositiveInteger(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "hive.server2.idle.operation.timeout") == 0) {
        char *end;
        long timeout = strtol(value, &end, 10);
        return (*end == '\0' && timeout >= 0) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }

    return VALIDATION_OK;
}
// Example usage and cleanup
void free_hive_config_result(ConfigResult* result) {
    if (result) {
        free(result->canonical_name);
        free(result->value);
        free(result->config_file);
        free(result);
    }
}

#define CONFIGURATION_TAG "configuration"
#define PROPERTY_TAG "property"
#define NAME_TAG "name"
#define VALUE_TAG "value"

ConfigStatus modify_hive_config(const char* config_param, const char* value, const char* configuration_file) {
    const char *hive_home = getenv("HIVE_HOME");
    char path_buffer[PATH_MAX];
    char *file_path = NULL;

    // Check if configuration_file is an absolute path
    if (configuration_file[0] == '/') {
        snprintf(path_buffer, sizeof(path_buffer), "%s", configuration_file);
        if (access(path_buffer, F_OK) == 0) {
            file_path = strdup(path_buffer);
        } else {
            return FILE_NOT_FOUND;
        }
    } else {
        // Check HIVE_HOME/conf directory first
        if (hive_home != NULL) {
            snprintf(path_buffer, sizeof(path_buffer), "%s/conf/%s", hive_home, configuration_file);
            if (access(path_buffer, F_OK) == 0) {
                file_path = strdup(path_buffer);
            }
        }

        // Fallback to standard configuration directories
        if (!file_path) {
            const char *fallback_dirs[] = {
                "/opt/hive/conf",        // Red Hat-based systems
                "/usr/local/hive/conf",   // Debian-based systems
                NULL
            };

            for (int i = 0; fallback_dirs[i] != NULL; i++) {
                snprintf(path_buffer, sizeof(path_buffer), "%s/%s", fallback_dirs[i], configuration_file);
                if (access(path_buffer, F_OK) == 0) {
                    file_path = strdup(path_buffer);
                    break;
                }
            }
        }

        if (!file_path) {
            return FILE_NOT_FOUND;
        }
    }

    if (strcmp(configuration_file, "beeline-log4j2.properties") == 0 ||
        strcmp(configuration_file, "hive-exec-log4j2.properties") == 0 ||
        strcmp(configuration_file, "parquet-logging.properties") == 0 ||
        strcmp(configuration_file, "llap-cli-log4j2.properties") == 0 ||
        strcmp(configuration_file, "llap-daemon-log4j2.properties") == 0 ||
        strcmp(configuration_file, "atlas-application.properties") == 0 ||
        strcmp(configuration_file, "hive-log4j2.properties") == 0) {
        configure_hadoop_property(file_path, config_param, value);
        return SUCCESS;
    }
    // Parse XML document
    xmlDoc *doc = xmlReadFile(file_path, NULL, 0);
    if (!doc) {
        free(file_path);
        return XML_PARSE_ERROR;
    }

    xmlNode *root = xmlDocGetRootElement(doc);
    if (!root || xmlStrcmp(root->name, BAD_CAST CONFIGURATION_TAG) != 0) {
        xmlFreeDoc(doc);
        free(file_path);
        return XML_INVALID_ROOT;
    }

    xmlNode *target_prop = NULL;
    // Search for existing property
    for (xmlNode *node = root->children; node; node = node->next) {
        if (node->type == XML_ELEMENT_NODE && xmlStrcmp(node->name, BAD_CAST PROPERTY_TAG) == 0) {
            xmlNode *name_node = NULL;

            for (xmlNode *child = node->children; child; child = child->next) {
                if (child->type == XML_ELEMENT_NODE && xmlStrcmp(child->name, BAD_CAST NAME_TAG) == 0) {
                    name_node = child;
                    break;
                }
            }

            if (name_node) {
                xmlChar *name_content = xmlNodeGetContent(name_node);
                if (xmlStrcmp(name_content, BAD_CAST config_param) == 0) {
                    target_prop = node;
                    xmlFree(name_content);
                    break;
                }
                xmlFree(name_content);
            }
        }
    }

    // Update or add property
    if (target_prop) {
        xmlNode *value_node = NULL;
        for (xmlNode *child = target_prop->children; child; child = child->next) {
            if (child->type == XML_ELEMENT_NODE && xmlStrcmp(child->name, BAD_CAST VALUE_TAG) == 0) {
                value_node = child;
                break;
            }
        }

        if (value_node) {
            xmlNodeSetContent(value_node, BAD_CAST value);
        } else {
            if (!xmlNewTextChild(target_prop, NULL, BAD_CAST VALUE_TAG, BAD_CAST value)) {
                xmlFreeDoc(doc);
                free(file_path);
                return XML_UPDATE_ERROR;
            }
        }
    } else {
        xmlNode *new_prop = xmlNewNode(NULL, BAD_CAST PROPERTY_TAG);
        if (!new_prop) {
            xmlFreeDoc(doc);
            free(file_path);
            return XML_UPDATE_ERROR;
        }

        if (!xmlNewTextChild(new_prop, NULL, BAD_CAST NAME_TAG, BAD_CAST config_param)) {
            xmlFreeNode(new_prop);
            xmlFreeDoc(doc);
            free(file_path);
            return XML_UPDATE_ERROR;
        }

        if (!xmlNewTextChild(new_prop, NULL, BAD_CAST VALUE_TAG, BAD_CAST value)) {
            xmlFreeNode(new_prop);
            xmlFreeDoc(doc);
            free(file_path);
            return XML_UPDATE_ERROR;
        }

        xmlAddChild(root, new_prop);
    }

    // Save changes with XML formatting preserved
    if (xmlSaveFormatFileEnc(file_path, doc, "UTF-8", 1) < 0) {
        xmlFreeDoc(doc);
        free(file_path);
        return FILE_WRITE_ERROR;
    }

    // Cleanup resources
    xmlFreeDoc(doc);
    free(file_path);

    return SUCCESS;
}

