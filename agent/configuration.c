#include "utiles.h"
#include "configuration.h"
#include "protocol.h"

void configure_target_component(Component target) {
    ConfigStatus status;

    switch(target) {
        // HDFS Configuration
    case HDFS:
        //	 ZooKeeper Failover Controller settings
        status = modify_hdfs_config("ha.failover-controller.active-standby-elector.zk.op.retries", "120", "hdfs-site.xml");
        handle_result(status, "ha.failover-controller.active-standby-elector.zk.op.retries", "120", "hdfs-site.xml");

        //	 I/O properties
        status = modify_hdfs_config("io.file.buffer.size", "131072", "hdfs-site.xml");
        handle_result(status, "io.file.buffer.size", "131072", "hdfs-site.xml");
        status = modify_hdfs_config("io.serializations", "org.apache.hadoop.io.serializer.WritableSerialization", "hdfs-site.xml");
        handle_result(status, "io.serializations", "org.apache.hadoop.io.serializer.WritableSerialization", "hdfs-site.xml");
        status = modify_hdfs_config("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.SnappyCodec", "hdfs-site.xml");
        handle_result(status, "io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.SnappyCodec", "hdfs-site.xml");

        //	 File system properties
        status = modify_hdfs_config("fs.defaultFS", "hdfs://localhost:8020/", "core-site.xml");
        handle_result(status, "fs.defaultFS", "hdfs://localhost:8020/", "core-site.xml");
        status = modify_hdfs_config("fs.trash.interval", "360", "hdfs-site.xml");
        handle_result(status, "fs.trash.interval", "360", "hdfs-site.xml");

        //	 IPC properties
        status = modify_hdfs_config("ipc.client.idlethreshold", "8000", "hdfs-site.xml");
        handle_result(status, "ipc.client.idlethreshold", "8000", "hdfs-site.xml");
        status = modify_hdfs_config("ipc.client.connection.maxidletime", "30000", "hdfs-site.xml");
        handle_result(status, "ipc.client.connection.maxidletime", "30000", "hdfs-site.xml");
        status = modify_hdfs_config("ipc.client.connect.max.retries", "50", "hdfs-site.xml");
        handle_result(status, "ipc.client.connect.max.retries", "50", "hdfs-site.xml");
        status = modify_hdfs_config("ipc.server.tcpnodelay", "true", "hdfs-site.xml");
        handle_result(status, "ipc.server.tcpnodelay", "true", "hdfs-site.xml");

        //	 Web Interface Configuration
        status = modify_hdfs_config("mapreduce.jobtracker.webinterface.trusted", "false", "hdfs-site.xml");
        handle_result(status, "mapreduce.jobtracker.webinterface.trusted", "false", "hdfs-site.xml");

        //	 Security properties
        status = modify_hdfs_config("hadoop.http.authentication.type", "simple", "hdfs-site.xml");
        handle_result(status, "hadoop.http.authentication.type", "simple", "hdfs-site.xml");
        status = modify_hdfs_config("hadoop.security.authentication", "simple", "hdfs-site.xml");
        handle_result(status, "hadoop.security.authentication", "simple", "hdfs-site.xml");
        status = modify_hdfs_config("hadoop.security.authorization", "false", "hdfs-site.xml");
        handle_result(status, "hadoop.security.authorization", "false", "hdfs-site.xml");
        status = modify_hdfs_config("hadoop.security.auth_to_local", "DEFAULT", "hdfs-site.xml");
        handle_result(status, "hadoop.security.auth_to_local", "DEFAULT", "hdfs-site.xml");

        //	 Network topology
        status = modify_hdfs_config("net.topology.script.file.name", "/etc/hadoop/conf/topology_script.py", "hdfs-site.xml");
        handle_result(status, "net.topology.script.file.name", "/etc/hadoop/conf/topology_script.py", "hdfs-site.xml");

        //	 Proxy user (empty value)
        status = modify_hdfs_config("hadoop.proxyuser", "", "hdfs-site.xml");
        handle_result(status, "hadoop.proxyuser", "", "hdfs-site.xml");

        //	 Cloud storage properties
        status = modify_hdfs_config("fs.gs.path.encoding", "uri-path", "hdfs-site.xml");
        handle_result(status, "fs.gs.path.encoding", "uri-path", "hdfs-site.xml");
        status = modify_hdfs_config("fs.gs.working.dir", "/", "hdfs-site.xml");
        handle_result(status, "fs.gs.working.dir", "/", "hdfs-site.xml");
        //	 Client protocol ACLs
        status = modify_hdfs_config("security.client.protocol.acl", "*", "hadoop-policy.xml");
        handle_result(status, "security.client.protocol.acl", "*", "hadoop-policy.xml");
        status = modify_hdfs_config("security.client.datanode.protocol.acl", "*", "hadoop-policy.xml");
        handle_result(status, "security.client.datanode.protocol.acl", "*", "hadoop-policy.xml");

        //	 DataNode protocol ACLs
        status = modify_hdfs_config("security.datanode.protocol.acl", "*", "hadoop-policy.xml");
        handle_result(status, "security.datanode.protocol.acl", "*", "hadoop-policy.xml");
        status = modify_hdfs_config("security.inter.datanode.protocol.acl", "*", "hadoop-policy.xml");
        handle_result(status, "security.inter.datanode.protocol.acl", "*", "hadoop-policy.xml");

        //	 NameNode protocol ACLs
        status = modify_hdfs_config("security.namenode.protocol.acl", "*", "hadoop-policy.xml");
        handle_result(status, "security.namenode.protocol.acl", "*", "hadoop-policy.xml");

        //	 JobTracker/TaskTracker protocol ACLs
        status = modify_hdfs_config("security.inter.tracker.protocol.acl", "*", "hadoop-policy.xml");
        handle_result(status, "security.inter.tracker.protocol.acl", "*", "hadoop-policy.xml");
        status = modify_hdfs_config("security.job.client.protocol.acl", "*", "hadoop-policy.xml");
        handle_result(status, "security.job.client.protocol.acl", "*", "hadoop-policy.xml");
        status = modify_hdfs_config("security.job.task.protocol.acl", "*", "hadoop-policy.xml");
        handle_result(status, "security.job.task.protocol.acl", "*", "hadoop-policy.xml");

        //	 Administrative protocol ACLs
        status = modify_hdfs_config("security.admin.operations.protocol.acl", "hadoop", "hadoop-policy.xml");
        handle_result(status, "security.admin.operations.protocol.acl", "hadoop", "hadoop-policy.xml");
        status = modify_hdfs_config("security.refresh.usertogroups.mappings.protocol.acl", "hadoop", "hadoop-policy.xml");
        handle_result(status, "security.refresh.usertogroups.mappings.protocol.acl", "hadoop", "hadoop-policy.xml");
        status = modify_hdfs_config("security.refresh.policy.protocol.acl", "hadoop", "hadoop-policy.xml");
        handle_result(status, "security.refresh.policy.protocol.acl", "hadoop", "hadoop-policy.xml");

        status = modify_hdfs_config("hadoop_security_log_max_backup_size", "256", "log4j.properties");
        handle_result(status, "hadoop_security_log_max_backup_size", "256", "log4j.properties");
        status = modify_hdfs_config("hadoop_security_log_number_of_backup_files", "20", "log4j.properties");
        handle_result(status, "hadoop_security_log_number_of_backup_files", "20", "log4j.properties");
        status = modify_hdfs_config("hadoop_log_max_backup_size", "256", "log4j.properties");
        handle_result(status, "hadoop_log_max_backup_size", "256", "log4j.properties");
        status = modify_hdfs_config("hadoop_log_number_of_backup_files", "10", "log4j.properties");
        handle_result(status, "hadoop_log_number_of_backup_files", "10", "log4j.properties");

        //	 Root logger configuration
        status = modify_hdfs_config("log4j.rootLogger", "INFO,console,EventCounter", "log4j.properties");
        handle_result(status, "log4j.rootLogger", "INFO,console,EventCounter", "log4j.properties");
        status = modify_hdfs_config("log4j.threshhold", "ALL", "log4j.properties");
        handle_result(status, "log4j.threshhold", "ALL", "log4j.properties");

        //	 Daily Rolling File Appender (DRFA)
        status = modify_hdfs_config("log4j.appender.DRFA", "org.apache.log4j.DailyRollingFileAppender", "log4j.properties");
        handle_result(status, "log4j.appender.DRFA", "org.apache.log4j.DailyRollingFileAppender", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.DRFA.File", "${hadoop.log.dir}/${hadoop.log.file}", "log4j.properties");
        handle_result(status, "log4j.appender.DRFA.File", "${hadoop.log.dir}/${hadoop.log.file}", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.DRFA.DatePattern", ".yyyy-MM-dd", "log4j.properties");
        handle_result(status, "log4j.appender.DRFA.DatePattern", ".yyyy-MM-dd", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.DRFA.layout", "org.apache.log4j.PatternLayout", "log4j.properties");
        handle_result(status, "log4j.appender.DRFA.layout", "org.apache.log4j.PatternLayout", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.DRFA.layout.ConversionPattern", "%d{ISO8601} %p %c: %m%n", "log4j.properties");
        handle_result(status, "log4j.appender.DRFA.layout.ConversionPattern", "%d{ISO8601} %p %c: %m%n", "log4j.properties");

        //	 Console Appender
        status = modify_hdfs_config("log4j.appender.console", "org.apache.log4j.ConsoleAppender", "log4j.properties");
        handle_result(status, "log4j.appender.console", "org.apache.log4j.ConsoleAppender", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.console.target", "System.err", "log4j.properties");
        handle_result(status, "log4j.appender.console.target", "System.err", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.console.layout", "org.apache.log4j.PatternLayout", "log4j.properties");
        handle_result(status, "log4j.appender.console.layout", "org.apache.log4j.PatternLayout", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.console.layout.ConversionPattern", "%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n", "log4j.properties");
        handle_result(status, "log4j.appender.console.layout.ConversionPattern", "%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n", "log4j.properties");

        //	 TaskLog Appender (TLA)
        status = modify_hdfs_config("log4j.appender.TLA", "org.apache.hadoop.mapred.TaskLogAppender", "log4j.properties");
        handle_result(status, "log4j.appender.TLA", "org.apache.hadoop.mapred.TaskLogAppender", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.TLA.taskId", "${hadoop.tasklog.taskid}", "log4j.properties");
        handle_result(status, "log4j.appender.TLA.taskId", "${hadoop.tasklog.taskid}", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.TLA.isCleanup", "${hadoop.tasklog.iscleanup}", "log4j.properties");
        handle_result(status, "log4j.appender.TLA.isCleanup", "${hadoop.tasklog.iscleanup}", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.TLA.totalLogFileSize", "${hadoop.tasklog.totalLogFileSize}", "log4j.properties");
        handle_result(status, "log4j.appender.TLA.totalLogFileSize", "${hadoop.tasklog.totalLogFileSize}", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.TLA.layout", "org.apache.log4j.PatternLayout", "log4j.properties");
        handle_result(status, "log4j.appender.TLA.layout", "org.apache.log4j.PatternLayout", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.TLA.layout.ConversionPattern", "%d{ISO8601} %p %c: %m%n", "log4j.properties");
        handle_result(status, "log4j.appender.TLA.layout.ConversionPattern", "%d{ISO8601} %p %c: %m%n", "log4j.properties");

        //	 Security Audit Appender (DRFAS/RFAS)
        status = modify_hdfs_config("log4j.category.SecurityLogger", "${hadoop.security.logger}", "log4j.properties");
        handle_result(status, "log4j.category.SecurityLogger", "${hadoop.security.logger}", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.DRFAS", "org.apache.log4j.DailyRollingFileAppender", "log4j.properties");
        handle_result(status, "log4j.appender.DRFAS", "org.apache.log4j.DailyRollingFileAppender", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.DRFAS.File", "${hadoop.log.dir}/${hadoop.security.log.file}", "log4j.properties");
        handle_result(status, "log4j.appender.DRFAS.File", "${hadoop.log.dir}/${hadoop.security.log.file}", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.DRFAS.layout", "org.apache.log4j.PatternLayout", "log4j.properties");
        handle_result(status, "log4j.appender.DRFAS.layout", "org.apache.log4j.PatternLayout", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.DRFAS.layout.ConversionPattern", "%d{ISO8601} %p %c: %m%n", "log4j.properties");
        handle_result(status, "log4j.appender.DRFAS.layout.ConversionPattern", "%d{ISO8601} %p %c: %m%n", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.DRFAS.DatePattern", ".yyyy-MM-dd", "log4j.properties");
        handle_result(status, "log4j.appender.DRFAS.DatePattern", ".yyyy-MM-dd", "log4j.properties");

        //	 Rolling File Appender (RFA)
        status = modify_hdfs_config("log4j.appender.RFA", "org.apache.log4j.RollingFileAppender", "log4j.properties");
        handle_result(status, "log4j.appender.RFA", "org.apache.log4j.RollingFileAppender", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.RFA.File", "${hadoop.log.dir}/${hadoop.log.file}", "log4j.properties");
        handle_result(status, "log4j.appender.RFA.File", "${hadoop.log.dir}/${hadoop.log.file}", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.RFA.MaxFileSize", "${hadoop.security.log.maxfilesize}", "log4j.properties");
        handle_result(status, "log4j.appender.RFA.MaxFileSize", "${hadoop.security.log.maxfilesize}", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.RFA.MaxBackupIndex", "${hadoop.security.log.maxbackupindex}", "log4j.properties");
        handle_result(status, "log4j.appender.RFA.MaxBackupIndex", "${hadoop.security.log.maxbackupindex}", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.RFA.layout", "org.apache.log4j.PatternLayout", "log4j.properties");
        handle_result(status, "log4j.appender.RFA.layout", "org.apache.log4j.PatternLayout", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.RFA.layout.ConversionPattern", "%d{ISO8601} %-5p %c{2} (%F:%M(%L)) - %m%n", "log4j.properties");
        handle_result(status, "log4j.appender.RFA.layout.ConversionPattern", "%d{ISO8601} %-5p %c{2} (%F:%M(%L)) - %m%n", "log4j.properties");

        //	 HDFS Audit Logging
        status = modify_hdfs_config("log4j.logger.org.apache.hadoop.hdfs.server.namenode.FSNamesystem.audit", "${hdfs.audit.logger}", "log4j.properties");
        handle_result(status, "log4j.logger.org.apache.hadoop.hdfs.server.namenode.FSNamesystem.audit", "${hdfs.audit.logger}", "log4j.properties");
        status = modify_hdfs_config("log4j.additivity.org.apache.hadoop.hdfs.server.namenode.FSNamesystem.audit", "false", "log4j.properties");
        handle_result(status, "log4j.additivity.org.apache.hadoop.hdfs.server.namenode.FSNamesystem.audit", "false", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.DRFAAUDIT", "org.apache.log4j.DailyRollingFileAppender", "log4j.properties");
        handle_result(status, "log4j.appender.DRFAAUDIT", "org.apache.log4j.DailyRollingFileAppender", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.DRFAAUDIT.File", "${hadoop.log.dir}/hdfs-audit.log", "log4j.properties");
        handle_result(status, "log4j.appender.DRFAAUDIT.File", "${hadoop.log.dir}/hdfs-audit.log", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.DRFAAUDIT.layout", "org.apache.log4j.PatternLayout", "log4j.properties");
        handle_result(status, "log4j.appender.DRFAAUDIT.layout", "org.apache.log4j.PatternLayout", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.DRFAAUDIT.layout.ConversionPattern", "%d{ISO8601} %p %c{2}: %m%n", "log4j.properties");
        handle_result(status, "log4j.appender.DRFAAUDIT.layout.ConversionPattern", "%d{ISO8601} %p %c{2}: %m%n", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.DRFAAUDIT.DatePattern", ".yyyy-MM-dd", "log4j.properties");
        handle_result(status, "log4j.appender.DRFAAUDIT.DatePattern", ".yyyy-MM-dd", "log4j.properties");

        //	 MapReduce Audit Logging
        status = modify_hdfs_config("log4j.logger.org.apache.hadoop.mapred.AuditLogger", "${mapred.audit.logger}", "log4j.properties");
        handle_result(status, "log4j.logger.org.apache.hadoop.mapred.AuditLogger", "${mapred.audit.logger}", "log4j.properties");
        status = modify_hdfs_config("log4j.additivity.org.apache.hadoop.mapred.AuditLogger", "false", "log4j.properties");
        handle_result(status, "log4j.additivity.org.apache.hadoop.mapred.AuditLogger", "false", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.MRAUDIT", "org.apache.log4j.DailyRollingFileAppender", "log4j.properties");
        handle_result(status, "log4j.appender.MRAUDIT", "org.apache.log4j.DailyRollingFileAppender", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.MRAUDIT.File", "${hadoop.log.dir}/mapred-audit.log", "log4j.properties");
        handle_result(status, "log4j.appender.MRAUDIT.File", "${hadoop.log.dir}/mapred-audit.log", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.MRAUDIT.layout", "org.apache.log4j.PatternLayout", "log4j.properties");
        handle_result(status, "log4j.appender.MRAUDIT.layout", "org.apache.log4j.PatternLayout", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.MRAUDIT.layout.ConversionPattern", "%d{ISO8601} %p %c{2}: %m%n", "log4j.properties");
        handle_result(status, "log4j.appender.MRAUDIT.layout.ConversionPattern", "%d{ISO8601} %p %c{2}: %m%n", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.MRAUDIT.DatePattern", ".yyyy-MM-dd", "log4j.properties");
        handle_result(status, "log4j.appender.MRAUDIT.DatePattern", ".yyyy-MM-dd", "log4j.properties");

        //	 Custom Logging Levels
        status = modify_hdfs_config("log4j.logger.org.apache.hadoop.metrics2", "${hadoop.metrics.log.level}", "log4j.properties");
        handle_result(status, "log4j.logger.org.apache.hadoop.metrics2", "${hadoop.metrics.log.level}", "log4j.properties");
        status = modify_hdfs_config("log4j.logger.org.jets3t.service.impl.rest.httpclient.RestS3Service", "ERROR", "log4j.properties");
        handle_result(status, "log4j.logger.org.jets3t.service.impl.rest.httpclient.RestS3Service", "ERROR", "log4j.properties");

        //	 Special Appenders
        status = modify_hdfs_config("log4j.appender.NullAppender", "org.apache.log4j.varia.NullAppender", "log4j.properties");
        handle_result(status, "log4j.appender.NullAppender", "org.apache.log4j.varia.NullAppender", "log4j.properties");
        status = modify_hdfs_config("log4j.appender.EventCounter", "org.apache.hadoop.log.metrics.EventCounter", "log4j.properties");
        handle_result(status, "log4j.appender.EventCounter", "org.apache.hadoop.log.metrics.EventCounter", "log4j.properties");

        //	 Warning Suppression
        status = modify_hdfs_config("log4j.logger.org.apache.hadoop.conf.Configuration.deprecation", "WARN", "log4j.properties");
        handle_result(status, "log4j.logger.org.apache.hadoop.conf.Configuration.deprecation", "WARN", "log4j.properties");
        status = modify_hdfs_config("log4j.logger.org.apache.commons.beanutils", "WARN", "log4j.properties");
        handle_result(status, "log4j.logger.org.apache.commons.beanutils", "WARN", "log4j.properties");

        //	 RPC Service Configuration
        status = modify_hdfs_config("dfs.federation.router.rpc.enable", "true", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.rpc.enable", "true", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.rpc-address", "0.0.0.0:20010", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.rpc-address", "0.0.0.0:20010", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.rpc-bind-host", "0.0.0.0", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.rpc-bind-host", "0.0.0.0", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.handler.count", "10", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.handler.count", "10", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.handler.queue.size", "100", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.handler.queue.size", "100", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.reader.count", "1", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.reader.count", "1", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.reader.queue.size", "100", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.reader.queue.size", "100", "hdfs-rbf-site.xml");

        //	 Connection Pool Configuration
        status = modify_hdfs_config("dfs.federation.router.connection.creator.queue-size", "100", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.connection.creator.queue-size", "100", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.connection.pool-size", "1", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.connection.pool-size", "1", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.connection.min-active-ratio", "0.5f", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.connection.min-active-ratio", "0.5f", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.connection.clean.ms", "10000", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.connection.clean.ms", "10000", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.connection.pool.clean.ms", "60000", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.connection.pool.clean.ms", "60000", "hdfs-rbf-site.xml");

        //	 Metrics and Monitoring
        status = modify_hdfs_config("dfs.federation.router.metrics.enable", "true", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.metrics.enable", "true", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.dn-report.time-out", "1000", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.dn-report.time-out", "1000", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.dn-report.cache-expire", "10s", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.dn-report.cache-expire", "10s", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.metrics.class", "org.apache.hadoop.hdfs.server.federation.metrics.FederationRPCPerformanceMonitor", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.metrics.class", "org.apache.hadoop.hdfs.server.federation.metrics.FederationRPCPerformanceMonitor", "hdfs-rbf-site.xml");

        //	 Admin Service Configuration
        status = modify_hdfs_config("dfs.federation.router.admin.enable", "true", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.admin.enable", "true", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.admin-address", "0.0.0.0:8111", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.admin-address", "0.0.0.0:8111", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.admin-bind-host", "0.0.0.0", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.admin-bind-host", "0.0.0.0", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.admin.handler.count", "1", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.admin.handler.count", "1", "hdfs-rbf-site.xml");

        //	 Web Interface Configuration
        status = modify_hdfs_config("dfs.federation.router.http-address", "0.0.0.0:50071", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.http-address", "0.0.0.0:50071", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.http-bind-host", "0.0.0.0", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.http-bind-host", "0.0.0.0", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.https-address", "0.0.0.0:50072", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.https-address", "0.0.0.0:50072", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.https-bind-host", "0.0.0.0", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.https-bind-host", "0.0.0.0", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.http.enable", "true", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.http.enable", "true", "hdfs-rbf-site.xml");

        //	 Resolver Configuration
        status = modify_hdfs_config("dfs.federation.router.file.resolver.client.class", "org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.file.resolver.client.class", "org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.namenode.resolver.client.class", "org.apache.hadoop.hdfs.server.federation.resolver.MembershipNamenodeResolver", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.namenode.resolver.client.class", "org.apache.hadoop.hdfs.server.federation.resolver.MembershipNamenodeResolver", "hdfs-rbf-site.xml");

        //	 State Store Configuration
        status = modify_hdfs_config("dfs.federation.router.store.enable", "true", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.store.enable", "true", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.store.serializer", "org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreSerializerPBImpl", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.store.serializer", "org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreSerializerPBImpl", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.store.driver.class", "org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreZooKeeperImpl", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.store.driver.class", "org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreZooKeeperImpl", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.store.connection.test", "60000", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.store.connection.test", "60000", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.cache.ttl", "1m", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.cache.ttl", "1m", "hdfs-rbf-site.xml");

        //	 Membership Configuration
        status = modify_hdfs_config("dfs.federation.router.store.membership.expiration", "300000", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.store.membership.expiration", "300000", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.store.membership.expiration.deletion", "-1", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.store.membership.expiration.deletion", "-1", "hdfs-rbf-site.xml");

        //	 Heartbeat Configuration
        status = modify_hdfs_config("dfs.federation.router.heartbeat.enable", "true", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.heartbeat.enable", "true", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.heartbeat.interval", "5000", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.heartbeat.interval", "5000", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.heartbeat-state.interval", "5s", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.heartbeat-state.interval", "5s", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.namenode.heartbeat.enable", "true", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.namenode.heartbeat.enable", "true", "hdfs-rbf-site.xml");

        //	 Router State Configuration
        status = modify_hdfs_config("dfs.federation.router.store.router.expiration", "5m", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.store.router.expiration", "5m", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.store.router.expiration.deletion", "-1", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.store.router.expiration.deletion", "-1", "hdfs-rbf-site.xml");

        //	 Safe Mode Configuration
        status = modify_hdfs_config("dfs.federation.router.safemode.enable", "true", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.safemode.enable", "true", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.safemode.extension", "30s", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.safemode.extension", "30s", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.safemode.expiration", "3m", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.safemode.expiration", "3m", "hdfs-rbf-site.xml");

        //	 Monitoring Configuration
        status = modify_hdfs_config("dfs.federation.router.monitor.localnamenode.enable", "true", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.monitor.localnamenode.enable", "true", "hdfs-rbf-site.xml");

        //	 Mount Table Configuration
        status = modify_hdfs_config("dfs.federation.router.mount-table.max-cache-size", "10000", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.mount-table.max-cache-size", "10000", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.mount-table.cache.enable", "true", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.mount-table.cache.enable", "true", "hdfs-rbf-site.xml");
        //
        //			 Quota Configuration
        status = modify_hdfs_config("dfs.federation.router.quota.enable", "false", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.quota.enable", "false", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.quota-cache.update.interval", "60s", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.quota-cache.update.interval", "60s", "hdfs-rbf-site.xml");

        //			 Client Configuration
        status = modify_hdfs_config("dfs.federation.router.client.thread-size", "32", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.client.thread-size", "32", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.client.retry.max.attempts", "3", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.client.retry.max.attempts", "3", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.client.reject.overload", "false", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.client.reject.overload", "false", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.client.allow-partial-listing", "true", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.client.allow-partial-listing", "true", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.client.mount-status.time-out", "1s", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.client.mount-status.time-out", "1s", "hdfs-rbf-site.xml");

        //			 Connection Configuration
        status = modify_hdfs_config("dfs.federation.router.connect.max.retries.on.timeouts", "0", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.connect.max.retries.on.timeouts", "0", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.connect.timeout", "2s", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.connect.timeout", "2s", "hdfs-rbf-site.xml");

        //			 Cache Update Configuration
        status = modify_hdfs_config("dfs.federation.router.mount-table.cache.update", "true", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.mount-table.cache.update", "true", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.mount-table.cache.update.timeout", "1m", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.mount-table.cache.update.timeout", "1m", "hdfs-rbf-site.xml");
        status = modify_hdfs_config("dfs.federation.router.mount-table.cache.update.client.max.time", "5m", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.mount-table.cache.update.client.max.time", "5m", "hdfs-rbf-site.xml");

        //			 Security Configuration
        status = modify_hdfs_config("dfs.federation.router.secret.manager.class", "org.apache.hadoop.hdfs.server.federation.router.security.token.ZKDelegationTokenSecretManagerImpl", "hdfs-rbf-site.xml");
        handle_result(status, "dfs.federation.router.secret.manager.class", "org.apache.hadoop.hdfs.server.federation.router.security.token.ZKDelegationTokenSecretManagerImpl", "hdfs-rbf-site.xml");

        //			 NameNode Configuration
        status = modify_hdfs_config("dfs.namenode.name.dir", "/hadoop/hdfs/namenode", "hdfs-site.xml");
        handle_result(status, "dfs.namenode.name.dir", "/hadoop/hdfs/namenode", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.support.append", "true", "hdfs-site.xml");
        handle_result(status, "dfs.support.append", "true", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.webhdfs.enabled", "true", "hdfs-site.xml");
        handle_result(status, "dfs.webhdfs.enabled", "true", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.namenode.checkpoint.dir", "/hadoop/hdfs/namesecondary", "hdfs-site.xml");
        handle_result(status, "dfs.namenode.checkpoint.dir", "/hadoop/hdfs/namesecondary", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.namenode.checkpoint.edits.dir", "${dfs.namenode.checkpoint.dir}", "hdfs-site.xml");
        handle_result(status, "dfs.namenode.checkpoint.edits.dir", "${dfs.namenode.checkpoint.dir}", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.namenode.checkpoint.period", "21600", "hdfs-site.xml");
        handle_result(status, "dfs.namenode.checkpoint.period", "21600", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.namenode.checkpoint.txns", "1000000", "hdfs-site.xml");
        handle_result(status, "dfs.namenode.checkpoint.txns", "1000000", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.namenode.handler.count", "100", "hdfs-site.xml");
        handle_result(status, "dfs.namenode.handler.count", "100", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.namenode.http-address", "localhost:50070", "hdfs-site.xml");
        handle_result(status, "dfs.namenode.http-address", "localhost:50070", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.namenode.https-address", "localhost:50470", "hdfs-site.xml");
        handle_result(status, "dfs.namenode.https-address", "localhost:50470", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.namenode.rpc-address", "localhost:8020", "hdfs-site.xml");
        handle_result(status, "dfs.namenode.rpc-address", "localhost:8020", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.namenode.secondary.http-address", "localhost:50090", "hdfs-site.xml");
        handle_result(status, "dfs.namenode.secondary.http-address", "localhost:50090", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.namenode.safemode.threshold-pct", "0.999", "hdfs-site.xml");
        handle_result(status, "dfs.namenode.safemode.threshold-pct", "0.999", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.namenode.name.dir.restore", "true", "hdfs-site.xml");
        handle_result(status, "dfs.namenode.name.dir.restore", "true", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.namenode.accesstime.precision", "0", "hdfs-site.xml");
        handle_result(status, "dfs.namenode.accesstime.precision", "0", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.namenode.avoid.read.stale.datanode", "true", "hdfs-site.xml");
        handle_result(status, "dfs.namenode.avoid.read.stale.datanode", "true", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.namenode.avoid.write.stale.datanode", "true", "hdfs-site.xml");
        handle_result(status, "dfs.namenode.avoid.write.stale.datanode", "true", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.namenode.write.stale.datanode.ratio", "1.0f", "hdfs-site.xml");
        handle_result(status, "dfs.namenode.write.stale.datanode.ratio", "1.0f", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.namenode.stale.datanode.interval", "30000", "hdfs-site.xml");
        handle_result(status, "dfs.namenode.stale.datanode.interval", "30000", "hdfs-site.xml");

        //			 DataNode Configuration
        status = modify_hdfs_config("dfs.datanode.data.dir", "/hadoop/hdfs/data", "hdfs-site.xml");
        handle_result(status, "dfs.datanode.data.dir", "/hadoop/hdfs/data", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.datanode.failed.volumes.tolerated", "0", "hdfs-site.xml");
        handle_result(status, "dfs.datanode.failed.volumes.tolerated", "0", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.datanode.address", "0.0.0.0:50010", "hdfs-site.xml");
        handle_result(status, "dfs.datanode.address", "0.0.0.0:50010", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.datanode.http.address", "0.0.0.0:50075", "hdfs-site.xml");
        handle_result(status, "dfs.datanode.http.address", "0.0.0.0:50075", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.datanode.https.address", "0.0.0.0:50475", "hdfs-site.xml");
        handle_result(status, "dfs.datanode.https.address", "0.0.0.0:50475", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.datanode.ipc.address", "0.0.0.0:8010", "hdfs-site.xml");
        handle_result(status, "dfs.datanode.ipc.address", "0.0.0.0:8010", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.datanode.du.reserved", "1073741824", "hdfs-site.xml");
        handle_result(status, "dfs.datanode.du.reserved", "1073741824", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.datanode.data.dir.perm", "750", "hdfs-site.xml");
        handle_result(status, "dfs.datanode.data.dir.perm", "750", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.datanode.max.transfer.threads", "1024", "hdfs-site.xml");
        handle_result(status, "dfs.datanode.max.transfer.threads", "1024", "hdfs-site.xml");

        //			 Block and Replication Configuration
        status = modify_hdfs_config("dfs.blocksize", "134217728", "hdfs-site.xml");
        handle_result(status, "dfs.blocksize", "134217728", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.replication", "3", "hdfs-site.xml");
        handle_result(status, "dfs.replication", "3", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.replication.max", "50", "hdfs-site.xml");
        handle_result(status, "dfs.replication.max", "50", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.blockreport.initialDelay", "120", "hdfs-site.xml");
        handle_result(status, "dfs.blockreport.initialDelay", "120", "hdfs-site.xml");

        //			 Heartbeat and Network Configuration
        status = modify_hdfs_config("dfs.heartbeat.interval", "3", "hdfs-site.xml");
        handle_result(status, "dfs.heartbeat.interval", "3", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.https.port", "50470", "hdfs-site.xml");
        handle_result(status, "dfs.https.port", "50470", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.http.policy", "HTTP_ONLY", "hdfs-site.xml");
        handle_result(status, "dfs.http.policy", "HTTP_ONLY", "hdfs-site.xml");

        //			 Permissions and Security
        status = modify_hdfs_config("fs.permissions.umask-mode", "022", "hdfs-site.xml");
        handle_result(status, "fs.permissions.umask-mode", "022", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.permissions.enabled", "true", "hdfs-site.xml");
        handle_result(status, "dfs.permissions.enabled", "true", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.permissions.superusergroup", "hdfs", "hdfs-site.xml");
        handle_result(status, "dfs.permissions.superusergroup", "hdfs", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.block.access.token.enable", "true", "hdfs-site.xml");
        handle_result(status, "dfs.block.access.token.enable", "true", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.cluster.administrators", " hdfs", "hdfs-site.xml");
        handle_result(status, "dfs.cluster.administrators", " hdfs", "hdfs-site.xml");

        //			 JournalNode Configuration
        status = modify_hdfs_config("dfs.journalnode.http-address", "0.0.0.0:8480", "hdfs-site.xml");
        handle_result(status, "dfs.journalnode.http-address", "0.0.0.0:8480", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.journalnode.https-address", "0.0.0.0:8481", "hdfs-site.xml");
        handle_result(status, "dfs.journalnode.https-address", "0.0.0.0:8481", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.journalnode.edits.dir", "/grid/0/hdfs/journal", "hdfs-site.xml");
        handle_result(status, "dfs.journalnode.edits.dir", "/grid/0/hdfs/journal", "hdfs-site.xml");

        //			 Short-Circuit Local Reads
        status = modify_hdfs_config("dfs.client.read.shortcircuit", "true", "hdfs-site.xml");
        handle_result(status, "dfs.client.read.shortcircuit", "true", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.domain.socket.path", "/var/lib/hadoop-hdfs/dn_socket", "hdfs-site.xml");
        handle_result(status, "dfs.domain.socket.path", "/var/lib/hadoop-hdfs/dn_socket", "hdfs-site.xml");
        status = modify_hdfs_config("dfs.client.read.shortcircuit.streams.cache.size", "4096", "hdfs-site.xml");
        handle_result(status, "dfs.client.read.shortcircuit.streams.cache.size", "4096", "hdfs-site.xml");

        //			 Host Management
        status = modify_hdfs_config("dfs.hosts.exclude", "/etc/hadoop/conf/dfs.exclude", "hdfs-site.xml");
        handle_result(status, "dfs.hosts.exclude", "/etc/hadoop/conf/dfs.exclude", "hdfs-site.xml");
        status = modify_hdfs_config("manage.include.files", "false", "hdfs-site.xml");
        handle_result(status, "manage.include.files", "false", "hdfs-site.xml");

        //			 Balancing Configuration
        status = modify_hdfs_config("dfs.datanode.balance.bandwidthPerSec", "6250000", "hdfs-site.xml");
        handle_result(status, "dfs.datanode.balance.bandwidthPerSec", "6250000", "hdfs-site.xml");

        //			 Audit Enablement Configuration
        status = modify_hdfs_config("xasecure.audit.is.enabled", "true", "ranger-hdfs-audit.xml");
        handle_result(status, "xasecure.audit.is.enabled", "true", "ranger-hdfs-audit.xml");

        //			 HDFS Audit Configuration
        status = modify_hdfs_config("xasecure.audit.destination.hdfs", "true", "ranger-hdfs-audit.xml");
        handle_result(status, "xasecure.audit.destination.hdfs", "true", "ranger-hdfs-audit.xml");
        status = modify_hdfs_config("xasecure.audit.destination.hdfs.dir", "hdfs:NAMENODE_HOSTNAME:8020/ranger/audit", "ranger-hdfs-audit.xml");
        handle_result(status, "xasecure.audit.destination.hdfs.dir", "hdfs:NAMENODE_HOSTNAME:8020/ranger/audit", "ranger-hdfs-audit.xml");
        status = modify_hdfs_config("xasecure.audit.destination.hdfs.batch.filespool.dir", "/var/log/hadoop/hdfs/audit/hdfs/spool", "ranger-hdfs-audit.xml");
        handle_result(status, "xasecure.audit.destination.hdfs.batch.filespool.dir", "/var/log/hadoop/hdfs/audit/hdfs/spool", "ranger-hdfs-audit.xml");

        //			 Solr Audit Configuration
        status = modify_hdfs_config("xasecure.audit.destination.solr", "false", "ranger-hdfs-audit.xml");
        handle_result(status, "xasecure.audit.destination.solr", "false", "ranger-hdfs-audit.xml");
        status = modify_hdfs_config("xasecure.audit.destination.solr.urls", "", "ranger-hdfs-audit.xml");
        handle_result(status, "xasecure.audit.destination.solr.urls", "", "ranger-hdfs-audit.xml");
        status = modify_hdfs_config("xasecure.audit.destination.solr.zookeepers", "NONE", "ranger-hdfs-audit.xml");
        handle_result(status, "xasecure.audit.destination.solr.zookeepers", "NONE", "ranger-hdfs-audit.xml");
        status = modify_hdfs_config("xasecure.audit.destination.solr.batch.filespool.dir", "/var/log/hadoop/hdfs/audit/solr/spool", "ranger-hdfs-audit.xml");
        handle_result(status, "xasecure.audit.destination.solr.batch.filespool.dir", "/var/log/hadoop/hdfs/audit/solr/spool", "ranger-hdfs-audit.xml");

        //			 Additional Audit Settings
        status = modify_hdfs_config("xasecure.audit.provider.summary.enabled", "false", "ranger-hdfs-audit.xml");
        handle_result(status, "xasecure.audit.provider.summary.enabled", "false", "ranger-hdfs-audit.xml");
        status = modify_hdfs_config("ranger.plugin.hdfs.ambari.cluster.name", "{{cluster_name}}", "ranger-hdfs-audit.xml");
        handle_result(status, "ranger.plugin.hdfs.ambari.cluster.name", "{{cluster_name}}", "ranger-hdfs-audit.xml");
        //			 Basic Plugin Configuration
        status = modify_hdfs_config("policy_user", "ambari-qa", "ranger-hdfs-plugin.properties");
        handle_result(status, "policy_user", "ambari-qa", "ranger-hdfs-plugin.properties");
        status = modify_hdfs_config("hadoop.rpc.protection", "authentication", "ranger-hdfs-plugin.properties");
        handle_result(status, "hadoop.rpc.protection", "authentication", "ranger-hdfs-plugin.properties");
        status = modify_hdfs_config("common.name.for.certificate", "", "ranger-hdfs-plugin.properties");
        handle_result(status, "common.name.for.certificate", "", "ranger-hdfs-plugin.properties");
        status = modify_hdfs_config("ranger-hdfs-plugin-enabled", "No", "ranger-hdfs-plugin.properties");
        handle_result(status, "ranger-hdfs-plugin-enabled", "No", "ranger-hdfs-plugin.properties");

        //			 Repository Configuration
        status = modify_hdfs_config("REPOSITORY_CONFIG_USERNAME", "hadoop", "ranger-hdfs-plugin.properties");
        handle_result(status, "REPOSITORY_CONFIG_USERNAME", "hadoop", "ranger-hdfs-plugin.properties");
        status = modify_hdfs_config("REPOSITORY_CONFIG_PASSWORD", "hadoop", "ranger-hdfs-plugin.properties");
        handle_result(status, "REPOSITORY_CONFIG_PASSWORD", "hadoop", "ranger-hdfs-plugin.properties");

        //			 External Admin Configuration
        status = modify_hdfs_config("external_admin_username", "", "ranger-hdfs-plugin.properties");
        handle_result(status, "external_admin_username", "", "ranger-hdfs-plugin.properties");
        status = modify_hdfs_config("external_admin_password", "", "ranger-hdfs-plugin.properties");
        handle_result(status, "external_admin_password", "", "ranger-hdfs-plugin.properties");
        status = modify_hdfs_config("external_ranger_admin_username", "", "ranger-hdfs-plugin.properties");
        handle_result(status, "external_ranger_admin_username", "", "ranger-hdfs-plugin.properties");
        status = modify_hdfs_config("external_ranger_admin_password", "", "ranger-hdfs-plugin.properties");
        handle_result(status, "external_ranger_admin_password", "", "ranger-hdfs-plugin.properties");

        //			 SSL Keystore Configuration
        status = modify_hdfs_config("xasecure.policymgr.clientssl.keystore", "", "ranger-hdfs-policymgr-ssl.xml");
        handle_result(status, "xasecure.policymgr.clientssl.keystore", "", "ranger-hdfs-policymgr-ssl.xml");
        status = modify_hdfs_config("xasecure.policymgr.clientssl.keystore.password", "", "ranger-hdfs-policymgr-ssl.xml");
        handle_result(status, "xasecure.policymgr.clientssl.keystore.password", "", "ranger-hdfs-policymgr-ssl.xml");
        status = modify_hdfs_config("xasecure.policymgr.clientssl.keystore.credential.file", "jceks:file{{credential_file}}", "ranger-hdfs-policymgr-ssl.xml");
        handle_result(status, "xasecure.policymgr.clientssl.keystore.credential.file", "jceks:file{{credential_file}}", "ranger-hdfs-policymgr-ssl.xml");

        //			 SSL Truststore Configuration
        status = modify_hdfs_config("xasecure.policymgr.clientssl.truststore", "", "ranger-hdfs-policymgr-ssl.xml");
        handle_result(status, "xasecure.policymgr.clientssl.truststore", "", "ranger-hdfs-policymgr-ssl.xml");
        status = modify_hdfs_config("xasecure.policymgr.clientssl.truststore.password", "changeit", "ranger-hdfs-policymgr-ssl.xml");
        handle_result(status, "xasecure.policymgr.clientssl.truststore.password", "changeit", "ranger-hdfs-policymgr-ssl.xml");
        status = modify_hdfs_config("xasecure.policymgr.clientssl.truststore.credential.file", "jceks:file{{credential_file}}", "ranger-hdfs-policymgr-ssl.xml");
        handle_result(status, "xasecure.policymgr.clientssl.truststore.credential.file", "jceks:file{{credential_file}}", "ranger-hdfs-policymgr-ssl.xml");

        //			 Ranger Service Configuration
        status = modify_hdfs_config("ranger.plugin.hdfs.service.name", "{{repo_name}}", "ranger-hdfs-security.xml");
        handle_result(status, "ranger.plugin.hdfs.service.name", "{{repo_name}}", "ranger-hdfs-security.xml");

        //			 Policy Source Configuration
        status = modify_hdfs_config("ranger.plugin.hdfs.policy.source.impl", "org.apache.ranger.admin.client.RangerAdminRESTClient", "ranger-hdfs-security.xml");
        handle_result(status, "ranger.plugin.hdfs.policy.source.impl", "org.apache.ranger.admin.client.RangerAdminRESTClient", "ranger-hdfs-security.xml");
        status = modify_hdfs_config("ranger.plugin.hdfs.policy.rest.url", "{{policymgr_mgr_url}}", "ranger-hdfs-security.xml");
        handle_result(status, "ranger.plugin.hdfs.policy.rest.url", "{{policymgr_mgr_url}}", "ranger-hdfs-security.xml");
        status = modify_hdfs_config("ranger.plugin.hdfs.policy.rest.ssl.config.file", "/etc/hadoop/conf/ranger-policymgr-ssl.xml", "ranger-hdfs-security.xml");
        handle_result(status, "ranger.plugin.hdfs.policy.rest.ssl.config.file", "/etc/hadoop/conf/ranger-policymgr-ssl.xml", "ranger-hdfs-security.xml");

        //			 Policy Polling and Caching
        status = modify_hdfs_config("ranger.plugin.hdfs.policy.pollIntervalMs", "30000", "ranger-hdfs-security.xml");
        handle_result(status, "ranger.plugin.hdfs.policy.pollIntervalMs", "30000", "ranger-hdfs-security.xml");
        status = modify_hdfs_config("ranger.plugin.hdfs.policy.cache.dir", "/etc/ranger/{{repo_name}}/policycache", "ranger-hdfs-security.xml");
        handle_result(status, "ranger.plugin.hdfs.policy.cache.dir", "/etc/ranger/{{repo_name}}/policycache", "ranger-hdfs-security.xml");

        //			 Authorization Fallback
        status = modify_hdfs_config("xasecure.add-hadoop-authorization", "true", "ranger-hdfs-security.xml");
        handle_result(status, "xasecure.add-hadoop-authorization", "true", "ranger-hdfs-security.xml");
        //			 Truststore Configuration
        status = modify_hdfs_config("ssl.client.truststore.location", "", "ssl-client.xml");
        handle_result(status, "ssl.client.truststore.location", "", "ssl-client.xml");
        status = modify_hdfs_config("ssl.client.truststore.type", "jks", "ssl-client.xml");
        handle_result(status, "ssl.client.truststore.type", "jks", "ssl-client.xml");
        status = modify_hdfs_config("ssl.client.truststore.password", "", "ssl-client.xml");
        handle_result(status, "ssl.client.truststore.password", "", "ssl-client.xml");
        status = modify_hdfs_config("ssl.client.truststore.reload.interval", "10000", "ssl-client.xml");
        handle_result(status, "ssl.client.truststore.reload.interval", "10000", "ssl-client.xml");

        //			 Keystore Configuration
        status = modify_hdfs_config("ssl.client.keystore.type", "jks", "ssl-client.xml");
        handle_result(status, "ssl.client.keystore.type", "jks", "ssl-client.xml");
        status = modify_hdfs_config("ssl.client.keystore.location", "", "ssl-client.xml");
        handle_result(status, "ssl.client.keystore.location", "", "ssl-client.xml");
        status = modify_hdfs_config("ssl.client.keystore.password", "", "ssl-client.xml");
        handle_result(status, "ssl.client.keystore.password", "", "ssl-client.xml");

        //			 Truststore Configuration
        status = modify_hdfs_config("ssl.server.truststore.location", "/etc/security/serverKeys/all.jks", "ssl-server.xml");
        handle_result(status, "ssl.server.truststore.location", "/etc/security/serverKeys/all.jks", "ssl-server.xml");
        status = modify_hdfs_config("ssl.server.truststore.type", "jks", "ssl-server.xml");
        handle_result(status, "ssl.server.truststore.type", "jks", "ssl-server.xml");
        status = modify_hdfs_config("ssl.server.truststore.password", "bigdata", "ssl-server.xml");
        handle_result(status, "ssl.server.truststore.password", "bigdata", "ssl-server.xml");
        status = modify_hdfs_config("ssl.server.truststore.reload.interval", "10000", "ssl-server.xml");
        handle_result(status, "ssl.server.truststore.reload.interval", "10000", "ssl-server.xml");

        //			 Keystore Configuration
        status = modify_hdfs_config("ssl.server.keystore.type", "jks", "ssl-server.xml");
        handle_result(status, "ssl.server.keystore.type", "jks", "ssl-server.xml");
        status = modify_hdfs_config("ssl.server.keystore.location", "/etc/security/serverKeys/keystore.jks", "ssl-server.xml");
        handle_result(status, "ssl.server.keystore.location", "/etc/security/serverKeys/keystore.jks", "ssl-server.xml");
        status = modify_hdfs_config("ssl.server.keystore.password", "bigdata", "ssl-server.xml");
        handle_result(status, "ssl.server.keystore.password", "bigdata", "ssl-server.xml");
        status = modify_hdfs_config("ssl.server.keystore.keypassword", "bigdata", "ssl-server.xml");
        handle_result(status, "ssl.server.keystore.keypassword", "bigdata", "ssl-server.xml");
        //			 Update Hadoop MapReduce configuration parameters in mapred-site.xml
        status = modify_hdfs_config("mapreduce.task.io.sort.mb", "358", "mapred-site.xml");
        handle_result(status, "mapreduce.task.io.sort.mb", "358", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.map.sort.spill.percent", "0.7", "mapred-site.xml");
        handle_result(status, "mapreduce.map.sort.spill.percent", "0.7", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.task.io.sort.factor", "100", "mapred-site.xml");
        handle_result(status, "mapreduce.task.io.sort.factor", "100", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.cluster.administrators", " hadoop", "mapred-site.xml"); // Note leading space
        handle_result(status, "mapreduce.cluster.administrators", " hadoop", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.reduce.shuffle.parallelcopies", "30", "mapred-site.xml");
        handle_result(status, "mapreduce.reduce.shuffle.parallelcopies", "30", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.map.speculative", "false", "mapred-site.xml");
        handle_result(status, "mapreduce.map.speculative", "false", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.reduce.speculative", "false", "mapred-site.xml");
        handle_result(status, "mapreduce.reduce.speculative", "false", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.job.reduce.slowstart.completedmaps", "0.05", "mapred-site.xml");
        handle_result(status, "mapreduce.job.reduce.slowstart.completedmaps", "0.05", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.job.counters.max", "130", "mapred-site.xml");
        handle_result(status, "mapreduce.job.counters.max", "130", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.reduce.shuffle.merge.percent", "0.66", "mapred-site.xml");
        handle_result(status, "mapreduce.reduce.shuffle.merge.percent", "0.66", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.reduce.shuffle.input.buffer.percent", "0.7", "mapred-site.xml");
        handle_result(status, "mapreduce.reduce.shuffle.input.buffer.percent", "0.7", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.output.fileoutputformat.compress.type", "BLOCK", "mapred-site.xml");
        handle_result(status, "mapreduce.output.fileoutputformat.compress.type", "BLOCK", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.reduce.input.buffer.percent", "0.0", "mapred-site.xml");
        handle_result(status, "mapreduce.reduce.input.buffer.percent", "0.0", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.map.output.compress", "false", "mapred-site.xml");
        handle_result(status, "mapreduce.map.output.compress", "false", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.task.timeout", "300000", "mapred-site.xml");
        handle_result(status, "mapreduce.task.timeout", "300000", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.map.memory.mb", "512", "mapred-site.xml");
        handle_result(status, "mapreduce.map.memory.mb", "512", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.reduce.memory.mb", "1024", "mapred-site.xml");
        handle_result(status, "mapreduce.reduce.memory.mb", "1024", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.shuffle.port", "13562", "mapred-site.xml");
        handle_result(status, "mapreduce.shuffle.port", "13562", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.jobhistory.intermediate-done-dir", "/mr-history/tmp", "mapred-site.xml");
        handle_result(status, "mapreduce.jobhistory.intermediate-done-dir", "/mr-history/tmp", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.jobhistory.done-dir", "/mr-history/done", "mapred-site.xml");
        handle_result(status, "mapreduce.jobhistory.done-dir", "/mr-history/done", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.jobhistory.address", "localhost:10020", "mapred-site.xml");
        handle_result(status, "mapreduce.jobhistory.address", "localhost:10020", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.jobhistory.webapp.address", "localhost:19888", "mapred-site.xml");
        handle_result(status, "mapreduce.jobhistory.webapp.address", "localhost:19888", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.framework.name", "yarn", "mapred-site.xml");
        handle_result(status, "mapreduce.framework.name", "yarn", "mapred-site.xml");
        status = modify_hdfs_config("yarn.app.mapreduce.am.staging-dir", "/user", "mapred-site.xml");
        handle_result(status, "yarn.app.mapreduce.am.staging-dir", "/user", "mapred-site.xml");
        status = modify_hdfs_config("yarn.app.mapreduce.am.resource.mb", "512", "mapred-site.xml");
        handle_result(status, "yarn.app.mapreduce.am.resource.mb", "512", "mapred-site.xml");
        status = modify_hdfs_config("yarn.app.mapreduce.am.command-opts", "-Xmx410m", "mapred-site.xml");
        handle_result(status, "yarn.app.mapreduce.am.command-opts", "-Xmx410m", "mapred-site.xml");
        status = modify_hdfs_config("yarn.app.mapreduce.am.admin-command-opts", "-server -XX:NewRatio=8 -Djava.net.preferIPv4Stack=true -Dhadoop.metrics.log.level=WARN", "mapred-site.xml");
        handle_result(status, "yarn.app.mapreduce.am.admin-command-opts", "-server -XX:NewRatio=8 -Djava.net.preferIPv4Stack=true -Dhadoop.metrics.log.level=WARN", "mapred-site.xml");
        status = modify_hdfs_config("yarn.app.mapreduce.am.log.level", "INFO", "mapred-site.xml");
        handle_result(status, "yarn.app.mapreduce.am.log.level", "INFO", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.admin.map.child.java.opts", "-server -XX:NewRatio=8 -Djava.net.preferIPv4Stack=true -Dhadoop.metrics.log.level=WARN", "mapred-site.xml");
        handle_result(status, "mapreduce.admin.map.child.java.opts", "-server -XX:NewRatio=8 -Djava.net.preferIPv4Stack=true -Dhadoop.metrics.log.level=WARN", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.admin.reduce.child.java.opts", "-server -XX:NewRatio=8 -Djava.net.preferIPv4Stack=true -Dhadoop.metrics.log.level=WARN", "mapred-site.xml");
        handle_result(status, "mapreduce.admin.reduce.child.java.opts", "-server -XX:NewRatio=8 -Djava.net.preferIPv4Stack=true -Dhadoop.metrics.log.level=WARN", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.application.classpath", "{{hadoop_conf_dir}},{{hadoop_home}}/*,{{hadoop_home}}/lib/*,{{hadoop_hdfs_home}}/*,{{hadoop_hdfs_home}}/lib/*,{{hadoop_yarn_home}}/*,{{hadoop_yarn_home}}/lib/*,{{hadoop_mapred_home}}/*,{{hadoop_mapred_home}}/lib/*", "mapred-site.xml");
        handle_result(status, "mapreduce.application.classpath", "{{hadoop_conf_dir}},{{hadoop_home}}/*,{{hadoop_home}}/lib/*,{{hadoop_hdfs_home}}/*,{{hadoop_hdfs_home}}/lib/*,{{hadoop_yarn_home}}/*,{{hadoop_yarn_home}}/lib/*,{{hadoop_mapred_home}}/*,{{hadoop_mapred_home}}/lib/*", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.am.max-attempts", "2", "mapred-site.xml");
        handle_result(status, "mapreduce.am.max-attempts", "2", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.map.java.opts", "-Xmx410m", "mapred-site.xml");
        handle_result(status, "mapreduce.map.java.opts", "-Xmx410m", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.reduce.java.opts", "-Xmx756m", "mapred-site.xml");
        handle_result(status, "mapreduce.reduce.java.opts", "-Xmx756m", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.map.log.level", "INFO", "mapred-site.xml");
        handle_result(status, "mapreduce.map.log.level", "INFO", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.reduce.log.level", "INFO", "mapred-site.xml");
        handle_result(status, "mapreduce.reduce.log.level", "INFO", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.admin.user.env", "LD_LIBRARY_PATH={{hadoop_home}}/lib/native", "mapred-site.xml");
        handle_result(status, "mapreduce.admin.user.env", "LD_LIBRARY_PATH={{hadoop_home}}/lib/native", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.output.fileoutputformat.compress", "false", "mapred-site.xml");
        handle_result(status, "mapreduce.output.fileoutputformat.compress", "false", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.jobhistory.http.policy", "HTTP_ONLY", "mapred-site.xml");
        handle_result(status, "mapreduce.jobhistory.http.policy", "HTTP_ONLY", "mapred-site.xml");
        status = modify_hdfs_config("mapreduce.job.queuename", "default", "mapred-site.xml");
        handle_result(status, "mapreduce.job.queuename", "default", "mapred-site.xml");
        //			 Update YARN configuration parameters in yarn-site.xml
        status = modify_hdfs_config("yarn.resourcemanager.hostname", "localhost", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.hostname", "localhost", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.resource-tracker.address", "localhost:8025", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.resource-tracker.address", "localhost:8025", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.scheduler.address", "localhost:8030", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.scheduler.address", "localhost:8030", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.address", "localhost:8050", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.address", "localhost:8050", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.admin.address", "localhost:8141", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.admin.address", "localhost:8141", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.scheduler.class", "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.scheduler.class", "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler", "yarn-site.xml");
        status = modify_hdfs_config("yarn.scheduler.minimum-allocation-mb", "512", "yarn-site.xml");
        handle_result(status, "yarn.scheduler.minimum-allocation-mb", "512", "yarn-site.xml");
        status = modify_hdfs_config("yarn.scheduler.maximum-allocation-mb", "5120", "yarn-site.xml");
        handle_result(status, "yarn.scheduler.maximum-allocation-mb", "5120", "yarn-site.xml");
        status = modify_hdfs_config("yarn.acl.enable", "false", "yarn-site.xml");
        handle_result(status, "yarn.acl.enable", "false", "yarn-site.xml");
        status = modify_hdfs_config("yarn.admin.acl", "yarn,yarn-ats", "yarn-site.xml");
        handle_result(status, "yarn.admin.acl", "yarn,yarn-ats", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.address", "0.0.0.0:45454", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.address", "0.0.0.0:45454", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.resource.memory-mb", "5120", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.resource.memory-mb", "5120", "yarn-site.xml");
        status = modify_hdfs_config("yarn.application.classpath", "{{hadoop_conf_dir}},{{hadoop_home}}/*,{{hadoop_home}}/lib/*,{{hadoop_hdfs_home}}/*,{{hadoop_hdfs_home}}/lib/*,{{hadoop_yarn_home}}/*,{{hadoop_yarn_home}}/lib/*", "yarn-site.xml");
        handle_result(status, "yarn.application.classpath", "{{hadoop_conf_dir}},{{hadoop_home}}/*,{{hadoop_home}}/lib/*,{{hadoop_hdfs_home}}/*,{{hadoop_hdfs_home}}/lib/*,{{hadoop_yarn_home}}/*,{{hadoop_yarn_home}}/lib/*", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.vmem-pmem-ratio", "2.1", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.vmem-pmem-ratio", "2.1", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.container-executor.class", "org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.container-executor.class", "org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.linux-container-executor.group", "hadoop", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.linux-container-executor.group", "hadoop", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.aux-services", "mapreduce_shuffle,{{timeline_collector}}", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.aux-services", "mapreduce_shuffle,{{timeline_collector}}", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.aux-services.mapreduce_shuffle.class", "org.apache.hadoop.mapred.ShuffleHandler", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.aux-services.mapreduce_shuffle.class", "org.apache.hadoop.mapred.ShuffleHandler", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.log-dirs", "/hadoop/yarn/log", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.log-dirs", "/hadoop/yarn/log", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.local-dirs", "/hadoop/yarn/local", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.local-dirs", "/hadoop/yarn/local", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.container-monitor.interval-ms", "3000", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.container-monitor.interval-ms", "3000", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.health-checker.interval-ms", "135000", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.health-checker.interval-ms", "135000", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.health-checker.script.timeout-ms", "60000", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.health-checker.script.timeout-ms", "60000", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.log.retain-seconds", "604800", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.log.retain-seconds", "604800", "yarn-site.xml");
        status = modify_hdfs_config("yarn.log-aggregation-enable", "true", "yarn-site.xml");
        handle_result(status, "yarn.log-aggregation-enable", "true", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.remote-app-log-dir", "/app-logs", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.remote-app-log-dir", "/app-logs", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.remote-app-log-dir-suffix", "logs", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.remote-app-log-dir-suffix", "logs", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.log-aggregation.compression-type", "gz", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.log-aggregation.compression-type", "gz", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.delete.debug-delay-sec", "0", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.delete.debug-delay-sec", "0", "yarn-site.xml");
        status = modify_hdfs_config("yarn.log-aggregation.retain-seconds", "2592000", "yarn-site.xml");
        handle_result(status, "yarn.log-aggregation.retain-seconds", "2592000", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.admin-env", "MALLOC_ARENA_MAX=$MALLOC_ARENA_MAX", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.admin-env", "MALLOC_ARENA_MAX=$MALLOC_ARENA_MAX", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.disk-health-checker.min-healthy-disks", "0.25", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.disk-health-checker.min-healthy-disks", "0.25", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.am.max-attempts", "2", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.am.max-attempts", "2", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.webapp.address", "localhost:8088", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.webapp.address", "localhost:8088", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.webapp.https.address", "localhost:8090", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.webapp.https.address", "localhost:8090", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.vmem-check-enabled", "false", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.vmem-check-enabled", "false", "yarn-site.xml");
        status = modify_hdfs_config("yarn.log.server.url", "http:localhost:19888/jobhistory/logs", "yarn-site.xml");
        handle_result(status, "yarn.log.server.url", "http:localhost:19888/jobhistory/logs", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.nodes.exclude-path", "/etc/hadoop/conf/yarn.exclude", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.nodes.exclude-path", "/etc/hadoop/conf/yarn.exclude", "yarn-site.xml");
        status = modify_hdfs_config("manage.include.files", "false", "yarn-site.xml");
        handle_result(status, "manage.include.files", "false", "yarn-site.xml");
        status = modify_hdfs_config("yarn.http.policy", "HTTP_ONLY", "yarn-site.xml");
        handle_result(status, "yarn.http.policy", "HTTP_ONLY", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.enabled", "true", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.enabled", "true", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.generic-application-history.store-class", "org.apache.hadoop.yarn.server.applicationhistoryservice.NullApplicationHistoryStore", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.generic-application-history.store-class", "org.apache.hadoop.yarn.server.applicationhistoryservice.NullApplicationHistoryStore", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.leveldb-timeline-store.path", "/var/log/hadoop-yarn/timeline", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.leveldb-timeline-store.path", "/var/log/hadoop-yarn/timeline", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.webapp.address", "localhost:8188", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.webapp.address", "localhost:8188", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.webapp.https.address", "localhost:8190", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.webapp.https.address", "localhost:8190", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.address", "localhost:10200", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.address", "localhost:10200", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.ttl-enable", "true", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.ttl-enable", "true", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.ttl-ms", "2678400000", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.ttl-ms", "2678400000", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.leveldb-timeline-store.ttl-interval-ms", "300000", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.leveldb-timeline-store.ttl-interval-ms", "300000", "yarn-site.xml");
        status = modify_hdfs_config("hadoop.registry.zk.quorum", "localhost:2181", "yarn-site.xml");
        handle_result(status, "hadoop.registry.zk.quorum", "localhost:2181", "yarn-site.xml");
        status = modify_hdfs_config("hadoop.registry.dns.bind-port", "53", "yarn-site.xml");
        handle_result(status, "hadoop.registry.dns.bind-port", "53", "yarn-site.xml");
        status = modify_hdfs_config("hadoop.registry.dns.zone-mask", "255.255.255.0", "yarn-site.xml");
        handle_result(status, "hadoop.registry.dns.zone-mask", "255.255.255.0", "yarn-site.xml");
        status = modify_hdfs_config("hadoop.registry.dns.zone-subnet", "172.17.0.0", "yarn-site.xml");
        handle_result(status, "hadoop.registry.dns.zone-subnet", "172.17.0.0", "yarn-site.xml");
        status = modify_hdfs_config("hadoop.registry.dns.enabled", "true", "yarn-site.xml");
        handle_result(status, "hadoop.registry.dns.enabled", "true", "yarn-site.xml");
        status = modify_hdfs_config("hadoop.registry.dns.domain-name", "EXAMPLE.COM", "yarn-site.xml");
        handle_result(status, "hadoop.registry.dns.domain-name", "EXAMPLE.COM", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.recovery.enabled", "true", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.recovery.enabled", "true", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.recovery.dir", "{{yarn_log_dir_prefix}}/nodemanager/recovery-state", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.recovery.dir", "{{yarn_log_dir_prefix}}/nodemanager/recovery-state", "yarn-site.xml");
        status = modify_hdfs_config("yarn.client.nodemanager-connect.retry-interval-ms", "10000", "yarn-site.xml");
        handle_result(status, "yarn.client.nodemanager-connect.retry-interval-ms", "10000", "yarn-site.xml");
        status = modify_hdfs_config("yarn.client.nodemanager-connect.max-wait-ms", "60000", "yarn-site.xml");
        handle_result(status, "yarn.client.nodemanager-connect.max-wait-ms", "60000", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.recovery.enabled", "true", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.recovery.enabled", "true", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.work-preserving-recovery.enabled", "true", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.work-preserving-recovery.enabled", "true", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.store.class", "org.apache.hadoop.yarn.server.resourcemanager.recovery.ZKRMStateStore", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.store.class", "org.apache.hadoop.yarn.server.resourcemanager.recovery.ZKRMStateStore", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.zk-address", "localhost:2181", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.zk-address", "localhost:2181", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.zk-state-store.parent-path", "/rmstore", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.zk-state-store.parent-path", "/rmstore", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.zk-acl", "world:anyone:rwcda", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.zk-acl", "world:anyone:rwcda", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.work-preserving-recovery.scheduling-wait-ms", "10000", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.work-preserving-recovery.scheduling-wait-ms", "10000", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.connect.retry-interval.ms", "30000", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.connect.retry-interval.ms", "30000", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.connect.max-wait.ms", "900000", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.connect.max-wait.ms", "900000", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.zk-retry-interval-ms", "1000", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.zk-retry-interval-ms", "1000", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.zk-num-retries", "1000", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.zk-num-retries", "1000", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.zk-timeout-ms", "10000", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.zk-timeout-ms", "10000", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.state-store.max-completed-applications", "${yarn.resourcemanager.max-completed-applications}", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.state-store.max-completed-applications", "${yarn.resourcemanager.max-completed-applications}", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.fs.state-store.retry-policy-spec", "2000, 500", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.fs.state-store.retry-policy-spec", "2000, 500", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.fs.state-store.uri", " ", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.fs.state-store.uri", " ", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.ha.enabled", "false", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.ha.enabled", "false", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.linux-container-executor.resources-handler.class", "org.apache.hadoop.yarn.server.nodemanager.util.DefaultLCEResourcesHandler", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.linux-container-executor.resources-handler.class", "org.apache.hadoop.yarn.server.nodemanager.util.DefaultLCEResourcesHandler", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.linux-container-executor.cgroups.hierarchy", "/yarn", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.linux-container-executor.cgroups.hierarchy", "/yarn", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.linux-container-executor.cgroups.mount", "false", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.linux-container-executor.cgroups.mount", "false", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.linux-container-executor.cgroups.mount-path", "/cgroup", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.linux-container-executor.cgroups.mount-path", "/cgroup", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.linux-container-executor.cgroups.strict-resource-usage", "false", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.linux-container-executor.cgroups.strict-resource-usage", "false", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.resource.cpu-vcores", "8", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.resource.cpu-vcores", "8", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.resource.percentage-physical-cpu-limit", "80", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.resource.percentage-physical-cpu-limit", "80", "yarn-site.xml");
        status = modify_hdfs_config("yarn.node-labels.fs-store.retry-policy-spec", "2000, 500", "yarn-site.xml");
        handle_result(status, "yarn.node-labels.fs-store.retry-policy-spec", "2000, 500", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.disk-health-checker.min-free-space-per-disk-mb", "1000", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.disk-health-checker.min-free-space-per-disk-mb", "1000", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage", "90", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage", "90", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.resource-plugins", "", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.resource-plugins", "", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.resource-plugins.gpu.allowed-gpu-devices", "auto", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.resource-plugins.gpu.allowed-gpu-devices", "auto", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.resource-plugins.gpu.path-to-discovery-executables", "", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.resource-plugins.gpu.path-to-discovery-executables", "", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.log-aggregation.roll-monitoring-interval-seconds", "3600", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.log-aggregation.roll-monitoring-interval-seconds", "3600", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.log-aggregation.debug-enabled", "false", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.log-aggregation.debug-enabled", "false", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.log-aggregation.num-log-files-per-app", "30", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.log-aggregation.num-log-files-per-app", "30", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.system-metrics-publisher.enabled", "true", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.system-metrics-publisher.enabled", "true", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.system-metrics-publisher.dispatcher.pool-size", "10", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.system-metrics-publisher.dispatcher.pool-size", "10", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.client.max-retries", "30", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.client.max-retries", "30", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.client.retry-interval-ms", "1000", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.client.retry-interval-ms", "1000", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.state-store-class", "org.apache.hadoop.yarn.server.timeline.recovery.LeveldbTimelineStateStore", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.state-store-class", "org.apache.hadoop.yarn.server.timeline.recovery.LeveldbTimelineStateStore", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.leveldb-state-store.path", "/hadoop/yarn/timeline", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.leveldb-state-store.path", "/hadoop/yarn/timeline", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.leveldb-timeline-store.path", "/hadoop/yarn/timeline", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.leveldb-timeline-store.path", "/hadoop/yarn/timeline", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.leveldb-timeline-store.read-cache-size", "104857600", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.leveldb-timeline-store.read-cache-size", "104857600", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.leveldb-timeline-store.start-time-read-cache-size", "10000", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.leveldb-timeline-store.start-time-read-cache-size", "10000", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.leveldb-timeline-store.start-time-write-cache-size", "10000", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.leveldb-timeline-store.start-time-write-cache-size", "10000", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.http-authentication.type", "simple", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.http-authentication.type", "simple", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.http-authentication.simple.anonymous.allowed", "true", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.http-authentication.simple.anonymous.allowed", "true", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.webapp.delegation-token-auth-filter.enabled", "false", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.webapp.delegation-token-auth-filter.enabled", "false", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.bind-host", "0.0.0.0", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.bind-host", "0.0.0.0", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.bind-host", "0.0.0.0", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.bind-host", "0.0.0.0", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.bind-host", "0.0.0.0", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.bind-host", "0.0.0.0", "yarn-site.xml");
        status = modify_hdfs_config("yarn.node-labels.fs-store.root-dir", "/system/yarn/node-labels", "yarn-site.xml");
        handle_result(status, "yarn.node-labels.fs-store.root-dir", "/system/yarn/node-labels", "yarn-site.xml");
        status = modify_hdfs_config("yarn.scheduler.minimum-allocation-vcores", "1", "yarn-site.xml");
        handle_result(status, "yarn.scheduler.minimum-allocation-vcores", "1", "yarn-site.xml");
        status = modify_hdfs_config("yarn.scheduler.maximum-allocation-vcores", "8", "yarn-site.xml");
        handle_result(status, "yarn.scheduler.maximum-allocation-vcores", "8", "yarn-site.xml");
        status = modify_hdfs_config("yarn.node-labels.enabled", "false", "yarn-site.xml");
        handle_result(status, "yarn.node-labels.enabled", "false", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.scheduler.monitor.enable", "true", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.scheduler.monitor.enable", "true", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.recovery.enabled", "true", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.recovery.enabled", "true", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.version", "2.0f", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.version", "2.0f", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.versions", "1.5f,2.0f", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.versions", "1.5f,2.0f", "yarn-site.xml");
        status = modify_hdfs_config("yarn.system-metricspublisher.enabled", "true", "yarn-site.xml");
        handle_result(status, "yarn.system-metricspublisher.enabled", "true", "yarn-site.xml");
        status = modify_hdfs_config("yarn.rm.system-metricspublisher.emit-container-events", "true", "yarn-site.xml");
        handle_result(status, "yarn.rm.system-metricspublisher.emit-container-events", "true", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.recovery.supervised", "true", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.recovery.supervised", "true", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.store-class", "org.apache.hadoop.yarn.server.timeline.EntityGroupFSTimelineStore", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.store-class", "org.apache.hadoop.yarn.server.timeline.EntityGroupFSTimelineStore", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.entity-group-fs-store.active-dir", "/ats/active/", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.entity-group-fs-store.active-dir", "/ats/active/", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.entity-group-fs-store.done-dir", "/ats/done/", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.entity-group-fs-store.done-dir", "/ats/done/", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.entity-group-fs-store.group-id-plugin-classes", "org.apache.hadoop.yarn.applications.distributedshell.DistributedShellTimelinePlugin", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.entity-group-fs-store.group-id-plugin-classes", "org.apache.hadoop.yarn.applications.distributedshell.DistributedShellTimelinePlugin", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.entity-group-fs-store.summary-store", "org.apache.hadoop.yarn.server.timeline.RollingLevelDBTimelineStore", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.entity-group-fs-store.summary-store", "org.apache.hadoop.yarn.server.timeline.RollingLevelDBTimelineStore", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.entity-group-fs-store.scan-interval-seconds", "60", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.entity-group-fs-store.scan-interval-seconds", "60", "yarn-site.xml");
        status = modify_hdfs_config("yarn.log.server.web-service.url", "http:localhost:8188/ws/v1/applicationhistory", "yarn-site.xml");
        handle_result(status, "yarn.log.server.web-service.url", "http:localhost:8188/ws/v1/applicationhistory", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.entity-group-fs-store.cleaner-interval-seconds", "3600", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.entity-group-fs-store.cleaner-interval-seconds", "3600", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.entity-group-fs-store.retain-seconds", "604800", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.entity-group-fs-store.retain-seconds", "604800", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.container-metrics.unregister-delay-ms", "60000", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.container-metrics.unregister-delay-ms", "60000", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.entity-group-fs-store.group-id-plugin-classpath", "", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.entity-group-fs-store.group-id-plugin-classpath", "", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.monitor.capacity.preemption.total_preemption_per_round", "0.1", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.monitor.capacity.preemption.total_preemption_per_round", "0.1", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.monitor.capacity.preemption.natural_termination_factor", "1", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.monitor.capacity.preemption.natural_termination_factor", "1", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.monitor.capacity.preemption.monitoring_interval", "15000", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.monitor.capacity.preemption.monitoring_interval", "15000", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.linux-container-executor.nonsecure-mode.limit-users", "true", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.linux-container-executor.nonsecure-mode.limit-users", "true", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.runtime.linux.allowed-runtimes", "default,docker", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.runtime.linux.allowed-runtimes", "default,docker", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.runtime.linux.docker.allowed-container-networks", "host,none,bridge", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.runtime.linux.docker.allowed-container-networks", "host,none,bridge", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.runtime.linux.docker.default-container-network", "host", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.runtime.linux.docker.default-container-network", "host", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.runtime.linux.docker.privileged-containers.allowed", "false", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.runtime.linux.docker.privileged-containers.allowed", "false", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.runtime.linux.docker.privileged-containers.acl", "", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.runtime.linux.docker.privileged-containers.acl", "", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.runtime.linux.docker.capabilities", "CHOWN,DAC_OVERRIDE,FSETID,FOWNER,MKNOD,NET_RAW,SETGID,SETUID,SETFCAP,SETPCAP,NET_BIND_SERVICE,SYS_CHROOT,KILL,AUDIT_WRITE", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.runtime.linux.docker.capabilities", "CHOWN,DAC_OVERRIDE,FSETID,FOWNER,MKNOD,NET_RAW,SETGID,SETUID,SETFCAP,SETPCAP,NET_BIND_SERVICE,SYS_CHROOT,KILL,AUDIT_WRITE", "yarn-site.xml");
        status = modify_hdfs_config("yarn.webapp.ui2.enable", "true", "yarn-site.xml");
        handle_result(status, "yarn.webapp.ui2.enable", "true", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.http-cross-origin.enabled", "true", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.http-cross-origin.enabled", "true", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.webapp.cross-origin.enabled", "true", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.webapp.cross-origin.enabled", "true", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.webapp.cross-origin.enabled", "true", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.webapp.cross-origin.enabled", "true", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.resource-plugins.gpu.docker-plugin", "nvidia-docker-v1", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.resource-plugins.gu.docker-plugin", "nvidia-docker-v1", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.resource-plugins.gpu.docker-plugin.nvidiadocker-v1.endpoint", "http:localhost:3476/v1.0/docker/cli", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.resource-plugins.gpu.docker-plugin.nvidiadocker-v1.endpoint", "http:localhost:3476/v1.0/docker/cli", "yarn-site.xml");
        status = modify_hdfs_config("yarn.webapp.api-service.enable", "true", "yarn-site.xml");
        handle_result(status, "yarn.webapp.api-service.enable", "true", "yarn-site.xml");
        status = modify_hdfs_config("yarn.service.framework.path", "/bigtop/apps/3.2.0/yarn/service-dep.tar.gz", "yarn-site.xml");
        handle_result(status, "yarn.service.framework.path", "/bigtop/apps/3.2.0/yarn/service-dep.tar.gz", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.aux-services.timeline_collector.class", "org.apache.hadoop.yarn.server.timelineservice.collector.PerNodeTimelineCollectorsAuxService", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.aux-services.timeline_collector.class", "org.apache.hadoop.yarn.server.timelineservice.collector.PerNodeTimelineCollectorsAuxService", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.reader.webapp.address", "localhost:8198", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.reader.webapp.address", "localhost:8198", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.reader.webapp.https.address", "localhost:8199", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.reader.webapp.https.address", "localhost:8199", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.hbase-schema.prefix", "prod.", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.hbase-schema.prefix", "prod.", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.hbase.configuration.file", "file:{{yarn_hbase_conf_dir}}/hbase-site.xml", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.hbase.configuration.file", "file:{{yarn_hbase_conf_dir}}/hbase-site.xml", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.hbase.coprocessor.jar.hdfs.location", "{{yarn_timeline_jar_location}}", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.hbase.coprocessor.jar.hdfs.location", "{{yarn_timeline_jar_location}}", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.monitor.capacity.preemption.intra-queue-preemption.enabled", "true", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.monitor.capacity.preemption.intra-queue-preemption.enabled", "true", "yarn-site.xml");
        status = modify_hdfs_config("yarn.scheduler.capacity.ordering-policy.priority-utilization.underutilized-preemption.enabled", "true", "yarn-site.xml");
        handle_result(status, "yarn.scheduler.capacity.ordering-policy.priority-utilization.underutilized-preemption.enabled", "true", "yarn-site.xml");
        status = modify_hdfs_config("yarn.resourcemanager.display.per-user-apps", "true", "yarn-site.xml");
        handle_result(status, "yarn.resourcemanager.display.per-user-apps", "true", "yarn-site.xml");
        status = modify_hdfs_config("yarn.service.system-service.dir", "/services", "yarn-site.xml");
        handle_result(status, "yarn.service.system-service.dir", "/services", "yarn-site.xml");
        status = modify_hdfs_config("yarn.timeline-service.generic-application-history.save-non-am-container-meta-info", "false", "yarn-site.xml");
        handle_result(status, "yarn.timeline-service.generic-application-history.save-non-am-container-meta-info", "false", "yarn-site.xml");
        status = modify_hdfs_config("hadoop.registry.dns.bind-address", "0.0.0.0", "yarn-site.xml");
        handle_result(status, "hadoop.registry.dns.bind-address", "0.0.0.0", "yarn-site.xml");
        status = modify_hdfs_config("hadoop.http.cross-origin.allowed-origins", "{{cross_origins}}", "yarn-site.xml");
        handle_result(status, "hadoop.http.cross-origin.allowed-origins", "{{cross_origins}}", "yarn-site.xml");
        status = modify_hdfs_config("yarn.nodemanager.resourcemanager.connect.wait.secs", "1800", "yarn-site.xml");
        handle_result(status, "yarn.nodemanager.resourcemanager.connect.wait.secs", "1800", "yarn-site.xml");
        //			 Set yarn-log4j configuration parameters
        status = modify_hdfs_config("yarn_rm_summary_log_max_backup_size", "256", "yarnservice-log4j.properties");
        handle_result(status, "yarn_rm_summary_log_max_backup_size", "256", "yarnservice-log4j.properties");
        status = modify_hdfs_config("yarn_rm_summary_log_number_of_backup_files", "20", "yarnservice-log4j.properties");
        handle_result(status, "yarn_rm_summary_log_number_of_backup_files", "20", "yarnservice-log4j.properties");
        status = modify_hdfs_config("yarn.log.dir", ".", "yarnservice-log4j.properties");
        handle_result(status, "yarn.log.dir", ".", "yarnservice-log4j.properties");
        status = modify_hdfs_config("hadoop.mapreduce.jobsummary.logger", "${hadoop.root.logger}", "yarnservice-log4j.properties");
        handle_result(status, "hadoop.mapreduce.jobsummary.logger", "${hadoop.root.logger}", "yarnservice-log4j.properties");
        status = modify_hdfs_config("hadoop.mapreduce.jobsummary.log.file", "hadoop-mapreduce.jobsummary.log", "yarnservice-log4j.properties");
        handle_result(status, "hadoop.mapreduce.jobsummary.log.file", "hadoop-mapreduce.jobsummary.log", "yarnservice-log4j.properties");
        status = modify_hdfs_config("yarn.server.resourcemanager.appsummary.log.file", "hadoop-mapreduce.jobsummary.log", "yarnservice-log4j.properties");
        handle_result(status, "yarn.server.resourcemanager.appsummary.log.file", "hadoop-mapreduce.jobsummary.log", "yarnservice-log4j.properties");
        status = modify_hdfs_config("yarn.server.resourcemanager.appsummary.logger", "${hadoop.root.logger}", "yarnservice-log4j.properties");
        handle_result(status, "yarn.server.resourcemanager.appsummary.logger", "${hadoop.root.logger}", "yarnservice-log4j.properties");
        status = modify_hdfs_config("log4j.appender.RMSUMMARY.File", "${yarn.log.dir}/${yarn.server.resourcemanager.appsummary.log.file}", "yarnservice-log4j.properties");
        handle_result(status, "log4j.appender.RMSUMMARY.File", "${yarn.log.dir}/${yarn.server.resourcemanager.appsummary.log.file}", "yarnservice-log4j.properties");
        status = modify_hdfs_config("log4j.appender.RMSUMMARY.MaxFileSize", "{{yarn_rm_summary_log_max_backup_size}}MB", "yarnservice-log4j.properties");
        handle_result(status, "log4j.appender.RMSUMMARY.MaxFileSize", "{{yarn_rm_summary_log_max_backup_size}}MB", "yarnservice-log4j.properties");
        status = modify_hdfs_config("log4j.appender.RMSUMMARY.MaxBackupIndex", "{{yarn_rm_summary_log_number_of_backup_files}}", "yarnservice-log4j.properties");
        handle_result(status, "log4j.appender.RMSUMMARY.MaxBackupIndex", "{{yarn_rm_summary_log_number_of_backup_files}}", "yarnservice-log4j.properties");
        status = modify_hdfs_config("log4j.appender.RMSUMMARY.layout.ConversionPattern", "%d{ISO8601} %p %c{2}: %m%n", "yarnservice-log4j.properties");
        handle_result(status, "log4j.appender.RMSUMMARY.layout.ConversionPattern", "%d{ISO8601} %p %c{2}: %m%n", "yarnservice-log4j.properties");
        status = modify_hdfs_config("log4j.appender.JSA.layout.ConversionPattern", "%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n", "yarnservice-log4j.properties");
        handle_result(status, "log4j.appender.JSA.layout.ConversionPattern", "%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n", "yarnservice-log4j.properties");
        status = modify_hdfs_config("log4j.logger.org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary", "${yarn.server.resourcemanager.appsummary.logger}", "yarnservice-log4j.properties");
        handle_result(status, "log4j.logger.org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary", "${yarn.server.resourcemanager.appsummary.logger}", "yarnservice-log4j.properties");
        status = modify_hdfs_config("log4j.additivity.org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary", "false", "yarnservice-log4j.properties");
        handle_result(status, "log4j.additivity.org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary", "false", "yarnservice-log4j.properties");
        status = modify_hdfs_config("rm.audit.logger", "${hadoop.root.logger}", "yarnservice-log4j.properties");
        handle_result(status, "rm.audit.logger", "${hadoop.root.logger}", "yarnservice-log4j.properties");
        status = modify_hdfs_config("log4j.logger.org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger", "${rm.audit.logger}", "yarnservice-log4j.properties");
        handle_result(status, "log4j.logger.org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger", "${rm.audit.logger}", "yarnservice-log4j.properties");
        status = modify_hdfs_config("log4j.additivity.org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger", "false", "yarnservice-log4j.properties");
        handle_result(status, "log4j.additivity.org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger", "false", "yarnservice-log4j.properties");
        status = modify_hdfs_config("log4j.appender.RMAUDIT.File", "${yarn.log.dir}/rm-audit.log", "yarnservice-log4j.properties");
        handle_result(status, "log4j.appender.RMAUDIT.File", "${yarn.log.dir}/rm-audit.log", "yarnservice-log4j.properties");
        status = modify_hdfs_config("log4j.appender.RMAUDIT.layout.ConversionPattern", "%d{ISO8601} %p %c{2}: %m%n", "yarnservice-log4j.properties");
        handle_result(status, "log4j.appender.RMAUDIT.layout.ConversionPattern", "%d{ISO8601} %p %c{2}: %m%n", "yarnservice-log4j.properties");
        status = modify_hdfs_config("nm.audit.logger", "${hadoop.root.logger}", "yarnservice-log4j.properties");
        handle_result(status, "nm.audit.logger", "${hadoop.root.logger}", "yarnservice-log4j.properties");
        status = modify_hdfs_config("log4j.logger.org.apache.hadoop.yarn.server.nodemanager.NMAuditLogger", "${nm.audit.logger}", "yarnservice-log4j.properties");
        handle_result(status, "log4j.logger.org.apache.hadoop.yarn.server.nodemanager.NMAuditLogger", "${nm.audit.logger}", "yarnservice-log4j.properties");
        status = modify_hdfs_config("log4j.additivity.org.apache.hadoop.yarn.server.nodemanager.NMAuditLogger", "false", "yarnservice-log4j.properties");
        handle_result(status, "log4j.additivity.org.apache.hadoop.yarn.server.nodemanager.NMAuditLogger", "false", "yarnservice-log4j.properties");
        status = modify_hdfs_config("log4j.appender.NMAUDIT.File", "${yarn.log.dir}/nm-audit.log", "yarnservice-log4j.properties");
        handle_result(status, "log4j.appender.NMAUDIT.File", "${yarn.log.dir}/nm-audit.log", "yarnservice-log4j.properties");
        status = modify_hdfs_config("log4j.appender.NMAUDIT.layout.ConversionPattern", "%d{ISO8601} %p %c{2}: %m%n", "yarnservice-log4j.properties");
        handle_result(status, "log4j.appender.NMAUDIT.layout.ConversionPattern", "%d{ISO8601} %p %c{2}: %m%n", "yarnservice-log4j.properties");
        //			 Update Capacity Scheduler configuration parameters
        status = modify_hdfs_config("yarn.scheduler.capacity.maximum-applications", "10000", "capacity-scheduler.xml");
        handle_result(status, "yarn.scheduler.capacity.maximum-applications", "10000", "capacity-scheduler.xml");
        status = modify_hdfs_config("yarn.scheduler.capacity.maximum-am-resource-percent", "0.2", "capacity-scheduler.xml");
        handle_result(status, "yarn.scheduler.capacity.maximum-am-resource-percent", "0.2", "capacity-scheduler.xml");
        status = modify_hdfs_config("yarn.scheduler.capacity.root.queues", "default", "capacity-scheduler.xml");
        handle_result(status, "yarn.scheduler.capacity.root.queues", "default", "capacity-scheduler.xml");
        status = modify_hdfs_config("yarn.scheduler.capacity.root.capacity", "100", "capacity-scheduler.xml");
        handle_result(status, "yarn.scheduler.capacity.root.capacity", "100", "capacity-scheduler.xml");
        status = modify_hdfs_config("yarn.scheduler.capacity.root.default.capacity", "100", "capacity-scheduler.xml");
        handle_result(status, "yarn.scheduler.capacity.root.default.capacity", "100", "capacity-scheduler.xml");
        status = modify_hdfs_config("yarn.scheduler.capacity.root.default.user-limit-factor", "1", "capacity-scheduler.xml");
        handle_result(status, "yarn.scheduler.capacity.root.default.user-limit-factor", "1", "capacity-scheduler.xml");
        status = modify_hdfs_config("yarn.scheduler.capacity.root.default.maximum-capacity", "100", "capacity-scheduler.xml");
        handle_result(status, "yarn.scheduler.capacity.root.default.maximum-capacity", "100", "capacity-scheduler.xml");
        status = modify_hdfs_config("yarn.scheduler.capacity.root.default.state", "RUNNING", "capacity-scheduler.xml");
        handle_result(status, "yarn.scheduler.capacity.root.default.state", "RUNNING", "capacity-scheduler.xml");
        status = modify_hdfs_config("yarn.scheduler.capacity.root.default.acl_submit_applications", "*", "capacity-scheduler.xml");
        handle_result(status, "yarn.scheduler.capacity.root.default.acl_submit_applications", "*", "capacity-scheduler.xml");
        status = modify_hdfs_config("yarn.scheduler.capacity.root.default.acl_administer_jobs", "*", "capacity-scheduler.xml");
        handle_result(status, "yarn.scheduler.capacity.root.default.acl_administer_jobs", "*", "capacity-scheduler.xml");
        status = modify_hdfs_config("yarn.scheduler.capacity.root.acl_administer_queue", "*", "capacity-scheduler.xml");
        handle_result(status, "yarn.scheduler.capacity.root.acl_administer_queue", "*", "capacity-scheduler.xml");
        status = modify_hdfs_config("yarn.scheduler.capacity.node-locality-delay", "40", "capacity-scheduler.xml");
        handle_result(status, "yarn.scheduler.capacity.node-locality-delay", "40", "capacity-scheduler.xml");
        status = modify_hdfs_config("yarn.scheduler.capacity.default.minimum-user-limit-percent", "100", "capacity-scheduler.xml");
        handle_result(status, "yarn.scheduler.capacity.default.minimum-user-limit-percent", "100", "capacity-scheduler.xml");

        //			 Set container-executor configuration parameters
        status = modify_hdfs_config("docker_module_enabled", "false", "container-executor.cfg");
        handle_result(status, "docker_module_enabled", "false", "container-executor.cfg");
        status = modify_hdfs_config("docker_binary", "/usr/bin/docker", "container-executor.cfg");
        handle_result(status, "docker_binary", "/usr/bin/docker", "container-executor.cfg");
        status = modify_hdfs_config("docker_allowed_devices", "", "container-executor.cfg");
        handle_result(status, "docker_allowed_devices", "", "container-executor.cfg");
        status = modify_hdfs_config("docker_allowed_ro-mounts", "", "container-executor.cfg");
        handle_result(status, "docker_allowed_ro-mounts", "", "container-executor.cfg");
        status = modify_hdfs_config("docker_allowed_rw-mounts", "", "container-executor.cfg");
        handle_result(status, "docker_allowed_rw-mounts", "", "container-executor.cfg");
        status = modify_hdfs_config("docker_allowed_volume-drivers", "", "container-executor.cfg");
        handle_result(status, "docker_allowed_volume-drivers", "", "container-executor.cfg");
        status = modify_hdfs_config("docker_privileged-containers_enabled", "false", "container-executor.cfg");
        handle_result(status, "docker_privileged-containers_enabled", "false", "container-executor.cfg");
        status = modify_hdfs_config("docker_trusted_registries", "", "container-executor.cfg");
        handle_result(status, "docker_trusted_registries", "", "container-executor.cfg");
        status = modify_hdfs_config("min_user_id", "1000", "container-executor.cfg");
        handle_result(status, "min_user_id", "1000", "container-executor.cfg");
        status = modify_hdfs_config("gpu_module_enabled", "false", "container-executor.cfg");
        handle_result(status, "gpu_module_enabled", "false", "container-executor.cfg");
        status = modify_hdfs_config("cgroup_root", "/sys/fs/cgroup", "container-executor.cfg");
        handle_result(status, "cgroup_root", "/sys/fs/cgroup", "container-executor.cfg");
        status = modify_hdfs_config("yarn_hierarchy", "yarn", "container-executor.cfg");
        handle_result(status, "yarn_hierarchy", "yarn", "container-executor.cfg");

        //			 Set template reference (special handling for content property)
        status = modify_hdfs_config("content.property-file-name", "container-executor.cfg.j2", "container-executor.xml");
        handle_result(status, "content.property-file-name", "container-executor.cfg.j2", "container-executor.xml");
        status = modify_hdfs_config("content.property-file-type", "text", "container-executor.xml");
        handle_result(status, "content.property-file-type", "text", "container-executor.xml");
        //			 Update Ranger YARN Audit configuration
        status = modify_hdfs_config("xasecure.audit.is.enabled", "true", "ranger-yarn-audit.xml");
        handle_result(status, "xasecure.audit.is.enabled", "true", "ranger-yarn-audit.xml");
        status = modify_hdfs_config("xasecure.audit.destination.db", "false", "ranger-yarn-audit.xml");
        handle_result(status, "xasecure.audit.destination.db", "false", "ranger-yarn-audit.xml");
        status = modify_hdfs_config("xasecure.audit.destination.db.jdbc.url", "{{audit_jdbc_url}}", "ranger-yarn-audit.xml");
        handle_result(status, "xasecure.audit.destination.db.jdbc.url", "{{audit_jdbc_url}}", "ranger-yarn-audit.xml");
        status = modify_hdfs_config("xasecure.audit.destination.db.user", "{{xa_audit_db_user}}", "ranger-yarn-audit.xml");
        handle_result(status, "xasecure.audit.destination.db.user", "{{xa_audit_db_user}}", "ranger-yarn-audit.xml");
        status = modify_hdfs_config("xasecure.audit.destination.db.password", "crypted", "ranger-yarn-audit.xml");
        handle_result(status, "xasecure.audit.destination.db.password", "crypted", "ranger-yarn-audit.xml");
        status = modify_hdfs_config("xasecure.audit.destination.db.jdbc.driver", "{{jdbc_driver}}", "ranger-yarn-audit.xml");
        handle_result(status, "xasecure.audit.destination.db.jdbc.driver", "{{jdbc_driver}}", "ranger-yarn-audit.xml");
        status = modify_hdfs_config("xasecure.audit.credential.provider.file", "jceks:file{{credential_file}}", "ranger-yarn-audit.xml");
        handle_result(status, "xasecure.audit.credential.provider.file", "jceks:file{{credential_file}}", "ranger-yarn-audit.xml");
        status = modify_hdfs_config("xasecure.audit.destination.db.batch.filespool.dir", "/var/log/hadoop/yarn/audit/db/spool", "ranger-yarn-audit.xml");
        handle_result(status, "xasecure.audit.destination.db.batch.filespool.dir", "/var/log/hadoop/yarn/audit/db/spool", "ranger-yarn-audit.xml");
        status = modify_hdfs_config("xasecure.audit.destination.hdfs", "true", "ranger-yarn-audit.xml");
        handle_result(status, "xasecure.audit.destination.hdfs", "true", "ranger-yarn-audit.xml");
        status = modify_hdfs_config("xasecure.audit.destination.hdfs.dir", "hdfs:NAMENODE_HOSTNAME:8020/ranger/audit", "ranger-yarn-audit.xml");
        handle_result(status, "xasecure.audit.destination.hdfs.dir", "hdfs:NAMENODE_HOSTNAME:8020/ranger/audit", "ranger-yarn-audit.xml");
        status = modify_hdfs_config("xasecure.audit.destination.hdfs.batch.filespool.dir", "/var/log/hadoop/yarn/audit/hdfs/spool", "ranger-yarn-audit.xml");
        handle_result(status, "xasecure.audit.destination.hdfs.batch.filespool.dir", "/var/log/hadoop/yarn/audit/hdfs/spool", "ranger-yarn-audit.xml");
        status = modify_hdfs_config("xasecure.audit.destination.solr", "false", "ranger-yarn-audit.xml");
        handle_result(status, "xasecure.audit.destination.solr", "false", "ranger-yarn-audit.xml");
        status = modify_hdfs_config("xasecure.audit.destination.solr.urls", "", "ranger-yarn-audit.xml");
        handle_result(status, "xasecure.audit.destination.solr.urls", "", "ranger-yarn-audit.xml");
        status = modify_hdfs_config("xasecure.audit.destination.solr.zookeepers", "NONE", "ranger-yarn-audit.xml");
        handle_result(status, "xasecure.audit.destination.solr.zookeepers", "NONE", "ranger-yarn-audit.xml");
        status = modify_hdfs_config("xasecure.audit.destination.solr.batch.filespool.dir", "/var/log/hadoop/yarn/audit/solr/spool", "ranger-yarn-audit.xml");
        handle_result(status, "xasecure.audit.destination.solr.batch.filespool.dir", "/var/log/hadoop/yarn/audit/solr/spool", "ranger-yarn-audit.xml");
        status = modify_hdfs_config("xasecure.audit.provider.summary.enabled", "false", "ranger-yarn-audit.xml");
        handle_result(status, "xasecure.audit.provider.summary.enabled", "false", "ranger-yarn-audit.xml");
        status = modify_hdfs_config("ranger.plugin.yarn.ambari.cluster.name", "{{cluster_name}}", "ranger-yarn-audit.xml");
        handle_result(status, "ranger.plugin.yarn.ambari.cluster.name", "{{cluster_name}}", "ranger-yarn-audit.xml");
        //			 Update Ranger YARN Plugin configuration
        status = modify_hdfs_config("policy_user", "ambari-qa", "ranger-yarn-plugin.properties");
        handle_result(status, "policy_user", "ambari-qa", "ranger-yarn-plugin.properties");
        status = modify_hdfs_config("hadoop.rpc.protection", "", "ranger-yarn-plugin.properties");
        handle_result(status, "hadoop.rpc.protection", "", "ranger-yarn-plugin.properties");
        status = modify_hdfs_config("common.name.for.certificate", "", "ranger-yarn-plugin.properties");
        handle_result(status, "common.name.for.certificate", "", "ranger-yarn-plugin.properties");
        status = modify_hdfs_config("ranger-yarn-plugin-enabled", "No", "ranger-yarn-plugin.properties");
        handle_result(status, "ranger-yarn-plugin-enabled", "No", "ranger-yarn-plugin.properties");
        status = modify_hdfs_config("REPOSITORY_CONFIG_USERNAME", "yarn", "ranger-yarn-plugin.properties");
        handle_result(status, "REPOSITORY_CONFIG_USERNAME", "yarn", "ranger-yarn-plugin.properties");
        status = modify_hdfs_config("REPOSITORY_CONFIG_PASSWORD", "yarn", "ranger-yarn-plugin.properties");
        handle_result(status, "REPOSITORY_CONFIG_PASSWORD", "yarn", "ranger-yarn-plugin.properties");
        status = modify_hdfs_config("external_admin_username", "", "ranger-yarn-plugin.properties");
        handle_result(status, "external_admin_username", "", "ranger-yarn-plugin.properties");
        status = modify_hdfs_config("external_admin_password", "", "ranger-yarn-plugin.properties");
        handle_result(status, "external_admin_password", "", "ranger-yarn-plugin.properties");
        status = modify_hdfs_config("external_ranger_admin_username", "", "ranger-yarn-plugin.properties");
        handle_result(status, "external_ranger_admin_username", "", "ranger-yarn-plugin.properties");
        status = modify_hdfs_config("external_ranger_admin_password", "", "ranger-yarn-plugin.properties");
        handle_result(status, "external_ranger_admin_password", "", "ranger-yarn-plugin.properties");
        //			 Update Ranger YARN SSL Policy Manager configuration
        status = modify_hdfs_config("xasecure.policymgr.clientssl.keystore", "", "ranger-yarn-policymgr-ssl.xml");
        handle_result(status, "xasecure.policymgr.clientssl.keystore", "", "ranger-yarn-policymgr-ssl.xml");
        status = modify_hdfs_config("xasecure.policymgr.clientssl.keystore.password", "", "ranger-yarn-policymgr-ssl.xml");
        handle_result(status, "xasecure.policymgr.clientssl.keystore.password", "", "ranger-yarn-policymgr-ssl.xml");
        status = modify_hdfs_config("xasecure.policymgr.clientssl.truststore", "", "ranger-yarn-policymgr-ssl.xml");
        handle_result(status, "xasecure.policymgr.clientssl.truststore", "", "ranger-yarn-policymgr-ssl.xml");
        status = modify_hdfs_config("xasecure.policymgr.clientssl.truststore.password", "changeit", "ranger-yarn-policymgr-ssl.xml");
        handle_result(status, "xasecure.policymgr.clientssl.truststore.password", "changeit", "ranger-yarn-policymgr-ssl.xml");
        status = modify_hdfs_config("xasecure.policymgr.clientssl.keystore.credential.file", "jceks:file{{credential_file}}", "ranger-yarn-policymgr-ssl.xml");
        handle_result(status, "xasecure.policymgr.clientssl.keystore.credential.file", "jceks:file{{credential_file}}", "ranger-yarn-policymgr-ssl.xml");
        status = modify_hdfs_config("xasecure.policymgr.clientssl.truststore.credential.file", "jceks:file{{credential_file}}", "ranger-yarn-policymgr-ssl.xml");
        handle_result(status, "xasecure.policymgr.clientssl.truststore.credential.file", "jceks:file{{credential_file}}", "ranger-yarn-policymgr-ssl.xml");
        //			 Update Ranger YARN Security configuration
        status = modify_hdfs_config("ranger.plugin.yarn.service.name", "{{repo_name}}", "ranger-yarn-security.xml");
        handle_result(status, "ranger.plugin.yarn.service.name", "{{repo_name}}", "ranger-yarn-security.xml");
        status = modify_hdfs_config("ranger.plugin.yarn.policy.source.impl", "org.apache.ranger.admin.client.RangerAdminRESTClient", "ranger-yarn-security.xml");
        handle_result(status, "ranger.plugin.yarn.policy.source.impl", "org.apache.ranger.admin.client.RangerAdminRESTClient", "ranger-yarn-security.xml");
        status = modify_hdfs_config("ranger.plugin.yarn.policy.rest.url", "{{policymgr_mgr_url}}", "ranger-yarn-security.xml");
        handle_result(status, "ranger.plugin.yarn.policy.rest.url", "{{policymgr_mgr_url}}", "ranger-yarn-security.xml");
        status = modify_hdfs_config("ranger.plugin.yarn.policy.rest.ssl.config.file", "/etc/hadoop/conf/ranger-policymgr-ssl-yarn.xml", "ranger-yarn-security.xml");
        handle_result(status, "ranger.plugin.yarn.policy.rest.ssl.config.file", "/etc/hadoop/conf/ranger-policymgr-ssl-yarn.xml", "ranger-yarn-security.xml");
        status = modify_hdfs_config("ranger.plugin.yarn.policy.pollIntervalMs", "30000", "ranger-yarn-security.xml");
        handle_result(status, "ranger.plugin.yarn.policy.pollIntervalMs", "30000", "ranger-yarn-security.xml");
        status = modify_hdfs_config("ranger.plugin.yarn.policy.cache.dir", "/etc/ranger/{{repo_name}}/policycache", "ranger-yarn-security.xml");
        handle_result(status, "ranger.plugin.yarn.policy.cache.dir", "/etc/ranger/{{repo_name}}/policycache", "ranger-yarn-security.xml");
        status = modify_hdfs_config("ranger.add-yarn-authorization", "true", "ranger-yarn-security.xml");
        handle_result(status, "ranger.add-yarn-authorization", "true", "ranger-yarn-security.xml");
        //			 Update YARN resource types configuration
        status = modify_hdfs_config("yarn.resource-types", "", "resource-types.xml");
        handle_result(status, "yarn.resource-types", "", "resource-types.xml");
        status = modify_hdfs_config("yarn.resource-types.yarn.io_gpu.maximum-allocation", "8", "resource-types.xml");
        handle_result(status, "yarn.resource-types.yarn.io_gpu.maximum-allocation", "8", "resource-types.xml");
        break;

        //			 HBase Configuration
    case HBASE:
        //			 Update simple properties in hbase-site.xml
        status = update_hbase_config("hbase_log_maxfilesize", "256", "hbase-site.xml");
        handle_result(status, "hbase_log_maxfilesize", "256", "hbase-site.xml");
        status = update_hbase_config("hbase_log_maxbackupindex", "20", "hbase-site.xml");
        handle_result(status, "hbase_log_maxbackupindex", "20", "hbase-site.xml");
        status = update_hbase_config("hbase_security_log_maxfilesize", "256", "hbase-site.xml");
        handle_result(status, "hbase_security_log_maxfilesize", "256", "hbase-site.xml");
        status = update_hbase_config("hbase_security_log_maxbackupindex", "20", "hbase-site.xml");
        handle_result(status, "hbase_security_log_maxbackupindex", "20", "hbase-site.xml");

        //			 Process log4j2.properties content line-by-line
        status = update_hbase_config("hbase.root.logger", "INFO,console", "log4j2.properties");
        handle_result(status, "hbase.root.logger", "INFO,console", "log4j2.properties");
        status = update_hbase_config("hbase.security.logger", "INFO,console", "log4j2.properties");
        handle_result(status, "hbase.security.logger", "INFO,console", "log4j2.properties");
        status = update_hbase_config("hbase.log.dir", ".", "log4j2.properties");
        handle_result(status, "hbase.log.dir", ".", "log4j2.properties");
        status = update_hbase_config("hbase.log.file", "hbase.log", "log4j2.properties");
        handle_result(status, "hbase.log.file", "hbase.log", "log4j2.properties");
        status = update_hbase_config("log4j.rootLogger", "${hbase.root.logger}", "log4j2.properties");
        handle_result(status, "log4j.rootLogger", "${hbase.root.logger}", "log4j2.properties");
        status = update_hbase_config("log4j.threshold", "ALL", "log4j2.properties");
        handle_result(status, "log4j.threshold", "ALL", "log4j2.properties");
        status = update_hbase_config("log4j.appender.DRFA", "org.apache.log4j.DailyRollingFileAppender", "log4j2.properties");
        handle_result(status, "log4j.appender.DRFA", "org.apache.log4j.DailyRollingFileAppender", "log4j2.properties");
        status = update_hbase_config("log4j.appender.DRFA.File", "${hbase.log.dir}/${hbase.log.file}", "log4j2.properties");
        handle_result(status, "log4j.appender.DRFA.File", "${hbase.log.dir}/${hbase.log.file}", "log4j2.properties");
        status = update_hbase_config("log4j.appender.DRFA.DatePattern", ".yyyy-MM-dd", "log4j2.properties");
        handle_result(status, "log4j.appender.DRFA.DatePattern", ".yyyy-MM-dd", "log4j2.properties");
        status = update_hbase_config("log4j.appender.DRFA.layout", "org.apache.log4j.PatternLayout", "log4j2.properties");
        handle_result(status, "log4j.appender.DRFA.layout", "org.apache.log4j.PatternLayout", "log4j2.properties");
        status = update_hbase_config("log4j.appender.DRFA.layout.ConversionPattern", "%d{ISO8601} %-5p [%t] %c{2}: %m%n", "log4j2.properties");
        handle_result(status, "log4j.appender.DRFA.layout.ConversionPattern", "%d{ISO8601} %-5p [%t] %c{2}: %m%n", "log4j2.properties");
        status = update_hbase_config("hbase.log.maxfilesize", "256MB", "log4j2.properties");
        handle_result(status, "hbase.log.maxfilesize", "256MB", "log4j2.properties");
        status = update_hbase_config("hbase.log.maxbackupindex", "20", "log4j2.properties");
        handle_result(status, "hbase.log.maxbackupindex", "20", "log4j2.properties");
        status = update_hbase_config("log4j.appender.RFA", "org.apache.log4j.RollingFileAppender", "log4j2.properties");
        handle_result(status, "log4j.appender.RFA", "org.apache.log4j.RollingFileAppender", "log4j2.properties");
        status = update_hbase_config("log4j.appender.RFA.File", "${hbase.log.dir}/${hbase.log.file}", "log4j2.properties");
        handle_result(status, "log4j.appender.RFA.File", "${hbase.log.dir}/${hbase.log.file}", "log4j2.properties");
        status = update_hbase_config("log4j.appender.RFA.MaxFileSize", "${hbase.log.maxfilesize}", "log4j2.properties");
        handle_result(status, "log4j.appender.RFA.MaxFileSize", "${hbase.log.maxfilesize}", "log4j2.properties");
        status = update_hbase_config("log4j.appender.RFA.MaxBackupIndex", "${hbase.log.maxbackupindex}", "log4j2.properties");
        handle_result(status, "log4j.appender.RFA.MaxBackupIndex", "${hbase.log.maxbackupindex}", "log4j2.properties");
        status = update_hbase_config("log4j.appender.RFA.layout", "org.apache.log4j.PatternLayout", "log4j2.properties");
        handle_result(status, "log4j.appender.RFA.layout", "org.apache.log4j.PatternLayout", "log4j2.properties");
        status = update_hbase_config("log4j.appender.RFA.layout.ConversionPattern", "%d{ISO8601} %-5p [%t] %c{2}: %m%n", "log4j2.properties");
        handle_result(status, "log4j.appender.RFA.layout.ConversionPattern", "%d{ISO8601} %-5p [%t] %c{2}: %m%n", "log4j2.properties");
        status = update_hbase_config("hbase.security.log.file", "SecurityAuth.audit", "log4j2.properties");
        handle_result(status, "hbase.security.log.file", "SecurityAuth.audit", "log4j2.properties");
        status = update_hbase_config("hbase.security.log.maxfilesize", "256MB", "log4j2.properties");
        handle_result(status, "hbase.security.log.maxfilesize", "256MB", "log4j2.properties");
        status = update_hbase_config("hbase.security.log.maxbackupindex", "20", "log4j2.properties");
        handle_result(status, "hbase.security.log.maxbackupindex", "20", "log4j2.properties");
        status = update_hbase_config("log4j.appender.RFAS", "org.apache.log4j.RollingFileAppender", "log4j2.properties");
        handle_result(status, "log4j.appender.RFAS", "org.apache.log4j.RollingFileAppender", "log4j2.properties");
        status = update_hbase_config("log4j.appender.RFAS.File", "${hbase.log.dir}/${hbase.security.log.file}", "log4j2.properties");
        handle_result(status, "log4j.appender.RFAS.File", "${hbase.log.dir}/${hbase.security.log.file}", "log4j2.properties");
        status = update_hbase_config("log4j.appender.RFAS.MaxFileSize", "${hbase.security.log.maxfilesize}", "log4j2.properties");
        handle_result(status, "log4j.appender.RFAS.MaxFileSize", "${hbase.security.log.maxfilesize}", "log4j2.properties");
        status = update_hbase_config("log4j.appender.RFAS.MaxBackupIndex", "${hbase.security.log.maxbackupindex}", "log4j2.properties");
        handle_result(status, "log4j.appender.RFAS.MaxBackupIndex", "${hbase.security.log.maxbackupindex}", "log4j2.properties");
        status = update_hbase_config("log4j.appender.RFAS.layout", "org.apache.log4j.PatternLayout", "log4j2.properties");
        handle_result(status, "log4j.appender.RFAS.layout", "org.apache.log4j.PatternLayout", "log4j2.properties");
        status = update_hbase_config("log4j.appender.RFAS.layout.ConversionPattern", "%d{ISO8601} %p %c: %m%n", "log4j2.properties");
        handle_result(status, "log4j.appender.RFAS.layout.ConversionPattern", "%d{ISO8601} %p %c: %m%n", "log4j2.properties");
        status = update_hbase_config("log4j.category.SecurityLogger", "${hbase.security.logger}", "log4j2.properties");
        handle_result(status, "log4j.category.SecurityLogger", "${hbase.security.logger}", "log4j2.properties");
        status = update_hbase_config("log4j.additivity.SecurityLogger", "false", "log4j2.properties");
        handle_result(status, "log4j.additivity.SecurityLogger", "false", "log4j2.properties");
        status = update_hbase_config("log4j.appender.NullAppender", "org.apache.log4j.varia.NullAppender", "log4j2.properties");
        handle_result(status, "log4j.appender.NullAppender", "org.apache.log4j.varia.NullAppender", "log4j2.properties");
        status = update_hbase_config("log4j.appender.console", "org.apache.log4j.ConsoleAppender", "log4j2.properties");
        handle_result(status, "log4j.appender.console", "org.apache.log4j.ConsoleAppender", "log4j2.properties");
        status = update_hbase_config("log4j.appender.console.target", "System.err", "log4j2.properties");
        handle_result(status, "log4j.appender.console.target", "System.err", "log4j2.properties");
        status = update_hbase_config("log4j.appender.console.layout", "org.apache.log4j.PatternLayout", "log4j2.properties");
        handle_result(status, "log4j.appender.console.layout", "org.apache.log4j.PatternLayout", "log4j2.properties");
        status = update_hbase_config("log4j.appender.console.layout.ConversionPattern", "%d{ISO8601} %-5p [%t] %c{2}: %m%n", "log4j2.properties");
        handle_result(status, "log4j.appender.console.layout.ConversionPattern", "%d{ISO8601} %-5p [%t] %c{2}: %m%n", "log4j2.properties");
        status = update_hbase_config("log4j.logger.org.apache.zookeeper", "ERROR", "log4j2.properties");
        handle_result(status, "log4j.logger.org.apache.zookeeper", "ERROR", "log4j2.properties");
        status = update_hbase_config("log4j.logger.org.apache.hadoop.hbase", "ERROR", "log4j2.properties");
        handle_result(status, "log4j.logger.org.apache.hadoop.hbase", "ERROR", "log4j2.properties");
        status = update_hbase_config("log4j.logger.org.apache.hadoop.hbase.zookeeper.ZKUtil", "INFO", "log4j2.properties");
        handle_result(status, "log4j.logger.org.apache.hadoop.hbase.zookeeper.ZKUtil", "INFO", "log4j2.properties");
        status = update_hbase_config("log4j.logger.org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher", "INFO", "log4j2.properties");
        handle_result(status, "log4j.logger.org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher", "INFO", "log4j2.properties");
        // Update HBase policy configuration parameters (to hbase-policy.xml)
        status = update_hbase_config("security.client.protocol.acl", "*", "hbase-policy.xml");
        handle_result(status, "security.client.protocol.acl", "*", "hbase-policy.xml");
        status = update_hbase_config("security.admin.protocol.acl", "*", "hbase-policy.xml");
        handle_result(status, "security.admin.protocol.acl", "*", "hbase-policy.xml");
        status = update_hbase_config("security.masterregion.protocol.acl", "*", "hbase-policy.xml");
        handle_result(status, "security.masterregion.protocol.acl", "*", "hbase-policy.xml");
        //			 Core HBase Configuration
        status = update_hbase_config("hbase.rootdir", "/apps/hbase/data", "hbase-site.xml");
        handle_result(status, "hbase.rootdir", "/apps/hbase/data", "hbase-site.xml");
        status = update_hbase_config("hbase.cluster.distributed", "true", "hbase-site.xml");
        handle_result(status, "hbase.cluster.distributed", "true", "hbase-site.xml");
        status = update_hbase_config("hbase.master.port", "16000", "hbase-site.xml");
        handle_result(status, "hbase.master.port", "16000", "hbase-site.xml");
        status = update_hbase_config("hbase.tmp.dir", "/tmp/hbase-${user.name}", "hbase-site.xml");
        handle_result(status, "hbase.tmp.dir", "/tmp/hbase-${user.name}", "hbase-site.xml");
        status = update_hbase_config("hbase.local.dir", "${hbase.tmp.dir}/local", "hbase-site.xml");
        handle_result(status, "hbase.local.dir", "${hbase.tmp.dir}/local", "hbase-site.xml");
        status = update_hbase_config("hbase.master.info.bindAddress", "0.0.0.0", "hbase-site.xml");
        handle_result(status, "hbase.master.info.bindAddress", "0.0.0.0", "hbase-site.xml");
        status = update_hbase_config("hbase.master.info.port", "16010", "hbase-site.xml");
        handle_result(status, "hbase.master.info.port", "16010", "hbase-site.xml");
        status = update_hbase_config("hbase.regionserver.info.port", "16030", "hbase-site.xml");
        handle_result(status, "hbase.regionserver.info.port", "16030", "hbase-site.xml");
        status = update_hbase_config("hbase.regionserver.handler.count", "30", "hbase-site.xml");
        handle_result(status, "hbase.regionserver.handler.count", "30", "hbase-site.xml");
        status = update_hbase_config("phoenix.rpc.index.handler.count", "10", "hbase-site.xml");
        handle_result(status, "phoenix.rpc.index.handler.count", "10", "hbase-site.xml");

        //			 Compaction and Memory Settings
        status = update_hbase_config("hbase.hregion.majorcompaction", "604800000", "hbase-site.xml");
        handle_result(status, "hbase.hregion.majorcompaction", "604800000", "hbase-site.xml");
        status = update_hbase_config("hbase.hregion.majorcompaction.jitter", "0.50", "hbase-site.xml");
        handle_result(status, "hbase.hregion.majorcompaction.jitter", "0.50", "hbase-site.xml");
        status = update_hbase_config("hbase.hregion.memstore.block.multiplier", "4", "hbase-site.xml");
        handle_result(status, "hbase.hregion.memstore.block.multiplier", "4", "hbase-site.xml");
        status = update_hbase_config("hbase.hregion.memstore.flush.size", "134217728", "hbase-site.xml");
        handle_result(status, "hbase.hregion.memstore.flush.size", "134217728", "hbase-site.xml");
        status = update_hbase_config("hbase.hregion.memstore.mslab.enabled", "true", "hbase-site.xml");
        handle_result(status, "hbase.hregion.memstore.mslab.enabled", "true", "hbase-site.xml");
        status = update_hbase_config("hbase.hregion.max.filesize", "10737418240", "hbase-site.xml");
        handle_result(status, "hbase.hregion.max.filesize", "10737418240", "hbase-site.xml");
        status = update_hbase_config("hbase.regionserver.global.memstore.size", "0.4", "hbase-site.xml");
        handle_result(status, "hbase.regionserver.global.memstore.size", "0.4", "hbase-site.xml");
        status = update_hbase_config("hfile.block.cache.size", "0.40", "hbase-site.xml");
        handle_result(status, "hfile.block.cache.size", "0.40", "hbase-site.xml");

        //Client and Scanner Settings
        status = update_hbase_config("hbase.client.scanner.caching", "100", "hbase-site.xml");
        handle_result(status, "hbase.client.scanner.caching", "100", "hbase-site.xml");
        status = update_hbase_config("hbase.client.retries.number", "35", "hbase-site.xml");
        handle_result(status, "hbase.client.retries.number", "35", "hbase-site.xml");
        status = update_hbase_config("hbase.rpc.timeout", "90000", "hbase-site.xml");
        handle_result(status, "hbase.rpc.timeout", "90000", "hbase-site.xml");
        status = update_hbase_config("hbase.client.keyvalue.maxsize", "1048576", "hbase-site.xml");
        handle_result(status, "hbase.client.keyvalue.maxsize", "1048576", "hbase-site.xml");

        // ZooKeeper Configuration
        status = update_hbase_config("zookeeper.session.timeout", "90000", "hbase-site.xml");
        handle_result(status, "zookeeper.session.timeout", "90000", "hbase-site.xml");
        status = update_hbase_config("hbase.zookeeper.property.clientPort", "2181", "hbase-site.xml");
        handle_result(status, "hbase.zookeeper.property.clientPort", "2181", "hbase-site.xml");
        status = update_hbase_config("hbase.zookeeper.quorum", "localhost", "hbase-site.xml");
        handle_result(status, "hbase.zookeeper.quorum", "localhost", "hbase-site.xml");
        status = update_hbase_config("hbase.zookeeper.useMulti", "true", "hbase-site.xml");
        handle_result(status, "hbase.zookeeper.useMulti", "true", "hbase-site.xml");
        status = update_hbase_config("zookeeper.znode.parent", "/hbase-unsecure", "hbase-site.xml");
        handle_result(status, "zookeeper.znode.parent", "/hbase-unsecure", "hbase-site.xml");
        status = update_hbase_config("zookeeper.recovery.retry", "6", "hbase-site.xml");
        handle_result(status, "zookeeper.recovery.retry", "6", "hbase-site.xml");

        //Security Configuration
        status = update_hbase_config("hbase.superuser", "hbase", "hbase-site.xml");
        handle_result(status, "hbase.superuser", "hbase", "hbase-site.xml");
        status = update_hbase_config("hbase.security.authentication", "simple", "hbase-site.xml");
        handle_result(status, "hbase.security.authentication", "simple", "hbase-site.xml");
        status = update_hbase_config("hbase.security.authorization", "false", "hbase-site.xml");
        handle_result(status, "hbase.security.authorization", "false", "hbase-site.xml");
        status = update_hbase_config("hbase.rpc.protection", "authentication", "hbase-site.xml");
        handle_result(status, "hbase.rpc.protection", "authentication", "hbase-site.xml");

        //Coprocessor Configuration
        status = update_hbase_config("hbase.coprocessor.region.classes", "org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint", "hbase-site.xml");
        handle_result(status, "hbase.coprocessor.region.classes", "org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint", "hbase-site.xml");
        status = update_hbase_config("hbase.coprocessor.master.classes", "", "hbase-site.xml");
        handle_result(status, "hbase.coprocessor.master.classes", "", "hbase-site.xml");
        status = update_hbase_config("hbase.coprocessor.regionserver.classes", "", "hbase-site.xml");
        handle_result(status, "hbase.coprocessor.regionserver.classes", "", "hbase-site.xml");

        //RegionServer Configuration
        status = update_hbase_config("hbase.regionserver.port", "16020", "hbase-site.xml");
        handle_result(status, "hbase.regionserver.port", "16020", "hbase-site.xml");
        status = update_hbase_config("hbase.regionserver.executor.openregion.threads", "20", "hbase-site.xml");
        handle_result(status, "hbase.regionserver.executor.openregion.threads", "20", "hbase-site.xml");
        status = update_hbase_config("hbase.regionserver.wal.codec", "org.apache.hadoop.hbase.regionserver.wal.WALCellCodec", "hbase-site.xml");
        handle_result(status, "hbase.regionserver.wal.codec", "org.apache.hadoop.hbase.regionserver.wal.WALCellCodec", "hbase-site.xml");

        // Master Configuration
        status = update_hbase_config("hbase.master.namespace.init.timeout", "2400000", "hbase-site.xml");
        handle_result(status, "hbase.master.namespace.init.timeout", "2400000", "hbase-site.xml");
        status = update_hbase_config("hbase.master.wait.on.regionservers.timeout", "30000", "hbase-site.xml");
        handle_result(status, "hbase.master.wait.on.regionservers.timeout", "30000", "hbase-site.xml");
        status = update_hbase_config("hbase.master.ui.readonly", "false", "hbase-site.xml");
        handle_result(status, "hbase.master.ui.readonly", "false", "hbase-site.xml");

        //			 Store and Compaction Settings
        status = update_hbase_config("hbase.hstore.compactionThreshold", "3", "hbase-site.xml");
        handle_result(status, "hbase.hstore.compactionThreshold", "3", "hbase-site.xml");
        status = update_hbase_config("hbase.hstore.compaction.max", "10", "hbase-site.xml");
        handle_result(status, "hbase.hstore.compaction.max", "10", "hbase-site.xml");
        status = update_hbase_config("hbase.hstore.blockingStoreFiles", "100", "hbase-site.xml");
        handle_result(status, "hbase.hstore.blockingStoreFiles", "100", "hbase-site.xml");

        //			 Phoenix Configuration
        status = update_hbase_config("phoenix.query.timeoutMs", "60000", "hbase-site.xml");
        handle_result(status, "phoenix.query.timeoutMs", "60000", "hbase-site.xml");
        status = update_hbase_config("phoenix.functions.allowUserDefinedFunctions", " ", "hbase-site.xml");
        handle_result(status, "phoenix.functions.allowUserDefinedFunctions", " ", "hbase-site.xml");

        //			 Bucket Cache Configuration
        status = update_hbase_config("hbase.bucketcache.ioengine", "", "hbase-site.xml");
        handle_result(status, "hbase.bucketcache.ioengine", "", "hbase-site.xml");
        status = update_hbase_config("hbase.bucketcache.size", "", "hbase-site.xml");
        handle_result(status, "hbase.bucketcache.size", "", "hbase-site.xml");
        status = update_hbase_config("hbase.bucketcache.percentage.in.combinedcache", "", "hbase-site.xml");
        handle_result(status, "hbase.bucketcache.percentage.in.combinedcache", "", "hbase-site.xml");

        //			 RPC Configuration
        status = update_hbase_config("hbase.region.server.rpc.scheduler.factory.class", "", "hbase-site.xml");
        handle_result(status, "hbase.region.server.rpc.scheduler.factory.class", "", "hbase-site.xml");
        status = update_hbase_config("hbase.rpc.controllerfactory.class", "", "hbase-site.xml");
        handle_result(status, "hbase.rpc.controllerfactory.class", "", "hbase-site.xml");

        //			 Miscellaneous
        status = update_hbase_config("hbase.defaults.for.version.skip", "true", "hbase-site.xml");
        handle_result(status, "hbase.defaults.for.version.skip", "true", "hbase-site.xml");
        status = update_hbase_config("dfs.domain.socket.path", "/var/lib/hadoop-hdfs/dn_socket", "hbase-site.xml");
        handle_result(status, "dfs.domain.socket.path", "/var/lib/hadoop-hdfs/dn_socket", "hbase-site.xml");
        status = update_hbase_config("hbase.bulkload.staging.dir", "/apps/hbase/staging", "hbase-site.xml");
        handle_result(status, "hbase.bulkload.staging.dir", "/apps/hbase/staging", "hbase-site.xml");

        //			 Ranger HBase Audit Configuration
        status = update_hbase_config("xasecure.audit.is.enabled", "true", "ranger-hbase-audit.xml");
        handle_result(status, "xasecure.audit.is.enabled", "true", "ranger-hbase-audit.xml");
        status = update_hbase_config("xasecure.audit.destination.hdfs", "true", "ranger-hbase-audit.xml");
        handle_result(status, "xasecure.audit.destination.hdfs", "true", "ranger-hbase-audit.xml");
        status = update_hbase_config("xasecure.audit.destination.hdfs.dir", "hdfs:NAMENODE_HOSTNAME:8020/ranger/audit", "ranger-hbase-audit.xml");
        handle_result(status, "xasecure.audit.destination.hdfs.dir", "hdfs:NAMENODE_HOSTNAME:8020/ranger/audit", "ranger-hbase-audit.xml");
        status = update_hbase_config("xasecure.audit.destination.hdfs.batch.filespool.dir", "/var/log/hbase/audit/hdfs/spool", "ranger-hbase-audit.xml");
        handle_result(status, "xasecure.audit.destination.hdfs.batch.filespool.dir", "/var/log/hbase/audit/hdfs/spool", "ranger-hbase-audit.xml");
        status = update_hbase_config("xasecure.audit.destination.solr", "false", "ranger-hbase-audit.xml");
        handle_result(status, "xasecure.audit.destination.solr", "false", "ranger-hbase-audit.xml");
        status = update_hbase_config("xasecure.audit.destination.solr.urls", "", "ranger-hbase-audit.xml");
        handle_result(status, "xasecure.audit.destination.solr.urls", "", "ranger-hbase-audit.xml");
        status = update_hbase_config("xasecure.audit.destination.solr.zookeepers", "NONE", "ranger-hbase-audit.xml");
        handle_result(status, "xasecure.audit.destination.solr.zookeepers", "NONE", "ranger-hbase-audit.xml");
        status = update_hbase_config("xasecure.audit.destination.solr.batch.filespool.dir", "/var/log/hbase/audit/solr/spool", "ranger-hbase-audit.xml");
        handle_result(status, "xasecure.audit.destination.solr.batch.filespool.dir", "/var/log/hbase/audit/solr/spool", "ranger-hbase-audit.xml");
        status = update_hbase_config("xasecure.audit.provider.summary.enabled", "true", "ranger-hbase-audit.xml");
        handle_result(status, "xasecure.audit.provider.summary.enabled", "true", "ranger-hbase-audit.xml");
        status = update_hbase_config("ranger.plugin.hbase.ambari.cluster.name", "{{cluster_name}}", "ranger-hbase-audit.xml");
        handle_result(status, "ranger.plugin.hbase.ambari.cluster.name", "{{cluster_name}}", "ranger-hbase-audit.xml");
        //			 Ranger HBase Plugin Configuration
        status = update_hbase_config("common.name.for.certificate", "", "ranger-hbase-plugin.properties");
        handle_result(status, "common.name.for.certificate", "", "ranger-hbase-plugin.properties");
        status = update_hbase_config("policy_user", "ambari-qa", "ranger-hbase-plugin.properties");
        handle_result(status, "policy_user", "ambari-qa", "ranger-hbase-plugin.properties");
        status = update_hbase_config("ranger-hbase-plugin-enabled", "No", "ranger-hbase-plugin.properties");
        handle_result(status, "ranger-hbase-plugin-enabled", "No", "ranger-hbase-plugin.properties");
        status = update_hbase_config("REPOSITORY_CONFIG_USERNAME", "hbase", "ranger-hbase-plugin.properties");
        handle_result(status, "REPOSITORY_CONFIG_USERNAME", "hbase", "ranger-hbase-plugin.properties");
        status = update_hbase_config("REPOSITORY_CONFIG_PASSWORD", "hbase", "ranger-hbase-plugin.properties");
        handle_result(status, "REPOSITORY_CONFIG_PASSWORD", "hbase", "ranger-hbase-plugin.properties");

        //			 External Ranger Admin Configuration
        status = update_hbase_config("external_admin_username", "", "ranger-hbase-plugin.properties");
        handle_result(status, "external_admin_username", "", "ranger-hbase-plugin.properties");
        status = update_hbase_config("external_admin_password", "", "ranger-hbase-plugin.properties");
        handle_result(status, "external_admin_password", "", "ranger-hbase-plugin.properties");
        status = update_hbase_config("external_ranger_admin_username", "", "ranger-hbase-plugin.properties");
        handle_result(status, "external_ranger_admin_username", "", "ranger-hbase-plugin.properties");
        status = update_hbase_config("external_ranger_admin_password", "", "ranger-hbase-plugin.properties");
        handle_result(status, "external_ranger_admin_password", "", "ranger-hbase-plugin.properties");
        //			 Ranger Policy Manager SSL Configuration
        status = update_hbase_config("xasecure.policymgr.clientssl.keystore", "", "ranger-hbase-policymgr-ssl.xml");
        handle_result(status, "xasecure.policymgr.clientssl.keystore", "", "ranger-hbase-policymgr-ssl.xml");
        status = update_hbase_config("xasecure.policymgr.clientssl.keystore.password", "", "ranger-hbase-policymgr-ssl.xml");
        handle_result(status, "xasecure.policymgr.clientssl.keystore.password", "", "ranger-hbase-policymgr-ssl.xml");
        status = update_hbase_config("xasecure.policymgr.clientssl.truststore", "", "ranger-hbase-policymgr-ssl.xml");
        handle_result(status, "xasecure.policymgr.clientssl.truststore", "", "ranger-hbase-policymgr-ssl.xml");
        status = update_hbase_config("xasecure.policymgr.clientssl.truststore.password", "changeit", "ranger-hbase-policymgr-ssl.xml");
        handle_result(status, "xasecure.policymgr.clientssl.truststore.password", "changeit", "ranger-hbase-policymgr-ssl.xml");
        status = update_hbase_config("xasecure.policymgr.clientssl.keystore.credential.file", "jceks:file{{credential_file}}", "ranger-hbase-policymgr-ssl.xml");
        handle_result(status, "xasecure.policymgr.clientssl.keystore.credential.file", "jceks:file{{credential_file}}", "ranger-hbase-policymgr-ssl.xml");
        status = update_hbase_config("xasecure.policymgr.clientssl.truststore.credential.file", "jceks:file{{credential_file}}", "ranger-hbase-policymgr-ssl.xml");
        handle_result(status, "xasecure.policymgr.clientssl.truststore.credential.file", "jceks:file{{credential_file}}", "ranger-hbase-policymgr-ssl.xml");
        //			 Ranger HBase Security Configuration
        status = update_hbase_config("ranger.plugin.hbase.service.name", "{{repo_name}}", "ranger-hbase-security.xml");
        handle_result(status, "ranger.plugin.hbase.service.name", "{{repo_name}}", "ranger-hbase-security.xml");
        status = update_hbase_config("ranger.plugin.hbase.policy.source.impl", "org.apache.ranger.admin.client.RangerAdminRESTClient", "ranger-hbase-security.xml");
        handle_result(status, "ranger.plugin.hbase.policy.source.impl", "org.apache.ranger.admin.client.RangerAdminRESTClient", "ranger-hbase-security.xml");
        status = update_hbase_config("ranger.plugin.hbase.policy.rest.url", "{{policymgr_mgr_url}}", "ranger-hbase-security.xml");
        handle_result(status, "ranger.plugin.hbase.policy.rest.url", "{{policymgr_mgr_url}}", "ranger-hbase-security.xml");
        status = update_hbase_config("ranger.plugin.hbase.policy.rest.ssl.config.file", "/etc/hbase/conf/ranger-policymgr-ssl.xml", "ranger-hbase-security.xml");
        handle_result(status, "ranger.plugin.hbase.policy.rest.ssl.config.file", "/etc/hbase/conf/ranger-policymgr-ssl.xml", "ranger-hbase-security.xml");
        status = update_hbase_config("ranger.plugin.hbase.policy.pollIntervalMs", "30000", "ranger-hbase-security.xml");
        handle_result(status, "ranger.plugin.hbase.policy.pollIntervalMs", "30000", "ranger-hbase-security.xml");
        status = update_hbase_config("ranger.plugin.hbase.policy.cache.dir", "/etc/ranger/{{repo_name}}/policycache", "ranger-hbase-security.xml");
        handle_result(status, "ranger.plugin.hbase.policy.cache.dir", "/etc/ranger/{{repo_name}}/policycache", "ranger-hbase-security.xml");
        status = update_hbase_config("xasecure.hbase.update.xapolicies.on.grant.revoke", "true", "ranger-hbase-security.xml");
        handle_result(status, "xasecure.hbase.update.xapolicies.on.grant.revoke", "true", "ranger-hbase-security.xml");

        break;

        //			 Hive Configuration
    case HIVE:

        status = modify_hive_config("status", "INFO", "beeline-log4j2.properties");
        handle_result(status, "status", "INFO", "beeline-log4j2.properties");
        status = modify_hive_config("name", "BeelineLog4j2", "beeline-log4j2.properties");
        handle_result(status, "name", "BeelineLog4j2", "beeline-log4j2.properties");
        status = modify_hive_config("packages", "org.apache.hadoop.hive.ql.log", "beeline-log4j2.properties");
        handle_result(status, "packages", "org.apache.hadoop.hive.ql.log", "beeline-log4j2.properties");
        status = modify_hive_config("property.hive.log.level", "{{hive_log_level}}", "beeline-log4j2.properties");
        handle_result(status, "property.hive.log.level", "{{hive_log_level}}", "beeline-log4j2.properties");
        status = modify_hive_config("property.hive.root.logger", "console", "beeline-log4j2.properties");
        handle_result(status, "property.hive.root.logger", "console", "beeline-log4j2.properties");
        status = modify_hive_config("appenders", "console", "beeline-log4j2.properties");
        handle_result(status, "appenders", "console", "beeline-log4j2.properties");
        status = modify_hive_config("appender.console.type", "Console", "beeline-log4j2.properties");
        handle_result(status, "appender.console.type", "Console", "beeline-log4j2.properties");
        status = modify_hive_config("appender.console.name", "console", "beeline-log4j2.properties");
        handle_result(status, "appender.console.name", "console", "beeline-log4j2.properties");
        status = modify_hive_config("appender.console.target", "SYSTEM_ERR", "beeline-log4j2.properties");
        handle_result(status, "appender.console.target", "SYSTEM_ERR", "beeline-log4j2.properties");
        status = modify_hive_config("appender.console.layout.type", "PatternLayout", "beeline-log4j2.properties");
        handle_result(status, "appender.console.layout.type", "PatternLayout", "beeline-log4j2.properties");
        status = modify_hive_config("appender.console.layout.pattern", "%d{yy/MM/dd HH:mm:ss} [%t]: %p %c{2}: %m%n", "beeline-log4j2.properties");
        handle_result(status, "appender.console.layout.pattern", "%d{yy/MM/dd HH:mm:ss} [%t]: %p %c{2}: %m%n", "beeline-log4j2.properties");
        status = modify_hive_config("loggers", "HiveConnection", "beeline-log4j2.properties");
        handle_result(status, "loggers", "HiveConnection", "beeline-log4j2.properties");
        status = modify_hive_config("logger.HiveConnection.name", "org.apache.hive.jdbc.HiveConnection", "beeline-log4j2.properties");
        handle_result(status, "logger.HiveConnection.name", "org.apache.hive.jdbc.HiveConnection", "beeline-log4j2.properties");
        status = modify_hive_config("logger.HiveConnection.level", "INFO", "beeline-log4j2.properties");
        handle_result(status, "logger.HiveConnection.level", "INFO", "beeline-log4j2.properties");
        status = modify_hive_config("rootLogger.level", "${sys:hive.log.level}", "beeline-log4j2.properties");
        handle_result(status, "rootLogger.level", "${sys:hive.log.level}", "beeline-log4j2.properties");
        status = modify_hive_config("rootLogger.appenderRefs", "root", "beeline-log4j2.properties");
        handle_result(status, "rootLogger.appenderRefs", "root", "beeline-log4j2.properties");
        status = modify_hive_config("rootLogger.appenderRef.root.ref", "${sys:hive.root.logger}", "beeline-log4j2.properties");
        handle_result(status, "rootLogger.appenderRef.root.ref", "${sys:hive.root.logger}", "beeline-log4j2.properties");
        //			 Atlas Hive Hook Configuration
        status = modify_hive_config("atlas.hook.hive.synchronous", "false", "atlas-application.properties");
        handle_result(status, "atlas.hook.hive.synchronous", "false", "atlas-application.properties");
        status = modify_hive_config("atlas.hook.hive.numRetries", "3", "atlas-application.properties");
        handle_result(status, "atlas.hook.hive.numRetries", "3", "atlas-application.properties");
        status = modify_hive_config("atlas.hook.hive.minThreads", "5", "atlas-application.properties");
        handle_result(status, "atlas.hook.hive.minThreads", "5", "atlas-application.properties");
        status = modify_hive_config("atlas.hook.hive.maxThreads", "5", "atlas-application.properties");
        handle_result(status, "atlas.hook.hive.maxThreads", "5", "atlas-application.properties");
        status = modify_hive_config("atlas.hook.hive.keepAliveTime", "10", "atlas-application.properties");
        handle_result(status, "atlas.hook.hive.keepAliveTime", "10", "atlas-application.properties");
        status = modify_hive_config("atlas.hook.hive.queueSize", "1000", "atlas-application.properties");
        handle_result(status, "atlas.hook.hive.queueSize", "1000", "atlas-application.properties");
        //			 Root configuration
        status = modify_hive_config("status", "INFO", "hive-exec-log4j2.properties");
        handle_result(status, "status", "INFO", "hive-exec-log4j2.properties");
        status = modify_hive_config("name", "HiveExecLog4j2", "hive-exec-log4j2.properties");
        handle_result(status, "name", "HiveExecLog4j2", "hive-exec-log4j2.properties");
        status = modify_hive_config("packages", "org.apache.hadoop.hive.ql.log", "hive-exec-log4j2.properties");
        handle_result(status, "packages", "org.apache.hadoop.hive.ql.log", "hive-exec-log4j2.properties");

        //			 Property definitions
        status = modify_hive_config("property.hive.log.level", "{{hive_log_level}}", "hive-exec-log4j2.properties");
        handle_result(status, "property.hive.log.level", "{{hive_log_level}}", "hive-exec-log4j2.properties");
        status = modify_hive_config("property.hive.root.logger", "FA", "hive-exec-log4j2.properties");
        handle_result(status, "property.hive.root.logger", "FA", "hive-exec-log4j2.properties");
        status = modify_hive_config("property.hive.query.id", "hadoop", "hive-exec-log4j2.properties");
        handle_result(status, "property.hive.query.id", "hadoop", "hive-exec-log4j2.properties");
        status = modify_hive_config("property.hive.log.dir", "${sys:java.io.tmpdir}/${sys:user.name}", "hive-exec-log4j2.properties");
        handle_result(status, "property.hive.log.dir", "${sys:java.io.tmpdir}/${sys:user.name}", "hive-exec-log4j2.properties");
        status = modify_hive_config("property.hive.log.file", "${sys:hive.query.id}.log", "hive-exec-log4j2.properties");
        handle_result(status, "property.hive.log.file", "${sys:hive.query.id}.log", "hive-exec-log4j2.properties");

        //			 Appender declarations
        status = modify_hive_config("appenders", "console, FA", "hive-exec-log4j2.properties");
        handle_result(status, "appenders", "console, FA", "hive-exec-log4j2.properties");

        //			 Console appender configuration
        status = modify_hive_config("appender.console.type", "Console", "hive-exec-log4j2.properties");
        handle_result(status, "appender.console.type", "Console", "hive-exec-log4j2.properties");
        status = modify_hive_config("appender.console.name", "console", "hive-exec-log4j2.properties");
        handle_result(status, "appender.console.name", "console", "hive-exec-log4j2.properties");
        status = modify_hive_config("appender.console.target", "SYSTEM_ERR", "hive-exec-log4j2.properties");
        handle_result(status, "appender.console.target", "SYSTEM_ERR", "hive-exec-log4j2.properties");
        status = modify_hive_config("appender.console.layout.type", "PatternLayout", "hive-exec-log4j2.properties");
        handle_result(status, "appender.console.layout.type", "PatternLayout", "hive-exec-log4j2.properties");
        status = modify_hive_config("appender.console.layout.pattern", "%d{yy/MM/dd HH:mm:ss} [%t]: %p %c{2}: %m%n", "hive-exec-log4j2.properties");
        handle_result(status, "appender.console.layout.pattern", "%d{yy/MM/dd HH:mm:ss} [%t]: %p %c{2}: %m%n", "hive-exec-log4j2.properties");

        //			 File appender configuration
        status = modify_hive_config("appender.FA.type", "File", "hive-exec-log4j2.properties");
        handle_result(status, "appender.FA.type", "File", "hive-exec-log4j2.properties");
        status = modify_hive_config("appender.FA.name", "FA", "hive-exec-log4j2.properties");
        handle_result(status, "appender.FA.name", "FA", "hive-exec-log4j2.properties");
        status = modify_hive_config("appender.FA.fileName", "${sys:hive.log.dir}/${sys:hive.log.file}", "hive-exec-log4j2.properties");
        handle_result(status, "appender.FA.fileName", "${sys:hive.log.dir}/${sys:hive.log.file}", "hive-exec-log4j2.properties");
        status = modify_hive_config("appender.FA.layout.type", "PatternLayout", "hive-exec-log4j2.properties");
        handle_result(status, "appender.FA.layout.type", "PatternLayout", "hive-exec-log4j2.properties");
        status = modify_hive_config("appender.FA.layout.pattern", "%d{ISO8601} %-5p [%t]: %c{2} (%F:%M(%L)) - %m%n", "hive-exec-log4j2.properties");
        handle_result(status, "appender.FA.layout.pattern", "%d{ISO8601} %-5p [%t]: %c{2} (%F:%M(%L)) - %m%n", "hive-exec-log4j2.properties");

        //			 Logger declarations
        status = modify_hive_config("loggers", "NIOServerCnxn, ClientCnxnSocketNIO, DataNucleus, Datastore, JPOX", "hive-exec-log4j2.properties");
        handle_result(status, "loggers", "NIOServerCnxn, ClientCnxnSocketNIO, DataNucleus, Datastore, JPOX", "hive-exec-log4j2.properties");

        //			 Individual logger configurations
        status = modify_hive_config("logger.NIOServerCnxn.name", "org.apache.zookeeper.server.NIOServerCnxn", "hive-exec-log4j2.properties");
        handle_result(status, "logger.NIOServerCnxn.name", "org.apache.zookeeper.server.NIOServerCnxn", "hive-exec-log4j2.properties");
        status = modify_hive_config("logger.NIOServerCnxn.level", "WARN", "hive-exec-log4j2.properties");
        handle_result(status, "logger.NIOServerCnxn.level", "WARN", "hive-exec-log4j2.properties");
        status = modify_hive_config("logger.ClientCnxnSocketNIO.name", "org.apache.zookeeper.ClientCnxnSocketNIO", "hive-exec-log4j2.properties");
        handle_result(status, "logger.ClientCnxnSocketNIO.name", "org.apache.zookeeper.ClientCnxnSocketNIO", "hive-exec-log4j2.properties");
        status = modify_hive_config("logger.ClientCnxnSocketNIO.level", "WARN", "hive-exec-log4j2.properties");
        handle_result(status, "logger.ClientCnxnSocketNIO.level", "WARN", "hive-exec-log4j2.properties");
        status = modify_hive_config("logger.DataNucleus.name", "DataNucleus", "hive-exec-log4j2.properties");
        handle_result(status, "logger.DataNucleus.name", "DataNucleus", "hive-exec-log4j2.properties");
        status = modify_hive_config("logger.DataNucleus.level", "ERROR", "hive-exec-log4j2.properties");
        handle_result(status, "logger.DataNucleus.level", "ERROR", "hive-exec-log4j2.properties");
        status = modify_hive_config("logger.Datastore.name", "Datastore", "hive-exec-log4j2.properties");
        handle_result(status, "logger.Datastore.name", "Datastore", "hive-exec-log4j2.properties");
        status = modify_hive_config("logger.Datastore.level", "ERROR", "hive-exec-log4j2.properties");
        handle_result(status, "logger.Datastore.level", "ERROR", "hive-exec-log4j2.properties");
        status = modify_hive_config("logger.JPOX.name", "JPOX", "hive-exec-log4j2.properties");
        handle_result(status, "logger.JPOX.name", "JPOX", "hive-exec-log4j2.properties");
        status = modify_hive_config("logger.JPOX.level", "ERROR", "hive-exec-log4j2.properties");
        handle_result(status, "logger.JPOX.level", "ERROR", "hive-exec-log4j2.properties");

        //			 Root logger configuration
        status = modify_hive_config("rootLogger.level", "${sys:hive.log.level}", "hive-exec-log4j2.properties");
        handle_result(status, "rootLogger.level", "${sys:hive.log.level}", "hive-exec-log4j2.properties");
        status = modify_hive_config("rootLogger.appenderRefs", "root", "hive-exec-log4j2.properties");
        handle_result(status, "rootLogger.appenderRefs", "root", "hive-exec-log4j2.properties");
        status = modify_hive_config("rootLogger.appenderRef.root.ref", "${sys:hive.root.logger}", "hive-exec-log4j2.properties");
        handle_result(status, "rootLogger.appenderRef.root.ref", "${sys:hive.root.logger}", "hive-exec-log4j2.properties");
        //			 Standalone properties (outside content)
        status = modify_hive_config("hive2_log_maxfilesize", "256", "hive-site.xml");
        handle_result(status, "hive2_log_maxfilesize", "256", "hive-site.xml");
        status = modify_hive_config("hive2_log_maxbackupindex", "30", "hive-site.xml");
        handle_result(status, "hive2_log_maxbackupindex", "30", "hive-site.xml");

        //			 Log4j2 configuration content
        status = modify_hive_config("status", "INFO", "hive-log4j2.properties");
        handle_result(status, "status", "INFO", "hive-log4j2.properties");
        status = modify_hive_config("name", "HiveLog4j2", "hive-log4j2.properties");
        handle_result(status, "name", "HiveLog4j2", "hive-log4j2.properties");
        status = modify_hive_config("packages", "org.apache.hadoop.hive.ql.log", "hive-log4j2.properties");
        handle_result(status, "packages", "org.apache.hadoop.hive.ql.log", "hive-log4j2.properties");
        status = modify_hive_config("property.hive.log.level", "{{hive_log_level}}", "hive-log4j2.properties");
        handle_result(status, "property.hive.log.level", "{{hive_log_level}}", "hive-log4j2.properties");
        status = modify_hive_config("property.hive.root.logger", "DRFA", "hive-log4j2.properties");
        handle_result(status, "property.hive.root.logger", "DRFA", "hive-log4j2.properties");
        status = modify_hive_config("property.hive.log.dir", "${sys:java.io.tmpdir}/${sys:user.name}", "hive-log4j2.properties");
        handle_result(status, "property.hive.log.dir", "${sys:java.io.tmpdir}/${sys:user.name}", "hive-log4j2.properties");
        status = modify_hive_config("property.hive.log.file", "hive.log", "hive-log4j2.properties");
        handle_result(status, "property.hive.log.file", "hive.log", "hive-log4j2.properties");
        status = modify_hive_config("appenders", "console, DRFA", "hive-log4j2.properties");
        handle_result(status, "appenders", "console, DRFA", "hive-log4j2.properties");
        status = modify_hive_config("appender.console.type", "Console", "hive-log4j2.properties");
        handle_result(status, "appender.console.type", "Console", "hive-log4j2.properties");
        status = modify_hive_config("appender.console.name", "console", "hive-log4j2.properties");
        handle_result(status, "appender.console.name", "console", "hive-log4j2.properties");
        status = modify_hive_config("appender.console.target", "SYSTEM_ERR", "hive-log4j2.properties");
        handle_result(status, "appender.console.target", "SYSTEM_ERR", "hive-log4j2.properties");
        status = modify_hive_config("appender.console.layout.type", "PatternLayout", "hive-log4j2.properties");
        handle_result(status, "appender.console.layout.type", "PatternLayout", "hive-log4j2.properties");
        status = modify_hive_config("appender.console.layout.pattern", "%d{yy/MM/dd HH:mm:ss} [%t]: %p %c{2}: %m%n", "hive-log4j2.properties");
        handle_result(status, "appender.console.layout.pattern", "%d{yy/MM/dd HH:mm:ss} [%t]: %p %c{2}: %m%n", "hive-log4j2.properties");
        status = modify_hive_config("appender.DRFA.type", "RollingFile", "hive-log4j2.properties");
        handle_result(status, "appender.DRFA.type", "RollingFile", "hive-log4j2.properties");
        status = modify_hive_config("appender.DRFA.name", "DRFA", "hive-log4j2.properties");
        handle_result(status, "appender.DRFA.name", "DRFA", "hive-log4j2.properties");
        status = modify_hive_config("appender.DRFA.fileName", "${sys:hive.log.dir}/${sys:hive.log.file}", "hive-log4j2.properties");
        handle_result(status, "appender.DRFA.fileName", "${sys:hive.log.dir}/${sys:hive.log.file}", "hive-log4j2.properties");
        status = modify_hive_config("appender.DRFA.filePattern", "${sys:hive.log.dir}/${sys:hive.log.file}.%d{yyyy-MM-dd}_%i.gz", "hive-log4j2.properties");
        handle_result(status, "appender.DRFA.filePattern", "${sys:hive.log.dir}/${sys:hive.log.file}.%d{yyyy-MM-dd}_%i.gz", "hive-log4j2.properties");
        status = modify_hive_config("appender.DRFA.layout.type", "PatternLayout", "hive-log4j2.properties");
        handle_result(status, "appender.DRFA.layout.type", "PatternLayout", "hive-log4j2.properties");
        status = modify_hive_config("appender.DRFA.layout.pattern", "%d{ISO8601} %-5p [%t]: %c{2} (%F:%M(%L)) - %m%n", "hive-log4j2.properties");
        handle_result(status, "appender.DRFA.layout.pattern", "%d{ISO8601} %-5p [%t]: %c{2} (%F:%M(%L)) - %m%n", "hive-log4j2.properties");
        status = modify_hive_config("appender.DRFA.policies.type", "Policies", "hive-log4j2.properties");
        handle_result(status, "appender.DRFA.policies.type", "Policies", "hive-log4j2.properties");
        status = modify_hive_config("appender.DRFA.policies.time.type", "TimeBasedTriggeringPolicy", "hive-log4j2.properties");
        handle_result(status, "appender.DRFA.policies.time.type", "TimeBasedTriggeringPolicy", "hive-log4j2.properties");
        status = modify_hive_config("appender.DRFA.policies.time.interval", "1", "hive-log4j2.properties");
        handle_result(status, "appender.DRFA.policies.time.interval", "1", "hive-log4j2.properties");
        status = modify_hive_config("appender.DRFA.policies.time.modulate", "true", "hive-log4j2.properties");
        handle_result(status, "appender.DRFA.policies.time.modulate", "true", "hive-log4j2.properties");
        status = modify_hive_config("appender.DRFA.strategy.type", "DefaultRolloverStrategy", "hive-log4j2.properties");
        handle_result(status, "appender.DRFA.strategy.type", "DefaultRolloverStrategy", "hive-log4j2.properties");
        status = modify_hive_config("appender.DRFA.strategy.max", "{{hive2_log_maxbackupindex}}", "hive-log4j2.properties");
        handle_result(status, "appender.DRFA.strategy.max", "{{hive2_log_maxbackupindex}}", "hive-log4j2.properties");
        status = modify_hive_config("appender.DRFA.policies.fsize.type", "SizeBasedTriggeringPolicy", "hive-log4j2.properties");
        handle_result(status, "appender.DRFA.policies.fsize.type", "SizeBasedTriggeringPolicy", "hive-log4j2.properties");
        status = modify_hive_config("appender.DRFA.policies.fsize.size", "{{hive2_log_maxfilesize}}MB", "hive-log4j2.properties");
        handle_result(status, "appender.DRFA.policies.fsize.size", "{{hive2_log_maxfilesize}}MB", "hive-log4j2.properties");
        status = modify_hive_config("loggers", "NIOServerCnxn, ClientCnxnSocketNIO, DataNucleus, Datastore, JPOX", "hive-log4j2.properties");
        handle_result(status, "loggers", "NIOServerCnxn, ClientCnxnSocketNIO, DataNucleus, Datastore, JPOX", "hive-log4j2.properties");
        status = modify_hive_config("logger.NIOServerCnxn.name", "org.apache.zookeeper.server.NIOServerCnxn", "hive-log4j2.properties");
        handle_result(status, "logger.NIOServerCnxn.name", "org.apache.zookeeper.server.NIOServerCnxn", "hive-log4j2.properties");
        status = modify_hive_config("logger.NIOServerCnxn.level", "WARN", "hive-log4j2.properties");
        handle_result(status, "logger.NIOServerCnxn.level", "WARN", "hive-log4j2.properties");
        status = modify_hive_config("logger.ClientCnxnSocketNIO.name", "org.apache.zookeeper.ClientCnxnSocketNIO", "hive-log4j2.properties");
        handle_result(status, "logger.ClientCnxnSocketNIO.name", "org.apache.zookeeper.ClientCnxnSocketNIO", "hive-log4j2.properties");
        status = modify_hive_config("logger.ClientCnxnSocketNIO.level", "WARN", "hive-log4j2.properties");
        handle_result(status, "logger.ClientCnxnSocketNIO.level", "WARN", "hive-log4j2.properties");
        status = modify_hive_config("logger.DataNucleus.name", "DataNucleus", "hive-log4j2.properties");
        handle_result(status, "logger.DataNucleus.name", "DataNucleus", "hive-log4j2.properties");
        status = modify_hive_config("logger.DataNucleus.level", "ERROR", "hive-log4j2.properties");
        handle_result(status, "logger.DataNucleus.level", "ERROR", "hive-log4j2.properties");
        status = modify_hive_config("logger.Datastore.name", "Datastore", "hive-log4j2.properties");
        handle_result(status, "logger.Datastore.name", "Datastore", "hive-log4j2.properties");
        status = modify_hive_config("logger.Datastore.level", "ERROR", "hive-log4j2.properties");
        handle_result(status, "logger.Datastore.level", "ERROR", "hive-log4j2.properties");
        status = modify_hive_config("logger.JPOX.name", "JPOX", "hive-log4j2.properties");
        handle_result(status, "logger.JPOX.name", "JPOX", "hive-log4j2.properties");
        status = modify_hive_config("logger.JPOX.level", "ERROR", "hive-log4j2.properties");
        handle_result(status, "logger.JPOX.level", "ERROR", "hive-log4j2.properties");
        status = modify_hive_config("rootLogger.level", "${sys:hive.log.level}", "hive-log4j2.properties");
        handle_result(status, "rootLogger.level", "${sys:hive.log.level}", "hive-log4j2.properties");
        status = modify_hive_config("rootLogger.appenderRefs", "root", "hive-log4j2.properties");
        handle_result(status, "rootLogger.appenderRefs", "root", "hive-log4j2.properties");
        status = modify_hive_config("rootLogger.appenderRef.root.ref", "${sys:hive.root.logger}", "hive-log4j2.properties");
        handle_result(status, "rootLogger.appenderRef.root.ref", "${sys:hive.root.logger}", "hive-log4j2.properties");
        //			 Tez Configuration
        status = modify_hive_config("tez.session.am.dag.submit.timeout.secs", "0", "hive-site.xml");
        handle_result(status, "tez.session.am.dag.submit.timeout.secs", "0", "hive-site.xml");
        status = modify_hive_config("hive.server2.materializedviews.registry.impl", "DUMMY", "hive-site.xml");
        handle_result(status, "hive.server2.materializedviews.registry.impl", "DUMMY", "hive-site.xml");
        status = modify_hive_config("hive.server2.max.start.attempts", "5", "hive-site.xml");
        handle_result(status, "hive.server2.max.start.attempts", "5", "hive-site.xml");
        status = modify_hive_config("hive.server2.transport.mode", "binary", "hive-site.xml");
        handle_result(status, "hive.server2.transport.mode", "binary", "hive-site.xml");

        //			 File Format Settings
        status = modify_hive_config("hive.default.fileformat", "TextFile", "hive-site.xml");
        handle_result(status, "hive.default.fileformat", "TextFile", "hive-site.xml");

        //			 Metastore Security
        status = modify_hive_config("hive.metastore.sasl.enabled", "false", "hive-site.xml");
        handle_result(status, "hive.metastore.sasl.enabled", "false", "hive-site.xml");
        status = modify_hive_config("hive.metastore.execute.setugi", "true", "hive-site.xml");
        handle_result(status, "hive.metastore.execute.setugi", "true", "hive-site.xml");

        //			 Join Optimization
        status = modify_hive_config("hive.optimize.bucketmapjoin.sortedmerge", "false", "hive-site.xml");
        handle_result(status, "hive.optimize.bucketmapjoin.sortedmerge", "false", "hive-site.xml");

        //			 Tez Execution
        status = modify_hive_config("hive.tez.container.size", "682", "hive-site.xml");
        handle_result(status, "hive.tez.container.size", "682", "hive-site.xml");
        status = modify_hive_config("hive.tez.input.format", "org.apache.hadoop.hive.ql.io.HiveInputFormat", "hive-site.xml");
        handle_result(status, "hive.tez.input.format", "org.apache.hadoop.hive.ql.io.HiveInputFormat", "hive-site.xml");
        status = modify_hive_config("hive.tez.java.opts", "-server -Djava.net.preferIPv4Stack=true -XX:NewRatio=8 -XX:+UseNUMA -XX:+UseG1GC -XX:+ResizeTLAB -XX:+PrintGCDetails -verbose:gc -XX:+PrintGCTimeStamps", "hive-site.xml");
        handle_result(status, "hive.tez.java.opts", "-server -Djava.net.preferIPv4Stack=true -XX:NewRatio=8 -XX:+UseNUMA -XX:+UseG1GC -XX:+ResizeTLAB -XX:+PrintGCDetails -verbose:gc -XX:+PrintGCTimeStamps", "hive-site.xml");

        //			 Transactions
        status = modify_hive_config("hive.txn.timeout", "300", "hive-site.xml");
        handle_result(status, "hive.txn.timeout", "300", "hive-site.xml");
        status = modify_hive_config("hive.compactor.initiator.on", "true", "hive-site.xml");
        handle_result(status, "hive.compactor.initiator.on", "true", "hive-site.xml");
        status = modify_hive_config("hive.compactor.worker.threads", "5", "hive-site.xml");
        handle_result(status, "hive.compactor.worker.threads", "5", "hive-site.xml");
        status = modify_hive_config("hive.create.as.insert.only", "false", "hive-site.xml");
        handle_result(status, "hive.create.as.insert.only", "false", "hive-site.xml");
        status = modify_hive_config("metastore.create.as.acid", "false", "hive-site.xml");
        handle_result(status, "metastore.create.as.acid", "false", "hive-site.xml");
        status = modify_hive_config("hive.compactor.delta.num.threshold", "10", "hive-site.xml");
        handle_result(status, "hive.compactor.delta.num.threshold", "10", "hive-site.xml");
        status = modify_hive_config("hive.compactor.abortedtxn.threshold", "1000", "hive-site.xml");
        handle_result(status, "hive.compactor.abortedtxn.threshold", "1000", "hive-site.xml");

        //			 DataNucleus Cache
        status = modify_hive_config("datanucleus.cache.level2.type", "none", "hive-site.xml");
        handle_result(status, "datanucleus.cache.level2.type", "none", "hive-site.xml");

        //			 ZooKeeper
        status = modify_hive_config("hive.zookeeper.quorum", "localhost:2181", "hive-site.xml");
        handle_result(status, "hive.zookeeper.quorum", "localhost:2181", "hive-site.xml");

        //			 Metastore Connection
        status = modify_hive_config("hive.metastore.connect.retries", "24", "hive-site.xml");
        handle_result(status, "hive.metastore.connect.retries", "24", "hive-site.xml");
        status = modify_hive_config("hive.metastore.failure.retries", "24", "hive-site.xml");
        handle_result(status, "hive.metastore.failure.retries", "24", "hive-site.xml");
        status = modify_hive_config("hive.metastore.client.connect.retry.delay", "5s", "hive-site.xml");
        handle_result(status, "hive.metastore.client.connect.retry.delay", "5s", "hive-site.xml");
        status = modify_hive_config("hive.metastore.client.socket.timeout", "1800s", "hive-site.xml");
        handle_result(status, "hive.metastore.client.socket.timeout", "1800s", "hive-site.xml");

        //			 MapJoin
        status = modify_hive_config("hive.mapjoin.bucket.cache.size", "10000", "hive-site.xml");
        handle_result(status, "hive.mapjoin.bucket.cache.size", "10000", "hive-site.xml");

        //			 Security Delegation
        status = modify_hive_config("hive.cluster.delegation.token.store.class", "org.apache.hadoop.hive.thrift.ZooKeeperTokenStore", "hive-site.xml");
        handle_result(status, "hive.cluster.delegation.token.store.class", "org.apache.hadoop.hive.thrift.ZooKeeperTokenStore", "hive-site.xml");
        status = modify_hive_config("hive.cluster.delegation.token.store.zookeeper.connectString", "localhost:2181", "hive-site.xml");
        handle_result(status, "hive.cluster.delegation.token.store.zookeeper.connectString", "localhost:2181", "hive-site.xml");
        status = modify_hive_config("hive.server2.support.dynamic.service.discovery", "true", "hive-site.xml");
        handle_result(status, "hive.server2.support.dynamic.service.discovery", "true", "hive-site.xml");

        //			 Execution Directories
        status = modify_hive_config("hive.exec.scratchdir", "/tmp/hive", "hive-site.xml");
        handle_result(status, "hive.exec.scratchdir", "/tmp/hive", "hive-site.xml");
        status = modify_hive_config("hive.exec.submitviachild", "false", "hive-site.xml");
        handle_result(status, "hive.exec.submitviachild", "false", "hive-site.xml");
        status = modify_hive_config("hive.exec.submit.local.task.via.child", "true", "hive-site.xml");
        handle_result(status, "hive.exec.submit.local.task.via.child", "true", "hive-site.xml");

        //			 Compression
        status = modify_hive_config("hive.exec.compress.output", "false", "hive-site.xml");
        handle_result(status, "hive.exec.compress.output", "false", "hive-site.xml");
        status = modify_hive_config("hive.exec.compress.intermediate", "false", "hive-site.xml");
        handle_result(status, "hive.exec.compress.intermediate", "false", "hive-site.xml");

        //			 Reducers
        status = modify_hive_config("hive.exec.reducers.bytes.per.reducer", "67108864", "hive-site.xml");
        handle_result(status, "hive.exec.reducers.bytes.per.reducer", "67108864", "hive-site.xml");
        status = modify_hive_config("hive.exec.reducers.max", "1009", "hive-site.xml");
        handle_result(status, "hive.exec.reducers.max", "1009", "hive-site.xml");

        //			 Hooks
        status = modify_hive_config("hive.exec.pre.hooks", "", "hive-site.xml");
        handle_result(status, "hive.exec.pre.hooks", "", "hive-site.xml");
        status = modify_hive_config("hive.exec.post.hooks", "", "hive-site.xml");
        handle_result(status, "hive.exec.post.hooks", "", "hive-site.xml");
        status = modify_hive_config("hive.exec.failure.hooks", "", "hive-site.xml");
        handle_result(status, "hive.exec.failure.hooks", "", "hive-site.xml");

        //			 Parallel Execution
        status = modify_hive_config("hive.exec.parallel", "false", "hive-site.xml");
        handle_result(status, "hive.exec.parallel", "false", "hive-site.xml");
        status = modify_hive_config("hive.exec.parallel.thread.number", "8", "hive-site.xml");
        handle_result(status, "hive.exec.parallel.thread.number", "8", "hive-site.xml");
        status = modify_hive_config("hive.mapred.reduce.tasks.speculative.execution", "false", "hive-site.xml");
        handle_result(status, "hive.mapred.reduce.tasks.speculative.execution", "false", "hive-site.xml");

        //			 Dynamic Partitioning
        status = modify_hive_config("hive.exec.dynamic.partition", "true", "hive-site.xml");
        handle_result(status, "hive.exec.dynamic.partition", "true", "hive-site.xml");
        status = modify_hive_config("hive.exec.dynamic.partition.mode", "nonstrict", "hive-site.xml");
        handle_result(status, "hive.exec.dynamic.partition.mode", "nonstrict", "hive-site.xml");
        status = modify_hive_config("hive.exec.max.dynamic.partitions", "5000", "hive-site.xml");
        handle_result(status, "hive.exec.max.dynamic.partitions", "5000", "hive-site.xml");
        status = modify_hive_config("hive.exec.max.dynamic.partitions.pernode", "2000", "hive-site.xml");
        handle_result(status, "hive.exec.max.dynamic.partitions.pernode", "2000", "hive-site.xml");
        status = modify_hive_config("hive.exec.max.created.files", "100000", "hive-site.xml");
        handle_result(status, "hive.exec.max.created.files", "100000", "hive-site.xml");

        //			 Warehouse Directories
        status = modify_hive_config("hive.metastore.warehouse.dir", "/warehouse/tablespace/managed/hive", "hive-site.xml");
        handle_result(status, "hive.metastore.warehouse.dir", "/warehouse/tablespace/managed/hive", "hive-site.xml");
        status = modify_hive_config("hive.metastore.warehouse.external.dir", "/warehouse/tablespace/external/hive", "hive-site.xml");
        handle_result(status, "hive.metastore.warehouse.external.dir", "/warehouse/tablespace/external/hive", "hive-site.xml");

        //			 Lock Management
        status = modify_hive_config("hive.lock.manager", "", "hive-site.xml");
        handle_result(status, "hive.lock.manager", "", "hive-site.xml");

        //			 Metastore URIs
        status = modify_hive_config("hive.metastore.uris", "thrift:localhost:9083", "hive-site.xml");
        handle_result(status, "hive.metastore.uris", "thrift:localhost:9083", "hive-site.xml");
        status = modify_hive_config("hive.metastore.db.type", "{{hive_metastore_db_type}", "hive-site.xml");
        handle_result(status, "hive.metastore.db.type", "{{hive_metastore_db_type}", "hive-site.xml");

        //			 Database Connection (Sensitive)
        status = modify_hive_config("javax.jdo.option.ConnectionPassword", "", "hive-site.xml");//   PASSWORD TYPE
        handle_result(status, "javax.jdo.option.ConnectionPassword", "", "hive-site.xml");
        status = modify_hive_config("javax.jdo.option.ConnectionURL", "jdbc:mysql:localhost/hive?createDatabaseIfNotExist=true", "hive-site.xml");
        handle_result(status, "javax.jdo.option.ConnectionURL", "jdbc:mysql:localhost/hive?createDatabaseIfNotExist=true", "hive-site.xml");
        status = modify_hive_config("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver", "hive-site.xml");
        handle_result(status, "javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver", "hive-site.xml");
        status = modify_hive_config("javax.jdo.option.ConnectionUserName", "hive", "hive-site.xml");
        handle_result(status, "javax.jdo.option.ConnectionUserName", "hive", "hive-site.xml");

        //			 Metastore Server
        status = modify_hive_config("hive.metastore.server.max.threads", "100000", "hive-site.xml");
        handle_result(status, "hive.metastore.server.max.threads", "100000", "hive-site.xml");

        //			 Kerberos
        status = modify_hive_config("hive.metastore.kerberos.keytab.file", "/etc/security/keytabs/hive.service.keytab", "hive-site.xml");
        handle_result(status, "hive.metastore.kerberos.keytab.file", "/etc/security/keytabs/hive.service.keytab", "hive-site.xml");
        status = modify_hive_config("hive.metastore.kerberos.principal", "hive/_HOST@EXAMPLE.COM", "hive-site.xml");
        handle_result(status, "hive.metastore.kerberos.principal", "hive/_HOST@EXAMPLE.COM", "hive-site.xml");

        //			 Delegation Token Store
        status = modify_hive_config("hive.cluster.delegation.token.store.zookeeper.znode", "/hive/cluster/delegation", "hive-site.xml");
        handle_result(status, "hive.cluster.delegation.token.store.zookeeper.znode", "/hive/cluster/delegation", "hive-site.xml");

        //			 Metastore Caching
        status = modify_hive_config("hive.metastore.cache.pinobjtypes", "Table,Database,Type,FieldSchema,Order", "hive-site.xml");
        handle_result(status, "hive.metastore.cache.pinobjtypes", "Table,Database,Type,FieldSchema,Order", "hive-site.xml");
        status = modify_hive_config("hive.metastore.pre_event.listeners", "org.apache.hadoop.hive.ql.security.authorization.AuthorizationPreEventListener", "hive-site.xml");
        handle_result(status, "hive.metastore.pre_event.listeners", "org.apache.hadoop.hive.ql.security.authorization.AuthorizationPreEventListener", "hive-site.xml");
        status = modify_hive_config("hive.metastore.authorization.storage.checks", "false", "hive-site.xml");
        handle_result(status, "hive.metastore.authorization.storage.checks", "false", "hive-site.xml");

        //			 Session Timeouts
        status = modify_hive_config("hive.server2.idle.session.timeout", "1d", "hive-site.xml");
        handle_result(status, "hive.server2.idle.session.timeout", "1d", "hive-site.xml");
        status = modify_hive_config("hive.server2.idle.operation.timeout", "6h", "hive-site.xml");
        handle_result(status, "hive.server2.idle.operation.timeout", "6h", "hive-site.xml");

        //			 Optimization Flags
        status = modify_hive_config("hive.limit.optimize.enable", "false", "hive-site.xml");
        handle_result(status, "hive.limit.optimize.enable", "false", "hive-site.xml");
        status = modify_hive_config("hive.strict.managed.tables", "false", "hive-site.xml");
        handle_result(status, "hive.strict.managed.tables", "false", "hive-site.xml");
        status = modify_hive_config("hive.txn.strict.locking.mode", "false", "hive-site.xml");
        handle_result(status, "hive.txn.strict.locking.mode", "false", "hive-site.xml");
        status = modify_hive_config("hive.materializedview.rewriting.incremental", "false", "hive-site.xml");
        handle_result(status, "hive.materializedview.rewriting.incremental", "false", "hive-site.xml");

        //			 Map Aggregation
        status = modify_hive_config("hive.map.aggr", "true", "hive-site.xml");
        handle_result(status, "hive.map.aggr", "true", "hive-site.xml");
        status = modify_hive_config("hive.cbo.enable", "true", "hive-site.xml");
        handle_result(status, "hive.cbo.enable", "true", "hive-site.xml");
        status = modify_hive_config("hive.mapjoin.optimized.hashtable", "true", "hive-site.xml");
        handle_result(status, "hive.mapjoin.optimized.hashtable", "true", "hive-site.xml");
        status = modify_hive_config("hive.smbjoin.cache.rows", "10000", "hive-site.xml");
        handle_result(status, "hive.smbjoin.cache.rows", "10000", "hive-site.xml");
        status = modify_hive_config("hive.map.aggr.hash.percentmemory", "0.5", "hive-site.xml");
        handle_result(status, "hive.map.aggr.hash.percentmemory", "0.5", "hive-site.xml");
        status = modify_hive_config("hive.map.aggr.hash.force.flush.memory.threshold", "0.9", "hive-site.xml");
        handle_result(status, "hive.map.aggr.hash.force.flush.memory.threshold", "0.9", "hive-site.xml");
        status = modify_hive_config("hive.map.aggr.hash.min.reduction", "0.5", "hive-site.xml");
        handle_result(status, "hive.map.aggr.hash.min.reduction", "0.5", "hive-site.xml");

        //			 Merge Operations
        status = modify_hive_config("hive.merge.mapfiles", "true", "hive-site.xml");
        handle_result(status, "hive.merge.mapfiles", "true", "hive-site.xml");
        status = modify_hive_config("hive.merge.mapredfiles", "false", "hive-site.xml");
        handle_result(status, "hive.merge.mapredfiles", "false", "hive-site.xml");
        status = modify_hive_config("hive.merge.tezfiles", "false", "hive-site.xml");
        handle_result(status, "hive.merge.tezfiles", "false", "hive-site.xml");
        status = modify_hive_config("hive.merge.size.per.task", "256000000", "hive-site.xml");
        handle_result(status, "hive.merge.size.per.task", "256000000", "hive-site.xml");
        status = modify_hive_config("hive.merge.smallfiles.avgsize", "16000000", "hive-site.xml");
        handle_result(status, "hive.merge.smallfiles.avgsize", "16000000", "hive-site.xml");
        status = modify_hive_config("hive.merge.rcfile.block.level", "true", "hive-site.xml");
        handle_result(status, "hive.merge.rcfile.block.level", "true", "hive-site.xml");
        status = modify_hive_config("hive.merge.orcfile.stripe.level", "true", "hive-site.xml");
        handle_result(status, "hive.merge.orcfile.stripe.level", "true", "hive-site.xml");

        //			 ORC Format
        status = modify_hive_config("hive.orc.splits.include.file.footer", "false", "hive-site.xml");
        handle_result(status, "hive.orc.splits.include.file.footer", "false", "hive-site.xml");
        status = modify_hive_config("hive.orc.compute.splits.num.threads", "10", "hive-site.xml");
        handle_result(status, "hive.orc.compute.splits.num.threads", "10", "hive-site.xml");

        //			 Auto Join Conversion
        status = modify_hive_config("hive.auto.convert.join", "true", "hive-site.xml");
        handle_result(status, "hive.auto.convert.join", "true", "hive-site.xml");
        status = modify_hive_config("hive.auto.convert.join.noconditionaltask", "true", "hive-site.xml");
        handle_result(status, "hive.auto.convert.join.noconditionaltask", "true", "hive-site.xml");
        status = modify_hive_config("hive.auto.convert.join.noconditionaltask.size", "52428800", "hive-site.xml");
        handle_result(status, "hive.auto.convert.join.noconditionaltask.size", "52428800", "hive-site.xml");

        //			 Limit Optimization
        status = modify_hive_config("hive.limit.optimize.enable", "true", "hive-site.xml");
        handle_result(status, "hive.limit.optimize.enable", "true", "hive-site.xml");

        //			 Tez CPU/Logging
        status = modify_hive_config("hive.tez.cpu.vcores", "-1", "hive-site.xml");
        handle_result(status, "hive.tez.cpu.vcores", "-1", "hive-site.xml");
        status = modify_hive_config("hive.tez.log.level", "INFO", "hive-site.xml");
        handle_result(status, "hive.tez.log.level", "INFO", "hive-site.xml");

        //			 Join Enforcement
        status = modify_hive_config("hive.enforce.sortmergebucketmapjoin", "true", "hive-site.xml");
        handle_result(status, "hive.enforce.sortmergebucketmapjoin", "true", "hive-site.xml");
        status = modify_hive_config("hive.auto.convert.sortmerge.join", "true", "hive-site.xml");
        handle_result(status, "hive.auto.convert.sortmerge.join", "true", "hive-site.xml");
        status = modify_hive_config("hive.auto.convert.sortmerge.join.to.mapjoin", "true", "hive-site.xml");
        handle_result(status, "hive.auto.convert.sortmerge.join.to.mapjoin", "true", "hive-site.xml");

        //			 Query Optimization
        status = modify_hive_config("hive.optimize.constant.propagation", "true", "hive-site.xml");
        handle_result(status, "hive.optimize.constant.propagation", "true", "hive-site.xml");
        status = modify_hive_config("hive.optimize.metadataonly", "true", "hive-site.xml");
        handle_result(status, "hive.optimize.metadataonly", "true", "hive-site.xml");
        status = modify_hive_config("hive.optimize.null.scan", "true", "hive-site.xml");
        handle_result(status, "hive.optimize.null.scan", "true", "hive-site.xml");
        status = modify_hive_config("hive.optimize.bucketmapjoin", "true", "hive-site.xml");
        handle_result(status, "hive.optimize.bucketmapjoin", "true", "hive-site.xml");
        status = modify_hive_config("hive.optimize.reducededuplication", "true", "hive-site.xml");
        handle_result(status, "hive.optimize.reducededuplication", "true", "hive-site.xml");
        status = modify_hive_config("hive.optimize.reducededuplication.min.reducer", "4", "hive-site.xml");
        handle_result(status, "hive.optimize.reducededuplication.min.reducer", "4", "hive-site.xml");
        status = modify_hive_config("hive.optimize.sort.dynamic.partition", "false", "hive-site.xml");
        handle_result(status, "hive.optimize.sort.dynamic.partition", "false", "hive-site.xml");

        //			 Statistics
        status = modify_hive_config("hive.stats.autogather", "true", "hive-site.xml");
        handle_result(status, "hive.stats.autogather", "true", "hive-site.xml");
        status = modify_hive_config("hive.stats.dbclass", "fs", "hive-site.xml");
        handle_result(status, "hive.stats.dbclass", "fs", "hive-site.xml");
        status = modify_hive_config("hive.stats.fetch.partition.stats", "true", "hive-site.xml");
        handle_result(status, "hive.stats.fetch.partition.stats", "true", "hive-site.xml");
        status = modify_hive_config("hive.stats.fetch.column.stats", "false", "hive-site.xml");
        handle_result(status, "hive.stats.fetch.column.stats", "false", "hive-site.xml");

        //			 ZooKeeper Config
        status = modify_hive_config("hive.zookeeper.client.port", "2181", "hive-site.xml");
        handle_result(status, "hive.zookeeper.client.port", "2181", "hive-site.xml");
        status = modify_hive_config("hive.zookeeper.namespace", "hive_zookeeper_namespace", "hive-site.xml");
        handle_result(status, "hive.zookeeper.namespace", "hive_zookeeper_namespace", "hive-site.xml");

        //			 Transactions
        status = modify_hive_config("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager", "hive-site.xml");
        handle_result(status, "hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager", "hive-site.xml");
        status = modify_hive_config("hive.txn.max.open.batch", "1000", "hive-site.xml");
        handle_result(status, "hive.txn.max.open.batch", "1000", "hive-site.xml");
        status = modify_hive_config("hive.support.concurrency", "true", "hive-site.xml");
        handle_result(status, "hive.support.concurrency", "true", "hive-site.xml");

        //			 CLI
        status = modify_hive_config("hive.cli.print.header", "false", "hive-site.xml");
        handle_result(status, "hive.cli.print.header", "false", "hive-site.xml");

        //			 Compaction
        status = modify_hive_config("hive.compactor.worker.timeout", "86400", "hive-site.xml");
        handle_result(status, "hive.compactor.worker.timeout", "86400", "hive-site.xml");
        status = modify_hive_config("hive.compactor.check.interval", "300", "hive-site.xml");
        handle_result(status, "hive.compactor.check.interval", "300", "hive-site.xml");
        status = modify_hive_config("hive.compactor.delta.pct.threshold", "0.1f", "hive-site.xml");
        handle_result(status, "hive.compactor.delta.pct.threshold", "0.1f", "hive-site.xml");

        //			 Fetch Task
        status = modify_hive_config("hive.fetch.task.conversion", "more", "hive-site.xml");
        handle_result(status, "hive.fetch.task.conversion", "more", "hive-site.xml");
        status = modify_hive_config("hive.fetch.task.conversion.threshold", "1073741824", "hive-site.xml");
        handle_result(status, "hive.fetch.task.conversion.threshold", "1073741824", "hive-site.xml");
        status = modify_hive_config("hive.fetch.task.aggr", "false", "hive-site.xml");
        handle_result(status, "hive.fetch.task.aggr", "false", "hive-site.xml");

        //			 Security
        status = modify_hive_config("hive.security.metastore.authorization.manager", "org.apache.hadoop.hive.ql.security.authorization.StorageBasedAuthorizationProvider", "hive-site.xml");
        handle_result(status, "hive.security.metastore.authorization.manager", "org.apache.hadoop.hive.ql.security.authorization.StorageBasedAuthorizationProvider", "hive-site.xml");
        status = modify_hive_config("hive.security.metastore.authorization.auth.reads", "true", "hive-site.xml");
        handle_result(status, "hive.security.metastore.authorization.auth.reads", "true", "hive-site.xml");
        status = modify_hive_config("hive.security.metastore.authenticator.manager", "org.apache.hadoop.hive.ql.security.HadoopDefaultMetastoreAuthenticator", "hive-site.xml");
        handle_result(status, "hive.security.metastore.authenticator.manager", "org.apache.hadoop.hive.ql.security.HadoopDefaultMetastoreAuthenticator", "hive-site.xml");

        //			 HiveServer2
        status = modify_hive_config("hive.server2.logging.operation.enabled", "true", "hive-site.xml");
        handle_result(status, "hive.server2.logging.operation.enabled", "true", "hive-site.xml");
        status = modify_hive_config("hive.server2.logging.operation.log.location", "/tmp/hive/operation_logs", "hive-site.xml");
        handle_result(status, "hive.server2.logging.operation.log.location", "/tmp/hive/operation_logs", "hive-site.xml");
        status = modify_hive_config("hive.server2.zookeeper.namespace", "hiveserver2", "hive-site.xml");
        handle_result(status, "hive.server2.zookeeper.namespace", "hiveserver2", "hive-site.xml");
        status = modify_hive_config("hive.server2.thrift.http.port", "10001", "hive-site.xml");
        handle_result(status, "hive.server2.thrift.http.port", "10001", "hive-site.xml");
        status = modify_hive_config("hive.server2.thrift.port", "10000", "hive-site.xml");
        handle_result(status, "hive.server2.thrift.port", "10000", "hive-site.xml");
        status = modify_hive_config("hive.server2.thrift.sasl.qop", "auth", "hive-site.xml");
        handle_result(status, "hive.server2.thrift.sasl.qop", "auth", "hive-site.xml");
        status = modify_hive_config("hive.server2.thrift.max.worker.threads", "500", "hive-site.xml");
        handle_result(status, "hive.server2.thrift.max.worker.threads", "500", "hive-site.xml");
        status = modify_hive_config("hive.server2.allow.user.substitution", "true", "hive-site.xml");
        handle_result(status, "hive.server2.allow.user.substitution", "true", "hive-site.xml");

        //			 Authentication
        status = modify_hive_config("hive.server2.authentication.spnego.keytab", "/etc/security/keytabs/spnego.service.keytab", "hive-site.xml");
        handle_result(status, "hive.server2.authentication.spnego.keytab", "/etc/security/keytabs/spnego.service.keytab", "hive-site.xml");
        status = modify_hive_config("hive.server2.authentication", "NONE", "hive-site.xml");
        handle_result(status, "hive.server2.authentication", "NONE", "hive-site.xml");
        status = modify_hive_config("hive.server2.authentication.spnego.principal", "HTTP/_HOST@EXAMPLE.COM", "hive-site.xml");
        handle_result(status, "hive.server2.authentication.spnego.principal", "HTTP/_HOST@EXAMPLE.COM", "hive-site.xml");
        status = modify_hive_config("hive.metastore.event.db.notification.api.auth", "true", "hive-site.xml");
        handle_result(status, "hive.metastore.event.db.notification.api.auth", "true", "hive-site.xml");
        status = modify_hive_config("hive.server2.enable.doAs", "true", "hive-site.xml");
        handle_result(status, "hive.server2.enable.doAs", "true", "hive-site.xml");
        status = modify_hive_config("hive.server2.table.type.mapping", "CLASSIC", "hive-site.xml");
        handle_result(status, "hive.server2.table.type.mapping", "CLASSIC", "hive-site.xml");
        status = modify_hive_config("hive.server2.use.SSL", "false", "hive-site.xml");
        handle_result(status, "hive.server2.use.SSL", "false", "hive-site.xml");

        //			 User Directories
        status = modify_hive_config("hive.user.install.directory", "/user/", "hive-site.xml");
        handle_result(status, "hive.user.install.directory", "/user/", "hive-site.xml");

        //			 Vectorization
        status = modify_hive_config("hive.vectorized.groupby.maxentries", "100000", "hive-site.xml");
        handle_result(status, "hive.vectorized.groupby.maxentries", "100000", "hive-site.xml");
        status = modify_hive_config("hive.merge.nway.joins", "false", "hive-site.xml");
        handle_result(status, "hive.merge.nway.joins", "false", "hive-site.xml");

        //			 Tez Prewarming
        status = modify_hive_config("hive.prewarm.enabled", "false", "hive-site.xml");
        handle_result(status, "hive.prewarm.enabled", "false", "hive-site.xml");
        status = modify_hive_config("hive.prewarm.numcontainers", "3", "hive-site.xml");
        handle_result(status, "hive.prewarm.numcontainers", "3", "hive-site.xml");

        //			 Tez Optimization
        status = modify_hive_config("hive.convert.join.bucket.mapjoin.tez", "false", "hive-site.xml");
        handle_result(status, "hive.convert.join.bucket.mapjoin.tez", "false", "hive-site.xml");
        status = modify_hive_config("hive.tez.auto.reducer.parallelism", "true", "hive-site.xml");
        handle_result(status, "hive.tez.auto.reducer.parallelism", "true", "hive-site.xml");
        status = modify_hive_config("hive.tez.max.partition.factor", "2.0", "hive-site.xml");
        handle_result(status, "hive.tez.max.partition.factor", "2.0", "hive-site.xml");
        status = modify_hive_config("hive.tez.min.partition.factor", "0.25", "hive-site.xml");
        handle_result(status, "hive.tez.min.partition.factor", "0.25", "hive-site.xml");
        status = modify_hive_config("hive.tez.dynamic.partition.pruning", "true", "hive-site.xml");
        handle_result(status, "hive.tez.dynamic.partition.pruning", "true", "hive-site.xml");
        status = modify_hive_config("hive.tez.dynamic.partition.pruning.max.event.size", "1048576", "hive-site.xml");
        handle_result(status, "hive.tez.dynamic.partition.pruning.max.event.size", "1048576", "hive-site.xml");
        status = modify_hive_config("hive.tez.dynamic.partition.pruning.max.data.size", "104857600", "hive-site.xml");
        handle_result(status, "hive.tez.dynamic.partition.pruning.max.data.size", "104857600", "hive-site.xml");
        status = modify_hive_config("hive.tez.smb.number.waves", "0.5", "hive-site.xml");
        handle_result(status, "hive.tez.smb.number.waves", "0.5", "hive-site.xml");

        //			 Database Schema
        status = modify_hive_config("ambari.hive.db.schema.name", "hive", "hive-site.xml");
        handle_result(status, "ambari.hive.db.schema.name", "hive", "hive-site.xml");

        //			 Performance
        status = modify_hive_config("hive.vectorized.execution.enabled", "true", "hive-site.xml");
        handle_result(status, "hive.vectorized.execution.enabled", "true", "hive-site.xml");
        status = modify_hive_config("hive.optimize.index.filter", "true", "hive-site.xml");
        handle_result(status, "hive.optimize.index.filter", "true", "hive-site.xml");
        status = modify_hive_config("hive.vectorized.groupby.checkinterval", "4096", "hive-site.xml");
        handle_result(status, "hive.vectorized.groupby.checkinterval", "4096", "hive-site.xml");
        status = modify_hive_config("hive.vectorized.groupby.flush.percent", "0.1", "hive-site.xml");
        handle_result(status, "hive.vectorized.groupby.flush.percent", "0.1", "hive-site.xml");
        status = modify_hive_config("hive.compute.query.using.stats", "true", "hive-site.xml");
        handle_result(status, "hive.compute.query.using.stats", "true", "hive-site.xml");
        status = modify_hive_config("hive.limit.pushdown.memory.usage", "0.04", "hive-site.xml");
        handle_result(status, "hive.limit.pushdown.memory.usage", "0.04", "hive-site.xml");

        //			 Tez Sessions
        status = modify_hive_config("hive.server2.tez.sessions.per.default.queue", "1", "hive-site.xml");
        handle_result(status, "hive.server2.tez.sessions.per.default.queue", "1", "hive-site.xml");
        status = modify_hive_config("hive.driver.parallel.compilation", "true", "hive-site.xml");
        handle_result(status, "hive.driver.parallel.compilation", "true", "hive-site.xml");
        status = modify_hive_config("hive.server2.tez.initialize.default.sessions", "false", "hive-site.xml");
        handle_result(status, "hive.server2.tez.initialize.default.sessions", "false", "hive-site.xml");
        status = modify_hive_config("hive.server2.tez.default.queues", "default", "hive-site.xml");
        handle_result(status, "hive.server2.tez.default.queues", "default", "hive-site.xml");

        //			 Web UI
        status = modify_hive_config("hive.server2.webui.port", "10002", "hive-site.xml");
        handle_result(status, "hive.server2.webui.port", "10002", "hive-site.xml");
        status = modify_hive_config("hive.server2.webui.use.ssl", "false", "hive-site.xml");
        handle_result(status, "hive.server2.webui.use.ssl", "false", "hive-site.xml");
        status = modify_hive_config("hive.server2.webui.enable.cors", "true", "hive-site.xml");
        handle_result(status, "hive.server2.webui.enable.cors", "true", "hive-site.xml");
        status = modify_hive_config("hive.server2.webui.cors.allowed.headers", "X-Requested-With,Content-Type,Accept,Origin,X-Requested-By,x-requested-by", "hive-site.xml");
        handle_result(status, "hive.server2.webui.cors.allowed.headers", "X-Requested-With,Content-Type,Accept,Origin,X-Requested-By,x-requested-by", "hive-site.xml");

        //			 ORC Strategy
        status = modify_hive_config("hive.exec.orc.split.strategy", "HYBRID", "hive-site.xml");
        handle_result(status, "hive.exec.orc.split.strategy", "HYBRID", "hive-site.xml");
        status = modify_hive_config("hive.vectorized.execution.reduce.enabled", "true", "hive-site.xml");
        handle_result(status, "hive.vectorized.execution.reduce.enabled", "true", "hive-site.xml");

        //			 Authentication Providers
        status = modify_hive_config("hive.server2.authentication.ldap.url", " ", "hive-site.xml");
        handle_result(status, "hive.server2.authentication.ldap.url", " ", "hive-site.xml");
        status = modify_hive_config("hive.server2.authentication.ldap.baseDN", "", "hive-site.xml");
        handle_result(status, "hive.server2.authentication.ldap.baseDN", "", "hive-site.xml");
        status = modify_hive_config("hive.server2.authentication.kerberos.keytab", "/etc/security/keytabs/hive.service.keytab", "hive-site.xml");
        handle_result(status, "hive.server2.authentication.kerberos.keytab", "/etc/security/keytabs/hive.service.keytab", "hive-site.xml");
        status = modify_hive_config("hive.server2.authentication.kerberos.principal", "hive/_HOST@EXAMPLE.COM", "hive-site.xml");
        handle_result(status, "hive.server2.authentication.kerberos.principal", "hive/_HOST@EXAMPLE.COM", "hive-site.xml");
        status = modify_hive_config("hive.server2.authentication.pam.services", "", "hive-site.xml");
        handle_result(status, "hive.server2.authentication.pam.services", "", "hive-site.xml");
        status = modify_hive_config("hive.server2.custom.authentication.class", "", "hive-site.xml");
        handle_result(status, "hive.server2.custom.authentication.class", "", "hive-site.xml");

        //			 DataNucleus
        status = modify_hive_config("datanucleus.autoCreateSchema", "false", "hive-site.xml");
        handle_result(status, "datanucleus.autoCreateSchema", "false", "hive-site.xml");
        status = modify_hive_config("datanucleus.fixedDatastore", "true", "hive-site.xml");
        handle_result(status, "datanucleus.fixedDatastore", "true", "hive-site.xml");

        //			 Managed Table Format
        status = modify_hive_config("hive.default.fileformat.managed", "ORC", "hive-site.xml");
        handle_result(status, "hive.default.fileformat.managed", "ORC", "hive-site.xml");

        //			 Atlas Hooks
        status = modify_hive_config("atlas.hook.hive.minThreads", "1", "hive-site.xml");
        handle_result(status, "atlas.hook.hive.minThreads", "1", "hive-site.xml");
        status = modify_hive_config("atlas.hook.hive.maxThreads", "1", "hive-site.xml");
        handle_result(status, "atlas.hook.hive.maxThreads", "1", "hive-site.xml");

        //			 Query Data Directory
        status = modify_hive_config("hive.hook.proto.base-directory", "{hive_metastore_warehouse_external_dir}/sys.db/query_data/", "hive-site.xml");
        handle_result(status, "hive.hook.proto.base-directory", "{hive_metastore_warehouse_external_dir}/sys.db/query_data/", "hive-site.xml");

        //			 Interactive Execution
        status = modify_hive_config("hive.execution.mode", "container", "hive-site.xml");
        handle_result(status, "hive.execution.mode", "container", "hive-site.xml");
        status = modify_hive_config("hive.tez.input.generate.consistent.splits", "true", "hive-site.xml");
        handle_result(status, "hive.tez.input.generate.consistent.splits", "true", "hive-site.xml");
        status = modify_hive_config("hive.tez.exec.print.summary", "true", "hive-site.xml");
        handle_result(status, "hive.tez.exec.print.summary", "true", "hive-site.xml");

        //			 Vectorized MapJoin
        status = modify_hive_config("hive.vectorized.execution.mapjoin.native.enabled", "true", "hive-site.xml");
        handle_result(status, "hive.vectorized.execution.mapjoin.native.enabled", "true", "hive-site.xml");
        status = modify_hive_config("hive.vectorized.execution.mapjoin.minmax.enabled", "true", "hive-site.xml");
        handle_result(status, "hive.vectorized.execution.mapjoin.minmax.enabled", "true", "hive-site.xml");
        status = modify_hive_config("hive.vectorized.execution.mapjoin.native.fast.hashtable.enabled", "true", "hive-site.xml");
        handle_result(status, "hive.vectorized.execution.mapjoin.native.fast.hashtable.enabled", "true", "hive-site.xml");

        //			 Dynamic Partition HashJoin
        status = modify_hive_config("hive.optimize.dynamic.partition.hashjoin", "true", "hive-site.xml");
        handle_result(status, "hive.optimize.dynamic.partition.hashjoin", "true", "hive-site.xml");

        //			 Event Listeners
        status = modify_hive_config("hive.metastore.event.listeners", "", "hive-site.xml");
        handle_result(status, "hive.metastore.event.listeners", "", "hive-site.xml");

        //			 MapJoin Hybrid
        status = modify_hive_config("hive.mapjoin.hybridgrace.hashtable", "false", "hive-site.xml");
        handle_result(status, "hive.mapjoin.hybridgrace.hashtable", "false", "hive-site.xml");

        //			 Tez Features
        status = modify_hive_config("hive.tez.cartesian-product.enabled", "true", "hive-site.xml");
        handle_result(status, "hive.tez.cartesian-product.enabled", "true", "hive-site.xml");
        status = modify_hive_config("hive.tez.bucket.pruning", "true", "hive-site.xml");
        handle_result(status, "hive.tez.bucket.pruning", "true", "hive-site.xml");

        //			 Metrics
        status = modify_hive_config("hive.service.metrics.codahale.reporter.classes", "org.apache.hadoop.hive.common.metrics.metrics2.JsonFileMetricsReporter,org.apache.hadoop.hive.common.metrics.metrics2.JmxMetricsReporter,org.apache.hadoop.hive.common.metrics.metrics2.Metrics2Reporter", "hive-site.xml");
        handle_result(status, "hive.service.metrics.codahale.reporter.classes", "org.apache.hadoop.hive.common.metrics.metrics2.JsonFileMetricsReporter,org.apache.hadoop.hive.common.metrics.metrics2.JmxMetricsReporter,org.apache.hadoop.hive.common.metrics.metrics2.Metrics2Reporter", "hive-site.xml");

        //			 Replication
        status = modify_hive_config("hive.metastore.dml.events", "true", "hive-site.xml");
        handle_result(status, "hive.metastore.dml.events", "true", "hive-site.xml");
        status = modify_hive_config("hive.repl.cm.enabled", "", "hive-site.xml");
        handle_result(status, "hive.repl.cm.enabled", "", "hive-site.xml");
        status = modify_hive_config("hive.metastore.transactional.event.listeners", "org.apache.hive.hcatalog.listener.DbNotificationListener", "hive-site.xml");
        handle_result(status, "hive.metastore.transactional.event.listeners", "org.apache.hive.hcatalog.listener.DbNotificationListener", "hive-site.xml");
        status = modify_hive_config("hive.repl.cmrootdir", "", "hive-site.xml");
        handle_result(status, "hive.repl.cmrootdir", "", "hive-site.xml");
        status = modify_hive_config("hive.repl.rootdir", "", "hive-site.xml");
        handle_result(status, "hive.repl.rootdir", "", "hive-site.xml");

        //			 Vectorized UDF
        status = modify_hive_config("hive.vectorized.adaptor.usage.mode", "chosen", "hive-site.xml");
        handle_result(status, "hive.vectorized.adaptor.usage.mode", "chosen", "hive-site.xml");

        //			 Metastore Metrics
        status = modify_hive_config("hive.metastore.metrics.enabled", "true", "hivemetastore-site.xml");
        handle_result(status, "hive.metastore.metrics.enabled", "true", "hivemetastore-site.xml");
        status = modify_hive_config("hive.server2.metrics.enabled", "true", "hivemetastore-site.xml");
        handle_result(status, "hive.server2.metrics.enabled", "true", "hivemetastore-site.xml");
        status = modify_hive_config("hive.service.metrics.reporter", "HADOOP2", "hivemetastore-site.xml");
        handle_result(status, "hive.service.metrics.reporter", "HADOOP2", "hivemetastore-site.xml");
        status = modify_hive_config("hive.service.metrics.hadoop2.component", "hivemetastore", "hivemetastore-site.xml");
        handle_result(status, "hive.service.metrics.hadoop2.component", "hivemetastore", "hivemetastore-site.xml");

        //			 Compaction Settings
        status = modify_hive_config("hive.compactor.initiator.on", "true", "hivemetastore-site.xml");
        handle_result(status, "hive.compactor.initiator.on", "true", "hivemetastore-site.xml");
        status = modify_hive_config("hive.compactor.worker.threads", "5", "hivemetastore-site.xml");
        handle_result(status, "hive.compactor.worker.threads", "5", "hivemetastore-site.xml");

        //			 Event System Configuration
        status = modify_hive_config("hive.metastore.dml.events", "true", "hivemetastore-site.xml");
        handle_result(status, "hive.metastore.dml.events", "true", "hivemetastore-site.xml");
        status = modify_hive_config("hive.metastore.transactional.event.listeners", "org.apache.hive.hcatalog.listener.DbNotificationListener", "hivemetastore-site.xml");
        handle_result(status, "hive.metastore.transactional.event.listeners", "org.apache.hive.hcatalog.listener.DbNotificationListener", "hivemetastore-site.xml");
        status = modify_hive_config("hive.metastore.event.listeners", "", "hivemetastore-site.xml");
        handle_result(status, "hive.metastore.event.listeners", "", "hivemetastore-site.xml");
        //			 Security Configuration
        status = modify_hive_config("hive.security.authenticator.manager", "org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator", "hiveserver2-site.xml");
        handle_result(status, "hive.security.authenticator.manager", "org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator", "hiveserver2-site.xml");
        status = modify_hive_config("hive.security.authorization.manager", "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory", "hiveserver2-site.xml");
        handle_result(status, "hive.security.authorization.manager", "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory", "hiveserver2-site.xml");
        status = modify_hive_config("hive.security.authorization.enabled", "false", "hiveserver2-site.xml");
        handle_result(status, "hive.security.authorization.enabled", "false", "hiveserver2-site.xml");
        status = modify_hive_config("hive.conf.restricted.list", "hive.security.authenticator.manager,hive.security.authorization.manager,hive.users.in.admin.role", "hiveserver2-site.xml");
        handle_result(status, "hive.conf.restricted.list", "hive.security.authenticator.manager,hive.security.authorization.manager,hive.users.in.admin.role", "hiveserver2-site.xml");

        //			 Metrics Collection
        status = modify_hive_config("hive.metastore.metrics.enabled", "true", "hiveserver2-site.xml");
        handle_result(status, "hive.metastore.metrics.enabled", "true", "hiveserver2-site.xml");
        status = modify_hive_config("hive.server2.metrics.enabled", "true", "hiveserver2-site.xml");
        handle_result(status, "hive.server2.metrics.enabled", "true", "hiveserver2-site.xml");
        status = modify_hive_config("hive.service.metrics.reporter", "HADOOP2", "hiveserver2-site.xml");
        handle_result(status, "hive.service.metrics.reporter", "HADOOP2", "hiveserver2-site.xml");
        status = modify_hive_config("hive.service.metrics.hadoop2.component", "hiveserver2", "hiveserver2-site.xml");
        handle_result(status, "hive.service.metrics.hadoop2.component", "hiveserver2", "hiveserver2-site.xml");
        //			 Standalone log rotation settings
        status = modify_hive_config("llap_cli_log_maxfilesize", "256", "hive-site.xml");
        handle_result(status, "llap_cli_log_maxfilesize", "256", "hive-site.xml");
        status = modify_hive_config("llap_cli_log_maxbackupindex", "30", "hive-site.xml");
        handle_result(status, "llap_cli_log_maxbackupindex", "30", "hive-site.xml");

        //			 Log4j2 configuration content
        status = modify_hive_config("status", "WARN", "llap-cli-log4j2.properties");
        handle_result(status, "status", "WARN", "llap-cli-log4j2.properties");
        status = modify_hive_config("name", "LlapCliLog4j2", "llap-cli-log4j2.properties");
        handle_result(status, "name", "LlapCliLog4j2", "llap-cli-log4j2.properties");
        status = modify_hive_config("packages", "org.apache.hadoop.hive.ql.log", "llap-cli-log4j2.properties");
        handle_result(status, "packages", "org.apache.hadoop.hive.ql.log", "llap-cli-log4j2.properties");
        status = modify_hive_config("property.hive.log.level", "WARN", "llap-cli-log4j2.properties");
        handle_result(status, "property.hive.log.level", "WARN", "llap-cli-log4j2.properties");
        status = modify_hive_config("property.hive.root.logger", "console", "llap-cli-log4j2.properties");
        handle_result(status, "property.hive.root.logger", "console", "llap-cli-log4j2.properties");
        status = modify_hive_config("property.hive.log.dir", "${sys:java.io.tmpdir}/${sys:user.name}", "llap-cli-log4j2.properties");
        handle_result(status, "property.hive.log.dir", "${sys:java.io.tmpdir}/${sys:user.name}", "llap-cli-log4j2.properties");
        status = modify_hive_config("property.hive.log.file", "llap-cli.log", "llap-cli-log4j2.properties");
        handle_result(status, "property.hive.log.file", "llap-cli.log", "llap-cli-log4j2.properties");
        status = modify_hive_config("property.hive.llapstatus.consolelogger.level", "INFO", "llap-cli-log4j2.properties");
        handle_result(status, "property.hive.llapstatus.consolelogger.level", "INFO", "llap-cli-log4j2.properties");
        status = modify_hive_config("appenders", "console, DRFA, llapstatusconsole", "llap-cli-log4j2.properties");
        handle_result(status, "appenders", "console, DRFA, llapstatusconsole", "llap-cli-log4j2.properties");
        status = modify_hive_config("appender.console.type", "Console", "llap-cli-log4j2.properties");
        handle_result(status, "appender.console.type", "Console", "llap-cli-log4j2.properties");
        status = modify_hive_config("appender.console.name", "console", "llap-cli-log4j2.properties");
        handle_result(status, "appender.console.name", "console", "llap-cli-log4j2.properties");
        status = modify_hive_config("appender.console.target", "SYSTEM_ERR", "llap-cli-log4j2.properties");
        handle_result(status, "appender.console.target", "SYSTEM_ERR", "llap-cli-log4j2.properties");
        status = modify_hive_config("appender.console.layout.type", "PatternLayout", "llap-cli-log4j2.properties");
        handle_result(status, "appender.console.layout.type", "PatternLayout", "llap-cli-log4j2.properties");
        status = modify_hive_config("appender.console.layout.pattern", "%p %c{2}: %m%n", "llap-cli-log4j2.properties");
        handle_result(status, "appender.console.layout.pattern", "%p %c{2}: %m%n", "llap-cli-log4j2.properties");
        status = modify_hive_config("appender.llapstatusconsole.type", "Console", "llap-cli-log4j2.properties");
        handle_result(status, "appender.llapstatusconsole.type", "Console", "llap-cli-log4j2.properties");
        status = modify_hive_config("appender.llapstatusconsole.name", "llapstatusconsole", "llap-cli-log4j2.properties");
        handle_result(status, "appender.llapstatusconsole.name", "llapstatusconsole", "llap-cli-log4j2.properties");
        status = modify_hive_config("appender.llapstatusconsole.target", "SYSTEM_OUT", "llap-cli-log4j2.properties");
        handle_result(status, "appender.llapstatusconsole.target", "SYSTEM_OUT", "llap-cli-log4j2.properties");
        status = modify_hive_config("appender.llapstatusconsole.layout.type", "PatternLayout", "llap-cli-log4j2.properties");
        handle_result(status, "appender.llapstatusconsole.layout.type", "PatternLayout", "llap-cli-log4j2.properties");
        status = modify_hive_config("appender.llapstatusconsole.layout.pattern", "%m%n", "llap-cli-log4j2.properties");
        handle_result(status, "appender.llapstatusconsole.layout.pattern", "%m%n", "llap-cli-log4j2.properties");
        status = modify_hive_config("appender.DRFA.type", "RollingRandomAccessFile", "llap-cli-log4j2.properties");
        handle_result(status, "appender.DRFA.type", "RollingRandomAccessFile", "llap-cli-log4j2.properties");
        handle_result(status, "appender.DRFA.name", "DRFA", "llap-cli-log4j2.properties");
        status = modify_hive_config("appender.DRFA.fileName", "${sys:hive.log.dir}/${sys:hive.log.file}", "llap-cli-log4j2.properties");
        handle_result(status, "appender.DRFA.fileName", "${sys:hive.log.dir}/${sys:hive.log.file}", "llap-cli-log4j2.properties");
        status = modify_hive_config("appender.DRFA.filePattern", "${sys:hive.log.dir}/${sys:hive.log.file}.%d{yyyy-MM-dd}_%i", "llap-cli-log4j2.properties");
        handle_result(status, "appender.DRFA.filePattern", "${sys:hive.log.dir}/${sys:hive.log.file}.%d{yyyy-MM-dd}_%i", "llap-cli-log4j2.properties");
        status = modify_hive_config("appender.DRFA.layout.type", "PatternLayout", "llap-cli-log4j2.properties");
        handle_result(status, "appender.DRFA.layout.type", "PatternLayout", "llap-cli-log4j2.properties");
        status = modify_hive_config("appender.DRFA.layout.pattern", "%d{ISO8601} %5p [%t] %c{2}: %m%n", "llap-cli-log4j2.properties");
        handle_result(status, "appender.DRFA.layout.pattern", "%d{ISO8601} %5p [%t] %c{2}: %m%n", "llap-cli-log4j2.properties");
        status = modify_hive_config("appender.DRFA.policies.type", "Policies", "llap-cli-log4j2.properties");
        handle_result(status, "appender.DRFA.policies.type", "Policies", "llap-cli-log4j2.properties");
        status = modify_hive_config("appender.DRFA.policies.time.type", "TimeBasedTriggeringPolicy", "llap-cli-log4j2.properties");
        handle_result(status, "appender.DRFA.policies.time.type", "TimeBasedTriggeringPolicy", "llap-cli-log4j2.properties");
        status = modify_hive_config("appender.DRFA.policies.time.interval", "1", "llap-cli-log4j2.properties");
        handle_result(status, "appender.DRFA.policies.time.interval", "1", "llap-cli-log4j2.properties");
        status = modify_hive_config("appender.DRFA.policies.time.modulate", "true", "llap-cli-log4j2.properties");
        handle_result(status, "appender.DRFA.policies.time.modulate", "true", "llap-cli-log4j2.properties");
        status = modify_hive_config("appender.DRFA.strategy.type", "DefaultRolloverStrategy", "llap-cli-log4j2.properties");
        handle_result(status, "appender.DRFA.strategy.type", "DefaultRolloverStrategy", "llap-cli-log4j2.properties");
        status = modify_hive_config("appender.DRFA.strategy.max", "{{llap_cli_log_maxbackupindex}", "llap-cli-log4j2.properties");
        handle_result(status, "appender.DRFA.strategy.max", "{{llap_cli_log_maxbackupindex}", "llap-cli-log4j2.properties");
        status = modify_hive_config("appender.DRFA.policies.fsize.type", "SizeBasedTriggeringPolicy", "llap-cli-log4j2.properties");
        handle_result(status, "appender.DRFA.policies.fsize.type", "SizeBasedTriggeringPolicy", "llap-cli-log4j2.properties");
        status = modify_hive_config("appender.DRFA.policies.fsize.size", "{{llap_cli_log_maxfilesize}}MB", "llap-cli-log4j2.properties");
        handle_result(status, "appender.DRFA.policies.fsize.size", "{{llap_cli_log_maxfilesize}}MB", "llap-cli-log4j2.properties");
        status = modify_hive_config("loggers", "ZooKeeper, DataNucleus, Datastore, JPOX, HadoopConf, LlapStatusServiceDriverConsole", "llap-cli-log4j2.properties");
        handle_result(status, "loggers", "ZooKeeper, DataNucleus, Datastore, JPOX, HadoopConf, LlapStatusServiceDriverConsole", "llap-cli-log4j2.properties");
        status = modify_hive_config("logger.ZooKeeper.name", "org.apache.zookeeper", "llap-cli-log4j2.properties");
        handle_result(status, "logger.ZooKeeper.name", "org.apache.zookeeper", "llap-cli-log4j2.properties");
        status = modify_hive_config("logger.ZooKeeper.level", "WARN", "llap-cli-log4j2.properties");
        handle_result(status, "logger.ZooKeeper.level", "WARN", "llap-cli-log4j2.properties");
        status = modify_hive_config("logger.DataNucleus.name", "DataNucleus", "llap-cli-log4j2.properties");
        handle_result(status, "logger.DataNucleus.name", "DataNucleus", "llap-cli-log4j2.properties");
        status = modify_hive_config("logger.DataNucleus.level", "ERROR", "llap-cli-log4j2.properties");
        handle_result(status, "logger.DataNucleus.level", "ERROR", "llap-cli-log4j2.properties");
        status = modify_hive_config("logger.Datastore.name", "Datastore", "llap-cli-log4j2.properties");
        handle_result(status, "logger.Datastore.name", "Datastore", "llap-cli-log4j2.properties");
        status = modify_hive_config("logger.Datastore.level", "ERROR", "llap-cli-log4j2.properties");
        handle_result(status, "logger.Datastore.level", "ERROR", "llap-cli-log4j2.properties");
        status = modify_hive_config("logger.JPOX.name", "JPOX", "llap-cli-log4j2.properties");
        handle_result(status, "logger.JPOX.name", "JPOX", "llap-cli-log4j2.properties");
        status = modify_hive_config("logger.JPOX.level", "ERROR", "llap-cli-log4j2.properties");
        handle_result(status, "logger.JPOX.level", "ERROR", "llap-cli-log4j2.properties");
        status = modify_hive_config("logger.HadoopConf.name", "org.apache.hadoop.conf.Configuration", "llap-cli-log4j2.properties");
        handle_result(status, "logger.HadoopConf.name", "org.apache.hadoop.conf.Configuration", "llap-cli-log4j2.properties");
        status = modify_hive_config("logger.HadoopConf.level", "ERROR", "llap-cli-log4j2.properties");
        handle_result(status, "logger.HadoopConf.level", "ERROR", "llap-cli-log4j2.properties");
        status = modify_hive_config("logger.LlapStatusServiceDriverConsole.name", "LlapStatusServiceDriverConsole", "llap-cli-log4j2.properties");
        handle_result(status, "logger.LlapStatusServiceDriverConsole.name", "LlapStatusServiceDriverConsole", "llap-cli-log4j2.properties");
        status = modify_hive_config("logger.LlapStatusServiceDriverConsole.additivity", "false", "llap-cli-log4j2.properties");
        handle_result(status, "logger.LlapStatusServiceDriverConsole.additivity", "false", "llap-cli-log4j2.properties");
        status = modify_hive_config("logger.LlapStatusServiceDriverConsole.level", "${sys:hive.llapstatus.consolelogger.level}", "llap-cli-log4j2.properties");
        handle_result(status, "logger.LlapStatusServiceDriverConsole.level", "${sys:hive.llapstatus.consolelogger.level}", "llap-cli-log4j2.properties");
        status = modify_hive_config("rootLogger.level", "${sys:hive.log.level}", "llap-cli-log4j2.properties");
        handle_result(status, "rootLogger.level", "${sys:hive.log.level}", "llap-cli-log4j2.properties");
        status = modify_hive_config("rootLogger.appenderRefs", "root, DRFA", "llap-cli-log4j2.properties");
        handle_result(status, "rootLogger.appenderRefs", "root, DRFA", "llap-cli-log4j2.properties");
        status = modify_hive_config("rootLogger.appenderRef.root.ref", "${sys:hive.root.logger}", "llap-cli-log4j2.properties");
        handle_result(status, "rootLogger.appenderRef.root.ref", "${sys:hive.root.logger}", "llap-cli-log4j2.properties");
        status = modify_hive_config("rootLogger.appenderRef.DRFA.ref", "DRFA", "llap-cli-log4j2.properties");
        handle_result(status, "rootLogger.appenderRef.DRFA.ref", "DRFA", "llap-cli-log4j2.properties");
        status = modify_hive_config("logger.LlapStatusServiceDriverConsole.appenderRefs", "llapstatusconsole, DRFA", "llap-cli-log4j2.properties");
        handle_result(status, "logger.LlapStatusServiceDriverConsole.appenderRefs", "llapstatusconsole, DRFA", "llap-cli-log4j2.properties");
        status = modify_hive_config("logger.LlapStatusServiceDriverConsole.appenderRef.llapstatusconsole.ref", "llapstatusconsole", "llap-cli-log4j2.properties");
        handle_result(status, "logger.LlapStatusServiceDriverConsole.appenderRef.llapstatusconsole.ref", "llapstatusconsole", "llap-cli-log4j2.properties");
        status = modify_hive_config("logger.LlapStatusServiceDriverConsole.appenderRef.DRFA.ref", "DRFA", "llap-cli-log4j2.properties");
        handle_result(status, "logger.LlapStatusServiceDriverConsole.appenderRef.DRFA.ref", "DRFA", "llap-cli-log4j2.properties");

        //			 Standalone log rotation settings
        status = modify_hive_config("hive_llap_log_maxfilesize", "256", "hive-site.xml");
        handle_result(status, "hive_llap_log_maxfilesize", "256", "hive-site.xml");
        status = modify_hive_config("hive_llap_log_maxbackupindex", "240", "hive-site.xml");
        handle_result(status, "hive_llap_log_maxbackupindex", "240", "hive-site.xml");

        //			 Log4j2 configuration content
        status = modify_hive_config("status", "INFO", "llap-daemon-log4j2.properties");
        handle_result(status, "status", "INFO", "llap-daemon-log4j2.properties");
        status = modify_hive_config("name", "LlapDaemonLog4j2", "llap-daemon-log4j2.properties");
        handle_result(status, "name", "LlapDaemonLog4j2", "llap-daemon-log4j2.properties");
        status = modify_hive_config("packages", "org.apache.hadoop.hive.ql.log", "llap-daemon-log4j2.properties");
        handle_result(status, "packages", "org.apache.hadoop.hive.ql.log", "llap-daemon-log4j2.properties");
        status = modify_hive_config("property.llap.daemon.log.level", "{{hive_log_level}}", "llap-daemon-log4j2.properties");
        handle_result(status, "property.llap.daemon.log.level", "{{hive_log_level}}", "llap-daemon-log4j2.properties");
        status = modify_hive_config("property.llap.daemon.root.logger", "console", "llap-daemon-log4j2.properties");
        handle_result(status, "property.llap.daemon.root.logger", "console", "llap-daemon-log4j2.properties");
        status = modify_hive_config("property.llap.daemon.log.dir", ".", "llap-daemon-log4j2.properties");
        handle_result(status, "property.llap.daemon.log.dir", ".", "llap-daemon-log4j2.properties");
        status = modify_hive_config("property.llap.daemon.log.file", "llapdaemon.log", "llap-daemon-log4j2.properties");
        handle_result(status, "property.llap.daemon.log.file", "llapdaemon.log", "llap-daemon-log4j2.properties");
        status = modify_hive_config("property.llap.daemon.historylog.file", "llapdaemon_history.log", "llap-daemon-log4j2.properties");
        handle_result(status, "property.llap.daemon.historylog.file", "llapdaemon_history.log", "llap-daemon-log4j2.properties");
        status = modify_hive_config("property.llap.daemon.log.maxfilesize", "{{hive_llap_log_maxfilesize}}MB", "llap-daemon-log4j2.properties");
        handle_result(status, "property.llap.daemon.log.maxfilesize", "{{hive_llap_log_maxfilesize}}MB", "llap-daemon-log4j2.properties");
        status = modify_hive_config("property.llap.daemon.log.maxbackupindex", "{{hive_llap_log_maxbackupindex}}", "llap-daemon-log4j2.properties");
        handle_result(status, "property.llap.daemon.log.maxbackupindex", "{{hive_llap_log_maxbackupindex}}", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appenders", "console, RFA, HISTORYAPPENDER, query-routing", "llap-daemon-log4j2.properties");
        handle_result(status, "appenders", "console, RFA, HISTORYAPPENDER, query-routing", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.console.type", "Console", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.console.type", "Console", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.console.name", "console", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.console.name", "console", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.console.target", "SYSTEM_ERR", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.console.target", "SYSTEM_ERR", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.console.layout.type", "PatternLayout", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.console.layout.type", "PatternLayout", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.console.layout.pattern", "%d{ISO8601} %5p [%t (%X{fragmentId})] %c{2}: %m%n", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.console.layout.pattern", "%d{ISO8601} %5p [%t (%X{fragmentId})] %c{2}: %m%n", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.RFA.type", "RollingRandomAccessFile", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.RFA.type", "RollingRandomAccessFile", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.RFA.name", "RFA", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.RFA.name", "RFA", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.RFA.fileName", "${sys:llap.daemon.log.dir}/${sys:llap.daemon.log.file}", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.RFA.fileName", "${sys:llap.daemon.log.dir}/${sys:llap.daemon.log.file}", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.RFA.filePattern", "${sys:llap.daemon.log.dir}/${sys:llap.daemon.log.file}_%d{yyyy-MM-dd-HH}_%i.done", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.RFA.filePattern", "${sys:llap.daemon.log.dir}/${sys:llap.daemon.log.file}_%d{yyyy-MM-dd-HH}_%i.done", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.RFA.layout.type", "PatternLayout", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.RFA.layout.type", "PatternLayout", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.RFA.layout.pattern", "%d{ISO8601} %-5p [%t (%X{fragmentId})] %c: %m%n", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.RFA.layout.pattern", "%d{ISO8601} %-5p [%t (%X{fragmentId})] %c: %m%n", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.RFA.policies.type", "Policies", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.RFA.policies.type", "Policies", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.RFA.policies.time.type", "TimeBasedTriggeringPolicy", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.RFA.policies.time.type", "TimeBasedTriggeringPolicy", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.RFA.policies.time.interval", "1", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.RFA.policies.time.interval", "1", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.RFA.policies.time.modulate", "true", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.RFA.policies.time.modulate", "true", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.RFA.policies.size.type", "SizeBasedTriggeringPolicy", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.RFA.policies.size.type", "SizeBasedTriggeringPolicy", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.RFA.policies.size.size", "${sys:llap.daemon.log.maxfilesize}", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.RFA.policies.size.size", "${sys:llap.daemon.log.maxfilesize}", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.RFA.strategy.type", "DefaultRolloverStrategy", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.RFA.strategy.type", "DefaultRolloverStrategy", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.RFA.strategy.max", "${sys:llap.daemon.log.maxbackupindex}", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.RFA.strategy.max", "${sys:llap.daemon.log.maxbackupindex}", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.HISTORYAPPENDER.type", "RollingRandomAccessFile", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.HISTORYAPPENDER.type", "RollingRandomAccessFile", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.HISTORYAPPENDER.name", "HISTORYAPPENDER", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.HISTORYAPPENDER.name", "HISTORYAPPENDER", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.HISTORYAPPENDER.fileName", "${sys:llap.daemon.log.dir}/${sys:llap.daemon.historylog.file}", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.HISTORYAPPENDER.fileName", "${sys:llap.daemon.log.dir}/${sys:llap.daemon.historylog.file}", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.HISTORYAPPENDER.filePattern", "${sys:llap.daemon.log.dir}/${sys:llap.daemon.historylog.file}_%d{yyyy-MM-dd}_%i.done", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.HISTORYAPPENDER.filePattern", "${sys:llap.daemon.log.dir}/${sys:llap.daemon.historylog.file}_%d{yyyy-MM-dd}_%i.done", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.HISTORYAPPENDER.layout.type", "PatternLayout", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.HISTORYAPPENDER.layout.type", "PatternLayout", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.HISTORYAPPENDER.layout.pattern", "%m%n", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.HISTORYAPPENDER.layout.pattern", "%m%n", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.HISTORYAPPENDER.policies.type", "Policies", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.HISTORYAPPENDER.policies.type", "Policies", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.HISTORYAPPENDER.policies.size.type", "SizeBasedTriggeringPolicy", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.HISTORYAPPENDER.policies.size.type", "SizeBasedTriggeringPolicy", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.HISTORYAPPENDER.policies.size.size", "${sys:llap.daemon.log.maxfilesize}", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.HISTORYAPPENDER.policies.size.size", "${sys:llap.daemon.log.maxfilesize}", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.HISTORYAPPENDER.policies.time.type", "TimeBasedTriggeringPolicy", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.HISTORYAPPENDER.policies.time.type", "TimeBasedTriggeringPolicy", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.HISTORYAPPENDER.policies.time.interval", "1", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.HISTORYAPPENDER.policies.time.interval", "1", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.HISTORYAPPENDER.policies.time.modulate", "true", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.HISTORYAPPENDER.policies.time.modulate", "true", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.HISTORYAPPENDER.strategy.type", "DefaultRolloverStrategy", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.HISTORYAPPENDER.strategy.type", "DefaultRolloverStrategy", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.HISTORYAPPENDER.strategy.max", "${sys:llap.daemon.log.maxbackupindex}", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.HISTORYAPPENDER.strategy.max", "${sys:llap.daemon.log.maxbackupindex}", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.type", "Routing", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.type", "Routing", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.name", "query-routing", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.name", "query-routing", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.routes.type", "Routes", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.routes.type", "Routes", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.routes.pattern", "$${ctx:queryId}", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.routes.pattern", "$${ctx:queryId}", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.purgePolicy.type", "LlapRoutingAppenderPurgePolicy", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.purgePolicy.type", "LlapRoutingAppenderPurgePolicy", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.purgePolicy.name", "llapLogPurgerQueryRouting", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.purgePolicy.name", "llapLogPurgerQueryRouting", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.routes.route-default.type", "Route", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.routes.route-default.type", "Route", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.routes.route-default.key", "$${ctx:queryId}", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.routes.route-default.key", "$${ctx:queryId}", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.routes.route-default.ref", "RFA", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.routes.route-default.ref", "RFA", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.routes.route-mdc.type", "Route", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.routes.route-mdc.type", "Route", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.routes.route-mdc.file-mdc.type", "LlapWrappedAppender", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.routes.route-mdc.file-mdc.type", "LlapWrappedAppender", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.routes.route-mdc.file-mdc.name", "IrrelevantName-query-routing", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.routes.route-mdc.file-mdc.name", "IrrelevantName-query-routing", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.routes.route-mdc.file-mdc.app.type", "RandomAccessFile", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.routes.route-mdc.file-mdc.app.type", "RandomAccessFile", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.routes.route-mdc.file-mdc.app.name", "file-mdc", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.routes.route-mdc.file-mdc.app.name", "file-mdc", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.routes.route-mdc.file-mdc.app.fileName", "${sys:llap.daemon.log.dir}/${ctx:queryId}-${ctx:dagId}.log", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.routes.route-mdc.file-mdc.app.fileName", "${sys:llap.daemon.log.dir}/${ctx:queryId}-${ctx:dagId}.log", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.routes.route-mdc.file-mdc.app.layout.type", "PatternLayout", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.routes.route-mdc.file-mdc.app.layout.type", "PatternLayout", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.routes.route-mdc.file-mdc.app.layout.pattern", "%d{ISO8601} %5p [%t (%X{fragmentId})] %c{2}: %m%n", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.routes.route-mdc.file-mdc.app.layout.pattern", "%d{ISO8601} %5p [%t (%X{fragmentId})] %c{2}: %m%n", "llap-daemon-log4j2.properties");
        status = modify_hive_config("loggers", "PerfLogger, EncodedReader, NIOServerCnxn, ClientCnxnSocketNIO, DataNucleus, Datastore, JPOX, HistoryLogger, LlapIoImpl, LlapIoOrc, LlapIoCache, LlapIoLocking, TezSM, TezSS, TezHC", "llap-daemon-log4j2.properties");
        handle_result(status, "loggers", "PerfLogger, EncodedReader, NIOServerCnxn, ClientCnxnSocketNIO, DataNucleus, Datastore, JPOX, HistoryLogger, LlapIoImpl, LlapIoOrc, LlapIoCache, LlapIoLocking, TezSM, TezSS, TezHC", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.TezSM.name", "org.apache.tez.runtime.library.common.shuffle.impl.ShuffleManager.fetch", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.TezSM.name", "org.apache.tez.runtime.library.common.shuffle.impl.ShuffleManager.fetch", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.TezSM.level", "WARN", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.TezSM.level", "WARN", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.TezSS.name", "org.apache.tez.runtime.library.common.shuffle.orderedgrouped.ShuffleScheduler.fetch", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.TezSS.name", "org.apache.tez.runtime.library.common.shuffle.orderedgrouped.ShuffleScheduler.fetch", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.TezSS.level", "WARN", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.TezSS.level", "WARN", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.TezHC.name", "org.apache.tez.http.HttpConnection.url", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.TezHC.name", "org.apache.tez.http.HttpConnection.url", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.TezHC.level", "WARN", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.TezHC.level", "WARN", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.PerfLogger.name", "org.apache.hadoop.hive.ql.log.PerfLogger", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.PerfLogger.name", "org.apache.hadoop.hive.ql.log.PerfLogger", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.PerfLogger.level", "DEBUG", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.PerfLogger.level", "DEBUG", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.EncodedReader.name", "org.apache.hadoop.hive.ql.io.orc.encoded.EncodedReaderImpl", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.EncodedReader.name", "org.apache.hadoop.hive.ql.io.orc.encoded.EncodedReaderImpl", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.EncodedReader.level", "INFO", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.EncodedReader.level", "INFO", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.LlapIoImpl.name", "LlapIoImpl", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.LlapIoImpl.name", "LlapIoImpl", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.LlapIoImpl.level", "INFO", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.LlapIoImpl.level", "INFO", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.LlapIoOrc.name", "LlapIoOrc", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.LlapIoOrc.name", "LlapIoOrc", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.LlapIoOrc.level", "WARN", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.LlapIoOrc.level", "WARN", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.LlapIoCache.name", "LlapIoCache", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.LlapIoCache.name", "LlapIoCache", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.LlapIoCache.level", "WARN", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.LlapIoCache.level", "WARN", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.LlapIoLocking.name", "LlapIoLocking", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.LlapIoLocking.name", "LlapIoLocking", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.LlapIoLocking.level", "WARN", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.LlapIoLocking.level", "WARN", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.NIOServerCnxn.name", "org.apache.zookeeper.server.NIOServerCnxn", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.NIOServerCnxn.name", "org.apache.zookeeper.server.NIOServerCnxn", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.NIOServerCnxn.level", "WARN", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.NIOServerCnxn.level", "WARN", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.ClientCnxnSocketNIO.name", "org.apache.zookeeper.ClientCnxnSocketNIO", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.ClientCnxnSocketNIO.name", "org.apache.zookeeper.ClientCnxnSocketNIO", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.ClientCnxnSocketNIO.level", "WARN", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.ClientCnxnSocketNIO.level", "WARN", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.DataNucleus.name", "DataNucleus", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.DataNucleus.name", "DataNucleus", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.DataNucleus.level", "ERROR", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.DataNucleus.level", "ERROR", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.Datastore.name", "Datastore", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.Datastore.name", "Datastore", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.Datastore.level", "ERROR", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.Datastore.level", "ERROR", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.JPOX.name", "JPOX", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.JPOX.name", "JPOX", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.JPOX.level", "ERROR", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.JPOX.level", "ERROR", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.HistoryLogger.name", "org.apache.hadoop.hive.llap.daemon.HistoryLogger", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.HistoryLogger.name", "org.apache.hadoop.hive.llap.daemon.HistoryLogger", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.HistoryLogger.level", "INFO", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.HistoryLogger.level", "INFO", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.HistoryLogger.additivity", "false", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.HistoryLogger.additivity", "false", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.HistoryLogger.appenderRefs", "HistoryAppender", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.HistoryLogger.appenderRefs", "HistoryAppender", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.HistoryLogger.appenderRef.HistoryAppender.ref", "HISTORYAPPENDER", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.HistoryLogger.appenderRef.HistoryAppender.ref", "HISTORYAPPENDER", "llap-daemon-log4j2.properties");
        status = modify_hive_config("rootLogger.level", "${sys:llap.daemon.log.level}", "llap-daemon-log4j2.properties");
        handle_result(status, "rootLogger.level", "${sys:llap.daemon.log.level}", "llap-daemon-log4j2.properties");
        status = modify_hive_config("rootLogger.appenderRefs", "root", "llap-daemon-log4j2.properties");
        handle_result(status, "rootLogger.appenderRefs", "root", "llap-daemon-log4j2.properties");
        status = modify_hive_config("rootLogger.appenderRef.root.ref", "${sys:llap.daemon.root.logger}", "llap-daemon-log4j2.properties");
        handle_result(status, "rootLogger.appenderRef.root.ref", "${sys:llap.daemon.root.logger}", "llap-daemon-log4j2.properties");
        //			 ... continuing from previous response

        //			 Standalone log rotation settings
        status = modify_hive_config("hive_llap_log_maxfilesize", "256", "hive-site.xml");
        handle_result(status, "hive_llap_log_maxfilesize", "256", "hive-site.xml");
        status = modify_hive_config("hive_llap_log_maxbackupindex", "240", "hive-site.xml");
        handle_result(status, "hive_llap_log_maxbackupindex", "240", "hive-site.xml");

        //			 Log4j2 configuration content
        status = modify_hive_config("status", "INFO", "llap-daemon-log4j2.properties");
        handle_result(status, "status", "INFO", "llap-daemon-log4j2.properties");
        status = modify_hive_config("name", "LlapDaemonLog4j2", "llap-daemon-log4j2.properties");
        handle_result(status, "name", "LlapDaemonLog4j2", "llap-daemon-log4j2.properties");
        status = modify_hive_config("packages", "org.apache.hadoop.hive.ql.log", "llap-daemon-log4j2.properties");
        handle_result(status, "packages", "org.apache.hadoop.hive.ql.log", "llap-daemon-log4j2.properties");
        status = modify_hive_config("property.llap.daemon.log.level", "{{hive_log_level}}", "llap-daemon-log4j2.properties");
        handle_result(status, "property.llap.daemon.log.level", "{{hive_log_level}}", "llap-daemon-log4j2.properties");
        status = modify_hive_config("property.llap.daemon.root.logger", "console", "llap-daemon-log4j2.properties");
        handle_result(status, "property.llap.daemon.root.logger", "console", "llap-daemon-log4j2.properties");
        status = modify_hive_config("property.llap.daemon.log.dir", ".", "llap-daemon-log4j2.properties");
        handle_result(status, "property.llap.daemon.log.dir", ".", "llap-daemon-log4j2.properties");
        status = modify_hive_config("property.llap.daemon.log.file", "llapdaemon.log", "llap-daemon-log4j2.properties");
        handle_result(status, "property.llap.daemon.log.file", "llapdaemon.log", "llap-daemon-log4j2.properties");
        status = modify_hive_config("property.llap.daemon.historylog.file", "llapdaemon_history.log", "llap-daemon-log4j2.properties");
        handle_result(status, "property.llap.daemon.historylog.file", "llapdaemon_history.log", "llap-daemon-log4j2.properties");
        status = modify_hive_config("property.llap.daemon.log.maxfilesize", "{{hive_llap_log_maxfilesize}}MB", "llap-daemon-log4j2.properties");
        handle_result(status, "property.llap.daemon.log.maxfilesize", "{{hive_llap_log_maxfilesize}}MB", "llap-daemon-log4j2.properties");
        status = modify_hive_config("property.llap.daemon.log.maxbackupindex", "{{hive_llap_log_maxbackupindex}}", "llap-daemon-log4j2.properties");
        handle_result(status, "property.llap.daemon.log.maxbackupindex", "{{hive_llap_log_maxbackupindex}}", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appenders", "console, RFA, HISTORYAPPENDER, query-routing", "llap-daemon-log4j2.properties");
        handle_result(status, "appenders", "console, RFA, HISTORYAPPENDER, query-routing", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.console.type", "Console", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.console.type", "Console", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.console.name", "console", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.console.name", "console", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.console.target", "SYSTEM_ERR", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.console.target", "SYSTEM_ERR", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.console.layout.type", "PatternLayout", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.console.layout.type", "PatternLayout", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.console.layout.pattern", "%d{ISO8601} %5p [%t (%X{fragmentId})] %c{2}: %m%n", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.console.layout.pattern", "%d{ISO8601} %5 [%t (%X{fragmentId})] %c{2}: %m%n", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.RFA.type", "RollingRandomAccessFile", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.RFA.type", "RollingRandomAccessFile", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.RFA.name", "RFA", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.RFA.name", "RFA", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.RFA.fileName", "${sys:llap.daemon.log.dir}/${sys:llap.daemon.log.file}", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.RFA.fileName", "${sys:llap.daemon.log.dir}/${sys:llap.daemon.log.file}", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.RFA.filePattern", "${sys:llap.daemon.log.dir}/${sys:llap.daemon.log.file}_%d{yyyy-MM-dd-HH}_%i.done", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.RFA.filePattern", "${sys:llap.daemon.log.dir}/${sys:llap.daemon.log.file}_%d{yyyy-MM-dd-HH}_%i.done", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.RFA.layout.type", "PatternLayout", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.RFA.layout.type", "PatternLayout", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.RFA.layout.pattern", "%d{ISO8601} %-5p [%t (%X{fragmentId})] %c: %m%n", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.RFA.layout.pattern", "%d{ISO8601} %-5p [%t (%X{fragmentId})] %c: %m%n", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.RFA.policies.type", "Policies", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.RFA.policies.type", "Policies", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.RFA.policies.time.type", "TimeBasedTriggeringPolicy", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.RFA.policies.time.type", "TimeBasedTriggeringPolicy", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.RFA.policies.time.interval", "1", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.RFA.policies.time.interval", "1", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.RFA.policies.time.modulate", "true", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.RFA.policies.time.modulate", "true", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.RFA.policies.size.type", "SizeBasedTriggeringPolicy", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.RFA.policies.size.type", "SizeBasedTriggeringPolicy", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.RFA.policies.size.size", "${sys:llap.daemon.log.maxfilesize}", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.RFA.policies.size.size", "${sys:llap.daemon.log.maxfilesize}", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.RFA.strategy.type", "DefaultRolloverStrategy", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.RFA.strategy.type", "DefaultRolloverStrategy", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.RFA.strategy.max", "${sys:llap.daemon.log.maxbackupindex}", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.RFA.strategy.max", "${sys:llap.daemon.log.maxbackupindex}", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.HISTORYAPPENDER.type", "RollingRandomAccessFile", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.HISTORYAPPENDER.type", "RollingRandomAccessFile", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.HISTORYAPPENDER.name", "HISTORYAPPENDER", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.HISTORYAPPENDER.name", "HISTORYAPPENDER", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.HISTORYAPPENDER.fileName", "${sys:llap.daemon.log.dir}/${sys:llap.daemon.historylog.file}", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.HISTORYAPPENDER.fileName", "${sys:llap.daemon.log.dir}/${sys:llap.daemon.historylog.file}", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.HISTORYAPPENDER.filePattern", "${sys:llap.daemon.log.dir}/${sys:llap.daemon.historylog.file}_%d{yyyy-MM-dd}_%i.done", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.HISTORYAPPENDER.filePattern", "${sys:llap.daemon.log.dir}/${sys:llap.daemon.historylog.file}_%d{yyyy-MM-dd}_%i.done", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.HISTORYAPPENDER.layout.type", "PatternLayout", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.HISTORYAPPENDER.layout.type", "PatternLayout", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.HISTORYAPPENDER.layout.pattern", "%m%n", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.HISTORYAPPENDER.layout.pattern", "%m%n", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.HISTORYAPPENDER.policies.type", "Policies", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.HISTORYAPPENDER.policies.type", "Policies", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.HISTORYAPPENDER.policies.size.type", "SizeBasedTriggeringPolicy", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.HISTORYAPPENDER.policies.size.type", "SizeBasedTriggeringPolicy", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.HISTORYAPPENDER.policies.size.size", "${sys:llap.daemon.log.maxfilesize}", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.HISTORYAPPENDER.policies.size.size", "${sys:llap.daemon.log.maxfilesize}", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.HISTORYAPPENDER.policies.time.type", "TimeBasedTriggeringPolicy", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.HISTORYAPPENDER.policies.time.type", "TimeBasedTriggeringPolicy", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.HISTORYAPPENDER.policies.time.interval", "1", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.HISTORYAPPENDER.policies.time.interval", "1", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.HISTORYAPPENDER.policies.time.modulate", "true", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.HISTORYAPPENDER.policies.time.modulate", "true", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.HISTORYAPPENDER.strategy.type", "DefaultRolloverStrategy", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.HISTORYAPPENDER.strategy.type", "DefaultRolloverStrategy", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.HISTORYAPPENDER.strategy.max", "${sys:llap.daemon.log.maxbackupindex}", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.HISTORYAPPENDER.strategy.max", "${sys:llap.daemon.log.maxbackupindex}", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.type", "Routing", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.type", "Routing", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.name", "query-routing", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.name", "query-routing", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.routes.type", "Routes", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.routes.type", "Routes", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.routes.pattern", "$${ctx:queryId}", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.routes.pattern", "$${ctx:queryId}", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.purgePolicy.type", "LlapRoutingAppenderPurgePolicy", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.purgePolicy.type", "LlapRoutingAppenderPurgePolicy", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.purgePolicy.name", "llapLogPurgerQueryRouting", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.purgePolicy.name", "llapLogPurgerQueryRouting", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.routes.route-default.type", "Route", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.routes.route-default.type", "Route", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.routes.route-default.key", "$${ctx:queryId}", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.routes.route-default.key", "$${ctx:queryId}", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.routes.route-default.ref", "RFA", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.routes.route-default.ref", "RFA", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.routes.route-mdc.type", "Route", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.routes.route-mdc.type", "Route", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.routes.route-mdc.file-mdc.type", "LlapWrappedAppender", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.routes.route-mdc.file-mdc.type", "LlapWrappedAppender", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.routes.route-mdc.file-mdc.name", "IrrelevantName-query-routing", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.routes.route-mdc.file-mdc.name", "IrrelevantName-query-routing", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.routes.route-mdc.file-mdc.app.type", "RandomAccessFile", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.routes.route-mdc.file-mdc.app.type", "RandomAccessFile", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.routes.route-mdc.file-mdc.app.name", "file-mdc", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.routes.route-mdc.file-mdc.app.name", "file-mdc", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.routes.route-mdc.file-mdc.app.fileName", "${sys:llap.daemon.log.dir}/${ctx:queryId}-${ctx:dagId}.log", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.routes.route-mdc.file-mdc.app.fileName", "${sys:llap.daemon.log.dir}/${ctx:queryId}-${ctx:dagId}.log", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.routes.route-mdc.file-mdc.app.layout.type", "PatternLayout", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.routes.route-mdc.file-mdc.app.layout.type", "PatternLayout", "llap-daemon-log4j2.properties");
        status = modify_hive_config("appender.query-routing.routes.route-mdc.file-mdc.app.layout.pattern", "%d{ISO8601} %5p [%t (%X{fragmentId})] %c{2}: %m%n", "llap-daemon-log4j2.properties");
        handle_result(status, "appender.query-routing.routes.route-mdc.file-mdc.app.layout.pattern", "%d{ISO8601} %5p [%t (%X{fragmentId})] %c{2}: %m%n", "llap-daemon-log4j2.properties");
        status = modify_hive_config("loggers", "PerfLogger, EncodedReader, NIOServerCnxn, ClientCnxnSocketNIO, DataNucleus, Datastore, JPOX, HistoryLogger, LlapIoImpl, LlapIoOrc, LlapIoCache, LlapIoLocking, TezSM, TezSS, TezHC", "llap-daemon-log4j2.properties");
        handle_result(status, "loggers", "PerfLogger, EncodedReader, NIOServerCnxn, ClientCnxnSocketNIO, DataNucleus, Datastore, JPOX, HistoryLogger, LlapIoImpl, LlapIoOrc, LlapIoCache, LlapIoLocking, TezSM, TezSS, TezHC", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.TezSM.name", "org.apache.tez.runtime.library.common.shuffle.impl.ShuffleManager.fetch", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.TezSM.name", "org.apache.tez.runtime.library.common.shuffle.impl.ShuffleManager.fetch", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.TezSM.level", "WARN", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.TezSM.level", "WARN", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.TezSS.name", "org.apache.tez.runtime.library.common.shuffle.orderedgrouped.ShuffleScheduler.fetch", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.TezSS.name", "org.apache.tez.runtime.library.common.shuffle.orderedgrouped.ShuffleScheduler.fetch", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.TezSS.level", "WARN", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.TezSS.level", "WARN", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.TezHC.name", "org.apache.tez.http.HttpConnection.url", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.TezHC.name", "org.apache.tez.http.HttpConnection.url", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.TezHC.level", "WARN", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.TezHC.level", "WARN", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.PerfLogger.name", "org.apache.hadoop.hive.ql.log.PerLogger", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.PerfLogger.name", "org.apache.hadoop.hive.ql.log.PerfLogger", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.PerfLogger.level", "DEBUG", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.PerfLogger.level", "DEBUG", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.EncodedReader.name", "org.apache.hadoop.hive.ql.io.orc.encoded.EncodedReaderImpl", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.EncodedReader.name", "org.apache.hadoop.hive.ql.io.orc.encoded.EncodedReaderImpl", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.EncodedReader.level", "INFO", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.EncodedReader.level", "INFO", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.LlapIoImpl.name", "LlapIoImpl", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.LlapIoImpl.name", "LlapIoImpl", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.LlapIoImpl.level", "INFO", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.LlapIoImpl.level", "INFO", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.LlapIoOrc.name", "LlapIoOrc", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.LlapIoOrc.name", "LlapIoOrc", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.LlapIoOrc.level", "WARN", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.LlapIoOrc.level", "WARN", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.LlapIoCache.name", "LlapIoCache", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.LlapIoCache.name", "LlapIoCache", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.LlapIoCache.level", "WARN", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.LlapIoCache.level", "WARN", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.LlapIoLocking.name", "LlapIoLocking", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.LlapIoLocking.name", "LlapIoLocking", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.LlapIoLocking.level", "WARN", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.LlapIoLocking.level", "WARN", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.NIOServerCnxn.name", "org.apache.zookeeper.server.NIOServerCnxn", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.NIOServerCnxn.name", "org.apache.zookeeper.server.NIOServerCnxn", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.NIOServerCnxn.level", "WARN", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.NIOServerCnxn.level", "WARN", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.ClientCnxnSocketNIO.name", "org.apache.zookeeper.ClientCnxnSocketNIO", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.ClientCnxnSocketNIO.name", "org.apache.zookeeper.ClientCnxnSocketNIO", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.ClientCnxnSocketNIO.level", "WARN", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.ClientCnxnSocketNIO.level", "WARN", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.DataNucleus.name", "DataNucleus", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.DataNucleus.name", "DataNucleus", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.DataNucleus.level", "ERROR", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.DataNucleus.level", "ERROR", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.Datastore.name", "Datastore", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.Datastore.name", "Datastore", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.Datastore.level", "ERROR", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.Datastore.level", "ERROR", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.JPOX.name", "JPOX", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.JPOX.name", "JPOX", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.JPOX.level", "ERROR", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.JPOX.level", "ERROR", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.HistoryLogger.name", "org.apache.hadoop.hive.llap.daemon.HistoryLogger", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.HistoryLogger.name", "org.apache.hadoop.hive.llap.daemon.HistoryLogger", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.HistoryLogger.level", "INFO", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.HistoryLogger.level", "INFO", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.HistoryLogger.additivity", "false", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.HistoryLogger.additivity", "false", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.HistoryLogger.appenderRefs", "HistoryAppender", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.HistoryLogger.appenderRefs", "HistoryAppender", "llap-daemon-log4j2.properties");
        status = modify_hive_config("logger.HistoryLogger.appenderRef.HistoryAppender.ref", "HISTORYAPPENDER", "llap-daemon-log4j2.properties");
        handle_result(status, "logger.HistoryLogger.appenderRef.HistoryAppender.ref", "HISTORYAPPENDER", "llap-daemon-log4j2.properties");
        status = modify_hive_config("rootLogger.level", "${sys:llap.daemon.log.level}", "llap-daemon-log4j2.properties");
        handle_result(status, "rootLogger.level", "${sys:llap.daemon.log.level}", "llap-daemon-log4j2.properties");
        status = modify_hive_config("rootLogger.appenderRefs", "root", "llap-daemon-log4j2.properties");
        handle_result(status, "rootLogger.appenderRefs", "root", "llap-daemon-log4j2.properties");
        status = modify_hive_config("rootLogger.appenderRef.root.ref", "${sys:llap.daemon.root.logger}", "llap-daemon-log4j2.properties");
        handle_result(status, "rootLogger.appenderRef.root.ref", "${sys:llap.daemon.root.logger}", "llap-daemon-log4j2.properties");

        break;

        //	 Kafka Configuration
    case KAFKA:

        //	 Update Kafka configuration parameters in 'server.properties'
        status = modify_kafka_config("log.dirs", "/kafka-logs", "server.properties");
        handle_result(status, "log.dirs", "/kafka-logs", "server.properties");
        status = modify_kafka_config("zookeeper.connect", "localhost:2181", "server.properties");
        handle_result(status, "zookeeper.connect", "localhost:2181", "server.properties");
        status = modify_kafka_config("message.max.bytes", "1000000", "server.properties");
        handle_result(status, "message.max.bytes", "1000000", "server.properties");
        status = modify_kafka_config("num.network.threads", "3", "server.properties");
        handle_result(status, "num.network.threads", "3", "server.properties");
        status = modify_kafka_config("num.io.threads", "8", "server.properties");
        handle_result(status, "num.io.threads", "8", "server.properties");
        status = modify_kafka_config("queued.max.requests", "500", "server.properties");
        handle_result(status, "queued.max.requests", "500", "server.properties");
        status = modify_kafka_config("socket.send.buffer.bytes", "102400", "server.properties");
        handle_result(status, "socket.send.buffer.bytes", "102400", "server.properties");
        status = modify_kafka_config("socket.receive.buffer.bytes", "102400", "server.properties");
        handle_result(status, "socket.receive.buffer.bytes", "102400", "server.properties");
        status = modify_kafka_config("socket.request.max.bytes", "104857600", "server.properties");
        handle_result(status, "socket.request.max.bytes", "104857600", "server.properties");
        status = modify_kafka_config("num.partitions", "1", "server.properties");
        handle_result(status, "num.partitions", "1", "server.properties");
        status = modify_kafka_config("log.segment.bytes", "1073741824", "server.properties");
        handle_result(status, "log.segment.bytes", "1073741824", "server.properties");
        status = modify_kafka_config("log.roll.hours", "168", "server.properties");
        handle_result(status, "log.roll.hours", "168", "server.properties");
        status = modify_kafka_config("log.retention.bytes", "-1", "server.properties");
        handle_result(status, "log.retention.bytes", "-1", "server.properties");
        status = modify_kafka_config("log.retention.hours", "168", "server.properties");
        handle_result(status, "log.retention.hours", "168", "server.properties");
        status = modify_kafka_config("log.retention.check.interval.ms", "600000", "server.properties");
        handle_result(status, "log.retention.check.interval.ms", "600000", "server.properties");
        status = modify_kafka_config("log.index.size.max.bytes", "10485760", "server.properties");
        handle_result(status, "log.index.size.max.bytes", "10485760", "server.properties");
        status = modify_kafka_config("log.index.interval.bytes", "4096", "server.properties");
        handle_result(status, "log.index.interval.bytes", "4096", "server.properties");
        status = modify_kafka_config("auto.create.topics.enable", "true", "server.properties");
        handle_result(status, "auto.create.topics.enable", "true", "server.properties");
        status = modify_kafka_config("controller.socket.timeout.ms", "30000", "server.properties");
        handle_result(status, "controller.socket.timeout.ms", "30000", "server.properties");
        status = modify_kafka_config("controller.message.queue.size", "10", "server.properties");
        handle_result(status, "controller.message.queue.size", "10", "server.properties");
        status = modify_kafka_config("default.replication.factor", "1", "server.properties");
        handle_result(status, "default.replication.factor", "1", "server.properties");
        status = modify_kafka_config("replica.lag.time.max.ms", "10000", "server.properties");
        handle_result(status, "replica.lag.time.max.ms", "10000", "server.properties");
        status = modify_kafka_config("replica.lag.max.messages", "4000", "server.properties");
        handle_result(status, "replica.lag.max.messages", "4000", "server.properties");
        status = modify_kafka_config("replica.socket.timeout.ms", "30000", "server.properties");
        handle_result(status, "replica.socket.timeout.ms", "30000", "server.properties");
        status = modify_kafka_config("replica.socket.receive.buffer.bytes", "65536", "server.properties");
        handle_result(status, "replica.socket.receive.buffer.bytes", "65536", "server.properties");
        status = modify_kafka_config("replica.fetch.max.bytes", "1048576", "server.properties");
        handle_result(status, "replica.fetch.max.bytes", "1048576", "server.properties");
        status = modify_kafka_config("replica.fetch.wait.max.ms", "500", "server.properties");
        handle_result(status, "replica.fetch.wait.max.ms", "500", "server.properties");
        status = modify_kafka_config("replica.fetch.min.bytes", "1", "server.properties");
        handle_result(status, "replica.fetch.min.bytes", "1", "server.properties");
        status = modify_kafka_config("num.replica.fetchers", "1", "server.properties");
        handle_result(status, "num.replica.fetchers", "1", "server.properties");
        status = modify_kafka_config("replica.high.watermark.checkpoint.interval.ms", "5000", "server.properties");
        handle_result(status, "replica.high.watermark.checkpoint.interval.ms", "5000", "server.properties");
        status = modify_kafka_config("fetch.purgatory.purge.interval.requests", "10000", "server.properties");
        handle_result(status, "fetch.purgatory.purge.interval.requests", "10000", "server.properties");
        status = modify_kafka_config("producer.purgatory.purge.interval.requests", "10000", "server.properties");
        handle_result(status, "producer.purgatory.purge.interval.requests", "10000", "server.properties");
        status = modify_kafka_config("zookeeper.session.timeout.ms", "30000", "server.properties");
        handle_result(status, "zookeeper.session.timeout.ms", "30000", "server.properties");
        status = modify_kafka_config("zookeeper.connection.timeout.ms", "25000", "server.properties");
        handle_result(status, "zookeeper.connection.timeout.ms", "25000", "server.properties");
        status = modify_kafka_config("zookeeper.sync.time.ms", "2000", "server.properties");
        handle_result(status, "zookeeper.sync.time.ms", "200", "server.properties");
        status = modify_kafka_config("controlled.shutdown.max.retries", "3", "server.properties");
        handle_result(status, "controlled.shutdown.max.retries", "3", "server.properties");
        status = modify_kafka_config("controlled.shutdown.retry.backoff.ms", "5000", "server.properties");
        handle_result(status, "controlled.shutdown.retry.backoff.ms", "5000", "server.properties");
        status = modify_kafka_config("kafka.metrics.reporters", "{{metrics_reporters}}", "server.properties");
        handle_result(status, "kafka.metrics.reporters", "{{metrics_reporters}}", "server.properties");
        status = modify_kafka_config("kafka.ganglia.metrics.reporter.enabled", "true", "server.properties");
        handle_result(status, "kafka.ganglia.metrics.reporter.enabled", "true", "server.properties");
        status = modify_kafka_config("kafka.ganglia.metrics.host", "localhost", "server.properties");
        handle_result(status, "kafka.ganglia.metrics.host", "localhost", "server.properties");
        status = modify_kafka_config("kafka.ganglia.metrics.port", "8671", "server.properties");
        handle_result(status, "kafka.ganglia.metrics.port", "8671", "server.properties");
        status = modify_kafka_config("kafka.ganglia.metrics.group", "kafka", "server.properties");
        handle_result(status, "kafka.ganglia.metrics.group", "kafka", "server.properties");
        status = modify_kafka_config("kafka.timeline.metrics.reporter.enabled", "true", "server.properties");
        handle_result(status, "kafka.timeline.metrics.reporter.enabled", "true", "server.properties");
        status = modify_kafka_config("kafka.timeline.metrics.hosts", "{{ams_collector_hosts}}", "server.properties");
        handle_result(status, "kafka.timeline.metrics.hosts", "{{ams_collector_hosts}}", "server.properties");
        status = modify_kafka_config("kafka.timeline.metrics.port", "{{metric_collector_port}}", "server.properties");
        handle_result(status, "kafka.timeline.metrics.port", "{{metric_collector_port}}", "server.properties");
        status = modify_kafka_config("kafka.timeline.metrics.protocol", "{{metric_collector_protocol}}", "server.properties");
        handle_result(status, "kafka.timeline.metrics.protocol", "{{metric_collector_protocol}}", "server.properties");
        status = modify_kafka_config("kafka.timeline.metrics.truststore.path", "{{metric_truststore_path}}", "server.properties");
        handle_result(status, "kafka.timeline.metrics.truststore.path", "{{metric_truststore_path}}", "server.properties");
        status = modify_kafka_config("kafka.timeline.metrics.truststore.type", "{{metric_truststore_type}}", "server.properties");
        handle_result(status, "kafka.timeline.metrics.truststore.type", "{{metric_truststore_type}}", "server.properties");
        status = modify_kafka_config("kafka.timeline.metrics.truststore.password", "{{metric_truststore_password}}", "server.properties");
        handle_result(status, "kafka.timeline.metrics.truststore.password", "{{metric_truststore_password}}", "server.properties");
        status = modify_kafka_config("kafka.timeline.metrics.reporter.sendInterval", "5900", "server.properties");
        handle_result(status, "kafka.timeline.metrics.reporter.sendInterval", "5900", "server.properties");
        status = modify_kafka_config("kafka.timeline.metrics.maxRowCacheSize", "10000", "server.properties");
        handle_result(status, "kafka.timeline.metrics.maxRowCacheSize", "10000", "server.properties");
        status = modify_kafka_config("kafka.timeline.metrics.host_in_memory_aggregation", "{{host_in_memory_aggregation}}", "server.properties");
        handle_result(status, "kafka.timeline.metrics.host_in_memory_aggregation", "{{host_in_memory_aggregation}}", "server.properties");
        status = modify_kafka_config("kafka.timeline.metrics.host_in_memory_aggregation_port", "{{host_in_memory_aggregation_port}}", "server.properties");
        handle_result(status, "kafka.timeline.metrics.host_in_memory_aggregation_port", "{{host_in_memory_aggregation_port}}", "server.properties");
        status = modify_kafka_config("kafka.timeline.metrics.host_in_memory_aggregation_protocol", "{{host_in_memory_aggregation_protocol}}", "server.properties");
        handle_result(status, "kafka.timeline.metrics.host_in_memory_aggregation_protocol", "{{host_in_memory_aggregation_protocol}}", "server.properties");
        status = modify_kafka_config("listeners", "PLAINTEXT:localhost:9092", "server.properties");
        handle_result(status, "listeners", "PLAINTEXT:localhost:9092", "server.properties");
        status = modify_kafka_config("raw.listeners", "", "server.properties");
        handle_result(status, "raw.listeners", "", "server.properties");
        status = modify_kafka_config("controlled.shutdown.enable", "true", "server.properties");
        handle_result(status, "controlled.shutdown.enable", "true", "server.properties");
        status = modify_kafka_config("auto.leader.rebalance.enable", "true", "server.properties");
        handle_result(status, "auto.leader.rebalance.enable", "true", "server.properties");
        status = modify_kafka_config("num.recovery.threads.per.data.dir", "1", "server.properties");
        handle_result(status, "num.recovery.threads.per.data.dir", "1", "server.properties");
        status = modify_kafka_config("min.insync.replicas", "1", "server.properties");
        handle_result(status, "min.insync.replicas", "1", "server.properties");
        status = modify_kafka_config("leader.imbalance.per.broker.percentage", "10", "server.properties");
        handle_result(status, "leader.imbalance.per.broker.percentage", "10", "server.properties");
        status = modify_kafka_config("leader.imbalance.check.interval.seconds", "300", "server.properties");
        handle_result(status, "leader.imbalance.check.interval.seconds", "300", "server.properties");
        status = modify_kafka_config("offset.metadata.max.bytes", "4096", "server.properties");
        handle_result(status, "offset.metadata.max.bytes", "4096", "server.properties");
        status = modify_kafka_config("offsets.load.buffer.size", "5242880", "server.properties");
        handle_result(status, "offsets.load.buffer.size", "5242880", "server.properties");
        status = modify_kafka_config("offsets.topic.replication.factor", "3", "server.properties");
        handle_result(status, "offsets.topic.replication.factor", "3", "server.properties");
        status = modify_kafka_config("offsets.topic.num.partitions", "50", "server.properties");
        handle_result(status, "offsets.topic.num.partitions", "50", "server.properties");
        status = modify_kafka_config("offsets.topic.segment.bytes", "104857600", "server.properties");
        handle_result(status, "offsets.topic.segment.bytes", "104857600", "server.properties");
        status = modify_kafka_config("offsets.topic.compression.codec", "0", "server.properties");
        handle_result(status, "offsets.topic.compression.codec", "0", "server.properties");
        status = modify_kafka_config("offsets.retention.minutes", "86400000", "server.properties");
        handle_result(status, "offsets.retention.minutes", "86400000", "server.properties");
        status = modify_kafka_config("offsets.retention.check.interval.ms", "600000", "server.properties");
        handle_result(status, "offsets.retention.check.interval.ms", "600000", "server.properties");
        status = modify_kafka_config("offsets.commit.timeout.ms", "5000", "server.properties");
        handle_result(status, "offsets.commit.timeout.ms", "5000", "server.properties");
        status = modify_kafka_config("offsets.commit.required.acks", "-1", "server.properties");
        handle_result(status, "offsets.commit.required.acks", "-1", "server.properties");
        status = modify_kafka_config("delete.topic.enable", "true", "server.properties");
        handle_result(status, "delete.topic.enable", "true", "server.properties");
        status = modify_kafka_config("compression.type", "producer", "server.properties");
        handle_result(status, "compression.type", "producer", "server.properties");
        status = modify_kafka_config("external.kafka.metrics.exclude.pefix", "kafka.network.RequestMetrics,kafka.server.DelayedOperationPurgatory,kafka.server.BrokerTopicMetrics.BytesRejectedPerSec", "server.properties");
        handle_result(status, "external.kafka.metrics.exclude.prefix", "kafka.network.RequestMetrics,kafka.server.DelayedOperationPurgatory,kafka.server.BrokerTopicMetrics.BytesRejectedPerSec", "server.properties");
        status = modify_kafka_config("external.kafka.metrics.include.prefix", "kafka.network.RequestMetrics.ResponseQueueTimeMs.request.OffsetCommit.98percentile,kafka.network.RequestMetrics.ResponseQueueTimeMs.request.Offsets.95percentile,kafka.network.RequestMetrics.ResponseSendTimeMs.request.Fetch.95percentile,kafka.network.RequestMetrics.RequestsPerSec.request", "server.properties");
        handle_result(status, "external.kafka.metrics.include.prefix", "kafka.network.RequestMetrics.ResponseQueueTimeMs.request.OffsetCommit.98percentile,kafka.network.RequestMetrics.ResponseQueueTimeMs.request.Offsets.95percentile,kafka.network.RequestMetrics.ResponseSendTimeMs.request.Fetch.95percentile,kafka.network.RequestMetrics.RequestsPerSec.request", "server.properties");
        status = modify_kafka_config("sasl.enabled.mechanisms", "GSSAPI", "server.properties");
        handle_result(status, "sasl.enabled.mechanisms", "GSSAPI", "server.properties");
        status = modify_kafka_config("security.inter.broker.protocol", "PLAINTEXT", "server.properties");
        handle_result(status, "security.inter.broker.protocol", "PLAINTEXT", "server.properties");
        status = modify_kafka_config("sasl.mechanism.inter.broker.protocol", "GSSAPI", "server.properties");
        handle_result(status, "sasl.mechanism.inter.broker.protocol", "GSSAPI", "server.properties");
        status = modify_kafka_config("ssl.client.auth", "none", "server.properties");
        handle_result(status, "ssl.client.auth", "none", "server.properties");
        status = modify_kafka_config("ssl.key.password", "", "server.properties");
        handle_result(status, "ssl.key.password", "", "server.properties");
        status = modify_kafka_config("ssl.keystore.location", "", "server.properties");
        handle_result(status, "ssl.keystore.location", "", "server.properties");
        status = modify_kafka_config("ssl.keystore.password", "", "server.properties");
        handle_result(status, "ssl.keystore.password", "", "server.properties");
        status = modify_kafka_config("ssl.truststore.location", "", "server.properties");
        handle_result(status, "ssl.truststore.location", "", "server.properties");
        status = modify_kafka_config("ssl.truststore.password", "", "server.properties");
        handle_result(status, "ssl.truststore.password", "", "server.properties");
        status = modify_kafka_config("producer.metrics.enable", "false", "server.properties");
        handle_result(status, "producer.metrics.enable", "false", "server.properties");
        //	 Update Kafka Log4j configuration in 'log4j.properties'
        status = modify_kafka_config("kafka.logs.dir", "logs", "log4j.properties");
        handle_result(status, "kafka.logs.dir", "logs", "log4j.properties");
        status = modify_kafka_config("log4j.rootLogger", "INFO, stdout", "log4j.properties");
        handle_result(status, "log4j.rootLogger", "INFO, stdout", "log4j.properties");
        status = modify_kafka_config("log4j.appender.stdout", "org.apache.log4j.ConsoleAppender", "log4j.properties");
        handle_result(status, "log4j.appender.stdout", "org.apache.log4j.ConsoleAppender", "log4j.properties");
        status = modify_kafka_config("log4j.appender.stdout.layout", "org.apache.log4j.PatternLayout", "log4j.properties");
        handle_result(status, "log4j.appender.stdout.layout", "org.apache.log4j.PatternLayout", "log4j.properties");
        status = modify_kafka_config("log4j.appender.stdout.layout.ConversionPattern", "[%d{ISO8601}] %p %m (%c)%n", "log4j.properties");
        handle_result(status, "log4j.appender.stdout.layout.ConversionPattern", "[%d{ISO8601}] %p %m (%c)%n", "log4j.properties");
        status = modify_kafka_config("log4j.appender.kafkaAppender", "org.apache.log4j.DailyRollingFileAppender", "log4j.properties");
        handle_result(status, "log4j.appender.kafkaAppender", "org.apache.log4j.DailyRollingFileAppender", "log4j.properties");
        status = modify_kafka_config("log4j.appender.kafkaAppender.DatePattern", "'.'yyyy-MM-dd-HH", "log4j.properties");
        handle_result(status, "log4j.appender.kafkaAppender.DatePattern", "'.'yyyy-MM-dd-HH", "log4j.properties");
        status = modify_kafka_config("log4j.appender.kafkaAppender.File", "${kafka.logs.dir}/server.log", "log4j.properties");
        handle_result(status, "log4j.appender.kafkaAppender.File", "${kafka.logs.dir}/server.log", "log4j.properties");
        status = modify_kafka_config("log4j.appender.kafkaAppender.layout", "org.apache.log4j.PatternLayout", "log4j.properties");
        handle_result(status, "log4j.appender.kafkaAppender.layout", "org.apache.log4j.PatternLayout", "log4j.properties");
        status = modify_kafka_config("log4j.appender.kafkaAppender.layout.ConversionPattern", "[%d{ISO8601}] %p %m (%c)%n", "log4j.properties");
        handle_result(status, "log4j.appender.kafkaAppender.layout.ConversionPattern", "[%d{ISO8601}] %p %m (%c)%n", "log4j.properties");
        status = modify_kafka_config("log4j.appender.kafkaAppender.MaxFileSize", "256MB", "log4j.properties");
        handle_result(status, "log4j.appender.kafkaAppender.MaxFileSize", "256MB", "log4j.properties");
        status = modify_kafka_config("log4j.appender.kafkaAppender.MaxBackupIndex", "20", "log4j.properties");
        handle_result(status, "log4j.appender.kafkaAppender.MaxBackupIndex", "20", "log4j.properties");
        status = modify_kafka_config("log4j.appender.stateChangeAppender", "org.apache.log4j.DailyRollingFileAppender", "log4j.properties");
        handle_result(status, "log4j.appender.stateChangeAppender", "org.apache.log4j.DailyRollingFileAppender", "log4j.properties");
        status = modify_kafka_config("log4j.appender.stateChangeAppender.DatePattern", "'.'yyyy-MM-dd-HH", "log4j.properties");
        handle_result(status, "log4j.appender.stateChangeAppender.DatePattern", "'.'yyyy-MM-dd-HH", "log4j.properties");
        status = modify_kafka_config("log4j.appender.stateChangeAppender.File", "${kafka.logs.dir}/state-change.log", "log4j.properties");
        handle_result(status, "log4j.appender.stateChangeAppender.File", "${kafka.logs.dir}/state-change.log", "log4j.properties");
        status = modify_kafka_config("log4j.appender.stateChangeAppender.layout", "org.apache.log4j.PatternLayout", "log4j.properties");
        handle_result(status, "log4j.appender.stateChangeAppender.layout", "org.apache.log4j.PatternLayout", "log4j.properties");
        status = modify_kafka_config("log4j.appender.stateChangeAppender.layout.ConversionPattern", "[%d{ISO8601}] %p %m (%c)%n", "log4j.properties");
        handle_result(status, "log4j.appender.stateChangeAppender.layout.ConversionPattern", "[%d{ISO8601}] %p %m (%c)%n", "log4j.properties");
        status = modify_kafka_config("log4j.appender.requestAppender", "org.apache.log4j.DailyRollingFileAppender", "log4j.properties");
        handle_result(status, "log4j.appender.requestAppender", "org.apache.log4j.DailyRollingFileAppender", "log4j.properties");
        status = modify_kafka_config("log4j.appender.requestAppender.DatePattern", "'.'yyyy-MM-dd-HH", "log4j.properties");
        handle_result(status, "log4j.appender.requestAppender.DatePattern", "'.'yyyy-MM-dd-HH", "log4j.properties");
        status = modify_kafka_config("log4j.appender.requestAppender.File", "${kafka.logs.dir}/kafka-request.log", "log4j.properties");
        handle_result(status, "log4j.appender.requestAppender.File", "${kafka.logs.dir}/kafka-request.log", "log4j.properties");
        status = modify_kafka_config("log4j.appender.requestAppender.layout", "org.apache.log4j.PatternLayout", "log4j.properties");
        handle_result(status, "log4j.appender.requestAppender.layout", "org.apache.log4j.PatternLayout", "log4j.properties");
        status = modify_kafka_config("log4j.appender.requestAppender.layout.ConversionPattern", "[%d{ISO8601}] %p %m (%c)%n", "log4j.properties");
        handle_result(status, "log4j.appender.requestAppender.layout.ConversionPattern", "[%d{ISO8601}] %p %m (%c)%n", "log4j.properties");
        status = modify_kafka_config("log4j.appender.cleanerAppender", "org.apache.log4j.DailyRollingFileAppender", "log4j.properties");
        handle_result(status, "log4j.appender.cleanerAppender", "org.apache.log4j.DailyRollingFileAppender", "log4j.properties");
        status = modify_kafka_config("log4j.appender.cleanerAppender.DatePattern", "'.'yyyy-MM-dd-HH", "log4j.properties");
        handle_result(status, "log4j.appender.cleanerAppender.DatePattern", "'.'yyyy-MM-dd-HH", "log4j.properties");
        status = modify_kafka_config("log4j.appender.cleanerAppender.File", "${kafka.logs.dir}/log-cleaner.log", "log4j.properties");
        handle_result(status, "log4j.appender.cleanerAppender.File", "${kafka.logs.dir}/log-cleaner.log", "log4j.properties");
        status = modify_kafka_config("log4j.appender.cleanerAppender.layout", "org.apache.log4j.PatternLayout", "log4j.properties");
        handle_result(status, "log4j.appender.cleanerAppender.layout", "org.apache.log4j.PatternLayout", "log4j.properties");
        status = modify_kafka_config("log4j.appender.cleanerAppender.layout.ConversionPattern", "[%d{ISO8601}] %p %m (%c)%n", "log4j.properties");
        handle_result(status, "log4j.appender.cleanerAppender.layout.ConversionPattern", "[%d{ISO8601}] %p %m (%c)%n", "log4j.properties");
        status = modify_kafka_config("log4j.appender.controllerAppender", "org.apache.log4j.DailyRollingFileAppender", "log4j.properties");
        handle_result(status, "log4j.appender.controllerAppender", "org.apache.log4j.DailyRollingFileAppender", "log4j.properties");
        status = modify_kafka_config("log4j.appender.controllerAppender.DatePattern", "'.'yyyy-MM-dd-HH", "log4j.properties");
        handle_result(status, "log4j.appender.controllerAppender.DatePattern", "'.'yyyy-MM-dd-HH", "log4j.properties");
        status = modify_kafka_config("log4j.appender.controllerAppender.File", "${kafka.logs.dir}/controller.log", "log4j.properties");
        handle_result(status, "log4j.appender.controllerAppender.File", "${kafka.logs.dir}/controller.log", "log4j.properties");
        status = modify_kafka_config("log4j.appender.controllerAppender.layout", "org.apache.log4j.PatternLayout", "log4j.properties");
        handle_result(status, "log4j.appender.controllerAppender.layout", "org.apache.log4j.PatternLayout", "log4j.properties");
        status = modify_kafka_config("log4j.appender.controllerAppender.layout.ConversionPattern", "[%d{ISO8601}] %p %m (%c)%n", "log4j.properties");
        handle_result(status, "log4j.appender.controllerAppender.layout.ConversionPattern", "[%d{ISO8601}] %p %m (%c)%n", "log4j.properties");
        status = modify_kafka_config("log4j.appender.controllerAppender.MaxFileSize", "256MB", "log4j.properties");
        handle_result(status, "log4j.appender.controllerAppender.MaxFileSize", "256MB", "log4j.properties");
        status = modify_kafka_config("log4j.appender.controllerAppender.MaxBackupIndex", "20", "log4j.properties");
        handle_result(status, "log4j.appender.controllerAppender.MaxBackupIndex", "20", "log4j.properties");
        status = modify_kafka_config("log4j.logger.kafka", "INFO, kafkaAppender", "log4j.properties");
        handle_result(status, "log4j.logger.kafka", "INFO, kafkaAppender", "log4j.properties");
        status = modify_kafka_config("log4j.logger.kafka.network.RequestChannel$", "WARN, requestAppender", "log4j.properties");
        handle_result(status, "log4j.logger.kafka.network.RequestChannel$", "WARN, requestAppender", "log4j.properties");
        status = modify_kafka_config("log4j.additivity.kafka.network.RequestChannel$", "false", "log4j.properties");
        handle_result(status, "log4j.additivity.kafka.network.RequestChannel$", "false", "log4j.properties");
        status = modify_kafka_config("log4j.logger.kafka.request.logger", "WARN, requestAppender", "log4j.properties");
        handle_result(status, "log4j.logger.kafka.request.logger", "WARN, requestAppender", "log4j.properties");
        status = modify_kafka_config("log4j.additivity.kafka.request.logger", "false", "log4j.properties");
        handle_result(status, "log4j.additivity.kafka.request.logger", "false", "log4j.properties");
        status = modify_kafka_config("log4j.logger.kafka.controller", "TRACE, controllerAppender", "log4j.properties");
        handle_result(status, "log4j.logger.kafka.controller", "TRACE, controllerAppender", "log4j.properties");
        status = modify_kafka_config("log4j.additivity.kafka.controller", "false", "log4j.properties");
        handle_result(status, "log4j.additivity.kafka.controller", "false", "log4j.properties");
        status = modify_kafka_config("log4j.logger.kafka.log.LogCleaner", "INFO, cleanerAppender", "log4j.properties");
        handle_result(status, "log4j.logger.kafka.log.LogCleaner", "INFO, cleanerAppender", "log4j.properties");
        status = modify_kafka_config("log4j.additivity.kafka.log.LogCleaner", "false", "log4j.properties");
        handle_result(status, "log4j.additivity.kafka.log.LogCleaner", "false", "log4j.properties");
        status = modify_kafka_config("log4j.logger.state.change.logger", "TRACE, stateChangeAppender", "log4j.properties");
        handle_result(status, "log4j.logger.state.change.logger", "TRACE, stateChangeAppender", "log4j.properties");
        status = modify_kafka_config("log4j.additivity.state.change.logger", "false", "log4j.properties");
        handle_result(status, "log4j.additivity.state.change.logger", "false", "log4j.properties");
        //	 Update Kafka Client JAAS configuration in 'kafka_client_jaas.conf'
        status = modify_kafka_config("content",
                                     "  {% if kerberos_security_enabled %}\n"
                                     "  KafkaClient {\n"
                                     "  com.sun.security.auth.module.Krb5LoginModule required\n"
                                     "  useTicketCache=true\n"
                                     "  renewTicket=true\n"
                                     "  serviceName=\"{{kafka_bare_jaas_principal}}\";\n"
                                     "  };\n"
                                     "  {% endif %}",
                                     "kafka_client_jaas.conf");
        handle_result(status, "content", "JAAS configuration", "kafka_client_jaas.conf");
        // Update Kafka JAAS configuration in 'kafka_jaas.conf'
        status = modify_kafka_config("content",
                                     "          /**\n"
                                     "           * Example of SASL/PLAIN Configuration\n"
                                     "           *\n"
                                     "           * KafkaServer {\n"
                                     "           *   org.apache.kafka.common.security.plain.PlainLoginModule required\n"
                                     "           *   username=\"admin\"\n"
                                     "           *   password=\"admin-secret\"\n"
                                     "           *   user_admin=\"admin-secret\"\n"
                                     "           *   user_alice=\"alice-secret\";\n"
                                     "           *   };\n"
                                     "           *\n"
                                     "           * Example of SASL/SCRAM\n"
                                     "           *\n"
                                     "           * KafkaServer {\n"
                                     "           *   org.apache.kafka.common.security.scram.ScramLoginModule required\n"
                                     "           *   username=\"admin\"\n"
                                     "           *   password=\"admin-secret\"\n"
                                     "           *   };\n"
                                     "           *\n"
                                     //         * Example of Enabling multiple SASL mechanisms in a broker:\n"
                                     "           *\n"
                                     "           *   KafkaServer {\n"
                                     "           *\n"
                                     "           *    com.sun.security.auth.module.Krb5LoginModule required\n"
                                     "           *    useKeyTab=true\n"
                                     "           *    storeKey=true\n"
                                     "           *    keyTab=\"/etc/security/keytabs/kafka_server.keytab\"\n"
                                     "           *    principal=\"kafka/kafka1.hostname.com@EXAMPLE.COM\";\n"
                                     "           *\n"
                                     "           *    org.apache.kafka.common.security.plain.PlainLoginModule required\n"
                                     "           *    username=\"admin\"\n"
                                     "           *    password=\"admin-secret\"\n"
                                     "           *    user_admin=\"admin-secret\"\n"
                                     "           *    user_alice=\"alice-secret\";\n"
                                     "           *\n"
                                     "           *    org.apache.kafka.common.security.scram.ScramLoginModule required\n"
                                     "           *    username=\"scram-admin\"\n"
                                     "           *    password=\"scram-admin-secret\";\n"
                                     "           *    };\n"
                                     "           *\n"
                                     "           **/\n"
                                     "  \n"
                                     "          {% if kerberos_security_enabled %}\n"
                                     "  \n"
                                     "          KafkaServer {\n"
                                     "          com.sun.security.auth.module.Krb5LoginModule required\n"
                                     "          useKeyTab=true\n"
                                     "          keyTab=\"{{kafka_keytab_path}}\"\n"
                                     "          storeKey=true\n"
                                     "          useTicketCache=false\n"
                                     "          serviceName=\"{{kafka_bare_jaas_principal}}\"\n"
                                     "          principal=\"{{kafka_jaas_principal}}\";\n"
                                     "          };\n"
                                     "          KafkaClient {\n"
                                     "          com.sun.security.auth.module.Krb5LoginModule required\n"
                                     "          useTicketCache=true\n"
                                     "          renewTicket=true\n"
                                     "          serviceName=\"{{kafka_bare_jaas_principal}}\";\n"
                                     "          };\n"
                                     "          Client {\n"
                                     "          com.sun.security.auth.module.Krb5LoginModule required\n"
                                     "          useKeyTab=true\n"
                                     "          keyTab=\"{{kafka_keytab_path}}\"\n"
                                     "          storeKey=true\n"
                                     "          useTicketCache=false\n"
                                     "          serviceName=\"zookeeper\"\n"
                                     "          principal=\"{{kafka_jaas_principal}}\";\n"
                                     "          };\n"
                                     "          com.sun.security.jgss.krb5.initiate {\n"
                                     "          com.sun.security.auth.module.Krb5LoginModule required\n"
                                     "          renewTGT=false\n"
                                     "          doNotPrompt=true\n"
                                     "          useKeyTab=true\n"
                                     "          keyTab=\"{{kafka_keytab_path}}\"\n"
                                     "          storeKey=true\n"
                                     "          useTicketCache=false\n"
                                     "          serviceName=\"{{kafka_bare_jaas_principal}}\"\n"
                                     "          principal=\"{{kafka_jaas_principal}}\";\n"
                                     "          };\n"
                                     "  \n"
                                     "          {% endif %}\n"
                                     "     ",
            "kafka_jaas.conf");
        handle_result(status, "content", "JAAS configuration", "kafka_jaas.conf");
        // Update Ranger-Kafka audit configuration
        status = modify_kafka_config("xasecure.audit.is.enabled", "true", "ranger-kafka-audit.xml");
        handle_result(status, "xasecure.audit.is.enabled", "true", "ranger-kafka-audit.xml");
        status = modify_kafka_config("xasecure.audit.destination.hdfs", "true", "ranger-kafka-audit.xml");
        handle_result(status, "xasecure.audit.destination.hdfs", "true", "ranger-kafka-audit.xml");
        status = modify_kafka_config("xasecure.audit.destination.hdfs.dir", "hdfs:NAMENODE_HOSTNAME:8020/ranger/audit", "ranger-kafka-audit.xml");
        handle_result(status, "xasecure.audit.destination.hdfs.dir", "hdfs:NAMENODE_HOSTNAME:8020/ranger/audit", "ranger-kafka-audit.xml");
        status = modify_kafka_config("xasecure.audit.destination.hdfs.batch.filespool.dir", "/var/log/kafka/audit/hdfs/spool", "ranger-kafka-audit.xml");
        handle_result(status, "xasecure.audit.destination.hdfs.batch.filespool.dir", "/var/log/kafka/audit/hdfs/spool", "ranger-kafka-audit.xml");
        status = modify_kafka_config("xasecure.audit.destination.solr", "false", "ranger-kafka-audit.xml");
        handle_result(status, "xasecure.audit.destination.solr", "false", "ranger-kafka-audit.xml");
        status = modify_kafka_config("xasecure.audit.destination.solr.urls", "", "ranger-kafka-audit.xml");
        handle_result(status, "xasecure.audit.destination.solr.urls", "", "ranger-kafka-audit.xml");
        status = modify_kafka_config("xasecure.audit.destination.solr.zookeepers", "NONE", "ranger-kafka-audit.xml");
        handle_result(status, "xasecure.audit.destination.solr.zookeepers", "NONE", "ranger-kafka-audit.xml");
        status = modify_kafka_config("xasecure.audit.destination.solr.batch.filespool.dir", "/var/log/kafka/audit/solr/spool", "ranger-kafka-audit.xml");
        handle_result(status, "xasecure.audit.destination.solr.batch.filespool.dir", "/var/log/kafka/audit/solr/spool", "ranger-kafka-audit.xml");
        status = modify_kafka_config("xasecure.audit.provider.summary.enabled", "true", "ranger-kafka-audit.xml");
        handle_result(status, "xasecure.audit.provider.summary.enabled", "true", "ranger-kafka-audit.xml");
        status = modify_kafka_config("ranger.plugin.kafka.ambari.cluster.name", "{{cluster_name}}", "ranger-kafka-audit.xml");
        handle_result(status, "ranger.plugin.kafka.ambari.cluster.name", "{{cluster_name}}", "ranger-kafka-audit.xml");
        // Update Ranger-Kafka plugin properties
        status = modify_kafka_config("policy_user", "ambari-qa", "ranger-kafka-plugin.properties");
        handle_result(status, "policy_user", "ambari-qa", "ranger-kafka-plugin.properties");
        status = modify_kafka_config("hadoop.rpc.protection", "", "ranger-kafka-plugin.properties");
        handle_result(status, "hadoop.rpc.protection", "", "ranger-kafka-plugin.properties");
        status = modify_kafka_config("common.name.for.certificate", "", "ranger-kafka-plugin.properties");
        handle_result(status, "common.name.for.certificate", "", "ranger-kafka-plugin.properties");
        status = modify_kafka_config("zookeeper.connect", "localhost:2181", "ranger-kafka-plugin.properties");
        handle_result(status, "zookeeper.connect", "localhost:2181", "ranger-kafka-plugin.properties");
        status = modify_kafka_config("ranger-kafka-plugin-enabled", "No", "ranger-kafka-plugin.properties");
        handle_result(status, "ranger-kafka-plugin-enabled", "No", "ranger-kafka-plugin.properties");
        status = modify_kafka_config("REPOSITORY_CONFIG_PASSWORD", "kafka", "ranger-kafka-plugin.properties");
        handle_result(status, "REPOSITORY_CONFIG_PASSWORD", "kafka", "ranger-kafka-plugin.properties");
        status = modify_kafka_config("external_admin_username", "", "ranger-kafka-plugin.properties");
        handle_result(status, "external_admin_username", "", "ranger-kafka-plugin.properties");
        status = modify_kafka_config("external_admin_password", "", "ranger-kafka-plugin.properties");
        handle_result(status, "external_admin_password", "", "ranger-kafka-plugin.properties");
        status = modify_kafka_config("external_ranger_admin_username", "", "ranger-kafka-plugin.properties");
        handle_result(status, "external_ranger_admin_username", "", "ranger-kafka-plugin.properties");
        status = modify_kafka_config("external_ranger_admin_password", "", "ranger-kafka-plugin.properties");
        handle_result(status, "external_ranger_admin_password", "", "ranger-kafka-plugin.properties");
        status = modify_kafka_config("REPOSITORY_CONFIG_USERNAME", "kafka", "ranger-kafka-plugin.properties");
        handle_result(status, "REPOSITORY_CONFIG_USERNAME", "kafka", "ranger-kafka-plugin.properties");
        // Update Ranger-Kafka Policy Manager SSL configuration
        status = modify_kafka_config("xasecure.policymgr.clientssl.keystore", "", "ranger-kafka-policymgr-ssl.xml");
        handle_result(status, "xasecure.policymgr.clientssl.keystore", "", "ranger-kafka-policymgr-ssl.xml");
        status = modify_kafka_config("xasecure.policymgr.clientssl.keystore.password", "", "ranger-kafka-policymgr-ssl.xml");
        handle_result(status, "xasecure.policymgr.clientssl.keystore.password", "", "ranger-kafka-policymgr-ssl.xml");
        status = modify_kafka_config("xasecure.policymgr.clientssl.truststore", "", "ranger-kafka-policymgr-ssl.xml");
        handle_result(status, "xasecure.policymgr.clientssl.truststore", "", "ranger-kafka-policymgr-ssl.xml");
        status = modify_kafka_config("xasecure.policymgr.clientssl.truststore.password", "changeit", "ranger-kafka-policymgr-ssl.xml");
        handle_result(status, "xasecure.policymgr.clientssl.truststore.password", "changeit", "ranger-kafka-policymgr-ssl.xml");
        status = modify_kafka_config("xasecure.policymgr.clientssl.keystore.credential.file", "jceks:file/{{credential_file}}", "ranger-kafka-policymgr-ssl.xml");
        handle_result(status, "xasecure.policymgr.clientssl.keystore.credential.file", "jceks:file/{{credential_file}}", "ranger-kafka-policymgr-ssl.xml");
        status = modify_kafka_config("xasecure.policymgr.clientssl.truststore.credential.file", "jceks:file/{{credential_file}}", "ranger-kafka-policymgr-ssl.xml");
        handle_result(status, "xasecure.policymgr.clientssl.truststore.credential.file", "jceks:file/{{credential_file}}", "ranger-kafka-policymgr-ssl.xml");
        //Update Ranger-Kafka security configuration
        status = modify_kafka_config("ranger.plugin.kafka.service.name", "{{repo_name}}", "ranger-kafka-security.xml");
        handle_result(status, "ranger.plugin.kafka.service.name", "{{repo_name}}", "ranger-kafka-security.xml");
        status = modify_kafka_config("ranger.plugin.kafka.policy.source.impl", "org.apache.ranger.admin.client.RangerAdminRESTClient", "ranger-kafka-security.xml");
        handle_result(status, "ranger.plugin.kafka.policy.source.impl", "org.apache.ranger.admin.client.RangerAdminRESTClient", "ranger-kafka-security.xml");
        status = modify_kafka_config("ranger.plugin.kafka.policy.rest.url", "{{policymgr_mgr_url}}", "ranger-kafka-security.xml");
        handle_result(status, "ranger.plugin.kafka.policy.rest.url", "{{policymgr_mgr_url}}", "ranger-kafka-security.xml");
        status = modify_kafka_config("ranger.plugin.kafka.policy.rest.ssl.config.file", "/etc/kafka/conf/ranger-policymgr-ssl.xml", "ranger-kafka-security.xml");
        handle_result(status, "ranger.plugin.kafka.policy.rest.ssl.config.file", "/etc/kafka/conf/ranger-policymgr-ssl.xml", "ranger-kafka-security.xml");
        status = modify_kafka_config("ranger.plugin.kafka.policy.pollIntervalMs", "30000", "ranger-kafka-security.xml");
        handle_result(status, "ranger.plugin.kafka.policy.pollIntervalMs", "30000", "ranger-kafka-security.xml");
        status = modify_kafka_config("ranger.plugin.kafka.policy.cache.dir", "/etc/ranger/{{repo_name}}/policycache", "ranger-kafka-security.xml");
        handle_result(status, "ranger.plugin.kafka.policy.cache.dir", "/etc/ranger/{{repo_name}}/policycache", "ranger-kafka-security.xml");
        // Core Kafka Broker Configuration
        status = modify_kafka_config("log.dirs", "/kafka-logs", "server.properties");
        handle_result(status, "log.dirs", "/kafka-logs", "server.properties");
        status = modify_kafka_config("zookeeper.connect", "localhost:2181", "server.properties");
        handle_result(status, "zookeeper.connect", "localhost:2181", "server.properties");
        status = modify_kafka_config("message.max.bytes", "1000000", "server.properties");
        handle_result(status, "message.max.bytes", "1000000", "server.properties");
        status = modify_kafka_config("num.network.threads", "3", "server.properties");
        handle_result(status, "num.network.threads", "3", "server.properties");
        status = modify_kafka_config("num.io.threads", "8", "server.properties");
        handle_result(status, "num.io.threads", "8", "server.properties");
        status = modify_kafka_config("queued.max.requests", "500", "server.properties");
        handle_result(status, "queued.max.requests", "500", "server.properties");
        status = modify_kafka_config("socket.send.buffer.bytes", "102400", "server.properties");
        handle_result(status, "socket.send.buffer.bytes", "102400", "server.properties");
        status = modify_kafka_config("socket.receive.buffer.bytes", "102400", "server.properties");
        handle_result(status, "socket.receive.buffer.bytes", "102400", "server.properties");
        status = modify_kafka_config("socket.request.max.bytes", "104857600", "server.properties");
        handle_result(status, "socket.request.max.bytes", "104857600", "server.properties");

        // Log Management Configuration
        status = modify_kafka_config("num.partitions", "1", "server.properties");
        handle_result(status, "num.partitions", "1", "server.properties");
        status = modify_kafka_config("log.segment.bytes", "1073741824", "server.properties");
        handle_result(status, "log.segment.bytes", "1073741824", "server.properties");
        status = modify_kafka_config("log.roll.hours", "168", "server.properties");
        handle_result(status, "log.roll.hours", "168", "server.properties");
        status = modify_kafka_config("log.retention.bytes", "-1", "server.properties");
        handle_result(status, "log.retention.bytes", "-1", "server.properties");
        status = modify_kafka_config("log.retention.hours", "168", "server.properties");
        handle_result(status, "log.retention.hours", "168", "server.properties");
        status = modify_kafka_config("log.retention.check.interval.ms", "600000", "server.properties");
        handle_result(status, "log.retention.check.interval.ms", "600000", "server.properties");
        status = modify_kafka_config("log.index.size.max.bytes", "10485760", "server.properties");
        handle_result(status, "log.index.size.max.bytes", "10485760", "server.properties");
        status = modify_kafka_config("log.index.interval.bytes", "4096", "server.properties");
        handle_result(status, "log.index.interval.bytes", "4096", "server.properties");

        //Topic Management
        status = modify_kafka_config("auto.create.topics.enable", "true", "server.properties");
        handle_result(status, "auto.create.topics.enable", "true", "server.properties");
        status = modify_kafka_config("delete.topic.enable", "true", "server.properties");
        handle_result(status, "delete.topic.enable", "true", "server.properties");
        status = modify_kafka_config("compression.type", "producer", "server.properties");
        handle_result(status, "compression.type", "producer", "server.properties");

        //	 Replication Configuration
        status = modify_kafka_config("default.replication.factor", "1", "server.properties");
        handle_result(status, "default.replication.factor", "1", "server.properties");
        status = modify_kafka_config("replica.lag.time.max.ms", "10000", "server.properties");
        handle_result(status, "replica.lag.time.max.ms", "10000", "server.properties");
        status = modify_kafka_config("replica.lag.max.messages", "4000", "server.properties");
        handle_result(status, "replica.lag.max.messages", "4000", "server.properties");
        status = modify_kafka_config("replica.socket.timeout.ms", "30000", "server.properties");
        handle_result(status, "replica.socket.timeout.ms", "30000", "server.properties");
        status = modify_kafka_config("replica.socket.receive.buffer.bytes", "65536", "server.properties");
        handle_result(status, "replica.socket.receive.buffer.bytes", "65536", "server.properties");
        status = modify_kafka_config("replica.fetch.max.bytes", "1048576", "server.properties");
        handle_result(status, "replica.fetch.max.bytes", "1048576", "server.properties");
        status = modify_kafka_config("replica.fetch.wait.max.ms", "500", "server.properties");
        handle_result(status, "replica.fetch.wait.max.ms", "500", "server.properties");
        status = modify_kafka_config("replica.fetch.min.bytes", "1", "server.properties");
        handle_result(status, "replica.fetch.min.bytes", "1", "server.properties");
        status = modify_kafka_config("num.replica.fetchers", "1", "server.properties");
        handle_result(status, "num.replica.fetchers", "1", "server.properties");
        status = modify_kafka_config("min.insync.replicas", "1", "server.properties");
        handle_result(status, "min.insync.replicas", "1", "server.properties");

        //	 Controller Configuration
        status = modify_kafka_config("controller.socket.timeout.ms", "30000", "server.properties");
        handle_result(status, "controller.socket.timeout.ms", "30000", "server.properties");
        status = modify_kafka_config("controller.message.queue.size", "10", "server.properties");
        handle_result(status, "controller.message.queue.size", "10", "server.properties");
        status = modify_kafka_config("leader.imbalance.per.broker.percentage", "10", "server.properties");
        handle_result(status, "leader.imbalance.per.broker.percentage", "10", "server.properties");
        status = modify_kafka_config("leader.imbalance.check.interval.seconds", "300", "server.properties");
        handle_result(status, "leader.imbalance.check.interval.seconds", "300", "server.properties");

        //	 Offset Management
        status = modify_kafka_config("offsets.topic.replication.factor", "3", "server.properties");
        handle_result(status, "offsets.topic.replication.factor", "3", "server.properties");
        status = modify_kafka_config("offsets.topic.num.partitions", "50", "server.properties");
        handle_result(status, "offsets.topic.num.partitions", "50", "server.properties");
        status = modify_kafka_config("offsets.topic.segment.bytes", "104857600", "server.properties");
        handle_result(status, "offsets.topic.segment.bytes", "104857600", "server.properties");
        status = modify_kafka_config("offsets.topic.compression.codec", "0", "server.properties");
        handle_result(status, "offsets.topic.compression.codec", "0", "server.properties");
        status = modify_kafka_config("offsets.retention.minutes", "86400000", "server.properties");
        handle_result(status, "offsets.retention.minutes", "86400000", "server.properties");
        status = modify_kafka_config("offsets.load.buffer.size", "5242880", "server.properties");
        handle_result(status, "offsets.load.buffer.size", "5242880", "server.properties");
        status = modify_kafka_config("offsets.commit.timeout.ms", "5000", "server.properties");
        handle_result(status, "offsets.commit.timeout.ms", "5000", "server.properties");
        status = modify_kafka_config("offsets.commit.required.acks", "-1", "server.properties");
        handle_result(status, "offsets.commit.required.acks", "-1", "server.properties");

        //	 Zookeeper Configuration
        status = modify_kafka_config("zookeeper.session.timeout.ms", "30000", "server.properties");
        handle_result(status, "zookeeper.session.timeout.ms", "30000", "server.properties");
        status = modify_kafka_config("zookeeper.connection.timeout.ms", "25000", "server.properties");
        handle_result(status, "zookeeper.connection.timeout.ms", "25000", "server.properties");
        status = modify_kafka_config("zookeeper.sync.time.ms", "2000", "server.properties");
        handle_result(status, "zookeeper.sync.time.ms", "2000", "server.properties");

        //	 Shutdown Configuration
        status = modify_kafka_config("controlled.shutdown.enable", "true", "server.properties");
        handle_result(status, "controlled.shutdown.enable", "true", "server.properties");
        status = modify_kafka_config("controlled.shutdown.max.retries", "3", "server.properties");
        handle_result(status, "controlled.shutdown.max.retries", "3", "server.properties");
        status = modify_kafka_config("controlled.shutdown.retry.backoff.ms", "5000", "server.properties");
        handle_result(status, "controlled.shutdown.retry.backoff.ms", "5000", "server.properties");

        //	 Metrics Configuration
        status = modify_kafka_config("kafka.metrics.reporters", "{{metrics_reporters}}", "server.properties");
        handle_result(status, "kafka.metrics.reporters", "{{metrics_reporters}}", "server.properties");
        status = modify_kafka_config("kafka.ganglia.metrics.reporter.enabled", "true", "server.properties");
        handle_result(status, "kafka.ganglia.metrics.reporter.enabled", "true", "server.properties");
        status = modify_kafka_config("kafka.ganglia.metrics.host", "localhost", "server.properties");
        handle_result(status, "kafka.ganglia.metrics.host", "localhost", "server.properties");
        status = modify_kafka_config("kafka.ganglia.metrics.port", "8671", "server.properties");
        handle_result(status, "kafka.ganglia.metrics.port", "8671", "server.properties");
        status = modify_kafka_config("kafka.ganglia.metrics.group", "kafka", "server.properties");
        handle_result(status, "kafka.ganglia.metrics.group", "kafka", "server.properties");
        status = modify_kafka_config("kafka.timeline.metrics.reporter.enabled", "true", "server.properties");
        handle_result(status, "kafka.timeline.metrics.reporter.enabled", "true", "server.properties");
        status = modify_kafka_config("kafka.timeline.metrics.hosts", "{{ams_collector_hosts}}", "server.properties");
        handle_result(status, "kafka.timeline.metrics.hosts", "{{ams_collector_hosts}}", "server.properties");
        status = modify_kafka_config("kafka.timeline.metrics.port", "{{metric_collector_port}}", "server.properties");
        handle_result(status, "kafka.timeline.metrics.port", "{{metric_collector_port}}", "server.properties");
        status = modify_kafka_config("kafka.timeline.metrics.protocol", "{{metric_collector_protocol}}", "server.properties");
        handle_result(status, "kafka.timeline.metrics.protocol", "{{metric_collector_protocol}}", "server.properties");
        status = modify_kafka_config("kafka.timeline.metrics.truststore.path", "{{metric_truststore_path}}", "server.properties");
        handle_result(status, "kafka.timeline.metrics.truststore.path", "{{metric_truststore_path}}", "server.properties");
        status = modify_kafka_config("kafka.timeline.metrics.truststore.type", "{{metric_truststore_type}}", "server.properties");
        handle_result(status, "kafka.timeline.metrics.truststore.type", "{{metric_truststore_type}}", "server.properties");
        status = modify_kafka_config("kafka.timeline.metrics.truststore.password", "{{metric_truststore_password}}", "server.properties");
        handle_result(status, "kafka.timeline.metrics.truststore.password", "{{metric_truststore_password}}", "server.properties");
        status = modify_kafka_config("kafka.timeline.metrics.reporter.sendInterval", "5900", "server.properties");
        handle_result(status, "kafka.timeline.metrics.reporter.sendInterval", "5900", "server.properties");
        status = modify_kafka_config("kafka.timeline.metrics.maxRowCacheSize", "10000", "server.properties");
        handle_result(status, "kafka.timeline.metrics.maxRowCacheSize", "10000", "server.properties");
        status = modify_kafka_config("kafka.timeline.metrics.host_in_memory_aggregation", "{{host_in_memory_aggregation}}", "server.properties");
        handle_result(status, "kafka.timeline.metrics.host_in_memory_aggregation", "{{host_in_memory_aggregation}}", "server.properties");
        status = modify_kafka_config("kafka.timeline.metrics.host_in_memory_aggregation_port", "{{host_in_memory_aggregation_port}}", "server.properties");
        handle_result(status, "kafka.timeline.metrics.host_in_memory_aggregation_port", "{{host_in_memory_aggregation_port}}", "server.properties");
        status = modify_kafka_config("kafka.timeline.metrics.host_in_memory_aggregation_protocol", "{{host_in_memory_aggregation_protocol}}", "server.properties");
        handle_result(status, "kafka.timeline.metrics.host_in_memory_aggregation_protocol", "{{host_in_memory_aggregation_protocol}}", "server.properties");

        //	 Network Configuration
        status = modify_kafka_config("listeners", "PLAINTEXT:localhost:9092", "server.properties");
        handle_result(status, "listeners", "PLAINTEXT:localhost:9092", "server.properties");
        status = modify_kafka_config("raw.listeners", "", "server.properties");
        handle_result(status, "raw.listeners", "", "server.properties");

        //	 Security Configuration
        status = modify_kafka_config("sasl.enabled.mechanisms", "GSSAPI", "server.properties");
        handle_result(status, "sasl.enabled.mechanisms", "GSSAPI", "server.properties");
        status = modify_kafka_config("security.inter.broker.protocol", "PLAINTEXT", "server.properties");
        handle_result(status, "security.inter.broker.protocol", "PLAINTEXT", "server.properties");
        status = modify_kafka_config("sasl.mechanism.inter.broker.protocol", "GSSAPI", "server.properties");
        handle_result(status, "sasl.mechanism.inter.broker.protocol", "GSSAPI", "server.properties");
        status = modify_kafka_config("ssl.client.auth", "none", "server.properties");
        handle_result(status, "ssl.client.auth", "none", "server.properties");
        status = modify_kafka_config("ssl.key.password", "", "server.properties");
        handle_result(status, "ssl.key.password", "", "server.properties");
        status = modify_kafka_config("ssl.keystore.location", "", "server.properties");
        handle_result(status, "ssl.keystore.location", "", "server.properties");
        status = modify_kafka_config("ssl.keystore.password", "", "server.properties");
        handle_result(status, "ssl.keystore.password", "", "server.properties");
        status = modify_kafka_config("ssl.truststore.location", "", "server.properties");
        handle_result(status, "ssl.truststore.location", "", "server.properties");
        status = modify_kafka_config("ssl.truststore.password", "", "server.properties");
        handle_result(status, "ssl.truststore.password", "", "server.properties");

        //	 Monitoring Configuration
        status = modify_kafka_config("external.kafka.metrics.exclude.prefix", "kafka.network.RequestMetrics,kafka.server.DelayedOperationPurgatory,kafka.server.BrokerTopicMetrics.BytesRejectedPerSec", "server.properties");
        handle_result(status, "external.kafka.metrics.exclude.prefix", "kafka.network.RequestMetrics,kafka.server.DelayedOperationPurgatory,kafka.server.BrokerTopicMetrics.BytesRejectedPerSec", "server.properties");
        status = modify_kafka_config("external.kafka.metrics.include.prefix", "kafka.network.RequestMetrics.ResponseQueueTimeMs.request.OffsetCommit.98percentile,kafka.network.RequestMetrics.ResponseQueueTimeMs.request.Offsets.95percentile,kafka.network.RequestMetrics.ResponseSendTimeMs.request.Fetch.95percentile,kafka.network.RequestMetrics.RequestsPerSec.request", "server.properties");
        handle_result(status, "external.kafka.metrics.include.prefix", "kafka.network.RequestMetrics.ResponseQueueTimeMs.request.OffsetCommit.98percentile,kafka.network.RequestMetrics.ResponseQueueTimeMs.request.Offsets.95percentile,kafka.network.RequestMetrics.ResponseSendTimeMs.request.Fetch.95percentile,kafka.network.RequestMetrics.RequestsPerSec.request", "server.properties");
        status = modify_kafka_config("producer.metrics.enable", "false", "server.properties");
        handle_result(status, "producer.metrics.enable", "false", "server.properties");

        break;

        //	 Livy Configuration
    case LIVY:

        const char *config_file_livy= "livy.conf";
        //		 Update Livy environment setting
        status = set_livy_config("livy.environment", "production", config_file_livy);
        handle_result(status, "set_livy_config: livy.environment", "production", config_file_livy);

        //		 Set Livy server port
        status = set_livy_config("livy.server.port", "8999", config_file_livy);
        handle_result(status, "set_livy_config: livy.server.port", "8999", config_file_livy);

        //		 Configure session timeout
        status = set_livy_config("livy.server.session.timeout", "3600000", config_file_livy);
        handle_result(status, "set_livy_config: livy.server.session.timeout", "3600000", config_file_livy);

        //		 Enable user impersonation (first occurrence)
        status = set_livy_config("livy.impersonation.enabled", "true", config_file_livy);
        handle_result(status, "set_livy_config: livy.impersonation.enabled", "true", config_file_livy);

        //		 Enable user impersonation (second occurrence - duplicate in XML)
        status = set_livy_config("livy.impersonation.enabled", "true", config_file_livy);
        handle_result(status, "set_livy_config: livy.impersonation.enabled", "true", config_file_livy);

        //		 Activate CSRF protection
        status = set_livy_config("livy.server.csrf_protection.enabled", "true", config_file_livy);
        handle_result(status, "set_livy_config: livy.server.csrf_protection.enabled", "true", config_file_livy);

        //		 Define Spark master
        status = set_livy_config("livy.spark.master", "yarn", config_file_livy);
        handle_result(status, "set_livy_config: livy.spark.master", "yarn", config_file_livy);

        //		 Set Spark deploy mode
        status = set_livy_config("livy.spark.deploy-mode", "cluster", config_file_livy);
        handle_result(status, "set_livy_config: livy.spark.deploy-mode", "cluster", config_file_livy);

        //		 Configure HiveContext in interpreter
        status = set_livy_config("livy.repl.enableHiveContext", "true", config_file_livy);
        handle_result(status, "set_livy_config: livy.repl.enableHiveContext", "true", config_file_livy);

        //		 Set recovery mode
        status = set_livy_config("livy.server.recovery.mode", "recovery", config_file_livy);
        handle_result(status, "set_livy_config: livy.server.recovery.mode", "recovery", config_file_livy);

        //		 Define recovery state storage type
        status = set_livy_config("livy.server.recovery.state-store", "filesystem", config_file_livy);
        handle_result(status, "set_livy_config: livy.server.recovery.state-store", "filesystem", config_file_livy);

        //		 Configure recovery storage path
        status = set_livy_config("livy.server.recovery.state-store.url", "/livy-recovery", config_file_livy);
        handle_result(status, "set_livy_config: livy.server.recovery.state-store.url", "/livy-recovery", config_file_livy);

        //		 Enable access control
        status = set_livy_config("livy.server.access-control.enabled", "true", config_file_livy);
        handle_result(status, "set_livy_config: livy.server.access-control.enabled", "true", config_file_livy);

        //		 Set RSC launcher address (note: space character as value)
        status = set_livy_config("livy.rsc.launcher.address", " ", config_file_livy);
        handle_result(status, "set_livy_config: livy.rsc.launcher.address", " ", config_file_livy);

        //		 Set complete log4j configuration content
        status = set_livy_config(
            "content",
            "\n            # Set everything to be logged to the console\n            log4j.rootCategory=INFO, console\n            log4j.appender.console=org.apache.log4j.ConsoleAppender\n            log4j.appender.console.target=System.err\n            log4j.appender.console.layout=org.apache.log4j.PatternLayout\n            log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n\n\n            log4j.logger.org.eclipse.jetty=WARN\n        ",
            config_file_livy
            );
        handle_result(
            status,
            "set_livy_config: content",
            "\n            # Set everything to be logged to the console\n            log4j.rootCategory=INFO, console\n            log4j.appender.console=org.apache.log4j.ConsoleAppender\n            log4j.appender.console.target=System.err\n            log4j.appender.console.layout=org.apache.log4j.PatternLayout\n            log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n\n\n            log4j.logger.org.eclipse.jetty=WARN\n        ",
            config_file_livy
            );

        //		 Optional: Use minified version without extra whitespace
        status = set_livy_config("content",
                                 "# Set everything to logged to console\n"
                                 "log4j.rootCategory=INFO, console\n"
                                 "log4j.appender.console=org.apache.log4j.ConsoleAppender\n"
                                 "log4j.appender.console.target=System.err\n"
                                 "log4j.appender.console.layout=org.apache.log4j.PatternLayout\n"
                                 "log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n\n"
                                 "log4j.logger.org.eclipse.jetty=WARN\n",
                                 config_file_livy);
        handle_result(
            status,
            "set_livy_config: content",
            "# Set everything to logged to console\n"
            "log4j.rootCategory=INFO, console\n"
            "log4j.appender.console=org.apache.log4j.ConsoleAppender\n"
            "log4j.appender.console.target=System.err\n"
            "log4j.appender.console.layout=org.apache.log4j.PatternLayout\n"
            "log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n\n"
            "log4j.logger.org.eclipse.jetty=WARN\n",
            config_file_livy);

        //		 Set Spark configuration blacklist content
        status = set_livy_config(
            "content",
            "\n            #\n"
            "            # Configuration override / blacklist. Defines a list of properties that users are not allowed\n"
            "            # to override when starting Spark sessions.\n"
            "            #\n"
            "            # This file takes a list of property names (one per line). Empty lines and lines starting with \"#\"\n"
            "            # are ignored.\n"
            "            #\n\n"
            "            # Disallow overriding the master and the deploy mode.\n"
            "            spark.master\n"
            "            spark.submit.deployMode\n\n"
            "            # Disallow overriding the location of Spark cached jars.\n"
            "            spark.yarn.jar\n"
            "            spark.yarn.jars\n"
            "            spark.yarn.archive\n\n"
            "            # Don't allow users to override the RSC timeout.\n"
            "            livy.rsc.server.idle_timeout\n        ",
            config_file_livy
            );
        handle_result(
            status,
            "set_livy_config: content",
            "\n            #\n"
            "            # Configuration override / blacklist. Defines a list of properties that users are not allowed\n"
            "            # to override when starting Spark sessions.\n"
            "            #\n"
            "            # This file takes a list of property names (one per line). Empty lines and lines starting with \"#\"\n"
            "            # are ignored.\n"
            "            #\n\n"
            "            # Disallow overriding the master and the deploy mode.\n"
            "            spark.master\n"
            "            spark.submit.deployMode\n\n"
            "            # Disallow overriding the location of Spark cached jars.\n"
            "            spark.yarn.jar\n"
            "            spark.yarn.jars\n"
            "            spark.yarn.archive\n\n"
            "            # Don't allow users to override the RSC timeout.\n"
            "            livy.rsc.server.idle_timeout\n        ",
            config_file_livy
            );

        status = set_livy_config(
            "content",
            "#\n"
            "# Configuration override / blacklist\n"
            "# Defines properties users cannot override\n"
            "#\n\n"
            "# Disallow overriding master and deploy mode\n"
            "spark.master\n"
            "spark.submit.deployMode\n\n"
            "# Disallow overriding Spark cached jars\n"
            "spark.yarn.jar\n"
            "spark.yarn.jars\n"
            "spark.yarn.archive\n\n"
            "# Prevent RSC timeout override\n"
            "livy.rsc.server.idle_timeout\n",
            config_file_livy
            );
        handle_result(
            status,
            "set_livy_config: content",
            "#\n"
            "# Configuration override / blacklist\n"
            "# Defines properties users cannot override\n"
            "#\n\n"
            "# Disallow overriding master and deploy mode\n"
            "spark.master\n"
            "spark.submit.deployMode\n\n"
            "# Disallow overriding Spark cached jars\n"
            "spark.yarn.jar\n"
            "spark.yarn.jars\n"
            "spark.yarn.archive\n\n"
            "# Prevent RSC timeout override\n"
            "livy.rsc.server.idle_timeout\n",
            config_file_livy
            );


        break;

        //	 Storm Configuration
    case STORM:

        status = modify_storm_config("storm.zookeeper.servers", "[\"localhost\"]", "storm.yaml");
        handle_result(status, "storm.zookeeper.servers", "[\"localhost\"]", "storm.yaml");
        status = modify_storm_config("nimbus.seeds", "[\"localhost\"]", "storm.yaml");
        handle_result(status, "nimbus.seeds", "[\"localhost\"]", "storm.yaml");
        status = modify_storm_config("supervisor.slots.ports", "[6700, 6701, 6702, 6703]", "storm.yaml");
        handle_result(status, "supervisor.slots.ports", "[6700, 6701, 6702, 6703]", "storm.yaml");

        break;

        //	 Pig Configuration
    case PIG:

        status = update_pig_config("pig.default.mapred.partitioner",
                                   "org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTotalOrderPartitioner");
        handle_result(status, "pig.default.mapred.partitioner",
                      "org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTotalOrderPartitioner",
                      "pig.properties");
        status = update_pig_config("pig.tmpfilecompression", "true");
        handle_result(status, "pig.tmpfilecompression", "true", "pig.properties");
        status = update_pig_config("pig.maxCombinedSplitSize", "134217728");
        handle_result(status, "pig.maxCombinedSplitSize", "134217728", "pig.properties");
        break;

        //	 Presto Configuration
    case PRESTO:
        status = set_presto_config("coordinator", "true", "config.properties");
        handle_result(status, "coordinator", "true", "config.properties");

        status = set_presto_config("node-scheduler.include-coordinator", "true", "config.properties");
        handle_result(status, "node-scheduler.include-coordinator", "true", "config.properties");

        status = set_presto_config("http-server.http.port", "8080", "config.properties");
        handle_result(status, "http-server.http.port", "8080", "config.properties");

        status = set_presto_config("query.max-memory", "8GB", "config.properties");
        handle_result(status, "query.max-memory", "8GB", "config.properties");

        status = set_presto_config("query.max-memory-per-node", "1GB", "config.properties");
        handle_result(status, "query.max-memory-per-node", "1GB", "config.properties");

        status = set_presto_config("query.max-total-memory-per-node", "2GB", "config.properties");
        handle_result(status, "query.max-total-memory-per-node", "2GB", "config.properties");

        status = set_presto_config("discovery-server.enabled", "true", "config.properties");
        handle_result(status, "discovery-server.enabled", "true", "config.properties");

        status = set_presto_config("discovery.uri", "http://localhost:8080", "config.properties");
        handle_result(status, "discovery.uri", "http://localhost:8080", "config.properties");

// Configure tpch.properties
        status = set_presto_config("connector.name", "tpch", "tpch.properties");
        handle_result(status, "connector.name", "tpch", "tpch.properties");

        status = set_presto_config("tpch.splits-per-node", "4", "tpch.properties");
        handle_result(status, "tpch.splits-per-node", "4", "tpch.properties");
        
        status = set_presto_config("node.environment", "production", "node.properties");
        handle_result(status, "node.environment", "production", "node.properties");

        status = set_presto_config("node.id", "ffffffff-ffff-ffff-ffff-ffffffffffff", "node.properties");
        handle_result(status, "node.id", "ffffffff-ffff-ffff-ffff-ffffffffffff", "node.properties");
        const char *jvm_options = "-server\n"
                          "-Xmx4G\n"
                          "-XX:+UseG1GC\n"
                          "-XX:G1HeapRegionSize=32M\n"
                          "-XX:+UseGCOverheadLimit\n"
                          "-XX:+ExplicitGCInvokesConcurrent\n"
                          "-XX:+HeapDumpOnOutOfMemoryError\n"
                          "-XX:+ExitOnOutOfMemoryError\n";

        status = set_presto_config("node.id", jvm_options, "jvm.config");
        handle_result(status, "", jvm_options, "jvm.config");

     //   status = set_presto_config("node.data-dir", data_dir, "node.properties");
       // handle_result(status, "node.data-dir", data_dir, "node.properties");
        break;

        //	 Atlas Configuration
    case ATLAS:

        status = update_atlas_config("atlas.graph.storage.backend", "hbase", "atlas-application.properties");
        handle_result(status, "atlas.graph.storage.backend", "hbase", "atlas-application.properties");
        status = update_atlas_config("atlas.graph.storage.hostname", "localhost:2181", "atlas-application.properties");
        handle_result(status, "atlas.graph.storage.hostname", "localhost:2181", "atlas-application.properties");
        status = update_atlas_config("atlas.graph.index.search.backend", "solr", "atlas-application.properties");
        handle_result(status, "atlas.graph.index.search.backend", "solr", "atlas-application.properties");
        status = update_atlas_config("atlas.graph.index.search.solr.mode", "cloud", "atlas-application.properties");
        handle_result(status, "atlas.graph.index.search.solr.mode", "cloud", "atlas-application.properties");
        status = update_atlas_config("atlas.graph.index.search.solr.zookeeper-url", "localhost:2181/solr", "atlas-application.properties");
        handle_result(status, "atlas.graph.index.search.solr.zookeeper-url", "localhost:2181/solr", "atlas-application.properties");
        status = update_atlas_config("atlas.audit.hbase.zookeeper.quorum", "localhost", "atlas-application.properties");
        handle_result(status, "atlas.audit.hbase.zookeeper.quorum", "localhost", "atlas-application.properties");

        break;
        //
        //			 Ranger Configuration
    case RANGER:

        status = set_ranger_config("ranger.jpa.jdbc.url", "jdbc:mysql:localhost/ranger", "install.properties");
        handle_result(status, "ranger.jpa.jdbc.url", "jdbc:mysql:localhost/ranger", "install.properties");
        status = set_ranger_config("ranger.jpa.jdbc.user", "rangeradmin", "install.properties");
        handle_result(status, "ranger.jpa.jdbc.user", "rangeradmin", "install.properties");
        status = set_ranger_config("ranger.jpa.jdbc.password", "rangeradmin", "install.properties");
        handle_result(status, "ranger.jpa.jdbc.password", "rangeradmin", "install.properties");

        break;

        //			 Solr Configuration
    case SOLR:

        //			 Update Solr configuration parameters (from XML translation)
        status = update_solr_config("log_maxfilesize", "10", "solr.xml");
        handle_result(status, "update_solr_config: log_maxfilesize", "10", "solr.xml");
        status = update_solr_config("log_maxbackupindex", "9", "solr.xml");
        handle_result(status, "update_solr_config: log_maxbackupindex", "9", "solr.xml");
        status = update_solr_config("content", "", "solr.xml");
        handle_result(status, "update_solr_config: content", "", "solr.xml");

        //			 Update Solr security configuration parameters (from XML translation)
      //  status = update_solr_config("solr_ranger_audit_service_users", "{default_ranger_audit_users}", "solr_security_config.json");
        //handle_result(status, "update_solr_config: solr_ranger_audit_service_users", "{default_ranger_audit_users}", "solr_security_config.json");
       // status = update_solr_config("solr_role_ranger_admin", "ranger_admin_user", "solr_security_config.json");
       // handle_result(status, "update_solr_config: solr_role_ranger_admin", "ranger_admin_user", "solr_security_config.json");
        //status = update_solr_config("solr_role_ranger_audit", "ranger_audit_user", "solr_security_config.json");
        //handle_result(status, "update_solr_config: solr_role_ranger_audit", "ranger_audit_user", "solr_security_config.json");
        //status = update_solr_config("solr_role_atlas", "atlas_user", "solr_security_config.json");
        //handle_result(status, "update_solr_config: solr_role_atlas", "atlas_user", "solr_security_config.json");
        //status = update_solr_config("solr_role_logsearch", "logsearch_user", "solr_security_config.json");
        //handle_result(status, "update_solr_config: solr_role_logsearch", "logsearch_user", "solr_security_config.json");
        //status = update_solr_config("solr_role_logfeeder", "logfeeder_user", "solr_security_config.json");
       // handle_result(status, "update_solr_config: solr_role_logfeeder", "logfeeder_user", "solr_security_config.json");
        //status = update_solr_config("solr_role_dev", "dev", "solr_security_config.json");
        //handle_result(status, "update_solr_config: solr_role_dev", "dev", "solr_security_config.json");
       // status = update_solr_config("solr_security_manually_managed", "false", "solr_security_config.json");
       // handle_result(status, "update_solr_config: solr_security_manually_managed", "false", "solr_security_config.json");
       // status = update_solr_config("content", "", "solr_security_config.json");
        //handle_result(status, "update_solr_config: content", "", "solr_security_config.json");
        //
        //			 Updates the Solr XML template configuration (external file reference)
        status = update_solr_config("content", "", "solr.xml");
        handle_result(status, "update_solr_config: content", "", "solr.xml");

        break;

        //			 Spark Configuration
    case SPARK:

        const char* config_file ="spark-defaults.conf";
        const char* log4j_config_file ="spark-log4j.properties";
        const char* metrics_config_file = "spark-metrics.properties";
        const char* hive_site_file = "spark-hive.site";
        //				 Update Spark configuration parameters from XML
        status = update_spark_config("spark.yarn.queue", "default", config_file);
        handle_result(status, "update_spark_config: spark.yarn.queue", "default", config_file);
        status = update_spark_config("spark.history.provider", "org.apache.spark.deploy.history.FsHistoryProvider", config_file);
        handle_result(status, "update_spark_config: spark.history.provider", "org.apache.spark.deploy.history.FsHistoryProvider", config_file);
        status = update_spark_config("spark.history.ui.port", "18081", config_file);
        handle_result(status, "update_spark_config: spark.history.ui.port", "18081", config_file);
        status = update_spark_config("spark.history.fs.logDirectory", "hdfs:/spark-history/", config_file);
        handle_result(status, "update_spark_config: spark.history.fs.logDirectory", "hdfs:/spark-history/", config_file);
        status = update_spark_config("spark.history.kerberos.principal", "none", config_file);
        handle_result(status, "update_spark_config: spark.history.kerberos.principal", "none", config_file);
        status = update_spark_config("spark.history.kerberos.keytab", "none", config_file);
        handle_result(status, "update_spark_config: spark.history.kerberos.keytab", "none", config_file);
        status = update_spark_config("spark.eventLog.enabled", "true", config_file);
        handle_result(status, "update_spark_config: spark.eventLog.enabled", "true", config_file);
        status = update_spark_config("spark.eventLog.dir", "hdfs:/spark-history/", config_file);
        handle_result(status, "update_spark_config: spark.eventLog.dir", "hdfs:/spark-history/", config_file);
        status = update_spark_config("spark.yarn.historyServer.address", "{{spark_history_server_host}}:{{spark_history_ui_port}}", config_file);
        handle_result(status, "update_spark_config: spark.yarn.historyServer.address", "{{spark_history_server_host}}:{{spark_history_ui_port}}", config_file);
        status = update_spark_config("spark.scheduler.allocation.file", "file:/{{spark_conf_dir}}/spark-thrift-fairscheduler.xml", config_file);
        handle_result(status, "update_spark_config: spark.scheduler.allocation.file", "file:/{{spark_conf_dir}}/spark-thrift-fairscheduler.xml", config_file);
        status = update_spark_config("spark.scheduler.mode", "FAIR", config_file);
        handle_result(status, "update_spark_config: spark.scheduler.mode", "FAIR", config_file);
        status = update_spark_config("spark.hadoop.cacheConf", "false", config_file);
        handle_result(status, "update_spark_config: spark.hadoop.cacheConf", "false", config_file);
        status = update_spark_config("spark.yarn.executor.failuresValidityInterval", "2h", config_file);
        handle_result(status, "update_spark_config: spark.yarn.executor.failuresValidityInterval", "2h", config_file);
        status = update_spark_config("spark.yarn.maxAppAttempts", "1", config_file);
        handle_result(status, "update_spark_config: spark.yarn.maxAppAttempts", "1", config_file);
        status = update_spark_config("spark.history.fs.cleaner.enabled", "true", config_file);
        handle_result(status, "update_spark_config: spark.history.fs.cleaner.enabled", "true", config_file);
        status = update_spark_config("spark.history.fs.cleaner.interval", "7d", config_file);
        handle_result(status, "update_spark_config: spark.history.fs.cleaner.interval", "7d", config_file);
        status = update_spark_config("spark.history.fs.cleaner.maxAge", "90d", config_file);
        handle_result(status, "update_spark_config: spark.history.fs.cleaner.maxAge", "90d", config_file);
        status = update_spark_config("spark.sql.statistics.fallBackToHdfs", "true", config_file);
        handle_result(status, "update_spark_config: spark.sql.statistics.fallBackToHdfs", "true", config_file);
        status = update_spark_config("spark.sql.autoBroadcastJoinThreshold", "10MB", config_file);
        handle_result(status, "update_spark_config: spark.sql.autoBroadcastJoinThreshold", "10MB", config_file);
        status = update_spark_config("spark.io.compression.lz4.blockSize", "128kb", config_file);
        handle_result(status, "update_spark_config: spark.io.compression.lz4.blockSize", "128kb", config_file);
        status = update_spark_config("spark.sql.orc.filterPushdown", "true", config_file);
        handle_result(status, "update_spark_config: spark.sql.orc.filterPushdown", "true", config_file);
        status = update_spark_config("spark.sql.hive.convertMetastoreOrc", "true", config_file);
        handle_result(status, "update_spark_config: spark.sql.hive.convertMetastoreOrc", "true", config_file);
        status = update_spark_config("spark.shuffle.io.backLog", "8192", config_file);
        handle_result(status, "update_spark_config: spark.shuffle.io.backLog", "8192", config_file);
        status = update_spark_config("spark.shuffle.file.buffer", "1m", config_file);
        handle_result(status, "update_spark_config: spark.shuffle.file.buffer", "1m", config_file);
        status = update_spark_config("spark.master", "yarn", config_file);
        handle_result(status, "update_spark_config: spark.master", "yarn", config_file);
        status = update_spark_config("spark.executor.extraJavaOptions", "-XX:+UseNUMA", config_file);
        handle_result(status, "update_spark_config: spark.executor.extraJavaOptions", "-XX:+UseNUMA", config_file);
        status = update_spark_config("spark.sql.warehouse.dir", "{{spark_warehouse_dir}}", config_file);
        handle_result(status, "update_spark_config: spark.sql.warehouse.dir", "{{spark_warehouse_dir}}", config_file);
        status = update_spark_config("spark.sql.hive.metastore.version", "3.1.3", config_file);
        handle_result(status, "update_spark_config: spark.sql.hive.metastore.version", "3.1.3", config_file);
        status = update_spark_config("spark.sql.hive.metastore.jars", "{{hive_home}}/lib/*", config_file);
        handle_result(status, "update_spark_config: spark.sql.hive.metastore.jars", "{{hive_home}}/lib/*", config_file);
        status = update_spark_config("spark.history.store.path", "/var/lib/spark/shs_db", config_file);
        handle_result(status, "update_spark_config: spark.history.store.path", "/var/lib/spark/shs_db", config_file);

        //				 Update spark-log4j.properties configuration
        //	const char* log4j_config_file = "spark-log4j.properties"; //  Target configuration file

        status = update_spark_config("log4j.rootCategory", "INFO, console", log4j_config_file);
        handle_result(status, "update_spark_config: log4j.rootCategory", "INFO, console", log4j_config_file);
        status = update_spark_config("log4j.appender.console", "org.apache.log4j.ConsoleAppender", log4j_config_file);
        handle_result(status, "update_spark_config: log4j.appender.console", "org.apache.log4j.ConsoleAppender", log4j_config_file);
        status = update_spark_config("log4j.appender.console.target", "System.err", log4j_config_file);
        handle_result(status, "update_spark_config: log4j.appender.console.target", "System.err", log4j_config_file);
        status = update_spark_config("log4j.appender.console.layout", "org.apache.log4j.PatternLayout", log4j_config_file);
        handle_result(status, "update_spark_config: log4j.appender.console.layout", "org.apache.log4j.PatternLayout", log4j_config_file);
        status = update_spark_config("log4j.appender.console.layout.ConversionPattern", "%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n", log4j_config_file);
        handle_result(status, "update_spark_config: log4j.appender.console.layout.ConversionPattern", "%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n", log4j_config_file);
        status = update_spark_config("log4j.logger.org.eclipse.jetty", "WARN", log4j_config_file);
        handle_result(status, "update_spark_config: log4j.logger.org.eclipse.jetty", "WARN", log4j_config_file);
        status = update_spark_config("log4j.logger.org.eclipse.jetty.util.component.AbstractLifeCycle", "ERROR", log4j_config_file);
        handle_result(status, "update_spark_config: log4j.logger.org.eclipse.jetty.util.component.AbstractLifeCycle", "ERROR", log4j_config_file);
        status = update_spark_config("log4j.logger.org.apache.spark.repl.SparkIMain$exprTyper", "INFO", log4j_config_file);
        handle_result(status, "update_spark_config: log4j.logger.org.apache.spark.repl.SparkIMain$exprTyper", "INFO", log4j_config_file);
        status = update_spark_config("log4j.logger.org.apache.spark.repl.SparkILoop$SparkILoopInterpreter", "INFO", log4j_config_file);
        handle_result(status, "update_spark_config: log4j.logger.org.apache.spark.repl.SparkILoop$SparkILoopInterpreter", "INFO", log4j_config_file);

        //				 Update spark-metrics.properties configuration
        //	const char* metrics_config_file = "spark-metrics.properties";

        //				 Note: This implementation only includes actual configuration lines (not comments/examples)
        //				 from the XML content. Add/modify as needed for your specific requirements.

        //				 Enable JmxSink for all instances
        status = update_spark_config("*.sink.jmx.class", "org.apache.spark.metrics.sink.JmxSink", metrics_config_file);
        handle_result(status, "update_spark_config: *.sink.jmx.class", "org.apache.spark.metrics.sink.JmxSink", metrics_config_file);

        //				 Enable ConsoleSink for all instances with polling configuration
        status = update_spark_config("*.sink.console.class", "org.apache.spark.metrics.sink.ConsoleSink", metrics_config_file);
        handle_result(status, "update_spark_config: *.sink.console.class", "org.apache.spark.metrics.sink.ConsoleSink", metrics_config_file);
        status = update_spark_config("*.sink.console.period", "10", metrics_config_file);
        handle_result(status, "update_spark_config: *.sink.console.period", "10", metrics_config_file);
        status = update_spark_config("*.sink.console.unit", "seconds", metrics_config_file);
        handle_result(status, "update_spark_config: *.sink.console.unit", "seconds", metrics_config_file);

        //				 Master-specific console sink configuration
        status = update_spark_config("master.sink.console.period", "15", metrics_config_file);
        handle_result(status, "update_spark_config: master.sink.console.period", "15", metrics_config_file);
        status = update_spark_config("master.sink.console.unit", "seconds", metrics_config_file);
        handle_result(status, "update_spark_config: master.sink.console.unit", "seconds", metrics_config_file);

        //				 CSV Sink configuration
        status = update_spark_config("*.sink.csv.class", "org.apache.spark.metrics.sink.CsvSink", metrics_config_file);
        handle_result(status, "update_spark_config: *.sink.csv.class", "org.apache.spark.metrics.sink.CsvSink", metrics_config_file);
        status = update_spark_config("*.sink.csv.period", "1", metrics_config_file);
        handle_result(status, "update_spark_config: *.sink.csv.period", "1", metrics_config_file);
        status = update_spark_config("*.sink.csv.unit", "minutes", metrics_config_file);
        handle_result(status, "update_spark_config: *.sink.csv.unit", "minutes", metrics_config_file);
        status = update_spark_config("*.sink.csv.directory", "/tmp/", metrics_config_file);
        handle_result(status, "update_spark_config: *.sink.csv.directory", "/tmp/", metrics_config_file);

        //				 Worker-specific CSV configuration
        status = update_spark_config("worker.sink.csv.period", "10", metrics_config_file);
        handle_result(status, "update_spark_config: worker.sink.csv.period", "10", metrics_config_file);
        status = update_spark_config("worker.sink.csv.unit", "minutes", metrics_config_file);
        handle_result(status, "update_spark_config: worker.sink.csv.unit", "minutes", metrics_config_file);

        //				 JVM Source configuration for all components
        status = update_spark_config("master.source.jvm.class", "org.apache.spark.metrics.source.JvmSource", metrics_config_file);
        handle_result(status, "update_spark_config: master.source.jvm.class", "org.apache.spark.metrics.source.JvmSource", metrics_config_file);
        status = update_spark_config("worker.source.jvm.class", "org.apache.spark.metrics.source.JvmSource", metrics_config_file);
        handle_result(status, "update_spark_config: worker.source.jvm.class", "org.apache.spark.metrics.source.JvmSource", metrics_config_file);
        status = update_spark_config("driver.source.jvm.class", "org.apache.spark.metrics.source.JvmSource", metrics_config_file);
        handle_result(status, "update_spark_config: driver.source.jvm.class", "org.apache.spark.metrics.source.JvmSource", metrics_config_file);
        status = update_spark_config("executor.source.jvm.class", "org.apache.spark.metrics.source.JvmSource", metrics_config_file);
        handle_result(status, "update_spark_config: executor.source.jvm.class", "org.apache.spark.metrics.source.JvmSource", metrics_config_file);

        //				 Update spark-thrift-fairscheduler.xml configuration
        const char* fair_scheduler_file = "spark-thrift-fairscheduler.xml";

        //				 Since this is an XML allocation file, we'll update it with the complete content
        const char* fair_scheduler_content =
            "<?xml version=\"1.0\"?>\n"
            "<allocations>\n"
            "    <pool name=\"default\">\n"
            "        <schedulingMode>FAIR</schedulingMode>\n"
            "        <weight>1</weight>\n"
            "        <minShare>2</minShare>\n"
            "    </pool>\n"
            "</allocations>";

        //				 Use a special parameter name to indicate this is file content
        status = update_spark_config("fairscheduler.xml.content", fair_scheduler_content, fair_scheduler_file);
        handle_result(
            status,
            "update_spark_config: fairscheduler.xml.content",
            "<?xml version=\"1.0\"?>\n<allocations>\n    <pool name=\"default\">\n        <schedulingMode>FAIR</schedulingMode>\n        <weight>1</weight>\n        <minShare>2</minShare>\n    </pool>\n</allocations>",
            fair_scheduler_file
            );

        //Alternative approach for per-pool configuration (if supported by your system)
        //Note: This section is commented out in original code

        // Configure default pool
        status = update_spark_config("pool.default.schedulingMode", "FAIR", fair_scheduler_file);
        handle_result(status, "update_spark_config: pool.default.schedulingMode", "FAIR", fair_scheduler_file);
        status = update_spark_config("pool.default.weight", "1", fair_scheduler_file);
        handle_result(status, "update_spark_config: pool.default.weight", "1", fair_scheduler_file);
        status = update_spark_config("pool.default.minShare", "2", fair_scheduler_file);
        handle_result(status, "update_spark_config: pool.default.minShare", "2", fair_scheduler_file);

        // To add additional pools:
        status = update_spark_config("pool.high_priority.schedulingMode", "FAIR", fair_scheduler_file);
        handle_result(status, "update_spark_config: pool.high_priority.schedulingMode", "FAIR", fair_scheduler_file);
        status = update_spark_config("pool.high_priority.weight", "2", fair_scheduler_file);
        handle_result(status, "update_spark_config: pool.high_priority.weight", "2", fair_scheduler_file);
        status = update_spark_config("pool.high_priority.minShare", "4", fair_scheduler_file);
        handle_result(status, "update_spark_config: pool.high_priority.minShare", "4", fair_scheduler_file);


        //Update spark-hive-site-override configuration
        //	const char* hive_site_file = "spark-hive-site-override.xml"; //  or appropriate config file

        // Hive Server 2 impersonation settings
        status = update_spark_config("hive.server2.enable.doAs", "false", hive_site_file);
        handle_result(status, "update_spark_config: hive.server2.enable.doAs", "false", hive_site_file);

        // Metastore client settings
        status = update_spark_config("hive.metastore.client.socket.timeout", "1800", hive_site_file);
        handle_result(status, "update_spark_config: hive.metastore.client.socket.timeout", "1800", hive_site_file);
        status = update_spark_config("hive.metastore.client.connect.retry.delay", "5", hive_site_file);
        handle_result(status, "update_spark_config: hive.metastore.client.connect.retry.delay", "5", hive_site_file);

        //Hive Server 2 connection settings
        status = update_spark_config("hive.server2.thrift.port", "10016", hive_site_file);
        handle_result(status, "update_spark_config: hive.server2.thrift.port", "10016", hive_site_file);
        status = update_spark_config("hive.server2.thrift.http.port", "10002", hive_site_file);
        handle_result(status, "update_spark_config: hive.server2.thrift.http.port", "10002", hive_site_file);
        status = update_spark_config("hive.server2.transport.mode", "binary", hive_site_file);
        handle_result(status, "update_spark_config: hive.server2.transport.mode", "binary", hive_site_file);

        //Metastore and execution settings
        status = update_spark_config("metastore.catalog.default", "hive", hive_site_file);
        handle_result(status, "update_spark_config: metastore.catalog.default", "hive", hive_site_file);
        status = update_spark_config("hive.load.data.owner", "spark", hive_site_file);
        handle_result(status, "update_spark_config: hive.load.data.owner", "spark", hive_site_file);
        status = update_spark_config("hive.exec.scratchdir", "/tmp/spark", hive_site_file);
        handle_result(status, "update_spark_config: hive.exec.scratchdir", "/tmp/spark", hive_site_file);
        break;

        //Tez Configuration
    case TEZ:

        //	 Update Tez configuration parameters
        status = modify_tez_config("tez.lib.uris.classpath", "{{hadoop_conf_dir}},{{hadoop_home}}/*,{{hadoop_home}}/lib/*,{{hadoop_hdfs_home}}/*,{{hadoop_hdfs_home}}/lib/*,{{hadoop_yarn_home}}/*,{{hadoop_yarn_home}}/lib/*,{{tez_home}}/*,{{tez_home}}/lib/*,{{tez_conf_dir}}", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.lib.uris.classpath", "{{hadoop_conf_dir}},{{hadoop_home}}/*,{{hadoop_home}}/lib/*,{{hadoop_hdfs_home}}/*,{{hadoop_hdfs_home}}/lib/*,{{hadoop_yarn_home}}/*,{{hadoop_yarn_home}}/lib/*,{{tez_home}}/*,{{tez_home}}/lib/*,{{tez_conf_dir}}", "tez-site.xml");

        status = modify_tez_config("tez.lib.uris", "{{tez_lib_uris}}", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.lib.uris", "{{tez_lib_uris}}", "tez-site.xml");

        status = modify_tez_config("tez.cluster.additional.classpath.prefix", "/etc/hadoop/conf/secure", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.cluster.additional.classpath.prefix", "/etc/hadoop/conf/secure", "tez-site.xml");

        status = modify_tez_config("tez.am.log.level", "INFO", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.am.log.level", "INFO", "tez-site.xml");

        status = modify_tez_config("tez.generate.debug.artifacts", "false", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.generate.debug.artifacts", "false", "tez-site.xml");

        status = modify_tez_config("tez.staging-dir", "/tmp/${user.name}/staging", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.staging-dir", "/tmp/${user.name}/staging", "tez-site.xml");

        status = modify_tez_config("tez.am.resource.memory.mb", "2048", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.am.resource.memory.mb", "2048", "tez-site.xml");

        status = modify_tez_config("tez.am.launch.cmd-opts", "-XX:+PrintGCDetails -verbose:gc -XX:+PrintGCTimeStamps -XX:+UseNUMA -XX:+UseG1GC -XX:+ResizeTLAB{{heap_dump_opts}}", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.am.launch.cmd-opts", "-XX:+PrintGCDetails -verbose:gc -XX:+PrintGCTimeStamps -XX:+UseNUMA -XX:+UseG1GC -XX:+ResizeTLAB{{heap_dump_opts}}", "tez-site.xml");

        status = modify_tez_config("tez.am.launch.cluster-default.cmd-opts", "-server -Djava.net.preferIPv4Stack=true", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.am.launch.cluster-default.cmd-opts", "-server -Djava.net.preferIPv4Stack=true", "tez-site.xml");

        status = modify_tez_config("tez.am.launch.env", "LD_LIBRARY_PATH={{hadoop_home}}/lib/native", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.am.launch.env", "LD_LIBRARY_PATH={{hadoop_home}}/lib/native", "tez-site.xml");

        status = modify_tez_config("tez.task.resource.memory.mb", "1536", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.task.resource.memory.mb", "1536", "tez-site.xml");

        status = modify_tez_config("tez.task.launch.cmd-opts", "-XX:+PrintGCDetails -verbose:gc -XX:+PrintGCTimeStamps -XX:+UseNUMA -XX:+UseG1GC -XX:+ResizeTLAB{{heap_dump_opts}}", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.task.launch.cmd-opts", "-XX:+PrintGCDetails -verbose:gc -XX:+PrintGCTimeStamps -XX:+UseNUMA -XX:+UseG1GC -XX:+ResizeTLAB{{heap_dump_opts}}", "tez-site.xml");

        status = modify_tez_config("tez.task.launch.cluster-default.cmd-opts", "-server -Djava.net.preferIPv4Stack=true", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.task.launch.cluster-default.cmd-opts", "-server -Djava.net.preferIPv4Stack=true", "tez-site.xml");

        status = modify_tez_config("tez.task.launch.env", "LD_LIBRARY_PATH={{hadoop_home}}/lib/native", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.task.launch.env", "LD_LIBRARY_PATH={{hadoop_home}}/lib/native", "tez-site.xml");

        status = modify_tez_config("tez.shuffle-vertex-manager.min-src-fraction", "0.2", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.shuffle-vertex-manager.min-src-fraction", "0.2", "tez-site.xml");

        status = modify_tez_config("tez.shuffle-vertex-manager.max-src-fraction", "0.4", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.shuffle-vertex-manager.max-src-fraction", "0.4", "tez-site.xml");

        status = modify_tez_config("tez.am.am-rm.heartbeat.interval-ms.max", "250", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.am.am-rm.heartbeat.interval-ms.max", "250", "tez-site.xml");

        status = modify_tez_config("tez.grouping.split-waves", "1.7", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.grouping.split-waves", "1.7", "tez-site.xml");

        status = modify_tez_config("tez.grouping.min-size", "16777216", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.grouping.min-size", "16777216", "tez-site.xml");

        status = modify_tez_config("tez.grouping.max-size", "1073741824", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.grouping.max-size", "1073741824", "tez-site.xml");

        status = modify_tez_config("tez.am.container.reuse.enabled", "true", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.am.container.reuse.enabled", "true", "tez-site.xml");

        status = modify_tez_config("tez.am.container.reuse.rack-fallback.enabled", "true", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.am.container.reuse.rack-fallback.enabled", "true", "tez-site.xml");

        status = modify_tez_config("tez.am.container.reuse.non-local-fallback.enabled", "false", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.am.container.reuse.non-local-fallback.enabled", "false", "tez-site.xml");

        status = modify_tez_config("tez.am.container.idle.release-timeout-min.millis", "10000", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.am.container.idle.release-timeout-min.millis", "10000", "tez-site.xml");

        status = modify_tez_config("tez.am.container.idle.release-timeout-max.millis", "20000", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.am.container.idle.release-timeout-max.millis", "20000", "tez-site.xml");

        status = modify_tez_config("tez.am.container.reuse.locality.delay-allocation-millis", "250", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.am.container.reuse.locality.delay-allocation-millis", "250", "tez-site.xml");

        status = modify_tez_config("tez.am.max.app.attempts", "2", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.am.max.app.attempts", "2", "tez-site.xml");

        status = modify_tez_config("tez.am.maxtaskfailures.per.node", "10", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.am.maxtaskfailures.per.node", "10", "tez-site.xml");

        status = modify_tez_config("tez.task.am.heartbeat.counter.interval-ms.max", "4000", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.task.am.heartbeat.counter.interval-ms.max", "4000", "tez-site.xml");

        status = modify_tez_config("tez.task.get-task.sleep.interval-ms.max", "200", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.task.get-task.sleep.interval-ms.max", "200", "tez-site.xml");

        status = modify_tez_config("tez.task.max-events.per-heartbeat", "500", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.task.max-events.per-heartbeat", "500", "tez-site.xml");

        status = modify_tez_config("tez.session.client.timeout.secs", "-1", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.session.client.timeout.secs", "-1", "tez-site.xml");

        status = modify_tez_config("tez.session.am.dag.submit.timeout.secs", "300", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.session.am.dag.submit.timeout.secs", "300", "tez-site.xml");

        status = modify_tez_config("tez.runtime.compress", "true", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.runtime.compress", "true", "tez-site.xml");

        status = modify_tez_config("tez.runtime.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.runtime.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec", "tez-site.xml");

        status = modify_tez_config("tez.runtime.unordered.output.buffer.size-mb", "100", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.runtime.unordered.output.buffer.size-mb", "100", "tez-site.xml");

        status = modify_tez_config("tez.runtime.convert.user-payload.to.history-text", "false", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.runtime.convert.user-payload.to.history-text", "false", "tez-site.xml");

        status = modify_tez_config("tez.use.cluster.hadoop-libs", "false", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.use.cluster.hadoop-libs", "false", "tez-site.xml");

        status = modify_tez_config("tez.am.tez-ui.history-url.template", "__HISTORY_URL_BASE__?viewPath=%2F%23%2Ftez-app%2F__APPLICATION_ID__", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.am.tez-ui.history-url.template", "__HISTORY_URL_BASE__?viewPath=%2F%23%2Ftez-app%2F__APPLICATION_ID__", "tez-site.xml");

        status = modify_tez_config("tez.tez-ui.history-url.base", "", "tez-site.xml"); //  Empty value
        handle_result(status, "modify_tez_config: tez.tez-ui.history-url.base", "", "tez-site.xml");

        status = modify_tez_config("tez.am.view-acls", "*", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.am.view-acls", "*", "tez-site.xml");

        status = modify_tez_config("tez.runtime.optimize.local.fetch", "true", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.runtime.optimize.local.fetch", "true", "tez-site.xml");

        status = modify_tez_config("tez.task.generate.counters.per.io", "true", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.task.generate.counters.per.io", "true", "tez-site.xml");

        status = modify_tez_config("tez.runtime.sorter.class", "PIPELINED", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.runtime.sorter.class", "PIPELINED", "tez-site.xml");

        status = modify_tez_config("tez.runtime.pipelined.sorter.sort.threads", "2", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.runtime.pipelined.sorter.sort.threads", "2", "tez-site.xml");

        status = modify_tez_config("tez.runtime.io.sort.mb", "272", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.runtime.io.sort.mb", "272", "tez-site.xml");

        status = modify_tez_config("tez.counters.max", "10000", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.counters.max", "10000", "tez-site.xml");

        status = modify_tez_config("tez.counters.max.groups", "3000", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.counters.max.groups", "3000", "tez-site.xml");

        status = modify_tez_config("tez.runtime.shuffle.fetch.buffer.percent", "0.6", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.runtime.shuffle.fetch.buffer.percent", "0.6", "tez-site.xml");

        status = modify_tez_config("tez.runtime.shuffle.memory.limit.percent", "0.25", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.runtime.shuffle.memory.limit.percent", "0.25", "tez-site.xml");

        status = modify_tez_config("tez.history.logging.timeline-cache-plugin.old-num-dags-per-group", "5", "tez-site.xml");
        handle_result(status, "modify_tez_config: tez.history.logging.timeline-cache-plugin.old-num-dags-per-group", "5", "tez-site.xml");

        status = modify_tez_config("yarn.timeline-service.enabled", "false", "tez-site.xml");
        handle_result(status, "modify_tez_config: yarn.timeline-service.enabled", "false", "tez-site.xml");

        break;

        //		 Zeppelin Configuration
    case ZEPPELIN:

        char const* filename = "zeppelin-site.xml";
        //		 Update Zeppelin configuration parameters
        status = set_zeppelin_config(filename, "zeppelin.server.addr", "0.0.0.0");
        handle_result(status, "zeppelin.server.addr", "0.0.0.0", filename);

        status = set_zeppelin_config(filename, "zeppelin.server.port", "9995");
        handle_result(status, "zeppelin.server.port", "9995", filename);

        status = set_zeppelin_config(filename, "zeppelin.server.ssl.port", "9995");
        handle_result(status, "zeppelin.server.ssl.port", "9995", filename);

        status = set_zeppelin_config(filename, "zeppelin.notebook.dir", "notebook");
        handle_result(status, "zeppelin.notebook.dir", "notebook", filename);

        status = set_zeppelin_config(filename, "zeppelin.notebook.homescreen", " ");
        handle_result(status, "zeppelin.notebook.homescreen", " ", filename);

        status = set_zeppelin_config(filename, "zeppelin.notebook.homescreen.hide", "false");
        handle_result(status, "zeppelin.notebook.homescreen.hide", "false", filename);

        status = set_zeppelin_config(filename, "zeppelin.notebook.s3.user", "user");
        handle_result(status, "zeppelin.notebook.s3.user", "user", filename);

        status = set_zeppelin_config(filename, "zeppelin.notebook.s3.bucket", "zeppelin");
        handle_result(status, "zeppelin.notebook.s3.bucket", "zeppelin", filename);

        status = set_zeppelin_config(filename, "zeppelin.notebook.storage", "org.apache.zeppelin.notebook.repo.FileSystemNotebookRepo");
        handle_result(status, "zeppelin.notebook.storage", "org.apache.zeppelin.notebook.repo.FileSystemNotebookRepo", filename);

        status = set_zeppelin_config(filename, "zeppelin.config.storage.class", "org.apache.zeppelin.storage.FileSystemConfigStorage");
        handle_result(status, "zeppelin.config.storage.class", "org.apache.zeppelin.storage.FileSystemConfigStorage", filename);

        status = set_zeppelin_config(filename, "zeppelin.config.fs.dir", "conf");
        handle_result(status, "zeppelin.config.fs.dir", "conf", filename);

        status = set_zeppelin_config(filename, "zeppelin.interpreter.dir", "interpreter");
        handle_result(status, "zeppelin.interpreter.dir", "interpreter", filename);

        status = set_zeppelin_config(filename, "zeppelin.interpreters", "org.apache.zeppelin.spark.SparkInterpreter,org.apache.zeppelin.spark.PySparkInterpreter,org.apache.zeppelin.spark.SparkSqlInterpreter,org.apache.zeppelin.spark.DepInterpreter,org.apache.zeppelin.markdown.Markdown,org.apache.zeppelin.angular.AngularInterpreter,org.apache.zeppelin.shell.ShellInterpreter,org.apache.zeppelin.jdbc.JDBCInterpreter,org.apache.zeppelin.phoenix.PhoenixInterpreter,org.apache.zeppelin.livy.LivySparkInterpreter,org.apache.zeppelin.livy.LivyPySparkInterpreter,org.apache.zeppelin.livy.LivySparkRInterpreter,org.apache.zeppelin.livy.LivySparkSQLInterpreter");
        handle_result(status, "zeppelin.interpreters", "org.apache.zeppelin.spark.SparkInterpreter,org.apache.zeppelin.spark.PySparkInterpreter,org.apache.zeppelin.spark.SparkSqlInterpreter,org.apache.zeppelin.spark.DepInterpreter,org.apache.zeppelin.markdown.Markdown,org.apache.zeppelin.angular.AngularInterpreter,org.apache.zeppelin.shell.ShellInterpreter,org.apache.zeppelin.jdbc.JDBCInterpreter,org.apache.zeppelin.phoenix.PhoenixInterpreter,org.apache.zeppelin.livy.LivySparkInterpreter,org.apache.zeppelin.livy.LivyPySparkInterpreter,org.apache.zeppelin.livy.LivySparkRInterpreter,org.apache.zeppelin.livy.LivySparkSQLInterpreter", filename);

        status = set_zeppelin_config(filename, "zeppelin.interpreter.group.order", "spark,angular,jdbc,livy,md,sh");
        handle_result(status, "zeppelin.interpreter.group.order", "spark,angular,jdbc,livy,md,sh", filename);

        status = set_zeppelin_config(filename, "zeppelin.interpreter.connect.timeout", "30000");
        handle_result(status, "zeppelin.interpreter.connect.timeout", "30000", filename);

        status = set_zeppelin_config(filename, "zeppelin.ssl", "false");
        handle_result(status, "zeppelin.ssl", "false", filename);

        status = set_zeppelin_config(filename, "zeppelin.ssl.client.auth", "false");
        handle_result(status, "zeppelin.ssl.client.auth", "false", filename);

        status = set_zeppelin_config(filename, "zeppelin.ssl.keystore.path", "conf/keystore");
        handle_result(status, "zeppelin.ssl.keystore.path", "conf/keystore", filename);

        status = set_zeppelin_config(filename, "zeppelin.ssl.keystore.type", "JKS");
        handle_result(status, "zeppelin.ssl.keystore.type", "JKS", filename);

        status = set_zeppelin_config(filename, "zeppelin.ssl.keystore.password", "change me");
        handle_result(status, "zeppelin.ssl.keystore.password", "change me", filename);

        status = set_zeppelin_config(filename, "zeppelin.ssl.key.manager.password", "change me");
        handle_result(status, "zeppelin.ssl.key.manager.password", "change me", filename);

        status = set_zeppelin_config(filename, "zeppelin.ssl.truststore.path", "conf/truststore");
        handle_result(status, "zeppelin.ssl.truststore.path", "conf/truststore", filename);

        status = set_zeppelin_config(filename, "zeppelin.ssl.truststore.type", "JKS");
        handle_result(status, "zeppelin.ssl.truststore.type", "JKS", filename);

        status = set_zeppelin_config(filename, "zeppelin.ssl.truststore.password", "change me");
        handle_result(status, "zeppelin.ssl.truststore.password", "change me", filename);

        status = set_zeppelin_config(filename, "zeppelin.server.allowed.origins", "*");
        handle_result(status, "zeppelin.server.allowed.origins", "*", filename);

        status = set_zeppelin_config(filename, "zeppelin.anonymous.allowed", "false");
        handle_result(status, "zeppelin.anonymous.allowed", "false", filename);

        status = set_zeppelin_config(filename, "zeppelin.notebook.public", "false");
        handle_result(status, "zeppelin.notebook.public", "false", filename);

        status = set_zeppelin_config(filename, "zeppelin.websocket.max.text.message.size", "1024000");
        handle_result(status, "zeppelin.websocket.max.text.message.size", "1024000", filename);

        status = set_zeppelin_config(filename, "zeppelin.interpreter.config.upgrade", "true");
        handle_result(status, "zeppelin.interpreter.config.upgrade", "true", filename);


        //		 Update Log4j configuration parameters
        status = set_zeppelin_config("log4j.properties", "log4j.rootLogger", "INFO, dailyfile");
        handle_result(status, "log4j.rootLogger", "INFO, dailyfile", "log4j.properties");

        status = set_zeppelin_config("log4j.properties", "log4j.appender.stdout", "org.apache.log4j.ConsoleAppender");
        handle_result(status, "log4j.appender.stdout", "org.apache.log4j.ConsoleAppender", "log4j.properties");

        status = set_zeppelin_config("log4j.properties", "log4j.appender.stdout.layout", "org.apache.log4j.PatternLayout");
        handle_result(status, "log4j.appender.stdout.layout", "org.apache.log4j.PatternLayout", "log4j.properties");

        status = set_zeppelin_config("log4j.properties", "log4j.appender.stdout.layout.ConversionPattern", "%5p [%d{ISO8601}] ({%t} %F[%M]:%L) - %m%n");
        handle_result(status, "log4j.appender.stdout.layout.ConversionPattern", "%5p [%d{ISO8601}] ({%t} %F[%M]:%L) - %m%n", "log4j.properties");

        status = set_zeppelin_config("log4j.properties", "log4j.appender.dailyfile.DatePattern", ".yyyy-MM-dd");
        handle_result(status, "log4j.appender.dailyfile.DatePattern", ".yyyy-MM-dd", "log4j.properties");

        status = set_zeppelin_config("log4j.properties", "log4j.appender.dailyfile.Threshold", "INFO");
        handle_result(status, "log4j.appender.dailyfile.Threshold", "INFO", "log4j.properties");

        status = set_zeppelin_config("log4j.properties", "log4j.appender.dailyfile", "org.apache.log4j.DailyRollingFileAppender");
        handle_result(status, "log4j.appender.dailyfile", "org.apache.log4j.DailyRollingFileAppender", "log4j.properties");

        status = set_zeppelin_config("log4j.properties", "log4j.appender.dailyfile.File", "${zeppelin.log.file}");
        handle_result(status, "log4j.appender.dailyfile.File", "${zeppelin.log.file}", "log4j.properties");

        status = set_zeppelin_config("log4j.properties", "log4j.appender.dailyfile.layout", "org.apache.log4j.PatternLayout");
        handle_result(status, "log4j.appender.dailyfile.layout", "org.apache.log4j.PatternLayout", "log4j.properties");

        status = set_zeppelin_config("log4j.properties", "log4j.appender.dailyfile.layout.ConversionPattern", "%5p [%d{ISO8601}] ({%t} %F[%M]:%L) - %m%n");
        handle_result(status, "log4j.appender.dailyfile.layout.ConversionPattern", "%5p [%d{ISO8601}] ({%t} %F[%M]:%L) - %m%n", "log4j.properties");


        //		 Update Shiro configuration parameters
        status = set_zeppelin_config("zeppelin-shiro.ini", "users.admin", "$shiro1$SHA-256$500000$p6Be9+t2hdUXJQj2D0b1fg==$bea5JIMqcVF3J6eNZGWQ/3eeDByn5iEZDuGsEip06+M=, admin");
        handle_result(status, "users.admin", "$shiro1$SHA-256$500000$p6Be9+t2hdUXJQj2D0b1fg==$bea5JIMqcVF3J6eNZGWQ/3eeDByn5iEZDuGsEip06+M=, admin", "zeppelin-shiro.ini");

        status = set_zeppelin_config("zeppelin-shiro.ini", "users.user1", "$shiro1$SHA-256$500000$G2ymy/qmuZnGY6or4v2KfA==$v9fabqWgCNCgechtOUqAQenGDs0OSLP28q2wolPT4wU=, role1, role2");
        handle_result(status, "users.user1", "$shiro1$SHA-256$500000$G2ymy/qmuZnGY6or4v2KfA==$v9fabqWgCNCgechtOUqAQenGDs0OSLP28q2wolPT4wU=, role1, role2", "zeppelin-shiro.ini");

        status = set_zeppelin_config("zeppelin-shiro.ini", "users.user2", "$shiro1$SHA-256$500000$aHBgiuwSgAcP3Xt5mEzeFw==$KosBnN2BNKA9/KHBL0hnU/woJFl+xzJFj12NQ0fnjCU=, role3");
        handle_result(status, "users.user2", "$shiro1$SHA-256$500000$aHBgiuwSgAcP3Xt5mEzeFw==$KosBnN2BNKA9/KHBL0hnU/woJFl+xzJFj12NQ0fnjCU=, role3", "zeppelin-shiro.ini");

        status = set_zeppelin_config("zeppelin-shiro.ini", "users.user3", "$shiro1$SHA-256$500000$nf0GzH10GbYVoxa7DOlOSw==$ov/IA5W8mRWPwvAoBjNYxg3udJK0EmrVMvFCwcr9eAs=, role2");
        handle_result(status, "users.user3", "$shiro1$SHA-256$500000$nf0GzH10GbYVoxa7DOlOSw==$ov/IA5W8mRWPwvAoBjNYxg3udJK0EmrVMvFCwcr9eAs=, role2", "zeppelin-shiro.ini");

        status = set_zeppelin_config("zeppelin-shiro.ini", "main.passwordMatcher", "org.apache.shiro.authc.credential.PasswordMatcher");
        handle_result(status, "main.passwordMatcher", "org.apache.shiro.authc.credential.PasswordMatcher", "zeppelin-shiro.ini");

        status = set_zeppelin_config("zeppelin-shiro.ini", "main.iniRealm.credentialsMatcher", "$passwordMatcher");
        handle_result(status, "main.iniRealm.credentialsMatcher", "$passwordMatcher", "zeppelin-shiro.ini");

        status = set_zeppelin_config("zeppelin-shiro.ini", "main.sessionManager", "org.apache.shiro.web.session.mgt.DefaultWebSessionManager");
        handle_result(status, "main.sessionManager", "org.apache.shiro.web.session.mgt.DefaultWebSessionManager", "zeppelin-shiro.ini");

        status = set_zeppelin_config("zeppelin-shiro.ini", "main.cacheManager", "org.apache.shiro.cache.MemoryConstrainedCacheManager");
        handle_result(status, "main.cacheManager", "org.apache.shiro.cache.MemoryConstrainedCacheManager", "zeppelin-shiro.ini");

        status = set_zeppelin_config("zeppelin-shiro.ini", "main.securityManager.cacheManager", "$cacheManager");
        handle_result(status, "main.securityManager.cacheManager", "$cacheManager", "zeppelin-shiro.ini");

        status = set_zeppelin_config("zeppelin-shiro.ini", "main.cookie", "org.apache.shiro.web.servlet.SimpleCookie");
        handle_result(status, "main.cookie", "org.apache.shiro.web.servlet.SimpleCookie", "zeppelin-shiro.ini");

        status = set_zeppelin_config("zeppelin-shiro.ini", "main.cookie.name", "JSESSIONID");
        handle_result(status, "main.cookie.name", "JSESSIONID", "zeppelin-shiro.ini");

        status = set_zeppelin_config("zeppelin-shiro.ini", "main.cookie.httpOnly", "true");
        handle_result(status, "main.cookie.httpOnly", "true", "zeppelin-shiro.ini");

        status = set_zeppelin_config("zeppelin-shiro.ini", "main.sessionManager.sessionIdCookie", "$cookie");
        handle_result(status, "main.sessionManager.sessionIdCookie", "$cookie", "zeppelin-shiro.ini");

        status = set_zeppelin_config("zeppelin-shiro.ini", "main.securityManager.sessionManager", "$sessionManager");
        handle_result(status, "main.securityManager.sessionManager", "$sessionManager", "zeppelin-shiro.ini");

        status = set_zeppelin_config("zeppelin-shiro.ini", "main.securityManager.sessionManager.globalSessionTimeout", "86400000");
        handle_result(status, "main.securityManager.sessionManager.globalSessionTimeout", "86400000", "zeppelin-shiro.ini");

        status = set_zeppelin_config("zeppelin-shiro.ini", "main.shiro.loginUrl", "/api/login");
        handle_result(status, "main.shiro.loginUrl", "/api/login", "zeppelin-shiro.ini");

        status = set_zeppelin_config("zeppelin-shiro.ini", "roles.role1", "*");
        handle_result(status, "roles.role1", "*", "zeppelin-shiro.ini");

        status = set_zeppelin_config("zeppelin-shiro.ini", "roles.role2", "*");
        handle_result(status, "roles.role2", "*", "zeppelin-shiro.ini");

        status = set_zeppelin_config("zeppelin-shiro.ini", "roles.role3", "*");
        handle_result(status, "roles.role3", "*", "zeppelin-shiro.ini");

        status = set_zeppelin_config("zeppelin-shiro.ini", "roles.admin", "*");
        handle_result(status, "roles.admin", "*", "zeppelin-shiro.ini");

        status = set_zeppelin_config("zeppelin-shiro.ini", "urls./api/version", "anon");
        handle_result(status, "urls./api/version", "anon", "zeppelin-shiro.ini");

        status = set_zeppelin_config("zeppelin-shiro.ini", "urls./**", "authc");
        handle_result(status, "urls./**", "authc", "zeppelin-shiro.ini");

        break;

        //		 ZooKeeper Configuration
    case ZOOKEEPER:

        status = modify_zookeeper_config("tickTime", "3000", "zoo.cfg");
        handle_result(status, "modify_zookeeper_config: tickTime", "3000", "zoo.cfg");

        status = modify_zookeeper_config("initLimit", "10", "zoo.cfg");
        handle_result(status, "modify_zookeeper_config: initLimit", "10", "zoo.cfg");

        status = modify_zookeeper_config("syncLimit", "5", "zoo.cfg");
        handle_result(status, "modify_zookeeper_config: syncLimit", "5", "zoo.cfg");

        status = modify_zookeeper_config("clientPort", "2181", "zoo.cfg");
        handle_result(status, "modify_zookeeper_config: clientPort", "2181", "zoo.cfg");

        status = modify_zookeeper_config("dataDir", "/tmp/zookeeper", "zoo.cfg");
        handle_result(status, "modify_zookeeper_config: dataDir", "/hadoop/zookeeper", "zoo.cfg");

        status = modify_zookeeper_config("autopurge.snapRetainCount", "30", "zoo.cfg");
        handle_result(status, "modify_zookeeper_config: autopurge.snapRetainCount", "30", "zoo.cfg");

        status = modify_zookeeper_config("autopurge.purgeInterval", "24", "zoo.cfg");
        handle_result(status, "modify_zookeeper_config: autopurge.purgeInterval", "24", "zoo.cfg");

        status = modify_zookeeper_config("4lw.commands.whitelist", "ruok", "zoo.cfg");
        handle_result(status, "modify_zookeeper_config: 4lw.commands.whitelist", "ruok", "zoo.cfg");

        status = modify_zookeeper_config("admin.enableServer", "true", "zoo.cfg");
        handle_result(status, "modify_zookeeper_config: admin.enableServer", "true", "zoo.cfg");

        status = modify_zookeeper_config("admin.serverPort", "9393", "zoo.cfg");
        handle_result(status, "modify_zookeeper_config: admin.serverPort", "9393", "zoo.cfg");

        //			 Set log rotation properties (likely in a separate config file)
        status = modify_zookeeper_config("zookeeper_log_max_backup_size", "10", "zookeeper-env.properties");
        handle_result(status, "modify_zookeeper_config: zookeeper_log_max_backup_size", "10", "zookeeper-env.properties");

        status = modify_zookeeper_config("zookeeper_log_number_of_backup_files", "10", "zookeeper-env.properties");
        handle_result(status, "modify_zookeeper_config: zookeeper_log_number_of_backup_files", "10", "zookeeper-env.properties");

        //			 Set log rotation properties
        status = modify_zookeeper_config("zookeeper_log_max_backup_size", "10", "log4j.properties");
        handle_result(status, "modify_zookeeper_config: zookeeper_log_max_backup_size", "10", "log4j.properties");

        status = modify_zookeeper_config("zookeeper_log_number_of_backup_files", "10", "log4j.properties");
        handle_result(status, "modify_zookeeper_config: zookeeper_log_number_of_backup_files", "10", "log4j.properties");

        //			 Set individual log4j configuration lines
        status = modify_zookeeper_config("log4j.rootLogger", "INFO, CONSOLE, ROLLINGFILE", "log4j.properties");
        handle_result(status, "modify_zookeeper_config: log4j.rootLogger", "INFO, CONSOLE, ROLLINGFILE", "log4j.properties");

        status = modify_zookeeper_config("log4j.appender.CONSOLE", "org.apache.log4j.ConsoleAppender", "log4j.properties");
        handle_result(status, "modify_zookeeper_config: log4j.appender.CONSOLE", "org.apache.log4j.ConsoleAppender", "log4j.properties");

        status = modify_zookeeper_config("log4j.appender.CONSOLE.Threshold", "INFO", "log4j.properties");
        handle_result(status, "modify_zookeeper_config: log4j.appender.CONSOLE.Threshold", "INFO", "log4j.properties");

        status = modify_zookeeper_config("log4j.appender.CONSOLE.layout", "org.apache.log4j.PatternLayout", "log4j.properties");
        handle_result(status, "modify_zookeeper_config: log4j.appender.CONSOLE.layout", "org.apache.log4j.PatternLayout", "log4j.properties");

        status = modify_zookeeper_config("log4j.appender.CONSOLE.layout.ConversionPattern", "%d{ISO8601} - %-5p [%t:%C{1}@%L] - %m%n", "log4j.properties");
        handle_result(status, "modify_zookeeper_config: log4j.appender.CONSOLE.layout.ConversionPattern", "%d{ISO8601} - %-5p [%t:%C{1}@%L] - %m%n", "log4j.properties");

        status = modify_zookeeper_config("log4j.appender.ROLLINGFILE", "org.apache.log4j.RollingFileAppender", "log4j.properties");
        handle_result(status, "modify_zookeeper_config: log4j.appender.ROLLINGFILE", "org.apache.log4j.RollingFileAppender", "log4j.properties");

        status = modify_zookeeper_config("log4j.appender.ROLLINGFILE.Threshold", "DEBUG", "log4j.properties");
        handle_result(status, "modify_zookeeper_config: log4j.appender.ROLLINGFILE.Threshold", "DEBUG", "log4j.properties");

        status = modify_zookeeper_config("log4j.appender.ROLLINGFILE.File", "{{zk_log_dir}}/zookeeper.log", "log4j.properties");
        handle_result(status, "modify_zookeeper_config: log4j.appender.ROLLINGFILE.File", "{{zk_log_dir}}/zookeeper.log", "log4j.properties");

        status = modify_zookeeper_config("log4j.appender.ROLLINGFILE.MaxFileSize", "{{zookeeper_log_max_backup_size}}MB", "log4j.properties");
        handle_result(status, "modify_zookeeper_config: log4j.appender.ROLLINGFILE.MaxFileSize", "{{zookeeper_log_max_backup_size}}MB", "log4j.properties");

        status = modify_zookeeper_config("log4j.appender.ROLLINGFILE.layout", "org.apache.log4j.PatternLayout", "log4j.properties");
        handle_result(status, "modify_zookeeper_config: log4j.appender.ROLLINGFILE.layout", "org.apache.log4j.PatternLayout", "log4j.properties");

        status = modify_zookeeper_config("log4j.appender.ROLLINGFILE.layout.ConversionPattern", "%d{ISO8601} - %-5p [%t:%C{1}@%L] - %m%n", "log4j.properties");
        handle_result(status, "modify_zookeeper_config: log4j.appender.ROLLINGFILE.layout.ConversionPattern", "%d{ISO8601} - %-5p [%t:%C{1}@%L] - %m%n", "log4j.properties");

        status = modify_zookeeper_config("log4j.appender.TRACEFILE", "org.apache.log4j.FileAppender", "log4j.properties");
        handle_result(status, "modify_zookeeper_config: log4j.appender.TRACEFILE", "org.apache.log4j.FileAppender", "log4j.properties");

        status = modify_zookeeper_config("log4j.appender.TRACEFILE.Threshold", "TRACE", "log4j.properties");
        handle_result(status, "modify_zookeeper_config: log4j.appender.TRACEFILE.Threshold", "TRACE", "log4j.properties");

        status = modify_zookeeper_config("log4j.appender.TRACEFILE.File", "zookeeper_trace.log", "log4j.properties");
        handle_result(status, "modify_zookeeper_config: log4j.appender.TRACEFILE.File", "zookeeper_trace.log", "log4j.properties");

        status = modify_zookeeper_config("log4j.appender.TRACEFILE.layout", "org.apache.log4j.PatternLayout", "log4j.properties");
        handle_result(status, "modify_zookeeper_config: log4j.appender.TRACEFILE.layout", "org.apache.log4j.PatternLayout", "log4j.properties");

        status = modify_zookeeper_config("log4j.appender.TRACEFILE.layout.ConversionPattern", "%d{ISO8601} - %-5p [%t:%C{1}@%L][%x] - %m%n", "log4j.properties");
        handle_result(status, "modify_zookeeper_config: log4j.appender.TRACEFILE.layout.ConversionPattern", "%d{ISO8601} - %-5p [%t:%C{1}@%L][%x] - %m%n", "log4j.properties");

        status = modify_zookeeper_config(
            "log4j.appender.CONSOLE.layout.ConversionPattern",
            "%d{ISO8601} - %-5p [%t:%C{1}@%L] - %m%n",
            "log4j.properties"
            );
        handle_result(
            status,
            "modify_zookeeper_config: log4j.appender.CONSOLE.layout.ConversionPattern",
            "%d{ISO8601} - %-5p [%t:%C{1}@%L] - %m%n",
            "log4j.properties"
            );
        break;

        //		 Flink Configuration
    case FLINK:

        status = update_flink_config("jobmanager.archive.fs.dir", "hdfs:/completed-jobs/", "config.yaml");
        handle_result(status, "jobmanager.archive.fs.dir", "hdfs:/completed-jobs/", "config.yaml");
        status = update_flink_config("historyserver.archive.fs.dir", "hdfs:/completed-jobs/", "config.yaml");
        handle_result(status, "historyserver.archive.fs.dir", "hdfs:/completed-jobs/", "config.yaml");
        status = update_flink_config("historyserver.web.port", "8082", "config.yaml");
        handle_result(status, "historyserver.web.port", "8082", "config.yaml");
        status = update_flink_config("historyserver.archive.fs.refresh-interval", "10000", "config.yaml");
        handle_result(status, "historyserver.archive.fs.refresh-interval", "10000", "config.yaml");

        //		 Update security parameters
        status = update_flink_config("security.kerberos.login.keytab", "none", "config.yaml");
        handle_result(status, "security.kerberos.login.keytab", "none", "config.yaml");
        status = update_flink_config("security.kerberos.login.principal", "none", "config.yaml");
        handle_result(status, "security.kerberos.login.principal", "none", "config.yaml");
        //		 Translate template content into individual configuration parameters
        status = update_flink_config("jobmanager.rpc.address", "localhost", "config.yaml");
        handle_result(status, "jobmanager.rpc.address", "localhost", "config.yaml");
        status = update_flink_config("jobmanager.rpc.port", "6123", "config.yaml");
        handle_result(status, "jobmanager.rpc.port", "6123", "config.yaml");
        status = update_flink_config("jobmanager.bind-host", "localhost", "config.yaml");
        handle_result(status, "jobmanager.bind-host", "localhost", "config.yaml");
        status = update_flink_config("jobmanager.memory.process.size", "1024m", "config.yaml");
        handle_result(status, "jobmanager.memory.process.size", "1024m", "config.yaml");
        status = update_flink_config("taskmanager.bind-host", "localhost", "config.yaml");
        handle_result(status, "taskmanager.bind-host", "localhost", "config.yaml");
        status = update_flink_config("taskmanager.host", "localhost", "config.yaml");
        handle_result(status, "taskmanager.host", "localhost", "config.yaml");
        status = update_flink_config("taskmanager.memory.process.size", "1024m", "config.yaml");
        handle_result(status, "taskmanager.memory.process.size", "1024m", "config.yaml");
        status = update_flink_config("taskmanager.numberOfTaskSlots", "1", "config.yaml");
        handle_result(status, "taskmanager.numberOfTaskSlots", "1", "config.yaml");
        status = update_flink_config("parallelism.default", "1", "config.yaml");
        handle_result(status, "parallelism.default", "1", "config.yaml");
        status = update_flink_config("env.java.home", "{{java_home}}", "config.yaml");
        handle_result(status, "env.java.home", "{{java_home}}", "config.yaml");
        status = update_flink_config("env.hadoop.conf.dir", "{{hadoop_conf_dir}}", "config.yaml");
        handle_result(status, "env.hadoop.conf.dir", "{{hadoop_conf_dir}}", "config.yaml");
        status = update_flink_config("env.pid.dir", "{{flink_pid_dir}}", "config.yaml");
        handle_result(status, "env.pid.dir", "{{flink_pid_dir}}", "config.yaml");
        status = update_flink_config("env.log.dir", "{{flink_log_dir}}", "config.yaml");
        handle_result(status, "env.log.dir", "{{flink_log_dir}}", "config.yaml");
        status = update_flink_config("jobmanager.execution.failover-strategy", "region", "config.yaml");
        handle_result(status, "jobmanager.execution.failover-strategy", "region", "config.yaml");
        status = update_flink_config("rest.address", "localhost", "config.yaml");
        handle_result(status, "rest.address", "localhost", "config.yaml");
        status = update_flink_config("rest.bind-address", "localhost", "config.yaml");
        handle_result(status, "rest.bind-address", "localhost", "config.yaml");
        status = update_flink_config("jobmanager.archive.fs.dir", "{{jobmanager_archive_fs_dir}}", "config.yaml");
        handle_result(status, "jobmanager.archive.fs.dir", "{{jobmanager_archive_fs_dir}}", "config.yaml");
        status = update_flink_config("historyserver.web.port", "{{historyserver_web_port}}", "config.yaml");
        handle_result(status, "historyserver.web.port", "{{historyserver_web_port}}", "config.yaml");
        status = update_flink_config("historyserver.archive.fs.dir", "{{historyserver_archive_fs_dir}}", "config.yaml");
        handle_result(status, "historyserver.archive.fs.dir", "{{historyserver_archive_fs_dir}}", "config.yaml");
        status = update_flink_config("historyserver.archive.fs.refresh-interval", "{{historyserver_archive_fs_refresh_interval}}", "config.yaml");
        handle_result(status, "historyserver.archive.fs.refresh-interval", "{{historyserver_archive_fs_refresh_interval}}", "config.yaml");

        //		 Conditionally include Kerberos configuration
        status = update_flink_config("security.kerberos.login.use-ticket-cache", "{{'true' if security_enabled else ''}}", "config.yaml");
        handle_result(status, "security.kerberos.login.use-ticket-cache", "{{'true' if security_enabled else ''}}", "config.yaml");
        status = update_flink_config("security.kerberos.login.keytab", "{{security_kerberos_login_keytab if security_enabled else ''}}", "config.yaml");
        handle_result(status, "security.kerberos.login.keytab", "{{security_kerberos_login_keytab if security_enabled else ''}}", "config.yaml");
        status = update_flink_config("security.kerberos.login.principal", "{{security_kerberos_login_principal if security_enabled else ''}}", "config.yaml");
        handle_result(status, "security.kerberos.login.principal", "{{security_kerberos_login_principal if security_enabled else ''}}", "config.yaml");

        //		 Update with explicit values from XML properties
        status = update_flink_config("jobmanager.archive.fs.dir", "hdfs:/completed-jobs/", "config.yaml");
        handle_result(status, "jobmanager.archive.fs.dir", "hdfs:/completed-jobs/", "config.yaml");
        status = update_flink_config("historyserver.archive.fs.dir", "hdfs:/completed-jobs/", "config.yaml");
        handle_result(status, "historyserver.archive.fs.dir", "hdfs:/completed-jobs/", "config.yaml");
        status = update_flink_config("historyserver.web.port", "8082", "config.yaml");
        handle_result(status, "historyserver.web.port", "8082", "config.yaml");
        status = update_flink_config("historyserver.archive.fs.refresh-interval", "10000", "config.yaml");
        handle_result(status, "historyserver.archive.fs.refresh-interval", "10000", "config.yaml");
        status = update_flink_config("security.kerberos.login.keytab", "none", "config.yaml");
        handle_result(status, "security.kerberos.login.keytab", "none", "config.yaml");
        status = update_flink_config("security.kerberos.login.principal", "none", "config.yaml");
        handle_result(status, "security.kerberos.login.principal", "none", "config.yaml");
        //		 Update Flink log4j configuration parameters
        status = update_flink_config("monitorInterval", "30", "log4j.properties");
        handle_result(status, "monitorInterval", "30", "log4j.properties");
        status = update_flink_config("rootLogger.level", "INFO", "log4j.properties");
        handle_result(status, "rootLogger.level", "INFO", "log4j.properties");
        status = update_flink_config("rootLogger.appenderRef.file.ref", "FileAppender", "log4j.properties");
        handle_result(status, "rootLogger.appenderRef.file.ref", "FileAppender", "log4j.properties");
        status = update_flink_config("appender.file.name", "FileAppender", "log4j.properties");
        handle_result(status, "appender.file.name", "FileAppender", "log4j.properties");
        status = update_flink_config("appender.file.type", "FILE", "log4j.properties");
        handle_result(status, "appender.file.type", "FILE", "log4j.properties");
        status = update_flink_config("appender.file.append", "false", "log4j.properties");
        handle_result(status, "appender.file.append", "false", "log4j.properties");
        status = update_flink_config("appender.file.fileName", "${sys:log.file}", "log4j.properties");
        handle_result(status, "appender.file.fileName", "${sys:log.file}", "log4j.properties");
        status = update_flink_config("appender.file.layout.type", "PatternLayout", "log4j.properties");
        handle_result(status, "appender.file.layout.type", "PatternLayout", "log4j.properties");
        status = update_flink_config("appender.file.layout.pattern", "%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n", "log4j.properties");
        handle_result(status, "appender.file.layout.pattern", "%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n", "log4j.properties");
        status = update_flink_config("logger.yarn.name", "org.apache.flink.yarn", "log4j.properties");
        handle_result(status, "logger.yarn.name", "org.apache.flink.yarn", "log4j.properties");
        status = update_flink_config("logger.yarn.level", "INFO", "log4j.properties");
        handle_result(status, "logger.yarn.level", "INFO", "log4j.properties");
        status = update_flink_config("logger.yarn.appenderRef.console.ref", "ConsoleAppender", "log4j.properties");
        handle_result(status, "logger.yarn.appenderRef.console.ref", "ConsoleAppender", "log4j.properties");
        status = update_flink_config("logger.yarncli.name", "org.apache.flink.yarn.cli.FlinkYarnSessionCli", "log4j.properties");
        handle_result(status, "logger.yarncli.name", "org.apache.flink.yarn.cli.FlinkYarnSessionCli", "log4j.properties");
        status = update_flink_config("logger.yarncli.level", "INFO", "log4j.properties");
        handle_result(status, "logger.yarncli.level", "INFO", "log4j.properties");
        status = update_flink_config("logger.yarncli.appenderRef.console.ref", "ConsoleAppender", "log4j.properties");
        handle_result(status, "logger.yarncli.appenderRef.console.ref", "ConsoleAppender", "log4j.properties");
        status = update_flink_config("logger.hadoop.name", "org.apache.hadoop", "log4j.properties");
        handle_result(status, "logger.hadoop.name", "org.apache.hadoop", "log4j.properties");
        status = update_flink_config("logger.hadoop.level", "INFO", "log4j.properties");
        handle_result(status, "logger.hadoop.level", "INFO", "log4j.properties");
        status = update_flink_config("logger.hadoop.appenderRef.console.ref", "ConsoleAppender", "log4j.properties");
        handle_result(status, "logger.hadoop.appenderRef.console.ref", "ConsoleAppender", "log4j.properties");
        status = update_flink_config("logger.hive.name", "org.apache.hadoop.hive", "log4j.properties");
        handle_result(status, "logger.hive.name", "org.apache.hadoop.hive", "log4j.properties");
        status = update_flink_config("logger.hive.level", "INFO", "log4j.properties");
        handle_result(status, "logger.hive.level", "INFO", "log4j.properties");
        status = update_flink_config("logger.hive.additivity", "false", "log4j.properties");
        handle_result(status, "logger.hive.additivity", "false", "log4j.properties");
        status = update_flink_config("logger.hive.appenderRef.file.ref", "FileAppender", "log4j.properties");
        handle_result(status, "logger.hive.appenderRef.file.ref", "FileAppender", "log4j.properties");
        status = update_flink_config("logger.kubernetes.name", "org.apache.flink.kubernetes", "log4j.properties");
        handle_result(status, "logger.kubernetes.name", "org.apache.flink.kubernetes", "log4j.properties");
        status = update_flink_config("logger.kubernetes.level", "INFO", "log4j.properties");
        handle_result(status, "logger.kubernetes.level", "INFO", "log4j.properties");
        status = update_flink_config("logger.kubernetes.appenderRef.console.ref", "ConsoleAppender", "log4j.properties");
        handle_result(status, "logger.kubernetes.appenderRef.console.ref", "ConsoleAppender", "log4j.properties");
        status = update_flink_config("appender.console.name", "ConsoleAppender", "log4j.properties");
        handle_result(status, "appender.console.name", "ConsoleAppender", "log4j.properties");
        status = update_flink_config("appender.console.type", "CONSOLE", "log4j.properties");
        handle_result(status, "appender.console.type", "CONSOLE", "log4j.properties");
        status = update_flink_config("appender.console.layout.type", "PatternLayout", "log4j.properties");
        handle_result(status, "appender.console.layout.type", "PatternLayout", "log4j.properties");
        status = update_flink_config("appender.console.layout.pattern", "%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n", "log4j.properties");
        handle_result(status, "appender.console.layout.pattern", "%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n", "log4j.properties");
        status = update_flink_config("logger.hadoopnative.name", "org.apache.hadoop.util.NativeCodeLoader", "log4j.properties");
        handle_result(status, "logger.hadoopnative.name", "org.apache.hadoop.util.NativeCodeLoader", "log4j.properties");
        status = update_flink_config("logger.hadoopnative.level", "OFF", "log4j.properties");
        handle_result(status, "logger.hadoopnative.level", "OFF", "log4j.properties");
        status = update_flink_config("logger.netty.name", "org.jboss.netty.channel.DefaultChannelPipeline", "log4j.properties");
        handle_result(status, "logger.netty.name", "org.jboss.netty.channel.DefaultChannelPipeline", "log4j.properties");
        status = update_flink_config("logger.netty.level", "OFF", "log4j.properties");
        handle_result(status, "logger.netty.level", "OFF", "log4j.properties");
        //		 Update Flink console logging configuration
        status = update_flink_config("monitorInterval", "30", "log4j-console.properties");
        handle_result(status, "monitorInterval", "30", "log4j-console.properties");
        status = update_flink_config("rootLogger.level", "INFO", "log4j-console.properties");
        handle_result(status, "rootLogger.level", "INFO", "log4j-console.properties");
        status = update_flink_config("rootLogger.appenderRef.console.ref", "ConsoleAppender", "log4j-console.properties");
        handle_result(status, "rootLogger.appenderRef.console.ref", "ConsoleAppender", "log4j-console.properties");
        status = update_flink_config("rootLogger.appenderRef.rolling.ref", "RollingFileAppender", "log4j-console.properties");
        handle_result(status, "rootLogger.appenderRef.rolling.ref", "RollingFileAppender", "log4j-console.properties");
        status = update_flink_config("logger.akka.name", "akka", "log4j-console.properties");
        handle_result(status, "logger.akka.name", "akka", "log4j-console.properties");
        status = update_flink_config("logger.akka.level", "INFO", "log4j-console.properties");
        handle_result(status, "logger.akka.level", "INFO", "log4j-console.properties");
        status = update_flink_config("logger.kafka.name", "org.apache.kafka", "log4j-console.properties");
        handle_result(status, "logger.kafka.name", "org.apache.kafka", "log4j-console.properties");
        status = update_flink_config("logger.kafka.level", "INFO", "log4j-console.properties");
        handle_result(status, "logger.kafka.level", "INFO", "log4j-console.properties");
        status = update_flink_config("logger.hadoop.name", "org.apache.hadoop", "log4j-console.properties");
        handle_result(status, "logger.hadoop.name", "org.apache.hadoop", "log4j-console.properties");
        status = update_flink_config("logger.hadoop.level", "INFO", "log4j-console.properties");
        handle_result(status, "logger.hadoop.level", "INFO", "log4j-console.properties");
        status = update_flink_config("logger.zookeeper.name", "org.apache.zookeeper", "log4j-console.properties");
        handle_result(status, "logger.zookeeper.name", "org.apache.zookeeper", "log4j-console.properties");
        status = update_flink_config("logger.zookeeper.level", "INFO", "log4j-console.properties");
        handle_result(status, "logger.zookeeper.level", "INFO", "log4j-console.properties");
        status = update_flink_config("logger.shaded_zookeeper.name", "org.apache.flink.shaded.zookeeper3", "log4j-console.properties");
        handle_result(status, "logger.shaded_zookeeper.name", "org.apache.flink.shaded.zookeeper3", "log4j-console.properties");
        status = update_flink_config("logger.shaded_zookeeper.level", "INFO", "log4j-console.properties");
        handle_result(status, "logger.shaded_zookeeper.level", "INFO", "log4j-console.properties");
        status = update_flink_config("appender.console.name", "ConsoleAppender", "log4j-console.properties");
        handle_result(status, "appender.console.name", "ConsoleAppender", "log4j-console.properties");
        status = update_flink_config("appender.console.type", "CONSOLE", "log4j-console.properties");
        handle_result(status, "appender.console.type", "CONSOLE", "log4j-console.properties");
        status = update_flink_config("appender.console.layout.type", "PatternLayout", "log4j-console.properties");
        handle_result(status, "appender.console.layout.type", "PatternLayout", "log4j-console.properties");
        status = update_flink_config("appender.console.layout.pattern", "%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n", "log4j-console.properties");
        handle_result(status, "appender.console.layout.pattern", "%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n", "log4j-console.properties");
        status = update_flink_config("appender.rolling.name", "RollingFileAppender", "log4j-console.properties");
        handle_result(status, "appender.rolling.name", "RollingFileAppender", "log4j-console.properties");
        status = update_flink_config("appender.rolling.type", "RollingFile", "log4j-console.properties");
        handle_result(status, "appender.rolling.type", "RollingFile", "log4j-console.properties");
        status = update_flink_config("appender.rolling.append", "true", "log4j-console.properties");
        handle_result(status, "appender.rolling.append", "true", "log4j-console.properties");
        status = update_flink_config("appender.rolling.fileName", "${sys:log.file}", "log4j-console.properties");
        handle_result(status, "appender.rolling.fileName", "${sys:log.file}", "log4j-console.properties");
        status = update_flink_config("appender.rolling.filePattern", "${sys:log.file}.%i", "log4j-console.properties");
        handle_result(status, "appender.rolling.filePattern", "${sys:log.file}.%i", "log4j-console.properties");
        status = update_flink_config("appender.rolling.layout.type", "PatternLayout", "log4j-console.properties");
        handle_result(status, "appender.rolling.layout.type", "PatternLayout", "log4j-console.properties");
        status = update_flink_config("appender.rolling.layout.pattern", "%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n", "log4j-console.properties");
        handle_result(status, "appender.rolling.layout.pattern", "%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n", "log4j-console.properties");
        status = update_flink_config("appender.rolling.policies.type", "Policies", "log4j-console.properties");
        handle_result(status, "appender.rolling.policies.type", "Policies", "log4j-console.properties");
        status = update_flink_config("appender.rolling.policies.size.type", "SizeBasedTriggeringPolicy", "log4j-console.properties");
        handle_result(status, "appender.rolling.policies.size.type", "SizeBasedTriggeringPolicy", "log4j-console.properties");
        status = update_flink_config("appender.rolling.policies.size.size", "100MB", "log4j-console.properties");
        handle_result(status, "appender.rolling.policies.size.size", "100MB", "log4j-console.properties");
        status = update_flink_config("appender.rolling.policies.startup.type", "OnStartupTriggeringPolicy", "log4j-console.properties");
        handle_result(status, "appender.rolling.policies.startup.type", "OnStartupTriggeringPolicy", "log4j-console.properties");
        status = update_flink_config("appender.rolling.strategy.type", "DefaultRolloverStrategy", "log4j-console.properties");
        handle_result(status, "appender.rolling.strategy.type", "DefaultRolloverStrategy", "log4j-console.properties");
        status = update_flink_config("appender.rolling.strategy.max", "${env:MAX_LOG_FILE_NUMBER:-10}", "log4j-console.properties");
        handle_result(status, "appender.rolling.strategy.max", "${env:MAX_LOG_FILE_NUMBER:-10}", "log4j-console.properties");
        status = update_flink_config("logger.netty.name", "org.jboss.netty.channel.DefaultChannelPipeline", "log4j-console.properties");
        handle_result(status, "logger.netty.name", "org.jboss.netty.channel.DefaultChannelPipeline", "log4j-console.properties");
        status = update_flink_config("logger.netty.level", "OFF", "log4j-console.properties");
        handle_result(status, "logger.netty.level", "OFF", "log4j-console.properties");
        //		 Update Flink logging configuration parameters
        status = update_flink_config("monitorInterval", "30", "log4j.properties");
        handle_result(status, "monitorInterval", "30", "log4j.properties");
        status = update_flink_config("rootLogger.level", "INFO", "log4j.properties");
        handle_result(status, "rootLogger.level", "INFO", "log4j.properties");
        status = update_flink_config("rootLogger.appenderRef.file.ref", "MainAppender", "log4j.properties");
        handle_result(status, "rootLogger.appenderRef.file.ref", "MainAppender", "log4j.properties");
        status = update_flink_config("logger.akka.name", "akka", "log4j.properties");
        handle_result(status, "logger.akka.name", "akka", "log4j.properties");
        status = update_flink_config("logger.akka.level", "INFO", "log4j.properties");
        handle_result(status, "logger.akka.level", "INFO", "log4j.properties");
        status = update_flink_config("logger.kafka.name", "org.apache.kafka", "log4j.properties");
        handle_result(status, "logger.kafka.name", "org.apache.kafka", "log4j.properties");
        status = update_flink_config("logger.kafka.level", "INFO", "log4j.properties");
        handle_result(status, "logger.kafka.level", "INFO", "log4j.properties");
        status = update_flink_config("logger.hadoop.name", "org.apache.hadoop", "log4j.properties");
        handle_result(status, "logger.hadoop.name", "org.apache.hadoop", "log4j.properties");
        status = update_flink_config("logger.hadoop.level", "INFO", "log4j.properties");
        handle_result(status, "logger.hadoop.level", "INFO", "log4j.properties");
        status = update_flink_config("logger.zookeeper.name", "org.apache.zookeeper", "log4j.properties");
        handle_result(status, "logger.zookeeper.name", "org.apache.zookeeper", "log4j.properties");
        status = update_flink_config("logger.zookeeper.level", "INFO", "log4j.properties");
        handle_result(status, "logger.zookeeper.level", "INFO", "log4j.properties");
        status = update_flink_config("logger.shaded_zookeeper.name", "org.apache.flink.shaded.zookeeper3", "log4j.properties");
        handle_result(status, "logger.shaded_zookeeper.name", "org.apache.flink.shaded.zookeeper3", "log4j.properties");
        status = update_flink_config("logger.shaded_zookeeper.level", "INFO", "log4j.properties");
        handle_result(status, "logger.shaded_zookeeper.level", "INFO", "log4j.properties");
        status = update_flink_config("appender.main.name", "MainAppender", "log4j.properties");
        handle_result(status, "appender.main.name", "MainAppender", "log4j.properties");
        status = update_flink_config("appender.main.type", "RollingFile", "log4j.properties");
        handle_result(status, "appender.main.type", "RollingFile", "log4j.properties");
        status = update_flink_config("appender.main.append", "true", "log4j.properties");
        handle_result(status, "appender.main.append", "true", "log4j.properties");
        status = update_flink_config("appender.main.fileName", "${sys:log.file}", "log4j.properties");
        handle_result(status, "appender.main.fileName", "${sys:log.file}", "log4j.properties");
        status = update_flink_config("appender.main.filePattern", "${sys:log.file}.%i", "log4j.properties");
        handle_result(status, "appender.main.filePattern", "${sys:log.file}.%i", "log4j.properties");
        status = update_flink_config("appender.main.layout.type", "PatternLayout", "log4j.properties");
        handle_result(status, "appender.main.layout.type", "PatternLayout", "log4j.properties");
        status = update_flink_config("appender.main.layout.pattern", "%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n", "log4j.properties");
        handle_result(status, "appender.main.layout.pattern", "%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n", "log4j.properties");
        status = update_flink_config("appender.main.policies.type", "Policies", "log4j.properties");
        handle_result(status, "appender.main.policies.type", "Policies", "log4j.properties");
        status = update_flink_config("appender.main.policies.size.type", "SizeBasedTriggeringPolicy", "log4j.properties");
        handle_result(status, "appender.main.policies.size.type", "SizeBasedTriggeringPolicy", "log4j.properties");
        status = update_flink_config("appender.main.policies.size.size", "100MB", "log4j.properties");
        handle_result(status, "appender.main.policies.size.size", "100MB", "log4j.properties");
        status = update_flink_config("appender.main.policies.startup.type", "OnStartupTriggeringPolicy", "log4j.properties");
        handle_result(status, "appender.main.policies.startup.type", "OnStartupTriggeringPolicy", "log4j.properties");
        status = update_flink_config("appender.main.strategy.type", "DefaultRolloverStrategy", "log4j.properties");
        handle_result(status, "appender.main.strategy.type", "DefaultRolloverStrategy", "log4j.properties");
        status = update_flink_config("appender.main.strategy.max", "${env:MAX_LOG_FILE_NUMBER:-10}", "log4j.properties");
        handle_result(status, "appender.main.strategy.max", "${env:MAX_LOG_FILE_NUMBER:-10}", "log4j.properties");
        //		 Update Flink session logging configuration
        status = update_flink_config("monitorInterval", "30", "log4j-session.properties");
        handle_result(status, "monitorInterval", "30", "log4j-session.properties");
        status = update_flink_config("rootLogger.level", "INFO", "log4j-session.properties");
        handle_result(status, "rootLogger.level", "INFO", "log4j-session.properties");
        status = update_flink_config("rootLogger.appenderRef.console.ref", "ConsoleAppender", "log4j-session.properties");
        handle_result(status, "rootLogger.appenderRef.console.ref", "ConsoleAppender", "log4j-session.properties");
        status = update_flink_config("appender.console.name", "ConsoleAppender", "log4j-session.properties");
        handle_result(status, "appender.console.name", "ConsoleAppender", "log4j-session.properties");
        status = update_flink_config("appender.console.type", "CONSOLE", "log4j-session.properties");
        handle_result(status, "appender.console.type", "CONSOLE", "log4j-session.properties");
        status = update_flink_config("appender.console.layout.type", "PatternLayout", "log4j-session.properties");
        handle_result(status, "appender.console.layout.type", "PatternLayout", "log4j-session.properties");
        status = update_flink_config("appender.console.layout.pattern", "%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n", "log4j-session.properties");
        handle_result(status, "appender.console.layout.pattern", "%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n", "log4j-session.properties");
        status = update_flink_config("logger.netty.name", "org.jboss.netty.channel.DefaultChannelPipeline", "log4j-session.properties");
        handle_result(status, "logger.netty.name", "org.jboss.netty.channel.DefaultChannelPipeline", "log4j-session.properties");
        status = update_flink_config("logger.netty.level", "OFF", "log4j-session.properties");
        handle_result(status, "logger.netty.level", "OFF", "log4j-session.properties");
        status = update_flink_config("logger.zookeeper.name", "org.apache.zookeeper", "log4j-session.properties");
        handle_result(status, "logger.zookeeper.name", "org.apache.zookeeper", "log4j-session.properties");
        status = update_flink_config("logger.zookeeper.level", "WARN", "log4j-session.properties");
        handle_result(status, "logger.zookeeper.level", "WARN", "log4j-session.properties");
        status = update_flink_config("logger.shaded_zookeeper.name", "org.apache.flink.shaded.zookeeper3", "log4j-session.properties");
        handle_result(status, "logger.shaded_zookeeper.name", "org.apache.flink.shaded.zookeeper3", "log4j-session.properties");
        status = update_flink_config("logger.shaded_zookeeper.level", "WARN", "log4j-session.properties");
        handle_result(status, "logger.shaded_zookeeper.level", "WARN", "log4j-session.properties");
        status = update_flink_config("logger.curator.name", "org.apache.flink.shaded.org.apache.curator.framework", "log4j-session.properties");
        handle_result(status, "logger.curator.name", "org.apache.flink.shaded.org.apache.curator.framework", "log4j-session.properties");
        status = update_flink_config("logger.curator.level", "WARN", "log4j-session.properties");
        handle_result(status, "logger.curator.level", "WARN", "log4j-session.properties");
        status = update_flink_config("logger.runtimeutils.name", "org.apache.flink.runtime.util.ZooKeeperUtils", "log4j-session.properties");
        handle_result(status, "logger.runtimeutils.name", "org.apache.flink.runtime.util.ZooKeeperUtils", "log4j-session.properties");
        status = update_flink_config("logger.runtimeutils.level", "WARN", "log4j-session.properties");
        handle_result(status, "logger.runtimeutils.level", "WARN", "log4j-session.properties");
        status = update_flink_config("logger.runtimeleader.name", "org.apache.flink.runtime.leaderretrieval.ZooKeeperLeaderRetrievalDriver", "log4j-session.properties");
        handle_result(status, "logger.runtimeleader.name", "org.apache.flink.runtime.leaderretrieval.ZooKeeperLeaderRetrievalDriver", "log4j-session.properties");
        status = update_flink_config("logger.runtimeleader.level", "WARN", "log4j-session.properties");
        handle_result(status, "logger.runtimeleader.level", "WARN", "log4j-session.properties");

        break;

    default:
        fprintf(stderr, "Unsupported component: %d\n", target);
        break;
    }
}

