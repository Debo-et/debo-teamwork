
#include <libxml/parser.h>
#include <libxml/tree.h>
#include <libxml/xpath.h>

#include "utiles.h"


static const ConfigParam hdfs_configs[] = {
    // Core HDFS parameters (20)
    {"dfs.replication", "^dfs\\.replication$", "hdfs-site.xml"},
    {"dfs.namenode.name.dir", "^dfs\\.namenode\\.name\\.dir$", "hdfs-site.xml"},
    {"dfs.datanode.data.dir", "^dfs\\.datanode\\.data\\.dir$", "hdfs-site.xml"},
    {"fs.defaultFS", "^fs\\.defaultFS$", "core-site.xml"},
    {"hadoop.tmp.dir", "^hadoop\\.tmp\\.dir$", "core-site.xml"},
    {"dfs.blocksize", "^dfs\\.blocksize$", "hdfs-site.xml"},
    {"dfs.namenode.checkpoint.dir", "^dfs\\.namenode\\.checkpoint\\.dir$", "hdfs-site.xml"},
    {"dfs.permissions.enabled", "^dfs\\.permissions\\.enabled$", "hdfs-site.xml"},
    {"dfs.client.use.datanode.hostname", "^dfs\\.client\\.use\\.datanode\\.hostname$", "hdfs-site.xml"},
    {"dfs.datanode.address", "^dfs\\.datanode\\.address$", "hdfs-site.xml"},
    {"dfs.datanode.http.address", "^dfs\\.datanode\\.http\\.address$", "hdfs-site.xml"},
    {"dfs.datanode.ipc.address", "^dfs\\.datanode\\.ipc\\.address$", "hdfs-site.xml"},
    {"dfs.namenode.http-address", "^dfs\\.namenode\\.http-address$", "hdfs-site.xml"},
    {"dfs.namenode.https-address", "^dfs\\.namenode\\.https-address$", "hdfs-site.xml"},
    {"dfs.namenode.rpc-address", "^dfs\\.namenode\\.rpc-address$", "hdfs-site.xml"},
    {"dfs.hosts.exclude", "^dfs\\.hosts\\.exclude$", "hdfs-site.xml"},
    {"dfs.datanode.failed.volumes.tolerated", "^dfs\\.datanode\\.failed\\.volumes\\.tolerated$", "hdfs-site.xml"},
    {"dfs.datanode.max.transfer.threads", "^dfs\\.datanode\\.max\\.transfer\\.threads$", "hdfs-site.xml"},
    {"io.file.buffer.size", "^io\\.file\\.buffer\\.size$", "core-site.xml"},
    {"dfs.namenode.acls.enabled", "^dfs\\.namenode\\.acls\\.enabled$", "hdfs-site.xml"},

    // Storage management (6)
    {"dfs.datanode.du.reserved", "^dfs\\.datanode\\.du\\.reserved$", "hdfs-site.xml"},
    {"dfs.storage.policy.satisfier.mode", "^dfs\\.storage\\.policy\\.satisfier\\.mode$", "hdfs-site.xml"},
    {"dfs.namenode.num.extra.edits.retained", "^dfs\\.namenode\\.num\\.extra\\.edits\\.retained$", "hdfs-site.xml"},
    {"dfs.datanode.data.dir.perm", "^dfs\\.datanode\\.data\\.dir\\.perm$", "hdfs-site.xml"},
    {"dfs.namenode.delegation.key.update-interval", "^dfs\\.namenode\\.delegation\\.key\\.update-interval$", "hdfs-site.xml"},
    {"dfs.namenode.delegation.token.max-lifetime", "^dfs\\.namenode\\.delegation\\.token\\.max-lifetime$", "hdfs-site.xml"},

    // Fault tolerance (7)
    {"dfs.namenode.checkpoint.period", "^dfs\\.namenode\\.checkpoint\\.period$", "hdfs-site.xml"},
    {"dfs.namenode.num.checkpoints.retained", "^dfs\\.namenode\\.num\\.checkpoints\\.retained$", "hdfs-site.xml"},
    {"dfs.client.block.write.replace-datanode-on-failure.policy", "^dfs\\.client\\.block\\.write\\.replace-datanode-on-failure\\.policy$", "hdfs-site.xml"},
    {"dfs.client.block.write.replace-datanode-on-failure.enable", "^dfs\\.client\\.block\\.write\\.replace-datanode-on-failure\\.enable$", "hdfs-site.xml"},
    {"dfs.client.block.write.replace-datanode-on-failure.best-effort", "^dfs\\.client\\.block\\.write\\.replace-datanode-on-failure\\.best-effort$", "hdfs-site.xml"},
    {"dfs.namenode.replication.min", "^dfs\\.namenode\\.replication\\.min$", "hdfs-site.xml"},
    {"dfs.heartbeat.interval", "^dfs\\.heartbeat\\.interval$", "hdfs-site.xml"},

    // Performance tuning (10)
    {"dfs.client.read.shortcircuit", "^dfs\\.client\\.read\\.shortcircuit$", "hdfs-site.xml"},
    {"dfs.domain.socket.path", "^dfs\\.domain\\.socket\\.path$", "hdfs-site.xml"},
    {"dfs.client.socket-timeout", "^dfs\\.client\\.socket-timeout$", "hdfs-site.xml"},
    {"dfs.datanode.balance.bandwidthPerSec", "^dfs\\.datanode\\.balance\\.bandwidthPerSec$", "hdfs-site.xml"},
    {"dfs.client.max.block.acquire.failures", "^dfs\\.client\\.max\\.block\\.acquire\\.failures$", "hdfs-site.xml"},
    {"dfs.namenode.handler.count", "^dfs\\.namenode\\.handler\\.count$", "hdfs-site.xml"},
    {"dfs.datanode.handler.count", "^dfs\\.datanode\\.handler\\.count$", "hdfs-site.xml"},
    {"dfs.client.write.packet.size", "^dfs\\.client\\.write\\.packet\\.size$", "hdfs-site.xml"},
    {"dfs.replication.interval", "^dfs\\.replication\\.interval$", "hdfs-site.xml"},
    {"dfs.namenode.replication.work.multiplier.per.iteration", "^dfs\\.namenode\\.replication\\.work\\.multiplier\\.per\\.iteration$", "hdfs-site.xml"},

    // Security configurations (7)
    {"dfs.encrypt.data.transfer", "^dfs\\.encrypt\\.data\\.transfer$", "hdfs-site.xml"},
    {"dfs.encrypt.data.transfer.algorithm", "^dfs\\.encrypt\\.data\\.transfer\\.algorithm$", "hdfs-site.xml"},
    {"dfs.http.policy", "^dfs\\.http\\.policy$", "hdfs-site.xml"},
    {"dfs.https.port", "^dfs\\.https\\.port$", "hdfs-site.xml"},
    {"hadoop.security.authentication", "^hadoop\\.security\\.authentication$", "core-site.xml"},
    {"hadoop.security.authorization", "^hadoop\\.security\\.authorization$", "core-site.xml"},
    {"hadoop.rpc.protection", "^hadoop\\.rpc\\.protection$", "core-site.xml"},

    // Network/RPC settings (5)
    {"dfs.datanode.hostname", "^dfs\\.datanode\\.hostname$", "hdfs-site.xml"},
    {"dfs.namenode.secondary.http-address", "^dfs\\.namenode\\.secondary\\.http-address$", "hdfs-site.xml"},
    {"dfs.namenode.backup.address", "^dfs\\.namenode\\.backup\\.address$", "hdfs-site.xml"},
    {"dfs.journalnode.rpc-address", "^dfs\\.journalnode\\.rpc-address$", "hdfs-site.xml"},
    {"dfs.journalnode.http-address", "^dfs\\.journalnode\\.http-address$", "hdfs-site.xml"},

    // Cluster management (6)
    {"dfs.hosts", "^dfs\\.hosts$", "hdfs-site.xml"},
    {"dfs.namenode.safemode.threshold-pct", "^dfs\\.namenode\\.safemode\\.threshold-pct$", "hdfs-site.xml"},
    {"dfs.ha.automatic-failover.enabled", "^dfs\\.ha\\.automatic-failover\\.enabled$", "hdfs-site.xml"},
    {"dfs.namenode.audit.loggers", "^dfs\\.namenode\\.audit\\.loggers$", "hdfs-site.xml"},
    {"dfs.client.failover.proxy.provider", "^dfs\\.client\\.failover\\.proxy\\.provider$", "hdfs-site.xml"},
    {"dfs.namenode.replication.considerLoad", "^dfs\\.namenode\\.replication\\.considerLoad$", "hdfs-site.xml"},

    // Client behavior (5)
    {"dfs.client.retry.policy.enabled", "^dfs\\.client\\.retry\\.policy\\.enabled$", "hdfs-site.xml"},
    {"dfs.client.retry.max.attempts", "^dfs\\.client\\.retry\\.max\\.attempts$", "hdfs-site.xml"},
    {"dfs.client.failover.sleep.base.millis", "^dfs\\.client\\.failover\\.sleep\\.base\\.millis$", "hdfs-site.xml"},
    {"dfs.client.hedged.read.threadpool.size", "^dfs\\.client\\.hedged\\.read\\.threadpool\\.size$", "hdfs-site.xml"},
    {"dfs.client.hedged.read.threshold.millis", "^dfs\\.client\\.hedged\\.read\\.threshold\\.millis$", "hdfs-site.xml"},

    // DataNode advanced configs (4)
    {"dfs.datanode.max.locked.memory", "^dfs\\.datanode\\.max\\.locked\\.memory$", "hdfs-site.xml"},
    {"dfs.datanode.socket.write.timeout", "^dfs\\.datanode\\.socket\\.write\\.timeout$", "hdfs-site.xml"},
    {"dfs.image.compress", "^dfs\\.image\\.compress$", "hdfs-site.xml"},
    {"dfs.image.compression.codec", "^dfs\\.image\\.compression\\.codec$", "hdfs-site.xml"},

    // Quota management (2)
    {"dfs.namenode.quota.enabled", "^dfs\\.namenode\\.quota\\.enabled$", "hdfs-site.xml"},
    {"dfs.namenode.quota.update.interval", "^dfs\\.namenode\\.quota\\.update\\.interval$", "hdfs-site.xml"},

    // Ranger audit parameters from ranger-hdfs-audit.xml
    {"xasecure.audit.is.enabled", "^xasecure\\.audit\\.is\\.enabled$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.hdfs.is.enabled", "^xasecure\\.audit\\.hdfs\\.is\\.enabled$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.hdfs.is.async", "^xasecure\\.audit\\.hdfs\\.is\\.async$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.hdfs.async.max.queue.size", "^xasecure\\.audit\\.hdfs\\.async\\.max\\.queue\\.size$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.hdfs.async.max.flush.interval.ms", "^xasecure\\.audit\\.hdfs\\.async\\.max\\.flush\\.interval\\.ms$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.hdfs.config.encoding", "^xasecure\\.audit\\.hdfs\\.config\\.encoding$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.hdfs.config.destination.directory", "^xasecure\\.audit\\.hdfs\\.config\\.destination\\.directory$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.hdfs.config.destination.file", "^xasecure\\.audit\\.hdfs\\.config\\.destination\\.file$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.hdfs.config.destination.flush.interval.seconds", "^xasecure\\.audit\\.hdfs\\.config\\.destination\\.flush\\.interval\\.seconds$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.hdfs.config.destination.rollover.interval.seconds", "^xasecure\\.audit\\.hdfs\\.config\\.destination\\.rollover\\.interval\\.seconds$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.hdfs.config.destination.open.retry.interval.seconds", "^xasecure\\.audit\\.hdfs\\.config\\.destination\\.open\\.retry\\.interval\\.seconds$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.hdfs.config.local.buffer.directory", "^xasecure\\.audit\\.hdfs\\.config\\.local\\.buffer\\.directory$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.hdfs.config.local.buffer.file", "^xasecure\\.audit\\.hdfs\\.config\\.local\\.buffer\\.file$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.hdfs.config.local.buffer.file.buffer.size.bytes", "^xasecure\\.audit\\.hdfs\\.config\\.local\\.buffer\\.file\\.buffer\\.size\\.bytes$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.hdfs.config.local.buffer.flush.interval.seconds", "^xasecure\\.audit\\.hdfs\\.config\\.local\\.buffer\\.flush\\.interval\\.seconds$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.hdfs.config.local.buffer.rollover.interval.seconds", "^xasecure\\.audit\\.hdfs\\.config\\.local\\.buffer\\.rollover\\.interval\\.seconds$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.hdfs.config.local.archive.directory", "^xasecure\\.audit\\.hdfs\\.config\\.local\\.archive\\.directory$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.hdfs.config.local.archive.max.file.count", "^xasecure\\.audit\\.hdfs\\.config\\.local\\.archive\\.max\\.file\\.count$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.log4j.is.enabled", "^xasecure\\.audit\\.log4j\\.is\\.enabled$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.log4j.is.async", "^xasecure\\.audit\\.log4j\\.is\\.async$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.log4j.async.max.queue.size", "^xasecure\\.audit\\.log4j\\.async\\.max\\.queue\\.size$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.log4j.async.max.flush.interval.ms", "^xasecure\\.audit\\.log4j\\.async\\.max\\.flush\\.interval\\.ms$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.kafka.is.enabled", "^xasecure\\.audit\\.kafka\\.is\\.enabled$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.kafka.async.max.queue.size", "^xasecure\\.audit\\.kafka\\.async\\.max\\.queue\\.size$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.kafka.async.max.flush.interval.ms", "^xasecure\\.audit\\.kafka\\.async\\.max\\.flush\\.interval\\.ms$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.kafka.broker_list", "^xasecure\\.audit\\.kafka\\.broker_list$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.kafka.topic_name", "^xasecure\\.audit\\.kafka\\.topic_name$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.solr.is.enabled", "^xasecure\\.audit\\.solr\\.is\\.enabled$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.solr.async.max.queue.size", "^xasecure\\.audit\\.solr\\.async\\.max\\.queue\\.size$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.solr.async.max.flush.interval.ms", "^xasecure\\.audit\\.solr\\.async\\.max\\.flush\\.interval\\.ms$", "ranger-hdfs-audit.xml"},
    {"xasecure.audit.solr.solr_url", "^xasecure\\.audit\\.solr\\.solr_url$", "ranger-hdfs-audit.xml"},
    // SSL configuration parameters from ranger-policymgr-ssl.xml
    {"xasecure.policymgr.clientssl.keystore", "^xasecure\\.policymgr\\.clientssl\\.keystore$", "ranger-policymgr-ssl.xml"},
    {"xasecure.policymgr.clientssl.truststore", "^xasecure\\.policymgr\\.clientssl\\.truststore$", "ranger-policymgr-ssl.xml"},
    {"xasecure.policymgr.clientssl.keystore.credential.file", "^xasecure\\.policymgr\\.clientssl\\.keystore\\.credential\\.file$", "ranger-policymgr-ssl.xml"},
    {"xasecure.policymgr.clientssl.truststore.credential.file", "^xasecure\\.policymgr\\.clientssl\\.truststore\\.credential\\.file$", "ranger-policymgr-ssl.xml"},
    // Ranger security parameters from ranger-hdfs-security.xml
    {"ranger.plugin.hdfs.service.name", "^ranger\\.plugin\\.hdfs\\.service\\.name$", "ranger-hdfs-security.xml"},
    {"ranger.plugin.hdfs.policy.source.impl", "^ranger\\.plugin\\.hdfs\\.policy\\.source\\.impl$", "ranger-hdfs-security.xml"},
    {"ranger.plugin.hdfs.policy.rest.url", "^ranger\\.plugin\\.hdfs\\.policy\\.rest\\.url$", "ranger-hdfs-security.xml"},
    {"ranger.plugin.hdfs.policy.rest.ssl.config.file", "^ranger\\.plugin\\.hdfs\\.policy\\.rest\\.ssl\\.config\\.file$", "ranger-hdfs-security.xml"},
    {"ranger.plugin.hdfs.policy.pollIntervalMs", "^ranger\\.plugin\\.hdfs\\.policy\\.pollIntervalMs$", "ranger-hdfs-security.xml"},
    {"ranger.plugin.hdfs.policy.cache.dir", "^ranger\\.plugin\\.hdfs\\.policy\\.cache\\.dir$", "ranger-hdfs-security.xml"},
    {"ranger.plugin.hdfs.policy.rest.client.connection.timeoutMs", "^ranger\\.plugin\\.hdfs\\.policy\\.rest\\.client\\.connection\\.timeoutMs$", "ranger-hdfs-security.xml"},
    {"ranger.plugin.hdfs.policy.rest.client.read.timeoutMs", "^ranger\\.plugin\\.hdfs\\.policy\\.rest\\.client\\.read\\.timeoutMs$", "ranger-hdfs-security.xml"},
    {"xasecure.add-hadoop-authorization", "^xasecure\\.add-hadoop-authorization$", "ranger-hdfs-security.xml"},
    {"log4j.rootLogger", "^log4j\\.rootLogger$", "hdfs-log4j.properties"},
    {"log4j.threshhold", "^log4j\\.threshhold$", "hdfs-log4j.properties"},
    {"log4j.appender.stdout", "^log4j\\.appender\\.stdout$", "hdfs-log4j.properties"},
    {"log4j.appender.stdout.layout", "^log4j\\.appender\\.stdout\\.layout$", "hdfs-log4j.properties"},
    {"log4j.appender.stdout.layout.ConversionPattern", "^log4j\\.appender\\.stdout\\.layout\\.ConversionPattern$", "hdfs-log4j.properties"},
    {"log4j.appender.subprocess", "^log4j\\.appender\\.subprocess$", "hdfs-log4j.properties"},
    {"log4j.appender.subprocess.layout", "^log4j\\.appender\\.subprocess\\.layout$", "hdfs-log4j.properties"},
    {"log4j.appender.subprocess.layout.ConversionPattern", "^log4j\\.appender\\.subprocess\\.layout\\.ConversionPattern$", "hdfs-log4j.properties"}, {"log4j.logger.org.apache.hadoop.yarn.registry", "^log4j\\.logger\\.org\\.apache\\.hadoop\\.yarn\\.registry$", "hdfs-log4j.properties"}, {"log4j.logger.org.apache.hadoop.service", "^log4j\\.logger\\.org\\.apache\\.hadoop\\.service$", "hdfs-log4j.properties"}, {"log4j.logger.org.apache.hadoop.security.UserGroupInformation", "^log4j\\.logger\\.org\\.apache\\.hadoop\\.security\\.UserGroupInformation$", "hdfs-log4j.properties"}, {"log4j.logger.org.apache.hadoop.util.NativeCodeLoader", "^log4j\\.logger\\.org\\.apache\\.hadoop\\.util\\.NativeCodeLoader$", "hdfs-log4j.properties"}, {"log4j.logger.org.apache.hadoop.hdfs.server.datanode.BlockPoolSliceScanner", "^log4j\\.logger\\.org\\.apache\\.hadoop\\.hdfs\\.server\\.datanode\\.BlockPoolSliceScanner$", "hdfs-log4j.properties"},
    {"log4j.logger.org.apache.hadoop.hdfs.server.blockmanagement", "^log4j\\.logger\\.org\\.apache\\.hadoop\\.hdfs\\.server\\.blockmanagement$", "hdfs-log4j.properties"}, {"log4j.logger.org.apache.hadoop.hdfs.server.namenode.FSNamesystem.audit", "^log4j\\.logger\\.org\\.apache\\.hadoop\\.hdfs\\.server\\.namenode\\.FSNamesystem\\.audit$", "hdfs-log4j.properties"},
    {"log4j.logger.org.apache.hadoop.hdfs", "^log4j\\.logger\\.org\\.apache\\.hadoop\\.hdfs$", "hdfs-log4j.properties"}, {"log4j.logger.org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor", "^log4j\\.logger\\.org\\.apache\\.hadoop\\.yarn\\.server\\.nodemanager\\.containermanager\\.monitor$", "hdfs-log4j.properties"},
    {"log4j.logger.org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdaterImpl", "^log4j\\.logger\\.org\\.apache\\.hadoop\\.yarn\\.server\\.nodemanager\\.NodeStatusUpdaterImpl$", "hdfs-log4j.properties"},
    {"log4j.logger.org.apache.zookeeper", "^log4j\\.logger\\.org\\.apache\\.zookeeper$", "hdfs-log4j.properties"},
    {"log4j.logger.org.apache.zookeeper.ClientCnxn", "^log4j\\.logger\\.org\\.apache\\.zookeeper\\.ClientCnxn$", "hdfs-log4j.properties"}, {"log4j.logger.org.apache.hadoop.yarn.server.resourcemanager.security", "^log4j\\.logger\\.org\\.apache\\.hadoop\\.yarn\\.server\\.resourcemanager\\.security$", "hdfs-log4j.properties"},
    {"log4j.logger.org.apache.hadoop.metrics2", "^log4j\\.logger\\.org\\.apache\\.hadoop\\.metrics2$", "hdfs-log4j.properties"}, {"log4j.logger.org.apache.hadoop.util.HostsFileReader", "^log4j\\.logger\\.org\\.apache\\.hadoop\\.util\\.HostsFileReader$", "hdfs-log4j.properties"}, {"log4j.logger.org.apache.hadoop.yarn.event.AsyncDispatcher", "^log4j\\.logger\\.org\\.apache\\.hadoop\\.yarn\\.event\\.AsyncDispatcher$", "hdfs-log4j.properties"}, {"log4j.logger.org.apache.hadoop.security.token.delegation", "^log4j\\.logger\\.org\\.apache\\.hadoop\\.security\\.token\\.delegation$", "hdfs-log4j.properties"}, {"log4j.logger.org.apache.hadoop.yarn.util.AbstractLivelinessMonitor", "^log4j\\.logger\\.org\\.apache\\.hadoop\\.yarn\\.util\\.AbstractLivelinessMonitor$", "hdfs-log4j.properties"}, {"log4j.logger.org.apache.hadoop.yarn.server.nodemanager.security", "^log4j\\.logger\\.org\\.apache\\.hadoop\\.yarn\\.server\\.nodemanager\\.security$", "hdfs-log4j.properties"}, {"log4j.logger.org.apache.hadoop.yarn.server.resourcemanager.RMNMInfo", "^log4j\\.logger\\.org\\.apache\\.hadoop\\.yarn\\.server\\.resourcemanager\\.RMNMInfo$", "hdfs-log4j.properties"}, {"log4j.logger.org.apache.curator.framework.imps", "^log4j\\.logger\\.org\\.apache\\.curator\\.framework\\.imps$", "hdfs-log4j.properties"}, {"log4j.logger.org.apache.curator.framework.state.ConnectionStateManager", "^log4j\\.logger\\.org\\.apache\\.curator\\.framework\\.state\\.ConnectionStateManager$", "hdfs-log4j.properties"}, {"log4j.logger.org.apache.directory.api.ldap", "^log4j\\.logger\\.org\\.apache\\.directory\\.api\\.ldap$", "hdfs-log4j.properties"}, {"log4j.logger.org.apache.directory.server", "^log4j\\.logger\\.org\\.apache\\.directory\\.server$", "hdfs-log4j.properties"},
    // Service-Level Authorization Parameters from hadoop-policy.xml (11 entries)
    {"security.client.protocol.acl", "^security\\.client\\.protocol\\.acl$", "hadoop-policy.xml"},
    {"security.client.datanode.protocol.acl", "^security\\.client\\.datanode\\.protocol\\.acl$", "hadoop-policy.xml"},
    {"security.datanode.protocol.acl", "^security\\.datanode\\.protocol\\.acl$", "hadoop-policy.xml"},
    {"security.inter.datanode.protocol.acl", "^security\\.inter\\.datanode\\.protocol\\.acl$", "hadoop-policy.xml"},
    {"security.namenode.protocol.acl", "^security\\.namenode\\.protocol\\.acl$", "hadoop-policy.xml"},
    {"security.inter.tracker.protocol.acl", "^security\\.inter\\.tracker\\.protocol\\.acl$", "hadoop-policy.xml"},
    {"security.job.submission.protocol.acl", "^security\\.job\\.submission\\.protocol\\.acl$", "hadoop-policy.xml"},
    {"security.task.umbilical.protocol.acl", "^security\\.task\\.umbilical\\.protocol\\.acl$", "hadoop-policy.xml"},
    {"security.refresh.policy.protocol.acl", "^security\\.refresh\\.policy\\.protocol\\.acl$", "hadoop-policy.xml"},
    {"security.admin.operations.protocol.acl", "^security\\.admin\\.operations\\.protocol\\.acl$", "hadoop-policy.xml"},
    {"security.ha.service.protocol.acl", "^security\\.ha\\.service\\.protocol\\.acl$", "hadoop-policy.xml"},
    // RBF-specific parameters (49)
    {"dfs.federation.router.rpc.enable", "^dfs\\.federation\\.router\\.rpc\\.enable$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.rpc-address", "^dfs\\.federation\\.router\\.rpc-address$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.rpc-bind-host", "^dfs\\.federation\\.router\\.rpc-bind-host$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.handler.count", "^dfs\\.federation\\.router\\.handler\\.count$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.handler.queue.size", "^dfs\\.federation\\.router\\.handler\\.queue\\.size$", "hdfs-rbf-site.xml"},

    {"dfs.federation.router.reader.count", "^dfs\\.federation\\.router\\.reader\\.count$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.reader.queue.size", "^dfs\\.federation\\.router\\.reader\\.queue\\.size$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.connection.creator.queue-size", "^dfs\\.federation\\.router\\.connection\\.creator\\.queue-size$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.connection.pool-size", "^dfs\\.federation\\.router\\.connection\\.pool-size$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.connection.min-active-ratio", "^dfs\\.federation\\.router\\.connection\\.min-active-ratio$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.connection.clean.ms", "^dfs\\.federation\\.router\\.connection\\.clean\\.ms$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.connection.pool.clean.ms", "^dfs\\.federation\\.router\\.connection\\.pool\\.clean\\.ms$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.metrics.enable", "^dfs\\.federation\\.router\\.metrics\\.enable$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.dn-report.time-out", "^dfs\\.federation\\.router\\.dn-report\\.time-out$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.dn-report.cache-expire", "^dfs\\.federation\\.router\\.dn-report\\.cache-expire$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.metrics.class", "^dfs\\.federation\\.router\\.metrics\\.class$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.admin.enable", "^dfs\\.federation\\.router\\.admin\\.enable$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.admin-address", "^dfs\\.federation\\.router\\.admin-address$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.admin-bind-host", "^dfs\\.federation\\.router\\.admin-bind-host$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.admin.handler.count", "^dfs\\.federation\\.router\\.admin\\.handler\\.count$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.http-address", "^dfs\\.federation\\.router\\.http-address$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.http-bind-host", "^dfs\\.federation\\.router\\.http-bind-host$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.https-address", "^dfs\\.federation\\.router\\.https-address$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.https-bind-host", "^dfs\\.federation\\.router\\.https-bind-host$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.http.enable", "^dfs\\.federation\\.router\\.http\\.enable$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.file.resolver.client.class", "^dfs\\.federation\\.router\\.file\\.resolver\\.client\\.class$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.namenode.resolver.client.class", "^dfs\\.federation\\.router\\.namenode\\.resolver\\.client\\.class$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.store.enable", "^dfs\\.federation\\.router\\.store\\.enable$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.store.serializer", "^dfs\\.federation\\.router\\.store\\.serializer$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.store.driver.class", "^dfs\\.federation\\.router\\.store\\.driver\\.class$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.store.connection.test", "^dfs\\.federation\\.router\\.store\\.connection\\.test$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.cache.ttl", "^dfs\\.federation\\.router\\.cache\\.ttl$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.store.membership.expiration", "^dfs\\.federation\\.router\\.store\\.membership\\.expiration$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.store.membership.expiration.deletion", "^dfs\\.federation\\.router\\.store\\.membership\\.expiration\\.deletion$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.heartbeat.enable", "^dfs\\.federation\\.router\\.heartbeat\\.enable$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.heartbeat.interval", "^dfs\\.federation\\.router\\.heartbeat\\.interval$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.heartbeat-state.interval", "^dfs\\.federation\\.router\\.heartbeat-state\\.interval$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.namenode.heartbeat.enable", "^dfs\\.federation\\.router\\.namenode\\.heartbeat\\.enable$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.store.router.expiration", "^dfs\\.federation\\.router\\.store\\.router\\.expiration$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.safemode.enable", "^dfs\\.federation\\.router\\.safemode\\.enable$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.safemode.extension", "^dfs\\.federation\\.router\\.safemode\\.extension$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.safemode.expiration", "^dfs\\.federation\\.router\\.safemode\\.expiration$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.monitor.localnamenode.enable", "^dfs\\.federation\\.router\\.monitor\\.localnamenode\\.enable$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.mount-table.max-cache-size", "^dfs\\.federation\\.router\\.mount-table\\.max-cache-size$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.mount-table.cache.enable", "^dfs\\.federation\\.router\\.mount-table\\.cache\\.enable$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.quota.enable", "^dfs\\.federation\\.router\\.quota\\.enable$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.quota-cache.update.interval", "^dfs\\.federation\\.router\\.quota-cache\\.update\\.interval$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.client.thread-size", "^dfs\\.federation\\.router\\.client\\.thread-size$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.client.retry.max.attempts", "^dfs\\.federation\\.router\\.client\\.retry\\.max\\.attempts$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.client.reject.overload", "^dfs\\.federation\\.router\\.client\\.reject\\.overload$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.client.allow-partial-listing", "^dfs\\.federation\\.router\\.client\\.allow-partial-listing$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.client.mount-status.time-out", "^dfs\\.federation\\.router\\.client\\.mount-status\\.time-out$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.connect.max.retries.on.timeouts", "^dfs\\.federation\\.router\\.connect\\.max\\.retries\\.on\\.timeouts$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.connect.timeout", "^dfs\\.federation\\.router\\.connect\\.timeout$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.mount-table.cache.update", "^dfs\\.federation\\.router\\.mount-table\\.cache\\.update$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.mount-table.cache.update.timeout", "^dfs\\.federation\\.router\\.mount-table\\.cache\\.update\\.timeout$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.mount-table.cache.update.client.max.time", "^dfs\\.federation\\.router\\.mount-table\\.cache\\.update\\.client\\.max\\.time$", "hdfs-rbf-site.xml"},
    {"dfs.federation.router.secret.manager.class", "^dfs\\.federation\\.router\\.secret\\.manager\\.class$", "hdfs-rbf-site.xml"},
    // New SSL client parameters from ssl-client.xml (7 entries)
    {"ssl.client.truststore.location", "^ssl\\.client\\.truststore\\.location$", "ssl-client.xml"},
    {"ssl.client.truststore.type", "^ssl\\.client\\.truststore\\.type$", "ssl-client.xml"},
    {"ssl.client.truststore.password", "^ssl\\.client\\.truststore\\.password$", "ssl-client.xml"},
    {"ssl.client.truststore.reload.interval", "^ssl\\.client\\.truststore\\.reload\\.interval$", "ssl-client.xml"},
    {"ssl.client.keystore.type", "^ssl\\.client\\.keystore\\.type$", "ssl-client.xml"},
    {"ssl.client.keystore.location", "^ssl\\.client\\.keystore\\.location$", "ssl-client.xml"},
    {"ssl.client.keystore.password", "^ssl\\.client\\.keystore\\.password$", "ssl-client.xml"},
    {"ssl.server.truststore.location", "^ssl\\.server\\.truststore\\.location$", "ssl-server.xml"},
    {"ssl.server.truststore.type", "^ssl\\.server\\.truststore\\.type$", "ssl-server.xml"},
    {"ssl.server.truststore.password", "^ssl\\.server\\.truststore\\.password$", "ssl-server.xml"},
    {"ssl.server.truststore.reload.interval", "^ssl\\.server\\.truststore\\.reload\\.interval$", "ssl-server.xml"},
    {"ssl.server.keystore.type", "^ssl\\.server\\.keystore\\.type$", "ssl-server.xml"},
    {"ssl.server.keystore.location", "^ssl\\.server\\.keystore\\.location$", "ssl-server.xml"},
    {"ssl.server.keystore.password", "^ssl\\.server\\.keystore\\.password$", "ssl-server.xml"},
    {"ssl.server.keystore.keypassword", "^ssl\\.server\\.keystore\\.keypassword$", "ssl-server.xml"},

    // YARN parameters from yarn-site.xml
    {"yarn.resourcemanager.hostname", "^yarn\\.resourcemanager\\.hostname$", "yarn-site.xml"},
    {"yarn.resourcemanager.resource-tracker.address", "^yarn\\.resourcemanager\\.resource-tracker\\.address$", "yarn-site.xml"},
    {"yarn.resourcemanager.scheduler.address", "^yarn\\.resourcemanager\\.scheduler\\.address$", "yarn-site.xml"},
    {"yarn.resourcemanager.address", "^yarn\\.resourcemanager\\.address$", "yarn-site.xml"},
    {"yarn.resourcemanager.admin.address", "^yarn\\.resourcemanager\\.admin\\.address$", "yarn-site.xml"},
    {"yarn.resourcemanager.scheduler.class", "^yarn\\.resourcemanager\\.scheduler\\.class$", "yarn-site.xml"},
    {"yarn.scheduler.minimum-allocation-mb", "^yarn\\.scheduler\\.minimum-allocation-mb$", "yarn-site.xml"},
    {"yarn.scheduler.maximum-allocation-mb", "^yarn\\.scheduler\\.maximum-allocation-mb$", "yarn-site.xml"},
    {"yarn.acl.enable", "^yarn\\.acl\\.enable$", "yarn-site.xml"},
    {"yarn.admin.acl", "^yarn\\.admin\\.acl$", "yarn-site.xml"},
    {"yarn.nodemanager.address", "^yarn\\.nodemanager\\.address$", "yarn-site.xml"},
    {"yarn.nodemanager.resource.memory-mb", "^yarn\\.nodemanager\\.resource\\.memory-mb$", "yarn-site.xml"},
    {"yarn.application.classpath", "^yarn\\.application\\.classpath$", "yarn-site.xml"},
    {"yarn.nodemanager.vmem-pmem-ratio", "^yarn\\.nodemanager\\.vmem-pmem-ratio$", "yarn-site.xml"},
    {"yarn.nodemanager.container-executor.class", "^yarn\\.nodemanager\\.container-executor\\.class$", "yarn-site.xml"},
    {"yarn.nodemanager.linux-container-executor.group", "^yarn\\.nodemanager\\.linux-container-executor\\.group$", "yarn-site.xml"},
    {"yarn.nodemanager.aux-services", "^yarn\\.nodemanager\\.aux-services$", "yarn-site.xml"},
    {"yarn.nodemanager.aux-services.mapreduce_shuffle.class", "^yarn\\.nodemanager\\.aux-services\\.mapreduce_shuffle\\.class$", "yarn-site.xml"},
    {"yarn.nodemanager.log-dirs", "^yarn\\.nodemanager\\.log-dirs$", "yarn-site.xml"},
    {"yarn.nodemanager.local-dirs", "^yarn\\.nodemanager\\.local-dirs$", "yarn-site.xml"},
    {"yarn.nodemanager.container-monitor.interval-ms", "^yarn\\.nodemanager\\.container-monitor\\.interval-ms$", "yarn-site.xml"},
    {"yarn.nodemanager.health-checker.interval-ms", "^yarn\\.nodemanager\\.health-checker\\.interval-ms$", "yarn-site.xml"},
    {"yarn.nodemanager.health-checker.script.timeout-ms", "^yarn\\.nodemanager\\.health-checker\\.script\\.timeout-ms$", "yarn-site.xml"},
    {"yarn.nodemanager.log.retain-seconds", "^yarn\\.nodemanager\\.log\\.retain-seconds$", "yarn-site.xml"},
    {"yarn.log-aggregation-enable", "^yarn\\.log-aggregation-enable$", "yarn-site.xml"},
    {"yarn.nodemanager.remote-app-log-dir", "^yarn\\.nodemanager\\.remote-app-log-dir$", "yarn-site.xml"},
    {"yarn.nodemanager.remote-app-log-dir-suffix", "^yarn\\.nodemanager\\.remote-app-log-dir-suffix$", "yarn-site.xml"},
    {"yarn.nodemanager.log-aggregation.compression-type", "^yarn\\.nodemanager\\.log-aggregation\\.compression-type$", "yarn-site.xml"},
    {"yarn.nodemanager.delete.debug-delay-sec", "^yarn\\.nodemanager\\.delete\\.debug-delay-sec$", "yarn-site.xml"},
    {"yarn.log-aggregation.retain-seconds", "^yarn\\.log-aggregation\\.retain-seconds$", "yarn-site.xml"},
    {"yarn.nodemanager.admin-env", "^yarn\\.nodemanager\\.admin-env$", "yarn-site.xml"},
    {"yarn.nodemanager.disk-health-checker.min-healthy-disks", "^yarn\\.nodemanager\\.disk-health-checker\\.min-healthy-disks$", "yarn-site.xml"},
    {"yarn.resourcemanager.am.max-attempts", "^yarn\\.resourcemanager\\.am\\.max-attempts$", "yarn-site.xml"},
    {"yarn.resourcemanager.webapp.address", "^yarn\\.resourcemanager\\.webapp\\.address$", "yarn-site.xml"},
    {"yarn.resourcemanager.webapp.https.address", "^yarn\\.resourcemanager\\.webapp\\.https\\.address$", "yarn-site.xml"},
    {"yarn.nodemanager.vmem-check-enabled", "^yarn\\.nodemanager\\.vmem-check-enabled$", "yarn-site.xml"},
    {"yarn.log.server.url", "^yarn\\.log\\.server\\.url$", "yarn-site.xml"},
    {"yarn.resourcemanager.nodes.exclude-path", "^yarn\\.resourcemanager\\.nodes\\.exclude-path$", "yarn-site.xml"},
    {"manage.include.files", "^manage\\.include\\.files$", "yarn-site.xml"},
    {"yarn.http.policy", "^yarn\\.http\\.policy$", "yarn-site.xml"},
    {"yarn.timeline-service.enabled", "^yarn\\.timeline-service\\.enabled$", "yarn-site.xml"},
    {"yarn.timeline-service.generic-application-history.store-class", "^yarn\\.timeline-service\\.generic-application-history\\.store-class$", "yarn-site.xml"},
    {"yarn.timeline-service.leveldb-timeline-store.path", "^yarn\\.timeline-service\\.leveldb-timeline-store\\.path$", "yarn-site.xml"},
    {"yarn.timeline-service.webapp.address", "^yarn\\.timeline-service\\.webapp\\.address$", "yarn-site.xml"},
    {"yarn.timeline-service.webapp.https.address", "^yarn\\.timeline-service\\.webapp\\.https\\.address$", "yarn-site.xml"},
    {"yarn.timeline-service.address", "^yarn\\.timeline-service\\.address$", "yarn-site.xml"},
    {"yarn.timeline-service.ttl-enable", "^yarn\\.timeline-service\\.ttl-enable$", "yarn-site.xml"},
    {"yarn.timeline-service.ttl-ms", "^yarn\\.timeline-service\\.ttl-ms$", "yarn-site.xml"},
    {"yarn.timeline-service.leveldb-timeline-store.ttl-interval-ms", "^yarn\\.timeline-service\\.leveldb-timeline-store\\.ttl-interval-ms$", "yarn-site.xml"},
    {"hadoop.registry.zk.quorum", "^hadoop\\.registry\\.zk\\.quorum$", "yarn-site.xml"},
    {"hadoop.registry.dns.bind-port", "^hadoop\\.registry\\.dns\\.bind-port$", "yarn-site.xml"},
    {"hadoop.registry.dns.zone-mask", "^hadoop\\.registry\\.dns\\.zone-mask$", "yarn-site.xml"},
    {"hadoop.registry.dns.zone-subnet", "^hadoop\\.registry\\.dns\\.zone-subnet$", "yarn-site.xml"},
    {"hadoop.registry.dns.enabled", "^hadoop\\.registry\\.dns\\.enabled$", "yarn-site.xml"},
    {"hadoop.registry.dns.domain-name", "^hadoop\\.registry\\.dns\\.domain-name$", "yarn-site.xml"},
    {"yarn.nodemanager.recovery.enabled", "^yarn\\.nodemanager\\.recovery\\.enabled$", "yarn-site.xml"},
    {"yarn.nodemanager.recovery.dir", "^yarn\\.nodemanager\\.recovery\\.dir$", "yarn-site.xml"},
    {"yarn.client.nodemanager-connect.retry-interval-ms", "^yarn\\.client\\.nodemanager-connect\\.retry-interval-ms$", "yarn-site.xml"},
    {"yarn.client.nodemanager-connect.max-wait-ms", "^yarn\\.client\\.nodemanager-connect\\.max-wait-ms$", "yarn-site.xml"},
    {"yarn.resourcemanager.recovery.enabled", "^yarn\\.resourcemanager\\.recovery\\.enabled$", "yarn-site.xml"},
    {"yarn.resourcemanager.work-preserving-recovery.enabled", "^yarn\\.resourcemanager\\.work-preserving-recovery\\.enabled$", "yarn-site.xml"},
    {"yarn.resourcemanager.store.class", "^yarn\\.resourcemanager\\.store\\.class$", "yarn-site.xml"},
    {"yarn.resourcemanager.zk-address", "^yarn\\.resourcemanager\\.zk-address$", "yarn-site.xml"},
    {"yarn.resourcemanager.zk-state-store.parent-path", "^yarn\\.resourcemanager\\.zk-state-store\\.parent-path$", "yarn-site.xml"},
    {"yarn.resourcemanager.zk-acl", "^yarn\\.resourcemanager\\.zk-acl$", "yarn-site.xml"},
    {"yarn.resourcemanager.work-preserving-recovery.scheduling-wait-ms", "^yarn\\.resourcemanager\\.work-preserving-recovery\\.scheduling-wait-ms$", "yarn-site.xml"},
    {"yarn.resourcemanager.connect.retry-interval.ms", "^yarn\\.resourcemanager\\.connect\\.retry-interval\\.ms$", "yarn-site.xml"},
    {"yarn.resourcemanager.connect.max-wait.ms", "^yarn\\.resourcemanager\\.connect\\.max-wait\\.ms$", "yarn-site.xml"},
    {"yarn.resourcemanager.zk-retry-interval-ms", "^yarn\\.resourcemanager\\.zk-retry-interval-ms$", "yarn-site.xml"},
    {"yarn.resourcemanager.zk-num-retries", "^yarn\\.resourcemanager\\.zk-num-retries$", "yarn-site.xml"},
    {"yarn.resourcemanager.zk-timeout-ms", "^yarn\\.resourcemanager\\.zk-timeout-ms$", "yarn-site.xml"},
    {"yarn.resourcemanager.state-store.max-completed-applications", "^yarn\\.resourcemanager\\.state-store\\.max-completed-applications$", "yarn-site.xml"},
    {"yarn.resourcemanager.fs.state-store.retry-policy-spec", "^yarn\\.resourcemanager\\.fs\\.state-store\\.retry-policy-spec$", "yarn-site.xml"},
    {"yarn.resourcemanager.fs.state-store.uri", "^yarn\\.resourcemanager\\.fs\\.state-store\\.uri$", "yarn-site.xml"},
    {"yarn.resourcemanager.ha.enabled", "^yarn\\.resourcemanager\\.ha\\.enabled$", "yarn-site.xml"},
    {"yarn.nodemanager.linux-container-executor.resources-handler.class", "^yarn\\.nodemanager\\.linux-container-executor\\.resources-handler\\.class$", "yarn-site.xml"},
    {"yarn.nodemanager.linux-container-executor.cgroups.hierarchy", "^yarn\\.nodemanager\\.linux-container-executor\\.cgroups\\.hierarchy$", "yarn-site.xml"},
    {"yarn.nodemanager.linux-container-executor.cgroups.mount", "^yarn\\.nodemanager\\.linux-container-executor\\.cgroups\\.mount$", "yarn-site.xml"},
    {"yarn.nodemanager.linux-container-executor.cgroups.mount-path", "^yarn\\.nodemanager\\.linux-container-executor\\.cgroups\\.mount-path$", "yarn-site.xml"},
    {"yarn.nodemanager.linux-container-executor.cgroups.strict-resource-usage", "^yarn\\.nodemanager\\.linux-container-executor\\.cgroups\\.strict-resource-usage$", "yarn-site.xml"},
    {"yarn.nodemanager.resource.cpu-vcores", "^yarn\\.nodemanager\\.resource\\.cpu-vcores$", "yarn-site.xml"},
    {"yarn.nodemanager.resource.percentage-physical-cpu-limit", "^yarn\\.nodemanager\\.resource\\.percentage-physical-cpu-limit$", "yarn-site.xml"},
    {"yarn.node-labels.fs-store.retry-policy-spec", "^yarn\\.node-labels\\.fs-store\\.retry-policy-spec$", "yarn-site.xml"},
    {"yarn.nodemanager.disk-health-checker.min-free-space-per-disk-mb", "^yarn\\.nodemanager\\.disk-health-checker\\.min-free-space-per-disk-mb$", "yarn-site.xml"},
    {"yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage", "^yarn\\.nodemanager\\.disk-health-checker\\.max-disk-utilization-per-disk-percentage$", "yarn-site.xml"},
    {"yarn.nodemanager.resource-plugins", "^yarn\\.nodemanager\\.resource-plugins$", "yarn-site.xml"},
    {"yarn.nodemanager.resource-plugins.gpu.allowed-gpu-devices", "^yarn\\.nodemanager\\.resource-plugins\\.gpu\\.allowed-gpu-devices$", "yarn-site.xml"},
    {"yarn.nodemanager.resource-plugins.gpu.path-to-discovery-executables", "^yarn\\.nodemanager\\.resource-plugins\\.gpu\\.path-to-discovery-executables$", "yarn-site.xml"},
    {"yarn.nodemanager.log-aggregation.roll-monitoring-interval-seconds", "^yarn\\.nodemanager\\.log-aggregation\\.roll-monitoring-interval-seconds$", "yarn-site.xml"},
    {"yarn.nodemanager.log-aggregation.debug-enabled", "^yarn\\.nodemanager\\.log-aggregation\\.debug-enabled$", "yarn-site.xml"},
    {"yarn.nodemanager.log-aggregation.num-log-files-per-app", "^yarn\\.nodemanager\\.log-aggregation\\.num-log-files-per-app$", "yarn-site.xml"},
    {"yarn.resourcemanager.system-metrics-publisher.enabled", "^yarn\\.resourcemanager\\.system-metrics-publisher\\.enabled$", "yarn-site.xml"},
    {"yarn.resourcemanager.system-metrics-publisher.dispatcher.pool-size", "^yarn\\.resourcemanager\\.system-metrics-publisher\\.dispatcher\\.pool-size$", "yarn-site.xml"},
    {"yarn.timeline-service.client.max-retries", "^yarn\\.timeline-service\\.client\\.max-retries$", "yarn-site.xml"},
    {"yarn.timeline-service.client.retry-interval-ms", "^yarn\\.timeline-service\\.client\\.retry-interval-ms$", "yarn-site.xml"},
    {"yarn.timeline-service.state-store-class", "^yarn\\.timeline-service\\.state-store\\.class$", "yarn-site.xml"},
    {"yarn.timeline-service.leveldb-state-store.path", "^yarn\\.timeline-service\\.leveldb-state-store\\.path$", "yarn-site.xml"},
    {"yarn.timeline-service.leveldb-timeline-store.path", "^yarn\\.timeline-service\\.leveldb-timeline-store\\.path$", "yarn-site.xml"},
    {"yarn.timeline-service.leveldb-timeline-store.read-cache-size", "^yarn\\.timeline-service\\.leveldb-timeline-store\\.read-cache-size$", "yarn-site.xml"},
    {"yarn.timeline-service.leveldb-timeline-store.start-time-read-cache-size", "^yarn\\.timeline-service\\.leveldb-timeline-store\\.start-time-read-cache-size$", "yarn-site.xml"},
    {"yarn.timeline-service.leveldb-timeline-store.start-time-write-cache-size", "^yarn\\.timeline-service\\.leveldb-timeline-store\\.start-time-write-cache-size$", "yarn-site.xml"},
    {"yarn.timeline-service.http-authentication.type", "^yarn\\.timeline-service\\.http-authentication\\.type$", "yarn-site.xml"},
    {"yarn.timeline-service.http-authentication.simple.anonymous.allowed", "^yarn\\.timeline-service\\.http-authentication\\.simple\\.anonymous\\.allowed$", "yarn-site.xml"},
    {"yarn.resourcemanager.webapp.delegation-token-auth-filter.enabled", "^yarn\\.resourcemanager\\.webapp\\.delegation-token-auth-filter\\.enabled$", "yarn-site.xml"},
    {"yarn.resourcemanager.bind-host", "^yarn\\.resourcemanager\\.bind-host$", "yarn-site.xml"},
    {"yarn.nodemanager.bind-host", "^yarn\\.nodemanager\\.bind-host$", "yarn-site.xml"},
    {"yarn.timeline-service.bind-host", "^yarn\\.timeline-service\\.bind-host$", "yarn-site.xml"},
    {"yarn.node-labels.fs-store.root-dir", "^yarn\\.node-labels\\.fs-store\\.root-dir$", "yarn-site.xml"},
    {"yarn.scheduler.minimum-allocation-vcores", "^yarn\\.scheduler\\.minimum-allocation-vcores$", "yarn-site.xml"},
    {"yarn.scheduler.maximum-allocation-vcores", "^yarn\\.scheduler\\.maximum-allocation-vcores$", "yarn-site.xml"},
    {"yarn.node-labels.enabled", "^yarn\\.node-labels\\.enabled$", "yarn-site.xml"},
    {"yarn.resourcemanager.scheduler.monitor.enable", "^yarn\\.resourcemanager\\.scheduler\\.monitor\\.enable$", "yarn-site.xml"},
    {"yarn.timeline-service.recovery.enabled", "^yarn\\.timeline-service\\.recovery\\.enabled$", "yarn-site.xml"},
    {"yarn.authorization-provider", "^yarn\\.authorization-provider$", "yarn-site.xml"},
    {"yarn.timeline-service.version", "^yarn\\.timeline-service\\.version$", "yarn-site.xml"},
    {"yarn.timeline-service.versions", "^yarn\\.timeline-service\\.versions$", "yarn-site.xml"},
    {"yarn.system-metricspublisher.enabled", "^yarn\\.system-metricspublisher\\.enabled$", "yarn-site.xml"},
    {"yarn.rm.system-metricspublisher.emit-container-events", "^yarn\\.rm\\.system-metricspublisher\\.emit-container-events$", "yarn-site.xml"},
    {"yarn.nodemanager.recovery.supervised", "^yarn\\.nodemanager\\.recovery\\.supervised$", "yarn-site.xml"},
    {"yarn.timeline-service.store-class", "^yarn\\.timeline-service\\.store-class$", "yarn-site.xml"},
    {"yarn.timeline-service.entity-group-fs-store.active-dir", "^yarn\\.timeline-service\\.entity-group-fs-store\\.active-dir$", "yarn-site.xml"},
    {"yarn.timeline-service.entity-group-fs-store.done-dir", "^yarn\\.timeline-service\\.entity-group-fs-store\\.done-dir$", "yarn-site.xml"},
    {"yarn.timeline-service.entity-group-fs-store.group-id-plugin-classes", "^yarn\\.timeline-service\\.entity-group-fs-store\\.group-id-plugin-classes$", "yarn-site.xml"},
    {"yarn.timeline-service.entity-group-fs-store.summary-store", "^yarn\\.timeline-service\\.entity-group-fs-store\\.summary-store$", "yarn-site.xml"},
    {"yarn.timeline-service.entity-group-fs-store.scan-interval-seconds", "^yarn\\.timeline-service\\.entity-group-fs-store\\.scan-interval-seconds$", "yarn-site.xml"},
    {"yarn.log.server.web-service.url", "^yarn\\.log\\.server\\.web-service\\.url$", "yarn-site.xml"},
    {"yarn.timeline-service.entity-group-fs-store.cleaner-interval-seconds", "^yarn\\.timeline-service\\.entity-group-fs-store\\.cleaner-interval-seconds$", "yarn-site.xml"},
    {"yarn.timeline-service.entity-group-fs-store.retain-seconds", "^yarn\\.timeline-service\\.entity-group-fs-store\\.retain-seconds$", "yarn-site.xml"},
    {"yarn.nodemanager.container-metrics.unregister-delay-ms", "^yarn\\.nodemanager\\.container-metrics\\.unregister-delay-ms$", "yarn-site.xml"},
    {"yarn.timeline-service.entity-group-fs-store.group-id-plugin-classpath", "^yarn\\.timeline-service\\.entity-group-fs-store\\.group-id-plugin-classpath$", "yarn-site.xml"},
    {"yarn.resourcemanager.monitor.capacity.preemption.total_preemption_per_round", "^yarn\\.resourcemanager\\.monitor\\.capacity\\.preemption\\.total_preemption_per_round$", "yarn-site.xml"},
    {"yarn.resourcemanager.monitor.capacity.preemption.natural_termination_factor", "^yarn\\.resourcemanager\\.monitor\\.capacity\\.preemption\\.natural_termination_factor$", "yarn-site.xml"},
    {"yarn.resourcemanager.monitor.capacity.preemption.monitoring_interval", "^yarn\\.resourcemanager\\.monitor\\.capacity\\.preemption\\.monitoring_interval$", "yarn-site.xml"},
    {"yarn.nodemanager.linux-container-executor.nonsecure-mode.limit-users", "^yarn\\.nodemanager\\.linux-container-executor\\.nonsecure-mode\\.limit-users$", "yarn-site.xml"},
    {"yarn.nodemanager.runtime.linux.allowed-runtimes", "^yarn\\.nodemanager\\.runtime\\.linux\\.allowed-runtimes$", "yarn-site.xml"},
    {"yarn.nodemanager.runtime.linux.docker.allowed-container-networks", "^yarn\\.nodemanager\\.runtime\\.linux\\.docker\\.allowed-container-networks$", "yarn-site.xml"},
    {"yarn.nodemanager.runtime.linux.docker.default-container-network", "^yarn\\.nodemanager\\.runtime\\.linux\\.docker\\.default-container-network$", "yarn-site.xml"},
    {"yarn.nodemanager.runtime.linux.docker.privileged-containers.allowed", "^yarn\\.nodemanager\\.runtime\\.linux\\.docker\\.privileged-containers\\.allowed$", "yarn-site.xml"},
    {"yarn.nodemanager.runtime.linux.docker.privileged-containers.acl", "^yarn\\.nodemanager\\.runtime\\.linux\\.docker\\.privileged-containers\\.acl$", "yarn-site.xml"},
    {"yarn.nodemanager.runtime.linux.docker.capabilities", "^yarn\\.nodemanager\\.runtime\\.linux\\.docker\\.capabilities$", "yarn-site.xml"},
    {"yarn.webapp.ui2.enable", "^yarn\\.webapp\\.ui2\\.enable$", "yarn-site.xml"},
    {"yarn.timeline-service.http-cross-origin.enabled", "^yarn\\.timeline-service\\.http-cross-origin\\.enabled$", "yarn-site.xml"},
    {"yarn.resourcemanager.webapp.cross-origin.enabled", "^yarn\\.resourcemanager\\.webapp\\.cross-origin\\.enabled$", "yarn-site.xml"},
    {"yarn.nodemanager.webapp.cross-origin.enabled", "^yarn\\.nodemanager\\.webapp\\.cross-origin\\.enabled$", "yarn-site.xml"},
    {"yarn.nodemanager.resource-plugins.gpu.docker-plugin", "^yarn\\.nodemanager\\.resource-plugins\\.gpu\\.docker-plugin$", "yarn-site.xml"},
    {"yarn.nodemanager.resource-plugins.gpu.docker-plugin.nvidiadocker-v1.endpoint", "^yarn\\.nodemanager\\.resource-plugins\\.gpu\\.docker-plugin\\.nvidiadocker-v1\\.endpoint$", "yarn-site.xml"},
    {"yarn.webapp.api-service.enable", "^yarn\\.webapp\\.api-service\\.enable$", "yarn-site.xml"},
    {"yarn.service.framework.path", "^yarn\\.service\\.framework\\.path$", "yarn-site.xml"},
    {"yarn.nodemanager.aux-services.timeline_collector.class", "^yarn\\.nodemanager\\.aux-services\\.timeline_collector\\.class$", "yarn-site.xml"},
    {"yarn.timeline-service.reader.webapp.address", "^yarn\\.timeline-service\\.reader\\.webapp\\.address$", "yarn-site.xml"},
    {"yarn.timeline-service.reader.webapp.https.address", "^yarn\\.timeline-service\\.reader\\.webapp\\.https\\.address$", "yarn-site.xml"},
    {"yarn.timeline-service.hbase-schema.prefix", "^yarn\\.timeline-service\\.hbase-schema\\.prefix$", "yarn-site.xml"},
    {"yarn.timeline-service.hbase.configuration.file", "^yarn\\.timeline-service\\.hbase\\.configuration\\.file$", "yarn-site.xml"},
    {"yarn.timeline-service.hbase.coprocessor.jar.hdfs.location", "^yarn\\.timeline-service\\.hbase\\.coprocessor\\.jar\\.hdfs\\.location$", "yarn-site.xml"},
    {"yarn.resourcemanager.monitor.capacity.preemption.intra-queue-preemption.enabled", "^yarn\\.resourcemanager\\.monitor\\.capacity\\.preemption\\.intra-queue-preemption\\.enabled$", "yarn-site.xml"},
    {"yarn.scheduler.capacity.ordering-policy.priority-utilization.underutilized-preemption.enabled", "^yarn\\.scheduler\\.capacity\\.ordering-policy\\.priority-utilization\\.underutilized-preemption\\.enabled$", "yarn-site.xml"},
    {"yarn.resourcemanager.display.per-user-apps", "^yarn\\.resourcemanager\\.display\\.per-user-apps$", "yarn-site.xml"},
    {"yarn.service.system-service.dir", "^yarn\\.service\\.system-service\\.dir$", "yarn-site.xml"},
    {"yarn.timeline-service.generic-application-history.save-non-am-container-meta-info", "^yarn\\.timeline-service\\.generic-application-history\\.save-non-am-container-meta-info$", "yarn-site.xml"},
    {"hadoop.registry.dns.bind-address", "^hadoop\\.registry\\.dns\\.bind-address$", "yarn-site.xml"},
    {"hadoop.http.cross-origin.allowed-origins", "^hadoop\\.http\\.cross-origin\\.allowed-origins$", "yarn-site.xml"},
    {"yarn.nodemanager.resourcemanager.connect.wait.secs", "^yarn\\.nodemanager\\.resourcemanager\\.connect\\.wait\\.secs$", "yarn-site.xml"},
    // Log4j properties parameters (71)
    {"hadoop.root.logger", "^hadoop\\.root\\.logger$", "log4j.properties"},
    {"hadoop.log.dir", "^hadoop\\.log\\.dir$", "log4j.properties"},
    {"hadoop.log.file", "^hadoop\\.log\\.file$", "log4j.properties"},
    {"log4j.rootLogger", "^log4j\\.rootLogger$", "log4j.properties"},
    {"log4j.threshhold", "^log4j\\.threshhold$", "log4j.properties"},
    {"log4j.appender.DRFA", "^log4j\\.appender\\.DRFA$", "log4j.properties"},
    {"log4j.appender.DRFA.File", "^log4j\\.appender\\.DRFA\\.File$", "log4j.properties"},
    {"log4j.appender.DRFA.DatePattern", "^log4j\\.appender\\.DRFA\\.DatePattern$", "log4j.properties"},
    {"log4j.appender.DRFA.layout", "^log4j\\.appender\\.DRFA\\.layout$", "log4j.properties"},
    {"log4j.appender.DRFA.layout.ConversionPattern", "^log4j\\.appender\\.DRFA\\.layout\\.ConversionPattern$", "log4j.properties"},
    {"log4j.appender.console", "^log4j\\.appender\\.console$", "log4j.properties"},
    {"log4j.appender.console.target", "^log4j\\.appender\\.console\\.target$", "log4j.properties"},
    {"log4j.appender.console.layout", "^log4j\\.appender\\.console\\.layout$", "log4j.properties"},
    {"log4j.appender.console.layout.ConversionPattern", "^log4j\\.appender\\.console\\.layout\\.ConversionPattern$", "log4j.properties"},
    {"hadoop.tasklog.taskid", "^hadoop\\.tasklog\\.taskid$", "log4j.properties"},
    {"hadoop.tasklog.iscleanup", "^hadoop\\.tasklog\\.iscleanup$", "log4j.properties"},
    {"hadoop.tasklog.noKeepSplits", "^hadoop\\.tasklog\\.noKeepSplits$", "log4j.properties"},
    {"hadoop.tasklog.totalLogFileSize", "^hadoop\\.tasklog\\.totalLogFileSize$", "log4j.properties"},
    {"hadoop.tasklog.purgeLogSplits", "^hadoop\\.tasklog\\.purgeLogSplits$", "log4j.properties"},
    {"hadoop.tasklog.logsRetainHours", "^hadoop\\.tasklog\\.logsRetainHours$", "log4j.properties"},
    {"log4j.appender.TLA", "^log4j\\.appender\\.TLA$", "log4j.properties"},
    {"log4j.appender.TLA.taskId", "^log4j\\.appender\\.TLA\\.taskId$", "log4j.properties"},
    {"log4j.appender.TLA.isCleanup", "^log4j\\.appender\\.TLA\\.isCleanup$", "log4j.properties"},
    {"log4j.appender.TLA.totalLogFileSize", "^log4j\\.appender\\.TLA\\.totalLogFileSize$", "log4j.properties"},
    {"log4j.appender.TLA.layout", "^log4j\\.appender\\.TLA\\.layout$", "log4j.properties"},
    {"log4j.appender.TLA.layout.ConversionPattern", "^log4j\\.appender\\.TLA\\.layout\\.ConversionPattern$", "log4j.properties"},
    {"hadoop.security.logger", "^hadoop\\.security\\.logger$", "log4j.properties"},
    {"hadoop.security.log.maxfilesize", "^hadoop\\.security\\.log\\.maxfilesize$", "log4j.properties"},
    {"hadoop.security.log.maxbackupindex", "^hadoop\\.security\\.log\\.maxbackupindex$", "log4j.properties"},
    {"log4j.category.SecurityLogger", "^log4j\\.category\\.SecurityLogger$", "log4j.properties"},
    {"hadoop.security.log.file", "^hadoop\\.security\\.log\\.file$", "log4j.properties"},
    {"log4j.appender.DRFAS", "^log4j\\.appender\\.DRFAS$", "log4j.properties"},
    {"log4j.appender.DRFAS.File", "^log4j\\.appender\\.DRFAS\\.File$", "log4j.properties"},
    {"log4j.appender.DRFAS.layout", "^log4j\\.appender\\.DRFAS\\.layout$", "log4j.properties"},
    {"log4j.appender.DRFAS.layout.ConversionPattern", "^log4j\\.appender\\.DRFAS\\.layout\\.ConversionPattern$", "log4j.properties"},
    {"log4j.appender.DRFAS.DatePattern", "^log4j\\.appender\\.DRFAS\\.DatePattern$", "log4j.properties"},
    {"log4j.appender.RFAS", "^log4j\\.appender\\.RFAS$", "log4j.properties"},
    {"log4j.appender.RFAS.File", "^log4j\\.appender\\.RFAS\\.File$", "log4j.properties"},
    {"log4j.appender.RFAS.layout", "^log4j\\.appender\\.RFAS\\.layout$", "log4j.properties"},
    {"log4j.appender.RFAS.layout.ConversionPattern", "^log4j\\.appender\\.RFAS\\.layout\\.ConversionPattern$", "log4j.properties"},
    {"log4j.appender.RFAS.MaxFileSize", "^log4j\\.appender\\.RFAS\\.MaxFileSize$", "log4j.properties"},
    {"log4j.appender.RFAS.MaxBackupIndex", "^log4j\\.appender\\.RFAS\\.MaxBackupIndex$", "log4j.properties"},
    {"hdfs.audit.logger", "^hdfs\\.audit\\.logger$", "log4j.properties"},
    {"log4j.logger.org.apache.hadoop.hdfs.server.namenode.FSNamesystem.audit", "^log4j\\.logger\\.org\\.apache\\.hadoop\\.hdfs\\.server\\.namenode\\.FSNamesystem\\.audit$", "log4j.properties"},
    {"log4j.additivity.org.apache.hadoop.hdfs.server.namenode.FSNamesystem.audit", "^log4j\\.additivity\\.org\\.apache\\.hadoop\\.hdfs\\.server\\.namenode\\.FSNamesystem\\.audit$", "log4j.properties"},
    {"log4j.appender.DRFAAUDIT", "^log4j\\.appender\\.DRFAAUDIT$", "log4j.properties"},
    {"log4j.appender.DRFAAUDIT.File", "^log4j\\.appender\\.DRFAAUDIT\\.File$", "log4j.properties"},
    {"log4j.appender.DRFAAUDIT.layout", "^log4j\\.appender\\.DRFAAUDIT\\.layout$", "log4j.properties"},
    {"log4j.appender.DRFAAUDIT.layout.ConversionPattern", "^log4j\\.appender\\.DRFAAUDIT\\.layout\\.ConversionPattern$", "log4j.properties"},
    {"log4j.appender.DRFAAUDIT.DatePattern", "^log4j\\.appender\\.DRFAAUDIT\\.DatePattern$", "log4j.properties"},
    {"mapred.audit.logger", "^mapred\\.audit\\.logger$", "log4j.properties"},
    {"log4j.logger.org.apache.hadoop.mapred.AuditLogger", "^log4j\\.logger\\.org\\.apache\\.hadoop\\.mapred\\.AuditLogger$", "log4j.properties"},
    {"log4j.additivity.org.apache.hadoop.mapred.AuditLogger", "^log4j\\.additivity\\.org\\.apache\\.hadoop\\.mapred\\.AuditLogger$", "log4j.properties"},
    {"log4j.appender.MRAUDIT", "^log4j\\.appender\\.MRAUDIT$", "log4j.properties"},
    {"log4j.appender.MRAUDIT.File", "^log4j\\.appender\\.MRAUDIT\\.File$", "log4j.properties"},
    {"log4j.appender.MRAUDIT.layout", "^log4j\\.appender\\.MRAUDIT\\.layout$", "log4j.properties"},
    {"log4j.appender.MRAUDIT.layout.ConversionPattern", "^log4j\\.appender\\.MRAUDIT\\.layout\\.ConversionPattern$", "log4j.properties"},
    {"log4j.appender.MRAUDIT.DatePattern", "^log4j\\.appender\\.MRAUDIT\\.DatePattern$", "log4j.properties"},
    {"log4j.appender.RFA", "^log4j\\.appender\\.RFA$", "log4j.properties"},
    {"log4j.appender.RFA.File", "^log4j\\.appender\\.RFA\\.File$", "log4j.properties"},
    {"log4j.appender.RFA.MaxFileSize", "^log4j\\.appender\\.RFA\\.MaxFileSize$", "log4j.properties"},
    {"log4j.appender.RFA.MaxBackupIndex", "^log4j\\.appender\\.RFA\\.MaxBackupIndex$", "log4j.properties"},
    {"log4j.appender.RFA.layout", "^log4j\\.appender\\.RFA\\.layout$", "log4j.properties"},
    {"log4j.appender.RFA.layout.ConversionPattern", "^log4j\\.appender\\.RFA\\.layout\\.ConversionPattern$", "log4j.properties"},
    {"hadoop.metrics.log.level", "^hadoop\\.metrics\\.log\\.level$", "log4j.properties"},
    {"log4j.logger.org.apache.hadoop.metrics2", "^log4j\\.logger\\.org\\.apache\\.hadoop\\.metrics2$", "log4j.properties"},
    {"log4j.logger.org.jets3t.service.impl.rest.httpclient.RestS3Service", "^log4j\\.logger\\.org\\.jets3t\\.service\\.impl\\.rest\\.httpclient\\.RestS3Service$", "log4j.properties"},
    {"log4j.appender.NullAppender", "^log4j\\.appender\\.NullAppender$", "log4j.properties"},
    {"log4j.appender.EventCounter", "^log4j\\.appender\\.EventCounter$", "log4j.properties"},
    {"log4j.logger.org.apache.hadoop.conf.Configuration.deprecation", "^log4j\\.logger\\.org\\.apache\\.hadoop\\.conf\\.Configuration\\.deprecation$", "log4j.properties"},
    {"log4j.logger.org.apache.commons.beanutils", "^log4j\\.logger\\.org\\.apache\\.commons\\.beanutils$", "log4j.properties"},

    // New MapRed parameters from mapred-site.xml
    {"mapreduce.task.io.sort.mb", "^mapreduce\\.task\\.io\\.sort\\.mb$", "mapred-site.xml"},
    {"mapreduce.map.sort.spill.percent", "^mapreduce\\.map\\.sort\\.spill\\.percent$", "mapred-site.xml"},
    {"mapreduce.task.io.sort.factor", "^mapreduce\\.task\\.io\\.sort\\.factor$", "mapred-site.xml"},
    {"mapreduce.cluster.administrators", "^mapreduce\\.cluster\\.administrators$", "mapred-site.xml"},
    {"mapreduce.reduce.shuffle.parallelcopies", "^mapreduce\\.reduce\\.shuffle\\.parallelcopies$", "mapred-site.xml"},
    {"mapreduce.map.speculative", "^mapreduce\\.map\\.speculative$", "mapred-site.xml"},
    {"mapreduce.reduce.speculative", "^mapreduce\\.reduce\\.speculative$", "mapred-site.xml"},
    {"mapreduce.job.reduce.slowstart.completedmaps", "^mapreduce\\.job\\.reduce\\.slowstart\\.completedmaps$", "mapred-site.xml"},
    {"mapreduce.job.counters.max", "^mapreduce\\.job\\.counters\\.max$", "mapred-site.xml"},
    {"mapreduce.reduce.shuffle.merge.percent", "^mapreduce\\.reduce\\.shuffle\\.merge\\.percent$", "mapred-site.xml"},
    {"mapreduce.reduce.shuffle.input.buffer.percent", "^mapreduce\\.reduce\\.shuffle\\.input\\.buffer\\.percent$", "mapred-site.xml"},
    {"mapreduce.output.fileoutputformat.compress.type", "^mapreduce\\.output\\.fileoutputformat\\.compress\\.type$", "mapred-site.xml"},
    {"mapreduce.reduce.input.buffer.percent", "^mapreduce\\.reduce\\.input\\.buffer\\.percent$", "mapred-site.xml"},
    {"mapreduce.map.output.compress", "^mapreduce\\.map\\.output\\.compress$", "mapred-site.xml"},
    {"mapreduce.task.timeout", "^mapreduce\\.task\\.timeout$", "mapred-site.xml"},
    {"mapreduce.map.memory.mb", "^mapreduce\\.map\\.memory\\.mb$", "mapred-site.xml"},
    {"mapreduce.reduce.memory.mb", "^mapreduce\\.reduce\\.memory\\.mb$", "mapred-site.xml"},
    {"mapreduce.shuffle.port", "^mapreduce\\.shuffle\\.port$", "mapred-site.xml"},
    {"mapreduce.jobhistory.intermediate-done-dir", "^mapreduce\\.jobhistory\\.intermediate-done-dir$", "mapred-site.xml"},
    {"mapreduce.jobhistory.done-dir", "^mapreduce\\.jobhistory\\.done-dir$", "mapred-site.xml"},
    {"mapreduce.jobhistory.address", "^mapreduce\\.jobhistory\\.address$", "mapred-site.xml"},
    {"mapreduce.jobhistory.webapp.address", "^mapreduce\\.jobhistory\\.webapp\\.address$", "mapred-site.xml"},
    {"mapreduce.framework.name", "^mapreduce\\.framework\\.name$", "mapred-site.xml"},
    {"yarn.app.mapreduce.am.staging-dir", "^yarn\\.app\\.mapreduce\\.am\\.staging-dir$", "mapred-site.xml"},
    {"yarn.app.mapreduce.am.resource.mb", "^yarn\\.app\\.mapreduce\\.am\\.resource\\.mb$", "mapred-site.xml"},
    {"yarn.app.mapreduce.am.command-opts", "^yarn\\.app\\.mapreduce\\.am\\.command-opts$", "mapred-site.xml"},
    {"yarn.app.mapreduce.am.admin-command-opts", "^yarn\\.app\\.mapreduce\\.am\\.admin-command-opts$", "mapred-site.xml"},
    {"yarn.app.mapreduce.am.log.level", "^yarn\\.app\\.mapreduce\\.am\\.log\\.level$", "mapred-site.xml"},
    {"mapreduce.admin.map.child.java.opts", "^mapreduce\\.admin\\.map\\.child\\.java\\.opts$", "mapred-site.xml"},
    {"mapreduce.admin.reduce.child.java.opts", "^mapreduce\\.admin\\.reduce\\.child\\.java\\.opts$", "mapred-site.xml"},
    {"mapreduce.application.classpath", "^mapreduce\\.application\\.classpath$", "mapred-site.xml"},
    {"mapreduce.am.max-attempts", "^mapreduce\\.am\\.max-attempts$", "mapred-site.xml"},
    {"mapreduce.map.java.opts", "^mapreduce\\.map\\.java\\.opts$", "mapred-site.xml"},
    {"mapreduce.reduce.java.opts", "^mapreduce\\.reduce\\.java\\.opts$", "mapred-site.xml"},
    {"mapreduce.map.log.level", "^mapreduce\\.map\\.log\\.level$", "mapred-site.xml"},
    {"mapreduce.reduce.log.level", "^mapreduce\\.reduce\\.log\\.level$", "mapred-site.xml"},
    {"mapreduce.admin.user.env", "^mapreduce\\.admin\\.user\\.env$", "mapred-site.xml"},
    {"mapreduce.output.fileoutputformat.compress", "^mapreduce\\.output\\.fileoutputformat\\.compress$", "mapred-site.xml"},
    {"mapreduce.jobhistory.http.policy", "^mapreduce\\.jobhistory\\.http\\.policy$", "mapred-site.xml"},
    {"mapreduce.job.queuename", "^mapreduce\\.job\\.queuename$", "mapred-site.xml"},

    { "REPOSITORY_CONFIG_USERNAME", "^REPOSITORY_CONFIG_USERNAME$", "ranger-hdfs-plugin.properties" },
    { "REPOSITORY_CONFIG_PASSWORD", "^REPOSITORY_CONFIG_PASSWORD$", "ranger-hdfs-plugin.properties" },
    { "REPOSITORY_CONFIG_USER_PASSWORD", "^REPOSITORY_CONFIG_USER_PASSWORD$", "ranger-hdfs-plugin.properties" },
    { "REPOSITORY_TYPE", "^REPOSITORY_TYPE$", "ranger-hdfs-plugin.properties" },
    { "POLICY_DOWNLOAD_AUTH_USERS", "^POLICY_DOWNLOAD_AUTH_USERS$", "ranger-hdfs-plugin.properties" },
    { "REPOSITORY_CONFIG_BASE_URL", "^REPOSITORY_CONFIG_BASE_URL$", "ranger-hdfs-plugin.properties" },
    { "REPOSITORY_CONFIG_COMMON_NAME_FOR_CERTIFICATE", "^REPOSITORY_CONFIG_COMMON_NAME_FOR_CERTIFICATE$", "ranger-hdfs-plugin.properties" },
    { "REPOSITORY_CONFIG_POLICY_MGR_SSL_CERTIFICATE", "^REPOSITORY_CONFIG_POLICY_MGR_SSL_CERTIFICATE$", "ranger-hdfs-plugin.properties" },
    { "content.property-file-name", "^content\\.property-file-name$", "container-executor.cfg" },
    { "xasecure.audit.destination.db.jdbc.url", "^xasecure\\.audit\\.destination\\.db\\.jdbc\\.url$", "ranger-yarn-audit.xml" },
    { "REPOSITORY_CONFIG_USERNAME", "^REPOSITORY_CONFIG_USERNAME$", "ranger-hdfs-plugin.properties" },
    { "REPOSITORY_CONFIG_PASSWORD", "^REPOSITORY_CONFIG_PASSWORD$", "ranger-hdfs-plugin.properties" },
    { "REPOSITORY_CONFIG_USER_PASSWORD", "^REPOSITORY_CONFIG_USER_PASSWORD$", "ranger-hdfs-plugin.properties" },
    { "REPOSITORY_TYPE", "^REPOSITORY_TYPE$", "ranger-hdfs-plugin.properties" },
    { "POLICY_DOWNLOAD_AUTH_USERS", "^POLICY_DOWNLOAD_AUTH_USERS$", "ranger-hdfs-plugin.properties" },
    { "REPOSITORY_CONFIG_BASE_URL", "^REPOSITORY_CONFIG_BASE_URL$", "ranger-hdfs-plugin.properties" },
    { "REPOSITORY_CONFIG_COMMON_NAME_FOR_CERTIFICATE", "^REPOSITORY_CONFIG_COMMON_NAME_FOR_CERTIFICATE$", "ranger-hdfs-plugin.properties" },
    { "REPOSITORY_CONFIG_POLICY_MGR_SSL_CERTIFICATE", "^REPOSITORY_CONFIG_POLICY_MGR_SSL_CERTIFICATE$", "ranger-hdfs-plugin.properties" },



};

ValidationResult validateHdfsConfigParam(const char *param_name, const char *value) {
    // Check if parameter exists
    bool param_exists = false;
    for (size_t i = 0; i < sizeof(hdfs_configs)/sizeof(hdfs_configs[0]); i++) {
        if (strcmp(param_name, hdfs_configs[i].canonicalName) == 0) {
            param_exists = true;
            break;
        }
    }
    if (!param_exists) return ERROR_PARAM_NOT_FOUND;

    // Check value presence
    if (!value || strlen(value) == 0) return ERROR_VALUE_EMPTY;

    // Parameter-specific validation
    if (strcmp(param_name, "dfs.replication") == 0) {
        return isPositiveInteger(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }

    // =======================================================================
    // Ranger-specific validations
    // =======================================================================

    // Boolean parameters
    if (strstr(param_name, ".is.enabled") != NULL ||
        strstr(param_name, ".is.async") != NULL ||
        strcmp(param_name, "xasecure.add-hadoop-authorization") == 0) {
        return isValidBoolean(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }

    // Positive integers (queue sizes, counts, etc)
    if (strstr(param_name, ".max.queue.size") != NULL ||
        strstr(param_name, ".max.file.count") != NULL ||
        strstr(param_name, ".buffer.size.bytes") != NULL ||
        strstr(param_name, ".pollIntervalMs") != NULL ||
        strstr(param_name, ".timeoutMs") != NULL ||
        strstr(param_name, ".interval.ms") != NULL ||
        strstr(param_name, ".interval.seconds") != NULL) {
        return isPositiveInteger(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }

    // Data sizes (buffer sizes)
    if (strstr(param_name, ".buffer.size") != NULL) {
        return isDataSize(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }

    // Host:port lists
    if (strcmp(param_name, "xasecure.audit.kafka.broker_list") == 0) {
        return isValidHostPortList(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }

    // URLs (policy REST, Solr, credential stores)
    if (strstr(param_name, ".rest.url") != NULL ||
        strstr(param_name, ".solr_url") != NULL ||
        strstr(param_name, ".credential.file") != NULL) {
        return isValidUrl(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }

    // Paths and directories
    if (strstr(param_name, ".directory") != NULL ||
        strstr(param_name, ".file") != NULL ||
        strstr(param_name, ".keystore") != NULL ||
        strstr(param_name, ".truststore") != NULL ||
        strstr(param_name, ".cache.dir") != NULL ||
        strstr(param_name, ".config.file") != NULL) {
        return isValidPath(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }

    // Service name validation
    if (strcmp(param_name, "ranger.plugin.hdfs.service.name") == 0) {
        return (strlen(value) > 0) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }

    // Policy source implementation
    if (strcmp(param_name, "ranger.plugin.hdfs.policy.source.impl") == 0) {
        return (strstr(value, ".") != NULL) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }

    // Encoding can be empty but not NULL
    if (strcmp(param_name, "xasecure.audit.hdfs.config.encoding") == 0) {
        return VALIDATION_OK; // Empty string is allowed
    }

    else if (strcmp(param_name, "dfs.namenode.name.dir") == 0 ||
             strcmp(param_name, "dfs.datanode.data.dir") == 0 ||
             strcmp(param_name, "hadoop.tmp.dir") == 0 ||
             strcmp(param_name, "dfs.namenode.checkpoint.dir") == 0) {
        return (value[0] != '\0') ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "fs.defaultFS") == 0) {
        return strncmp(value, "hdfs://", 7) == 0 ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "dfs.blocksize") == 0 ||
             strcmp(param_name, "dfs.datanode.balance.bandwidthPerSec") == 0 ||
             strcmp(param_name, "dfs.client.write.packet.size") == 0) {
        return isDataSize(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "dfs.permissions.enabled") == 0 ||
             strcmp(param_name, "dfs.client.use.datanode.hostname") == 0 ||
             strcmp(param_name, "dfs.ha.automatic-failover.enabled") == 0 ||
             strcmp(param_name, "dfs.namenode.acls.enabled") == 0 ||
             strcmp(param_name, "hadoop.security.authorization") == 0 ||
             strcmp(param_name, "dfs.encrypt.data.transfer") == 0 ||
             strcmp(param_name, "dfs.client.block.write.replace-datanode-on-failure.enable") == 0 ||
             strcmp(param_name, "dfs.client.block.write.replace-datanode-on-failure.best-effort") == 0 ||
             strcmp(param_name, "dfs.client.read.shortcircuit") == 0 ||
             strcmp(param_name, "dfs.image.compress") == 0 ||
             strcmp(param_name, "dfs.namenode.quota.enabled") == 0 ||
             strcmp(param_name, "dfs.namenode.replication.considerLoad") == 0 ||
             strcmp(param_name, "dfs.client.retry.policy.enabled") == 0) {
        return isValidBoolean(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "dfs.datanode.address") == 0 ||
             strcmp(param_name, "dfs.datanode.http.address") == 0 ||
             strcmp(param_name, "dfs.datanode.ipc.address") == 0 ||
             strcmp(param_name, "dfs.namenode.http-address") == 0 ||
             strcmp(param_name, "dfs.namenode.https-address") == 0 ||
             strcmp(param_name, "dfs.namenode.rpc-address") == 0 ||
             strcmp(param_name, "dfs.namenode.secondary.http-address") == 0 ||
             strcmp(param_name, "dfs.namenode.backup.address") == 0 ||
             strcmp(param_name, "dfs.journalnode.rpc-address") == 0 ||
             strcmp(param_name, "dfs.journalnode.http-address") == 0) {
        return isValidHostPort(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "dfs.https.port") == 0) {
        return isValidPort(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "hadoop.security.authentication") == 0) {
        return (strcmp(value, "simple") == 0 || strcmp(value, "kerberos") == 0)
            ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "hadoop.rpc.protection") == 0) {
        return (strcmp(value, "authentication") == 0 ||
                strcmp(value, "integrity") == 0 ||
                strcmp(value, "privacy") == 0)
            ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "dfs.http.policy") == 0) {
        return (strcmp(value, "HTTP_ONLY") == 0 || strcmp(value, "HTTPS_ONLY") == 0)
            ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "dfs.namenode.safemode.threshold-pct") == 0) {
        char *end;
        float threshold = strtof(value, &end);
        if (*end != '\0' || threshold < 0.0f || threshold > 1.0f)
            return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "dfs.datanode.data.dir.perm") == 0) {
        char *end;
        long perm = strtol(value, &end, 8);
        if (*end != '\0' || perm < 0 || perm > 07777)
            return ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "dfs.datanode.failed.volumes.tolerated") == 0 ||
             strcmp(param_name, "dfs.namenode.num.checkpoints.retained") == 0) {
        return isNonNegativeInteger(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "dfs.namenode.handler.count") == 0 ||
             strcmp(param_name, "dfs.datanode.handler.count") == 0 ||
             strcmp(param_name, "dfs.datanode.max.transfer.threads") == 0 ||
             strcmp(param_name, "dfs.client.max.block.acquire.failures") == 0 ||
             strcmp(param_name, "dfs.namenode.replication.min") == 0 ||
             strcmp(param_name, "dfs.client.hedged.read.threadpool.size") == 0 ||
             strcmp(param_name, "dfs.client.retry.max.attempts") == 0) {
        return isPositiveInteger(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "dfs.heartbeat.interval") == 0 ||
             strcmp(param_name, "dfs.replication.interval") == 0 ||
             strcmp(param_name, "dfs.namenode.quota.update.interval") == 0) {
        return isValidDuration(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "dfs.domain.socket.path") == 0) {
        return (value[0] == '/') ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "dfs.storage.policy.satisfier.mode") == 0) {
        return (strcmp(value, "none") == 0 || strcmp(value, "all") == 0 ||
                strcmp(value, "random") == 0) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "dfs.encrypt.data.transfer.algorithm") == 0) {
        return (strcmp(value, "AES/CTR/NoPadding") == 0) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;

    }
    else if (strcmp(param_name, "dfs.client.failover.proxy.provider") == 0) {
        return (strchr(value, '.') != NULL) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "dfs.namenode.audit.loggers") == 0) {
        return isValidCommaSeparatedList(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "dfs.image.compression.codec") == 0) {
        return (strcmp(value, "zlib") == 0 || strcmp(value, "lz4") == 0 ||
                strcmp(value, "snappy") == 0) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }

    // Default validation for parameters without specific rules
    return VALIDATION_OK;
}


ConfigResult* find_hdfs_config(const char *param) {
    regex_t regex;
    int ret;
    for (size_t i = 0; i < sizeof(hdfs_configs)/sizeof(hdfs_configs[0]); ++i) {
        ret = regcomp(&regex, hdfs_configs[i].normalizedName, REG_EXTENDED);
        if (ret != 0) {
            char error_msg[100];
            regerror(ret, &regex, error_msg, sizeof(error_msg));
            fprintf(stderr, "Regex compilation failed for %s: %s\n", hdfs_configs[i].canonicalName, error_msg);
            continue;
        }

        ret = regexec(&regex, param, 0, NULL, 0);
        regfree(&regex);

        if (ret == 0) {
            ConfigResult *result = malloc(sizeof(ConfigResult));
            if (!result) return NULL;

            result->canonical_name = strdup(hdfs_configs[i].canonicalName);
            result->config_file = strdup(hdfs_configs[i].configFile);

            if (!result->canonical_name || !result->config_file) {
                free(result->canonical_name);
                free(result->config_file);
                free(result);
                return NULL;
            }

            return result;
        } else if (ret != REG_NOMATCH) {
            char error_msg[100];
            regerror(ret, &regex, error_msg, sizeof(error_msg));
            fprintf(stderr, "Regex match error for %s: %s\n", hdfs_configs[i].canonicalName, error_msg);
        }
    }
    return NULL;
}


ConfigStatus modify_hdfs_config(const char* config_param, const char* value, const char *filename) {
    char *file_path = NULL;
    char candidate_path[PATH_MAX];

    // 1. Check HADOOP_HOME environment variable
    const char *hadoop_home_env = getenv("HADOOP_HOME");
    if (hadoop_home_env) {
        snprintf(candidate_path, sizeof(candidate_path), "%s/etc/hadoop/%s", hadoop_home_env, filename);
        if (access(candidate_path, F_OK) == 0) {
            file_path = strdup(candidate_path);
        }
    }

    // 2. Check common OS-specific default paths if not found
    const char *default_paths[] = {
        "/etc/hadoop/conf",           // Red Hat/CentOS (Bigtop)
        "/usr/lib/hadoop/etc/hadoop", // Debian/Ubuntu
        "/usr/share/hadoop/etc/hadoop",// Alternative location
        "/etc/hadoop",                // Generic configuration
        "/opt/hadoop",                // Generic configuration
        "/usr/hadoop",                // Generic configuration
        "/usr/local/hadoop/etc/hadoop", // Default tarball install
        NULL
    };

    if (!file_path) {
        for (int i = 0; default_paths[i] != NULL; i++) {
            snprintf(candidate_path, sizeof(candidate_path), "%s/%s", default_paths[i], filename);
            if (access(candidate_path, F_OK) == 0) {
                file_path = strdup(candidate_path);
                break;
            }
        }
    }

    // 3. Fallback validation
    if (!file_path) {
        return FILE_NOT_FOUND;
    }

    if (strcmp(filename, "hdfs-log4j.properties") == 0 ||
        strcmp(filename, "log4j.properties") == 0 ||
        strcmp(filename, "ranger-hdfs-plugin.properties") == 0 ||
        strcmp(filename, "yarnservice-log4j.properties") == 0 ||
        strcmp(filename, "ranger-yarn-plugin.properties") == 0) {
        configure_hadoop_property(file_path, config_param, value);
        free(file_path);
        return SUCCESS;
    }

    if (strcmp(filename, "container-executor.cfg") == 0) {
        update_config( config_param, value, file_path);
        free(file_path);
        return SUCCESS;
    }

    // Parse the XML document
    xmlDoc *doc = xmlReadFile(file_path, NULL, 0);
    if (!doc) {
        free(file_path);
        return XML_PARSE_ERROR;
    }

    xmlNode *root = xmlDocGetRootElement(doc);
    if (!root || xmlStrcmp(root->name, (const xmlChar*)"configuration") != 0) {
        xmlFreeDoc(doc);
        free(file_path);
        return INVALID_CONFIG_FILE;
    }

    xmlNode *target_prop = NULL;
    for (xmlNode *node = root->children; node; node = node->next) {
        if (node->type == XML_ELEMENT_NODE && xmlStrcmp(node->name, (const xmlChar*)"property") == 0) {
            xmlChar *name = NULL;
            xmlNode *name_node = NULL;

            for (xmlNode *child = node->children; child; child = child->next) {
                if (child->type == XML_ELEMENT_NODE && xmlStrcmp(child->name, (const xmlChar*)"name") == 0) {
                    name_node = child;
                    break;
                }
            }

            if (name_node) {
                name = xmlNodeGetContent(name_node);
                if (xmlStrcmp(name, (const xmlChar*)config_param) == 0) {
                    target_prop = node;
                    xmlFree(name);
                    break;
                }
                xmlFree(name);
            }
        }
    }

    if (target_prop) {
        xmlNode *value_node = NULL;
        for (xmlNode *child = target_prop->children; child; child = child->next) {
            if (child->type == XML_ELEMENT_NODE && xmlStrcmp(child->name, (const xmlChar*)"value") == 0) {
                value_node = child;
                break;
            }
        }

        if (value_node) {
            xmlNodeSetContent(value_node, (const xmlChar*)value);
        } else {
            xmlNode *new_value = xmlNewTextChild(target_prop, NULL, (const xmlChar*)"value", (const xmlChar*)value);
            if (!new_value) {
                xmlFreeDoc(doc);
                free(file_path);
                return XML_UPDATE_ERROR;
            }
        }
    } else {
        xmlNode *new_prop = xmlNewNode(NULL, (const xmlChar*)"property");
        xmlNewTextChild(new_prop, NULL, (const xmlChar*)"name", (const xmlChar*)config_param);
        xmlNewTextChild(new_prop, NULL, (const xmlChar*)"value", (const xmlChar*)value);
        xmlAddChild(root, new_prop);
    }

    // Enable XML output indentation
    int oldIndent = xmlIndentTreeOutput;
    xmlIndentTreeOutput = 1;

    // Save the XML document with formatting
    int save_result = xmlSaveFormatFileEnc(file_path, doc, "UTF-8", 1);

    // Restore original indentation setting
    xmlIndentTreeOutput = oldIndent;

    if (save_result < 0) {
        xmlFreeDoc(doc);
        free(file_path);
        return SAVE_FAILED;
    }

    xmlFreeDoc(doc);
    free(file_path);

    return SUCCESS;
}


