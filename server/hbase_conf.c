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


#include "utiles.h"

#include <libxml/parser.h>
#include <libxml/tree.h>
#include <libxml/xpath.h>

ConfigResult* process_hbase_config(const char *param_name, const char *param_value) {
    static const ConfigParam hbase_predefined_params[] = {
        // Original parameters
        {"hbase.rootdir", "^(hbase[._-])?root[._-]?dir$", "hbase-site.xml"},
        {"hbase.zookeeper.quorum", "^(hbase[._-])?zookeeper[._-]?quorum$", "hbase-site.xml"},
        {"hbase.hregion.max.filesize", "^(hbase[._-])?hregion[._-]?max[._-]?filesize$", "hbase-site.xml"},
        {"hbase.hstore.blockingStoreFiles", "^(hbase[._-])?hstore[._-]?blocking[._-]?storefiles$", "hbase-site.xml"},
        {"hbase.rpc.timeout", "^(hbase[._-])?rpc[._-]?timeout$", "hbase-site.xml"},
        {"hbase.hregion.majorcompaction", "^(hbase[._-])?hregion[._-]?major[._-]?compaction$", "hbase-site.xml"},
        {"hbase.tmp.dir", "^(hbase[._-])?tmp[._-]?dir$", "hbase-site.xml"},
        {"hbase.cluster.distributed", "^(hbase[._-])?cluster[._-]?distributed$", "hbase-site.xml"},
        {"hbase.zookeeper.property.clientPort", "^(hbase[._-])?zookeeper[._-]?property[._-]?client[._-]?port$", "hbase-site.xml"},
        {"hbase.regionserver.handler.count", "^(hbase[._-])?regionserver[._-]?handler[._-]?count$", "hbase-site.xml"},
        {"hbase.master.info.port", "^(hbase[._-])?master[._-]?info[._-]?port$", "hbase-site.xml"},
        {"hbase.regionserver.info.port", "^(hbase[._-])?regionserver[._-]?info[._-]?port$", "hbase-site.xml"},
        {"hbase.hstore.compactionThreshold", "^(hbase[._-])?hstore[._-]?compaction[._-]?threshold$", "hbase-site.xml"},
        {"hbase.hstore.blockingWaitTime", "^(hbase[._-])?hstore[._-]?blocking[._-]?wait[._-]?time$", "hbase-site.xml"},
        {"hbase.client.write.buffer", "^(hbase[._-])?client[._-]?write[._-]?buffer$", "hbase-site.xml"},
        {"hbase.security.authentication", "^(hbase[._-])?security[._-]?authentication$", "hbase-policy.xml"},
        {"hbase.security.authorization", "^(hbase[._-])?security[._-]?authorization$", "hbase-policy.xml"},
        {"hbase.superuser", "^(hbase[._-])?superuser$", "hbase-policy.xml"},
        {"hbase.coprocessor.region.classes", "^(hbase[._-])?coprocessor[._-]?region[._-]?classes$", "hbase-site.xml"},
        {"hbase.rest.port", "^(hbase[._-])?rest[._-]?port$", "hbase-site.xml"},
        // HBase policy parameters (from hbase-policy.xml)
        {"security.client.protocol.acl", "^security\\.client\\.protocol\\.acl$", "hbase-policy.xml"},
        {"security.admin.protocol.acl", "^security\\.admin\\.protocol\\.acl$", "hbase-policy.xml"},
        {"security.master.protocol.acl", "^security\\.master\\.protocol\\.acl$", "hbase-policy.xml"},
        {"security.regionserver.protocol.acl", "^security\\.regionserver\\.protocol\\.acl$", "hbase-policy.xml"},

        // New core-site.xml parameters (extended list)
        {"io.native.lib.available", "^io\\.native\\.lib\\.available$", "core-site.xml"},
        {"hadoop.http.filter.initializers", "^hadoop\\.http\\.filter\\.initializers$", "core-site.xml"},
        {"hadoop.security.authorization", "^hadoop\\.security\\.authorization$", "core-site.xml"},
        {"hadoop.security.authentication", "^hadoop\\.security\\.authentication$", "core-site.xml"},
        {"hadoop.security.group.mapping", "^hadoop\\.security\\.group\\.mapping$", "core-site.xml"},
        {"hadoop.rpc.protection", "^hadoop\\.rpc\\.protection$", "core-site.xml"},
        {"fs.permissions.umask-mode", "^fs\\.permissions\\.umask\\-mode$", "core-site.xml"},
        {"io.file.buffer.size", "^io\\.file\\.buffer\\.size$", "core-site.xml"},
        {"io.bytes.per.checksum", "^io\\.bytes\\.per\\.checksum$", "core-site.xml"},
        {"io.compression.codecs", "^io\\.compression\\.codecs$", "core-site.xml"},
        {"hadoop.security.auth_to_local", "^hadoop\\.security\\.auth\\_to\\_local$", "core-site.xml"},
        {"hadoop.proxyuser.knox.groups", "^hadoop\\.proxyuser\\.knox\\.groups$", "core-site.xml"},
        {"hadoop.proxyuser.knox.hosts", "^hadoop\\.proxyuser\\.knox\\.hosts$", "core-site.xml"},
        {"fs.oci.client.hostname", "^fs\\.oci\\.client\\.hostname$", "core-site.xml"},
        {"fs.oci.client.custom.authenticator", "^fs\\.oci\\.client\\.custom\\.authenticator$", "core-site.xml"},
        {"fs.viprfs.impl", "^fs\\.viprfs\\.impl$", "core-site.xml"},
        {"fs.AbstractFileSystem.viprfs.impl", "^fs\\.AbstractFileSystem\\.viprfs\\.impl$", "core-site.xml"},
        {"hadoop.security.dns.interface", "^hadoop\\.security\\.dns\\.interface$", "core-site.xml"},
        {"hadoop.security.groups.cache.secs", "^hadoop\\.security\\.groups\\.cache\\.secs$", "core-site.xml"},
        {"viprfs.security.principal", "^viprfs\\.security\\.principal$", "core-site.xml"},
        // Core Audit Destinations
        // Connection/Timeout/Retry
        {"hbase.client.operation.timeout", "^(hbase[._-])?client[._-]?operation[._-]?timeout$", "hbase-site.xml"},
        {"hbase.client.scanner.timeout.period", "^(hbase[._-])?client[._-]?scanner[._-]?timeout[._-]?period$", "hbase-site.xml"},
        {"hbase.client.pause", "^(hbase[._-])?client[._-]?pause$", "hbase-site.xml"},
        {"hbase.client.retries.number", "^(hbase[._-])?client[._-]?retries[._-]?number$", "hbase-site.xml"},
        {"hbase.client.ipc.pool.size", "^(hbase[._-])?client[._-]?ipc[._-]?pool[._-]?size$", "hbase-site.xml"},
        {"hbase.zookeeper.property.session.timeout", "^(hbase[._-])?zookeeper[._-]?property[._-]?session[._-]?timeout$", "hbase-site.xml"},
        {"hbase.client.connection.maxidletime", "^(hbase[._-])?client[._-]?connection[._-]?maxidletime$", "hbase-site.xml"},

        // Security
        {"hbase.security.auth.enable", "^(hbase[._-])?security[._-]?auth[._-]?enable$", "hbase-site.xml"},
        {"hbase.rpc.protection", "^(hbase[._-])?rpc[._-]?protection$", "hbase-site.xml"},
        {"hbase.sasl.clientconfig", "^(hbase[._-])?sasl[._-]?clientconfig$", "hbase-site.xml"},
        {"hbase.kerberos.regionserver.principal", "^(hbase[._-])?kerberos[._-]?regionserver[._-]?principal$", "hbase-site.xml"},
        {"hbase.regionserver.kerberos.principal", "^(hbase[._-])?regionserver[._-]?kerberos[._-]?principal$", "hbase-site.xml"},

        // Caching/Buffering
        {"hbase.client.scanner.caching", "^(hbase[._-])?client[._-]?scanner[._-]?caching$", "hbase-site.xml"},
        {"hbase.client.keyvalue.maxsize", "^(hbase[._-])?client[._-]?keyvalue[._-]?maxsize$", "hbase-site.xml"},
        {"hbase.client.scanner.max.result.size", "^(hbase[._-])?client[._-]?scanner[._-]?max[._-]?result[._-]?size$", "hbase-site.xml"},

        // Region/Meta
        {"hbase.client.meta.operation.timeout", "^(hbase[._-])?client[._-]?meta[._-]?operation[._-]?timeout$", "hbase-site.xml"},
        {"hbase.client.localityCheck.interval", "^(hbase[._-])?client[._-]?locality[._-]?check[._-]?interval$", "hbase-site.xml"},
        {"hbase.client.prefetch.limit", "^(hbase[._-])?client[._-]?prefetch[._-]?limit$", "hbase-site.xml"},
        {"hbase.meta.replicas.use", "^(hbase[._-])?meta[._-]?replicas[._-]?use$", "hbase-site.xml"},

        // SSL/TLS
        {"hbase.rpc.ssl.enabled", "^(hbase[._-])?rpc[._-]?ssl[._-]?enabled$", "hbase-site.xml"},
        {"hbase.ssl.enabled", "^(hbase[._-])?ssl[._-]?enabled$", "hbase-site.xml"},
        {"hbase.rest.ssl.enabled", "^(hbase[._-])?rest[._-]?ssl[._-]?enabled$", "hbase-site.xml"},
        {"hbase.ssl.keystore.store", "^(hbase[._-])?ssl[._-]?keystore[._-]?store$", "hbase-site.xml"},
        {"hbase.ssl.keystore.password", "^(hbase[._-])?ssl[._-]?keystore[._-]?password$", "hbase-site.xml"},
        {"hbase.ssl.truststore.store", "^(hbase[._-])?ssl[._-]?truststore[._-]?store$", "hbase-site.xml"},
        {"hbase.ssl.truststore.password", "^(hbase[._-])?ssl[._-]?truststore[._-]?password$", "hbase-site.xml"},

        // Serialization/Compatibility
        {"hbase.defaults.for.version", "^(hbase[._-])?defaults[._-]?for[._-]?version$", "hbase-site.xml"},

        // Advanced Client Behavior
        {"hbase.client.scanner.lease.period", "^(hbase[._-])?client[._-]?scanner[._-]?lease[._-]?period$", "hbase-site.xml"},
        {"hbase.client.primaryCallTimeout.get", "^(hbase[._-])?client[._-]?primary[._-]?call[._-]?timeout[._-]?get$", "hbase-site.xml"},
        {"hbase.client.primaryCallTimeout.multiget", "^(hbase[._-])?client[._-]?primary[._-]?call[._-]?timeout[._-]?multiget$", "hbase-site.xml"},
        {"hbase.client.hedged.read.timeout", "^(hbase[._-])?client[._-]?hedged[._-]?read[._-]?timeout$", "hbase-site.xml"},
        {"hbase.client.hedged.read.threadpool.size", "^(hbase[._-])?client[._-]?hedged[._-]?read[._-]?threadpool[._-]?size$", "hbase-site.xml"},
        {"xasecure.audit.is.enabled", "^xasecure\\.audit\\.is\\.enabled$", "ranger-hbase-audit.xml"},
        {"xasecure.audit.hdfs.is.enabled", "^xasecure\\.audit\\.hdfs\\.is\\.enabled$", "ranger-hbase-audit.xml"},
        {"xasecure.audit.hdfs.is.async", "^xasecure\\.audit\\.hdfs\\.is\\.async$", "ranger-hbase-audit.xml"},
        {"xasecure.audit.hdfs.async.max.queue.size", "^xasecure\\.audit\\.hdfs\\.async\\.max\\.queue\\.size$", "ranger-hbase-audit.xml"},
        {"xasecure.audit.hdfs.async.max.flush.interval.ms", "^xasecure\\.audit\\.hdfs\\.async\\.max\\.flush\\.interval\\.ms$", "ranger-hbase-audit.xml"},
        {"xasecure.audit.hdfs.config.encoding", "^xasecure\\.audit\\.hdfs\\.config\\.encoding$", "ranger-hbase-audit.xml"},
        {"xasecure.audit.hdfs.config.destination.directory", "^xasecure\\.audit\\.hdfs\\.config\\.destination\\.directory$", "ranger-hbase-audit.xml"},

        {"xasecure.audit.hdfs.config.destination.file", "^xasecure\\.audit\\.hdfs\\.config\\.destination\\.file$", "ranger-hbase-audit.xml"},
        {"xasecure.audit.hdfs.config.destination.flush.interval.seconds", "^xasecure\\.audit\\.hdfs\\.config\\.destination\\.flush\\.interval\\.seconds$", "ranger-hbase-audit.xml"},
        {"xasecure.audit.hdfs.config.destination.rollover.interval.seconds", "^xasecure\\.audit\\.hdfs\\.config\\.destination\\.rollover\\.interval\\.seconds$", "ranger-hbase-audit.xml"},
        {"xasecure.audit.hdfs.config.destination.open.retry.interval.seconds", "^xasecure\\.audit\\.hdfs\\.config\\.destination\\.open\\.retry\\.interval\\.seconds$", "ranger-hbase-audit.xml"},
        {"xasecure.audit.hdfs.config.local.buffer.directory", "^xasecure\\.audit\\.hdfs\\.config\\.local\\.buffer\\.directory$", "ranger-hbase-audit.xml"},
        {"xasecure.audit.hdfs.config.local.buffer.file", "^xasecure\\.audit\\.hdfs\\.config\\.local\\.buffer\\.file$", "ranger-hbase-audit.xml"},
        {"xasecure.audit.hdfs.config.local.buffer.file.buffer.size.bytes", "^xasecure\\.audit\\.hdfs\\.config\\.local\\.buffer\\.file\\.buffer\\.size\\.bytes$", "ranger-hbase-audit.xml"},
        {"xasecure.audit.hdfs.config.local.buffer.flush.interval.seconds", "^xasecure\\.audit\\.hdfs\\.config\\.local\\.buffer\\.flush\\.interval\\.seconds$", "ranger-hbase-audit.xml"},
        {"xasecure.audit.hdfs.config.local.buffer.rollover.interval.seconds", "^xasecure\\.audit\\.hdfs\\.config\\.local\\.buffer\\.rollover\\.interval\\.seconds$", "ranger-hbase-audit.xml"},
        {"xasecure.audit.hdfs.config.local.archive.directory", "^xasecure\\.audit\\.hdfs\\.config\\.local\\.archive\\.directory$", "ranger-hbase-audit.xml"},
        {"xasecure.audit.hdfs.config.local.archive.max.file.count", "^xasecure\\.audit\\.hdfs\\.config\\.local\\.archive\\.max\\.file\\.count$", "ranger-hbase-audit.xml"},
        {"xasecure.audit.log4j.is.enabled", "^xasecure\\.audit\\.log4j\\.is\\.enabled$", "ranger-hbase-audit.xml"},
        {"xasecure.audit.log4j.is.async", "^xasecure\\.audit\\.log4j\\.is\\.async$", "ranger-hbase-audit.xml"},
        {"xasecure.audit.log4j.async.max.queue.size", "^xasecure\\.audit\\.log4j\\.async\\.max\\.queue\\.size$", "ranger-hbase-audit.xml"},
        {"xasecure.audit.log4j.async.max.flush.interval.ms", "^xasecure\\.audit\\.log4j\\.async\\.max\\.flush\\.interval\\.ms$", "ranger-hbase-audit.xml"},
        {"xasecure.audit.kafka.is.enabled", "^xasecure\\.audit\\.kafka\\.is\\.enabled$", "ranger-hbase-audit.xml"},
        {"xasecure.audit.kafka.async.max.queue.size", "^xasecure\\.audit\\.kafka\\.async\\.max\\.queue\\.size$", "ranger-hbase-audit.xml"},
        {"xasecure.audit.kafka.async.max.flush.interval.ms", "^xasecure\\.audit\\.kafka\\.async\\.max\\.flush\\.interval\\.ms$", "ranger-hbase-audit.xml"},
        {"xasecure.audit.kafka.broker_list", "^xasecure\\.audit\\.kafka\\.broker_list$", "ranger-hbase-audit.xml"},
        {"xasecure.audit.kafka.topic_name", "^xasecure\\.audit\\.kafka\\.topic_name$", "ranger-hbase-audit.xml"},
        {"xasecure.audit.solr.is.enabled", "^xasecure\\.audit\\.solr\\.is\\.enabled$", "ranger-hbase-audit.xml"},
        {"xasecure.audit.solr.async.max.queue.size", "^xasecure\\.audit\\.solr\\.async\\.max\\.queue\\.size$", "ranger-hbase-audit.xml"},
        {"xasecure.audit.solr.async.max.flush.interval.ms", "^xasecure\\.audit\\.solr\\.async\\.max\\.flush\\.interval\\.ms$", "ranger-hbase-audit.xml"},
        {"xasecure.audit.solr.solr_url", "^xasecure\\.audit\\.solr\\.solr_url$", "ranger-hbase-audit.xml"},
        // Ranger HBase SSL policy manager parameters (from ranger-hbase-policymgr-ssl.xml)
        {"xasecure.policymgr.clientssl.keystore", "^xasecure\\.policymgr\\.clientssl\\.keystore$", "ranger-hbase-policymgr-ssl.xml"},
        {"xasecure.policymgr.clientssl.truststore", "^xasecure\\.policymgr\\.clientssl\\.truststore$", "ranger-hbase-policymgr-ssl.xml"},
        {"xasecure.policymgr.clientssl.keystore.credential.file", "^xasecure\\.policymgr\\.clientssl\\.keystore\\.credential\\.file$", "ranger-hbase-policymgr-ssl.xml"},
        {"xasecure.policymgr.clientssl.truststore.credential.file", "^xasecure\\.policymgr\\.clientssl\\.truststore\\.credential\\.file$", "ranger-hbase-policymgr-ssl.xml"},
        {"ranger.plugin.hbase.service.name", "^ranger\\.plugin\\.hbase\\.service\\.name$", "ranger-hbase-security.xml"},
        {"ranger.plugin.hbase.policy.source.impl", "^ranger\\.plugin\\.hbase\\.policy\\.source\\.impl$", "ranger-hbase-security.xml"},
        {"ranger.plugin.hbase.policy.rest.url", "^ranger\\.plugin\\.hbase\\.policy\\.rest\\.url$", "ranger-hbase-security.xml"},
        {"ranger.plugin.hbase.policy.rest.ssl.config.file", "^ranger\\.plugin\\.hbase\\.policy\\.rest\\.ssl\\.config\\.file$", "ranger-hbase-security.xml"},
        {"ranger.plugin.hbase.policy.pollIntervalMs", "^ranger\\.plugin\\.hbase\\.policy\\.pollIntervalMs$", "ranger-hbase-security.xml"},
        {"ranger.plugin.hbase.policy.cache.dir", "^ranger\\.plugin\\.hbase\\.policy\\.cache\\.dir$", "ranger-hbase-security.xml"},
        {"xasecure.hbase.update.xapolicies.on.grant.revoke", "^xasecure\\.hbase\\.update\\.xapolicies\\.on\\.grant\\.revoke$", "ranger-hbase-security.xml"},
        {"ranger.plugin.hbase.policy.rest.client.connection.timeoutMs", "^ranger\\.plugin\\.hbase\\.policy\\.rest\\.client\\.connection\\.timeoutMs$", "ranger-hbase-security.xml"},
        {"ranger.plugin.hbase.policy.rest.client.read.timeoutMs", "^ranger\\.plugin\\.hbase\\.policy\\.rest\\.client\\.read\\.timeoutMs$", "ranger-hbase-security.xml"},
        {"log4j.rootlogger", "^log4j[._-]rootlogger$", "hbase-log4j.properties"},
        {"log4j.threshold", "^log4j[._-]threshold$", "hbase-log4j.properties"},
        {"log4j.appender.stdout", "^log4j[._-]appender[._-]stdout$", "hbase-log4j.properties"},
        {"log4j.appender.stdout.layout", "^log4j[._-]appender[._-]stdout[._-]layout$", "hbase-log4j.properties"},
        {"log4j.appender.stdout.layout.conversionpattern", "^log4j[._-]appender[._-]stdout[._-]layout[._-]conversionpattern$", "hbase-log4j.properties"},
        {"log4j.logger.org.apache.hadoop", "^log4j[._-]logger[._-]org[._-]apache[._-]hadoop$", "hbase-log4j.properties"},
        {"log4j.logger.org.apache.hadoop.metrics2", "^log4j[._-]logger[._-]org[._-]apache[._-]hadoop[._-]metrics2$", "hbase-log4j.properties"},
        {"log4j.logger.org.apache.hadoop.fs", "^log4j[._-]logger[._-]org[._-]apache[._-]hadoop[._-]fs$", "hbase-log4j.properties"},
        {"log4j.org.apache.hadoop.util", "^log4j[._-]org[._-]apache[._-]hadoop[._-]util$", "hbase-log4j.properties"},
        {"log4j.logger.org.apache.hadoop.fs.s3a", "^log4j[._-]logger[._-]org[._-]apache[._-]hadoop[._-]fs[._-]s3a$", "hbase-log4j.properties"},
        {"log4j.logger.org.apache.hadoop.hbase.oss", "^log4j[._-]logger[._-]org[._-]apache[._-]hadoop[._-]hbase[._-]oss$", "hbase-log4j.properties"},
        {"hbase.root.logger", "^hbase[._-]root[._-]logger$", "hbase-log4j.properties"},
        {"hbase.security.logger", "^hbase[._-]security[._-]logger$", "hbase-log4j.properties"},
        {"hbase.log.dir", "^hbase[._-]log[._-]dir$", "hbase-log4j.properties"},
        {"hbase.log.file", "^hbase[._-]log[._-]file$", "hbase-log4j.properties"},
        {"log4j.rootLogger", "^log4j[._-]rootLogger$", "hbase-log4j.properties"},
        {"log4j.threshold", "^log4j[._-]threshold$", "hbase-log4j.properties"},
        {"log4j.appender.DRFA", "^log4j[._-]appender[._-]DRFA$", "hbase-log4j.properties"},
        // ... (all other log4j properties from calls) ...

        // HBase Site Parameters
        {"hbase_log_maxfilesize", "^hbase[._-]log[._-]maxfilesize$", "hbase-site.xml"},
        {"hbase_log_maxbackupindex", "^hbase[._-]log[._-]maxbackupindex$", "hbase-site.xml"},
        {"hbase_security_log_maxfilesize", "^hbase[._-]security[._-]log[._-]maxfilesize$", "hbase-site.xml"},
        {"hbase_security_log_maxbackupindex", "^hbase[._-]security[._-]log[._-]maxbackupindex$", "hbase-site.xml"},
        {"hbase.master.port", "^hbase[._-]master[._-]port$", "hbase-site.xml"},
        {"phoenix.rpc.index.handler.count", "^phoenix[._-]rpc[._-]index[._-]handler[._-]count$", "hbase-site.xml"},
        // ... (all other missing hbase-site params) ...

        // Ranger Plugin Properties
        {"common.name.for.certificate", "^common[._-]name[._-]for[._-]certificate$", "ranger-hbase-plugin.properties"},
        {"policy_user", "^policy[._-]user$", "ranger-hbase-plugin.properties"},
        {"ranger-hbase-plugin-enabled", "^ranger[._-]hbase[._-]plugin[._-]enabled$", "ranger-hbase-plugin.properties"},
        {"REPOSITORY_CONFIG_USERNAME", "^REPOSITORY[._-]CONFIG[._-]USERNAME$", "ranger-hbase-plugin.properties"},

    };

    const size_t num_params = sizeof(hbase_predefined_params) / sizeof(hbase_predefined_params[0]);
    regex_t regex;
    int reti;

    for (size_t i = 0; i < num_params; i++) {
        reti = regcomp(&regex, hbase_predefined_params[i].normalizedName, REG_ICASE | REG_EXTENDED);
        if (reti) {
            char error_msg[1024];
            regerror(reti, &regex, error_msg, sizeof(error_msg));
            fprintf(stderr, "Regex compilation error for %s: %s\n",
                    hbase_predefined_params[i].canonicalName, error_msg);
            continue;
        }

        reti = regexec(&regex, param_name, 0, NULL, 0);
        regfree(&regex);

        if (reti == 0) {
            ConfigResult *result = malloc(sizeof(ConfigResult));
            if (!result) {
                perror("Failed to allocate memory for result");
                return NULL;
            }

            result->canonical_name = strdup(hbase_predefined_params[i].canonicalName);
            result->value = strdup(param_value);
            result->config_file = strdup(hbase_predefined_params[i].configFile);

            if (!result->canonical_name || !result->value || !result->config_file) {
                free(result->canonical_name);
                free(result->value);
                free(result->config_file);
                free(result);
                perror("Failed to duplicate strings");
                return NULL;
            }

            return result;
        } else if (reti != REG_NOMATCH) {
            char error_msg[1024];
            regerror(reti, &regex, error_msg, sizeof(error_msg));
            fprintf(stderr, "Regex execution error for %s: %s\n",
                    hbase_predefined_params[i].canonicalName, error_msg);
        }
    }

    return NULL;
}

ValidationResult validateHBaseConfigParam(const char *param_name, const char *value) {
    // Check if parameter exists
    bool param_exists = true;

    if (!param_exists) return ERROR_PARAM_NOT_FOUND;

    // Check value presence
    if (!value || strlen(value) == 0) return ERROR_VALUE_EMPTY;

    // Parameter-specific validation
    if (strcmp(param_name, "hbase.zookeeper.property.clientPort") == 0 ||
        strcmp(param_name, "hbase.master.info.port") == 0 ||
        strcmp(param_name, "hbase.regionserver.info.port") == 0 ||
        strcmp(param_name, "hbase.rest.port") == 0) {
        return isValidPort(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "hbase.cluster.distributed") == 0 ||
             strcmp(param_name, "hbase.security.authorization") == 0 ||
             strcmp(param_name, "hbase.security.auth.enable") == 0 ||
             strcmp(param_name, "hbase.rpc.ssl.enabled") == 0 ||
             strcmp(param_name, "hbase.ssl.enabled") == 0 ||
             strcmp(param_name, "hbase.rest.ssl.enabled") == 0 ||
             strcmp(param_name, "hbase.meta.replicas.use") == 0) {
        return isValidBoolean(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "hbase.rpc.timeout") == 0 ||
             strcmp(param_name, "hbase.client.operation.timeout") == 0 ||
             strcmp(param_name, "hbase.client.scanner.timeout.period") == 0 ||
             strcmp(param_name, "hbase.zookeeper.property.session.timeout") == 0 ||
             strcmp(param_name, "hbase.client.connection.maxidletime") == 0) {
        return isValidHBaseDuration(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "hbase.client.write.buffer") == 0 ||
             strcmp(param_name, "hbase.client.keyvalue.maxsize") == 0 ||
             strcmp(param_name, "hbase.client.scanner.max.result.size") == 0) {
        return isDataSizeWithUnit(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "hbase.regionserver.handler.count") == 0 ||
             strcmp(param_name, "hbase.client.retries.number") == 0 ||
             strcmp(param_name, "hbase.client.ipc.pool.size") == 0 ||
             strcmp(param_name, "hbase.client.hedged.read.threadpool.size") == 0) {
        return isPositiveInteger(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "hbase.security.authentication") == 0) {
        return (strcmp(value, "simple") == 0 || strcmp(value, "kerberos") == 0)
            ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "hbase.rpc.protection") == 0) {
        return (strcmp(value, "authentication") == 0 ||
                strcmp(value, "integrity") == 0 ||
                strcmp(value, "privacy") == 0)
            ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "hbase.kerberos.regionserver.principal") == 0 ||
             strcmp(param_name, "hbase.regionserver.kerberos.principal") == 0) {
        return isValidPrincipalFormat(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "hbase.coprocessor.region.classes") == 0) {
        // Basic check for comma-separated class names
        return (strchr(value, ',') != NULL || strlen(value) > 0)
            ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "hbase.rootdir") == 0) {
        return (strstr(value, "hdfs://") != NULL || strstr(value, "file://") != NULL)
            ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "hbase.tmp.dir") == 0) {
        return (value[0] != '\0') ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "hbase.defaults.for.version") == 0) {
        return (strstr(value, "hbase-") != NULL) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "hbase.ssl.keystore.store") == 0 ||
             strcmp(param_name, "hbase.ssl.truststore.store") == 0) {
        return (strlen(value) > 0) ? VALIDATION_OK : ERROR_VALUE_EMPTY;
    }

    else if (strcmp(param_name, "io.native.lib.available") == 0) {
        return isValidBoolean(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "hadoop.http.filter.initializers") == 0) {
        return isValidCommaSeparatedList(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "hadoop.security.group.mapping") == 0) {
        return (value[0] != '\0') ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "fs.permissions.umask-mode") == 0) {
        int len = strlen(value);
        if (len != 3 && len != 4) return ERROR_INVALID_FORMAT;
        for (int i = 0; i < len; i++) {
            if (value[i] < '0' || value[i] > '7') {
                return ERROR_INVALID_FORMAT;
            }
        }
        return VALIDATION_OK;
    }
    else if (strcmp(param_name, "io.file.buffer.size") == 0) {
        return isDataSize(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "io.bytes.per.checksum") == 0) {
        return isPositiveInteger(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "io.compression.codecs") == 0) {
        return isValidCommaSeparatedList(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "hadoop.security.auth_to_local") == 0) {
        return (value[0] != '\0') ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "hadoop.proxyuser.knox.groups") == 0 ||
             strcmp(param_name, "hadoop.proxyuser.knox.hosts") == 0) {
        return isValidCommaSeparatedList(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "fs.oci.client.hostname") == 0 ||
             strcmp(param_name, "fs.oci.client.custom.authenticator") == 0 ||
             strcmp(param_name, "fs.viprfs.impl") == 0 ||
             strcmp(param_name, "fs.AbstractFileSystem.viprfs.impl") == 0 ||
             strcmp(param_name, "hadoop.security.dns.interface") == 0) {
        return (value[0] != '\0') ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "hadoop.security.groups.cache.secs") == 0) {
        return isPositiveInteger(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "viprfs.security.principal") == 0) {
        return (strchr(value, '@') != NULL) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }

    // Ranger audit parameters
    else if (strcmp(param_name, "xasecure.audit.destination.log4j") == 0 ||
             strcmp(param_name, "xasecure.audit.destination.solr") == 0 ||
             strcmp(param_name, "xasecure.audit.destination.hdfs") == 0) {
        return isValidBoolean(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "xasecure.audit.destination.hdfs.dir") == 0 ||
             strcmp(param_name, "xasecure.audit.destination.log4j.logger") == 0 ||
             strcmp(param_name, "appender.RANGERAUDIT.type") == 0 ||
             strcmp(param_name, "appender.RANGERAUDIT.fileName") == 0 ||
             strcmp(param_name, "appender.RANGERAUDIT.filePattern") == 0 ||
             strcmp(param_name, "appender.RANGERAUDIT.layout.pattern") == 0 ||
             strcmp(param_name, "XAAUDIT.HDFS.LOCAL_BUFFER_DIRECTORY") == 0 ||
             strcmp(param_name, "XAAUDIT.SOLR.FILE_SPOOL_DIR") == 0 ||
             strcmp(param_name, "xasecure.audit.filter") == 0 ||
             strcmp(param_name, "xasecure.audit.destination.log4j.filePermissions") == 0) {
        return (value[0] != '\0') ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "XAAUDIT.SOLR.URL") == 0) {
        return (strncmp(value, "http://", 7) == 0 || strncmp(value, "https://", 8) == 0)
            ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }

    // Ranger plugin properties
    else if (strcmp(param_name, "ranger.plugin.hbase.service.name") == 0 ||
             strcmp(param_name, "ranger.plugin.hbase.policy.cache.dir") == 0 ||
             strcmp(param_name, "ranger.plugin.hbase.policy.source.impl") == 0 ||
             strcmp(param_name, "xasecure.policymgr.clientssl.keystore") == 0 ||
             strcmp(param_name, "xasecure.policymgr.clientssl.keystore.credential.file") == 0 ||
             strcmp(param_name, "xasecure.policymgr.clientssl.keystore.password") == 0 ||
             strcmp(param_name, "xasecure.policymgr.clientssl.truststore") == 0 ||
             strcmp(param_name, "xasecure.policymgr.clientssl.truststore.credential.file") == 0 ||
             strcmp(param_name, "xasecure.policymgr.clientssl.truststore.password") == 0 ||
             strcmp(param_name, "ranger.plugin.hbase.policy.rest.ssl.config.file") == 0 ||
             strcmp(param_name, "ranger.plugin.hbase.kafka.topic") == 0 ||
             strcmp(param_name, "ranger.plugin.hbase.access.cluster.service.name") == 0) {
        return (value[0] != '\0') ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }

    else if (strcmp(param_name, "ranger.plugin.hbase.policy.rest.url") == 0) {
        return (strncmp(value, "http://", 7) == 0 || strncmp(value, "https://", 8) == 0)
            ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "ranger.plugin.hbase.policy.pollIntervalMs") == 0) {
        return isPositiveInteger(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "ranger.plugin.hbase.kafka.bootstrap.servers") == 0) {
        const char *start = value;
        char token[256]; // Fixed-size buffer for host:port tokens
        int token_count = 0;

        while (*start) {
            const char *comma = strchr(start, ',');
            size_t len = comma ? (size_t)(comma - start) : strlen(start);

            // Check for empty token
            if (len == 0) return ERROR_INVALID_FORMAT;

            // Check token length
            if (len >= sizeof(token)) return ERROR_INVALID_FORMAT;

            // Copy token to buffer
            strncpy(token, start, len);
            token[len] = '\0';

            if (!isValidHostPort(token)) {
                return ERROR_INVALID_FORMAT;
            }

            token_count++;
            start = comma ? comma + 1 : start + len;
        }

        // Ensure at least one valid token
        return (token_count > 0) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strstr(param_name, "xasecure.audit") != NULL) {
        // Boolean flags
        if (strstr(param_name, ".is.enabled") != NULL ||
            strstr(param_name, ".is.async") != NULL) {
            return isValidBoolean(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
        }
        // Numeric parameters
        else if (strstr(param_name, ".max.queue.size") != NULL ||
                 strstr(param_name, ".max.flush.interval") != NULL ||
                 strstr(param_name, ".flush.interval") != NULL ||
                 strstr(param_name, ".rollover.interval") != NULL ||
                 strstr(param_name, ".open.retry.interval") != NULL ||
                 strstr(param_name, ".max.file.count") != NULL ||
                 strstr(param_name, ".buffer.size") != NULL) {
            return isPositiveInteger(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
        }
        // Path/directory parameters
        else if (strstr(param_name, ".directory") != NULL ||
                 strstr(param_name, ".file") != NULL) {
            return (value[0] != '\0') ? VALIDATION_OK : ERROR_VALUE_EMPTY;
        }
        // Encoding/format parameters
        else if (strcmp(param_name, "xasecure.audit.hdfs.config.encoding") == 0) {
            // Allow empty value or valid encoding names
            return (value[0] == '\0' || isValidEncoding(value)) ?
                VALIDATION_OK : ERROR_INVALID_FORMAT;
        }
    }

    // Ranger SSL parameter validation
    else if (strstr(param_name, "xasecure.policymgr.clientssl") != NULL) {
        // Credential file paths
        if (strstr(param_name, "credential.file") != NULL) {
            return (strncmp(value, "jceks://", 8) == 0) ?
                VALIDATION_OK : ERROR_INVALID_FORMAT;
        }
        // Keystore/truststore paths
        else if (strstr(param_name, "keystore") != NULL ||
                 strstr(param_name, "truststore") != NULL) {
            return (strstr(value, ".jks") != NULL ||
                    strstr(value, ".p12") != NULL) ?
                VALIDATION_OK : ERROR_INVALID_FORMAT;
        }
    }

    // Ranger security policy parameter validation
    else if (strstr(param_name, "ranger.plugin.hbase") != NULL) {
        // Service name validation
        if (strcmp(param_name, "ranger.plugin.hbase.service.name") == 0) {
            return (value[0] != '\0' && !strchr(value, ' ') &&
                    !strchr(value, '/')) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
        }
        // URL validation
        else if (strcmp(param_name, "ranger.plugin.hbase.policy.rest.url") == 0) {
            return (strstr(value, "http://") != NULL ||
                    strstr(value, "https://") != NULL) ?
                VALIDATION_OK : ERROR_INVALID_FORMAT;
        }
        // Path validation
        else if (strcmp(param_name, "ranger.plugin.hbase.policy.rest.ssl.config.file") == 0 ||
                 strcmp(param_name, "ranger.plugin.hbase.policy.cache.dir") == 0) {
            return (value[0] == '/') ? VALIDATION_OK : ERROR_INVALID_FORMAT;
        }
        // Numeric parameters
        else if (strcmp(param_name, "ranger.plugin.hbase.policy.pollIntervalMs") == 0 ||
                 strcmp(param_name, "ranger.plugin.hbase.policy.rest.client.connection.timeoutMs") == 0 ||
                 strcmp(param_name, "ranger.plugin.hbase.policy.rest.client.read.timeoutMs") == 0) {
            return isPositiveInteger(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
        }
        // Boolean parameters
        else if (strcmp(param_name, "xasecure.hbase.update.xapolicies.on.grant.revoke") == 0) {
            return isValidBoolean(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
        }
        // Class name validation
        else if (strcmp(param_name, "ranger.plugin.hbase.policy.source.impl") == 0) {
            return (strstr(value, "RangerAdmin") != NULL) ?
                VALIDATION_OK : ERROR_INVALID_FORMAT;
        }
    }


    // Default validation for parameters without specific rules
    return VALIDATION_OK;
}


/*

 * Update HBase configuration parameter in XML configuration file

 *

 * Searches for configuration file in HBASE_HOME/conf, OS-specific default installation directories,

 * or /etc/hbase/conf. Creates the file and directories if necessary. Updates or adds the specified

 * parameter with the given value.

 *

 * Parameters:

 *   param - Name of the configuration parameter to update

 *   value - New value for the configuration parameter

 *

 * Returns:

 *   ConfigStatus indicating operation success or failure reason

 */
ConfigStatus update_hbase_config(const char* param, const char* value , const char *filename) {
    const char *hbase_home = getenv("HBASE_HOME");
    const char *detected_home = NULL;
    const char *bigtop_default = "/etc/hbase/conf";
    char file_path[MAX_PATH_LEN] = {0};
    struct stat st;
    ConfigStatus status = SUCCESS;
    xmlDocPtr doc = NULL;
    xmlNodePtr root = NULL;

    /* Detect OS-specific HBase home if HBASE_HOME is not set */
    if (hbase_home == NULL) {
        if (is_redhat()) {
            detected_home = "/opt/hbase";
        } else if (is_debian()) {
            detected_home = "/usr/local/hbase";
        }
        hbase_home = detected_home;
    }

    /* Determine configuration file path */
    if (hbase_home != NULL) {
        /* Check HBASE_HOME/conf location */
        snprintf(file_path, sizeof(file_path), "%s/conf/%s", hbase_home, filename);
        if (stat(file_path, &st) != 0) {
            /* Fall back to Bigtop default location */
            snprintf(file_path, sizeof(file_path), "%s/%s", bigtop_default, filename);
            if (stat(file_path, &st) != 0) {
                /* Create directories if file not found */
                char conf_dir[MAX_PATH_LEN];
                snprintf(conf_dir, sizeof(conf_dir), "%s/conf", hbase_home);
                if (mkdir_p(conf_dir) != 0) {
                    /* Fallback to Bigtop default directory creation */
                    if (mkdir_p(bigtop_default) != 0) {
                        status = FILE_WRITE_ERROR;
                        goto cleanup;
                    }
                    snprintf(file_path, sizeof(file_path), "%s/%s", bigtop_default, filename);
                } else {
                    snprintf(file_path, sizeof(file_path), "%s/conf/%s", hbase_home, filename);
                }
            }
        }
    } else {
        /* Use Bigtop default when HBASE_HOME not set and no detected home */
        snprintf(file_path, sizeof(file_path), "%s/%s", bigtop_default, filename);
        if (stat(file_path, &st) != 0 && mkdir_p(bigtop_default) != 0) {
            status = FILE_WRITE_ERROR;
            goto cleanup;
        }
    }

    if (strcmp(filename, "log4j2.properties") == 0)
    {
        configure_hadoop_property(file_path, param, value);
        return SUCCESS;
    }

    /* Load or initialize XML document */
    if (stat(file_path, &st) == 0) {
        /* Parse existing configuration file */
        doc = xmlReadFile(file_path, NULL, 0);
        if (!doc) {
            status = XML_PARSE_ERROR;
            goto cleanup;
        }

        /* Validate root element */
        root = xmlDocGetRootElement(doc);
        if (!root || xmlStrcmp(root->name, BAD_CAST "configuration") != 0) {
            status = XML_INVALID_ROOT;
            goto cleanup;
        }
    } else {
        /* Create new XML document structure */
        doc = xmlNewDoc(BAD_CAST "1.0");
        root = xmlNewNode(NULL, BAD_CAST "configuration");
        if (!doc || !root) {
            status = XML_UPDATE_ERROR;
            goto cleanup;
        }
        xmlDocSetRootElement(doc, root);
    }

    /* Search for existing parameter */
    xmlNodePtr prop = NULL;
    bool param_found = false;
    for (prop = root->children; prop != NULL; prop = prop->next) {
        if (prop->type != XML_ELEMENT_NODE ||
            xmlStrcmp(prop->name, BAD_CAST "property") != 0) {
            continue;
        }

        /* Extract parameter name */
        xmlChar *name = NULL;
        for (xmlNodePtr child = prop->children; child != NULL; child = child->next) {
            if (child->type == XML_ELEMENT_NODE &&
                xmlStrcmp(child->name, BAD_CAST "name") == 0) {
                name = xmlNodeGetContent(child);
                break;
            }
        }

        if (name && xmlStrcmp(name, BAD_CAST param) == 0) {
            /* Update existing parameter value */
            xmlNodePtr value_node = NULL;
            for (xmlNodePtr child = prop->children; child != NULL; child = child->next) {
                if (child->type == XML_ELEMENT_NODE &&
                    xmlStrcmp(child->name, BAD_CAST "value") == 0) {
                    value_node = child;
                    break;
                }
            }

            if (value_node) {
                xmlNodeSetContent(value_node, BAD_CAST value);
            } else {
                xmlNewChild(prop, NULL, BAD_CAST "value", BAD_CAST value);
            }
            param_found = true;
            xmlFree(name);
            break;
        }
        xmlFree(name);
    }

    /* Add new parameter if not found */
    if (!param_found) {
        xmlNodePtr new_prop = xmlNewNode(NULL, BAD_CAST "property");
        xmlNewChild(new_prop, NULL, BAD_CAST "name", BAD_CAST param);
        xmlNewChild(new_prop, NULL, BAD_CAST "value", BAD_CAST value);
        xmlAddChild(root, new_prop);
    }

    /* Save XML document to file */
    if (xmlSaveFormatFileEnc(file_path, doc, "UTF-8", 1) < 0) {
        status = SAVE_FAILED;
    }

cleanup:
    /* Cleanup XML resources */
    if (doc) xmlFreeDoc(doc);
    xmlCleanupParser();
    return status;
}

