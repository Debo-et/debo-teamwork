#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libxml/parser.h>
#include <libxml/tree.h>
#include <libxml/xpath.h>
#include <unistd.h>
#include <limits.h>
#include <regex.h>
#include <sys/stat.h>
#include <errno.h>
#include <sys/stat.h>
#include <ctype.h>
#include <stdbool.h>



#include "utiles.h"

#define HBASE_CONFIG_FILE "hbase-site.xml"
#define HUE_CONFIG_FILE "hue.ini"
#define MAX_PATH_LEN 1024
#define SPARK_HOME_ENV "SPARK_HOME"
#define BIGTOP_DEFAULT_DIR "/usr/local/spark/conf"
#define SPARK_CONFIG_FILE_NAME "spark-defaults.conf"
#define MAX_LINE_LENGTH 8096
#define ZEPPELIN_CONFIG_FILE_NAME "zeppelin-site.xml"

#define SOLR_HOME_ENV "SOLR_HOME"
#define BIGTOP_DEFAULT_PATH "/usr/local/solr/conf/solrconfig.xml"
#define SOLR_CONFIG_FILE_NAME "solrconfig.xml"
#define PHOENIX_CONFIG_FILE "phoenix-site.xml"

static const char* get_filename_zoo() {
    return "zoo.cfg";
}

/* Helper functions to detect OS */
static int is_redhat() {
    struct stat st;
    return (stat("/etc/redhat-release", &st) == 0);
}

static int is_debian() {
    struct stat st;
    return (stat("/etc/debian_version", &st) == 0);
}

static const char* get_filename_oozie() {
    return "oozie-site.xml";
}

static void trim_whitespace(char *str) {
    char *end;

    // Trim leading space
    while (*str == ' ' || *str == '\t') {
        str++;
    }

    if (*str == 0) {
        return;
    }

    // Trim trailing space
    end = str + strlen(str) - 1;
    while (end > str && (*end == ' ' || *end == '\t' || *end == '\n' || *end == '\r')) {
        end--;
    }
    *(end + 1) = 0;
}
char *trim(char *str) {
    char *end;

    // Trim leading spaces
    while (isspace((unsigned char)*str)) str++;

    // Trim trailing spaces
    if (*str == 0) return str; // All spaces?

    end = str + strlen(str) - 1;
    while (end > str && isspace((unsigned char)*end)) end--;

    // Null-terminate the string
    *(end + 1) = '\0';

    return str;
  }

#define MAX_COMPONENTS 10


typedef struct {
    const char *name;
    const char *regex_str;
    const char *config_file;
} HdfsConfigParam;


static const HdfsConfigParam hdfs_configs[] = {
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
};
static bool isPositiveInteger(const char *value) {
    char *end;
    long num = strtol(value, &end, 10);
    return *end == '\0' && num > 0;
}

static bool isValidPort(const char *value) {
    char *end;
    long port = strtol(value, &end, 10);
    return *end == '\0' && port > 0 && port <= 65535;
}

static bool isValidBoolean(const char *value) {
    return strcmp(value, "true") == 0 || strcmp(value, "false") == 0;
}

static bool isValidHostPort(const char *value) {
    char *colon = strchr(value, ':');
    if (!colon) return false;
    return isValidPort(colon + 1);
}

static bool isDataSize(const char *value) {
    char *end;
    strtol(value, &end, 10);
    if (*end == '\0') return true;
    
    if (end == value) return false;
    return tolower(*end) == 'k' || tolower(*end) == 'm' || 
           tolower(*end) == 'g' || tolower(*end) == 't';
}

static bool isValidDuration(const char *value) {
    char *end;
    long num = strtol(value, &end, 10);
    if (num <= 0) return false;
    if (*end == '\0') return true; // Assume seconds if no unit
    if (strlen(end) != 1) return false;
    char unit = tolower(*end);
    return (unit == 's' || unit == 'm' || unit == 'h' || unit == 'd');
}

static bool isValidCommaSeparatedList(const char *value) {
    if (strlen(value) == 0) return false;
    char *copy = strdup(value);
    char *token = strtok(copy, ",");
    while (token != NULL) {
        if (strlen(token) == 0) {
            free(copy);
            return false;
        }
        token = strtok(NULL, ",");
    }
    free(copy);
    return true;
}

static bool isNonNegativeInteger(const char *value) {
    char *end;
    long num = strtol(value, &end, 10);
    return *end == '\0' && num >= 0;
}

ValidationResult validateHdfsConfigParam(const char *param_name, const char *value) {
    // Check if parameter exists
    bool param_exists = false;
    for (size_t i = 0; i < sizeof(hdfs_configs)/sizeof(hdfs_configs[0]); i++) {
        if (strcmp(param_name, hdfs_configs[i].name) == 0) {
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
        ret = regcomp(&regex, hdfs_configs[i].regex_str, REG_EXTENDED);
        if (ret != 0) {
            char error_msg[100];
            regerror(ret, &regex, error_msg, sizeof(error_msg));
            fprintf(stderr, "Regex compilation failed for %s: %s\n", hdfs_configs[i].name, error_msg);
            continue;
        }

        ret = regexec(&regex, param, 0, NULL, 0);
        regfree(&regex);

        if (ret == 0) {
            ConfigResult *result = malloc(sizeof(ConfigResult));
            if (!result) return NULL;
            
            result->canonical_name = strdup(hdfs_configs[i].name);
            result->config_file = strdup(hdfs_configs[i].config_file);
            
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
            fprintf(stderr, "Regex match error for %s: %s\n", hdfs_configs[i].name, error_msg);
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

    // Save the XML document
    if (xmlSaveFormatFileEnc(file_path, doc, "UTF-8", 1) < 0) {
        xmlFreeDoc(doc);
        free(file_path);
        return SAVE_FAILED;
    }

    xmlFreeDoc(doc);
    free(file_path);

    return SUCCESS;
}


////////////////////////////////////////////////yarn-site//////////////////////////////////////

ConfigFileType determine_yarn_config_file_type(const char* param) {
    size_t len = strlen(param);
    if (len >= 5 && strncmp(param, "yarn.", 5) == 0) {
        return YARN_SITE;
    } else if (len >= 4 && strncmp(param, "dfs.", 4) == 0) {
        return HDFS_SITE;
    } else {
        return CORE_SITE;
    }
}

ConfigStatus modify_yarn_config(const char *param, const char *value, ConfigFileType file_type) {
    const char *filename = NULL;
    switch (file_type) {
        case YARN_SITE:
            filename = "yarn-site.xml";
            break;
        case CORE_SITE:
            filename = "core-site.xml";
            break;
        default:
            return INVALID_FILE_TYPE;
    }

    const char *hadoop_home = getenv("HADOOP_HOME");
    char filepath[1024];
    int found = 0;

    // Check HADOOP_HOME path
    if (hadoop_home != NULL) {
        snprintf(filepath, sizeof(filepath), "%s/etc/hadoop/%s", hadoop_home, filename);
        if (access(filepath, F_OK) == 0) {
            found = 1;
        }
    }

    // Check Bigtop default paths if not found in HADOOP_HOME
    if (!found) {
        snprintf(filepath, sizeof(filepath), "/etc/hadoop/conf/%s", filename);
        if (access(filepath, F_OK) == 0) {
            found = 1;
        } else {
            snprintf(filepath, sizeof(filepath), "/usr/local/hadoop/%s", filename);
            if (access(filepath, F_OK) == 0) {
                found = 1;
            } else {
                return FILE_NOT_FOUND;
            }
        }
    }

    // Parse the XML document
    xmlDoc *doc = xmlReadFile(filepath, NULL, 0);
    if (doc == NULL) {
        return XML_PARSE_ERROR;
    }

    xmlNode *root = xmlDocGetRootElement(doc);
    if (root == NULL || xmlStrcmp(root->name, BAD_CAST "configuration") != 0) {
        xmlFreeDoc(doc);
        return INVALID_FILE_TYPE;
    }

    xmlNode *node;
    int property_found = 0;

    for (node = root->children; node != NULL; node = node->next) {
        if (node->type == XML_ELEMENT_NODE && xmlStrcmp(node->name, BAD_CAST "property") == 0) {
            xmlChar *name = NULL;
            xmlNode *value_node = NULL;

            // Extract the 'name' element content
            for (xmlNode *child = node->children; child != NULL; child = child->next) {
                if (child->type == XML_ELEMENT_NODE && xmlStrcmp(child->name, BAD_CAST "name") == 0) {
                    name = xmlNodeGetContent(child);
                    break;
                }
            }

            if (name != NULL && xmlStrcmp(name, BAD_CAST param) == 0) {
                // Locate or create the 'value' element
                for (xmlNode *child = node->children; child != NULL; child = child->next) {
                    if (child->type == XML_ELEMENT_NODE && xmlStrcmp(child->name, BAD_CAST "value") == 0) {
                        value_node = child;
                        break;
                    }
                }

                if (value_node != NULL) {
                    xmlNodeSetContent(value_node, BAD_CAST value);
                } else {
                    xmlNewTextChild(node, NULL, BAD_CAST "value", BAD_CAST value);
                }

                property_found = 1;
                xmlFree(name);
                break;
            }
            xmlFree(name);
        }
    }

    // Add new property if not found
    if (!property_found) {
        xmlNode *new_prop = xmlNewNode(NULL, BAD_CAST "property");
        xmlNewTextChild(new_prop, NULL, BAD_CAST "name", BAD_CAST param);
        xmlNewTextChild(new_prop, NULL, BAD_CAST "value", BAD_CAST value);
        xmlAddChild(root, new_prop);
    }

    // Save the modified XML document
    int save_result = xmlSaveFormatFileEnc(filepath, doc, "UTF-8", 1);
    xmlFreeDoc(doc);

    if (save_result == -1) {
        return SAVE_FAILED;
    }

    return SUCCESS;
}



//////////////////////hbase//////////////////////////////
typedef struct {
    const char *canonical_name;
    const char *regex_pattern;
    const char *config_file;
} HBaseConfigParam;

ConfigResult* process_hbase_config(const char *param_name, const char *param_value) {
    static const HBaseConfigParam hbase_predefined_params[] = {
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

        // Extended parameters
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
        {"hbase.client.hedged.read.threadpool.size", "^(hbase[._-])?client[._-]?hedged[._-]?read[._-]?threadpool[._-]?size$", "hbase-site.xml"}
    };

    const size_t num_params = sizeof(hbase_predefined_params) / sizeof(hbase_predefined_params[0]);
    regex_t regex;
    int reti;

    for (size_t i = 0; i < num_params; i++) {
        reti = regcomp(&regex, hbase_predefined_params[i].regex_pattern, REG_ICASE | REG_EXTENDED);
        if (reti) {
            char error_msg[1024];
            regerror(reti, &regex, error_msg, sizeof(error_msg));
            fprintf(stderr, "Regex compilation error for %s: %s\n", 
                    hbase_predefined_params[i].canonical_name, error_msg);
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

            result->canonical_name = strdup(hbase_predefined_params[i].canonical_name);
            result->value = strdup(param_value);
            result->config_file = strdup(hbase_predefined_params[i].config_file);

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
                    hbase_predefined_params[i].canonical_name, error_msg);
        }
    }

    return NULL;
}

// HBase-specific helper functions
static bool isValidHBaseDuration(const char *value) {
    char *end;
    long num = strtol(value, &end, 10);
    if (num <= 0) return false;
    if (*end == '\0') return true; // Assume milliseconds
    if (strlen(end) > 2) return false;
    
    // Allow ms/s/min/h/d suffixes
    return strcmp(end, "ms") == 0 || strcmp(end, "s") == 0 ||
           strcmp(end, "min") == 0 || strcmp(end, "h") == 0 ||
           strcmp(end, "d") == 0;
}

static bool isValidPrincipalFormat(const char *value) {
    return strchr(value, '/') != NULL && strchr(value, '@') != NULL;
}

static bool isDataSizeWithUnit(const char *value) {
    char *end;
    strtol(value, &end, 10);
    if (*end == '\0') return true; // No unit = bytes
    if (end == value) return false;
    return tolower(*end) == 'k' || tolower(*end) == 'm' || 
           tolower(*end) == 'g' || tolower(*end) == 't';
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

    // Default validation for parameters without specific rules
    return VALIDATION_OK;
}

static int mkdir_p(const char *path) {
    char tmp[MAX_PATH_LEN];
    char *p = NULL;
    size_t len;

    strncpy(tmp, path, sizeof(tmp));
    tmp[sizeof(tmp) - 1] = '\0';
    len = strlen(tmp);
    if (len == 0) return -1;
    if (tmp[len - 1] == '/') {
        tmp[len - 1] = '\0';
    }

    for (p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            if (mkdir(tmp, S_IRWXU) != 0 && errno != EEXIST) {
                return -1;
            }
            *p = '/';
        }
    }
    if (mkdir(tmp, S_IRWXU) != 0 && errno != EEXIST) {
        return -1;
    }
    return 0;
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
ConfigStatus update_hbase_config(const char* param, const char* value) {
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
        snprintf(file_path, sizeof(file_path), "%s/conf/%s", hbase_home, HBASE_CONFIG_FILE);
        if (stat(file_path, &st) != 0) {
            /* Fall back to Bigtop default location */
            snprintf(file_path, sizeof(file_path), "%s/%s", bigtop_default, HBASE_CONFIG_FILE);
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
                    snprintf(file_path, sizeof(file_path), "%s/%s", bigtop_default, HBASE_CONFIG_FILE);
                } else {
                    snprintf(file_path, sizeof(file_path), "%s/conf/%s", hbase_home, HBASE_CONFIG_FILE);
                }
            }
        }
    } else {
        /* Use Bigtop default when HBASE_HOME not set and no detected home */
        snprintf(file_path, sizeof(file_path), "%s/%s", bigtop_default, HBASE_CONFIG_FILE);
        if (stat(file_path, &st) != 0 && mkdir_p(bigtop_default) != 0) {
            status = FILE_WRITE_ERROR;
            goto cleanup;
        }
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


/////////////////////////////////SPARK/////////////////////////////////////////

// Predefined list of Spark configuration parameters with regex patterns and config files
static const struct {
    const char *canonical_name;
    const char *regex_pattern;
    const char *config_file;
} spark_param_configs[] = {
    // Original configurations
    { "spark.master", "^(spark[._-])?master$", "spark-defaults.conf" },
    { "spark.app.name", "^(spark[._-])?app[._-]name$", "spark-defaults.conf" },
    { "spark.executor.memory", "^(spark[._-])?executor[._-]memory$", "spark-defaults.conf" },
    { "spark.driver.memory", "^(spark[._-])?driver[._-]memory$", "spark-defaults.conf" },
    { "spark.serializer", "^(spark[._-])?serializer$", "spark-defaults.conf" },
    { "spark.sql.shuffle.partitions", "^(spark[._-])?sql[._-]shuffle[._-]partitions$", "spark-defaults.conf" },
    { "spark.default.parallelism", "^(spark[._-])?default[._-]parallelism$", "spark-defaults.conf" },
    { "spark.executor.cores", "^(spark[._-])?executor[._-]cores$", "spark-defaults.conf" },
    { "spark.shuffle.service.enabled", "^(spark[._-])?shuffle[._-]service[._-]enabled$", "spark-defaults.conf" },
    { "spark.dynamicAllocation.enabled", "^(spark[._-])?dynamicAllocation[._-]enabled$", "spark-defaults.conf" },
    { "spark.eventLog.enabled", "^(spark[._-])?eventLog[._-]enabled$", "spark-defaults.conf" },
    { "spark.yarn.queue", "^(spark[._-])?yarn[._-]queue$", "spark-defaults.conf" },
    { "spark.submit.deployMode", "^(spark[._-])?submit[._-]deployMode$", "spark-defaults.conf" },
    { "spark.network.timeout", "^(spark[._-])?network[._-]timeout$", "spark-defaults.conf" },
    { "spark.ui.port", "^(spark[._-])?ui[._-]port$", "spark-defaults.conf" },
    { "spark.driver.maxResultSize", "^(spark[._-])?driver[._-]maxResultSize$", "spark-defaults.conf" },
    { "spark.executor.instances", "^(spark[._-])?executor[._-]instances$", "spark-defaults.conf" },
    { "spark.sql.autoBroadcastJoinThreshold", "^(spark[._-])?sql[._-]autoBroadcastJoinThreshold$", "spark-defaults.conf" },
    { "spark.memory.fraction", "^(spark[._-])?memory[._-]fraction$", "spark-defaults.conf" },
    { "spark.locality.wait", "^(spark[._-])?locality[._-]wait$", "spark-defaults.conf" },
    
    // Additional comprehensive configurations
    { "spark.driver.cores", "^(spark[._-])?driver[._-]cores$", "spark-defaults.conf" },
    { "spark.memory.offHeap.enabled", "^(spark[._-])?memory[._-]offHeap[._-]enabled$", "spark-defaults.conf" },
    { "spark.memory.offHeap.size", "^(spark[._-])?memory[._-]offHeap[._-]size$", "spark-defaults.conf" },
    { "spark.executor.memoryOverhead", "^(spark[._-])?executor[._-]memoryOverhead$", "spark-defaults.conf" },
    { "spark.driver.memoryOverhead", "^(spark[._-])?driver[._-]memoryOverhead$", "spark-defaults.conf" },
    { "spark.shuffle.compress", "^(spark[._-])?shuffle[._-]compress$", "spark-defaults.conf" },
    { "spark.shuffle.spill.compress", "^(spark[._-])?shuffle[._-]spill[._-]compress$", "spark-defaults.conf" },
    { "spark.io.compression.codec", "^(spark[._-])?io[._-]compression[._-]codec$", "spark-defaults.conf" },
    { "spark.shuffle.file.buffer", "^(spark[._-])?shuffle[._-]file[._-]buffer$", "spark-defaults.conf" },
    { "spark.reducer.maxSizeInFlight", "^(spark[._-])?reducer[._-]maxSizeInFlight$", "spark-defaults.conf" },
    { "spark.dynamicAllocation.minExecutors", "^(spark[._-])?dynamicAllocation[._-]minExecutors$", "spark-defaults.conf" },
    { "spark.dynamicAllocation.maxExecutors", "^(spark[._-])?dynamicAllocation[._-]maxExecutors$", "spark-defaults.conf" },
    { "spark.dynamicAllocation.initialExecutors", "^(spark[._-])?dynamicAllocation[._-]initialExecutors$", "spark-defaults.conf" },
    { "spark.dynamicAllocation.executorIdleTimeout", "^(spark[._-])?dynamicAllocation[._-]executorIdleTimeout$", "spark-defaults.conf" },
    { "spark.sql.adaptive.enabled", "^(spark[._-])?sql[._-]adaptive[._-]enabled$", "spark-defaults.conf" },
    { "spark.sql.files.maxPartitionBytes", "^(spark[._-])?sql[._-]files[._-]maxPartitionBytes$", "spark-defaults.conf" },
    { "spark.sql.sources.partitionOverwriteMode", "^(spark[._-])?sql[._-]sources[._-]partitionOverwriteMode$", "spark-defaults.conf" },
    { "spark.sql.cbo.enabled", "^(spark[._-])?sql[._-]cbo[._-]enabled$", "spark-defaults.conf" },
    { "spark.streaming.backpressure.enabled", "^(spark[._-])?streaming[._-]backpressure[._-]enabled$", "spark-defaults.conf" },
    { "spark.streaming.kafka.maxRatePerPartition", "^(spark[._-])?streaming[._-]kafka[._-]maxRatePerPartition$", "spark-defaults.conf" },
    { "spark.ui.enabled", "^(spark[._-])?ui[._-]enabled$", "spark-defaults.conf" },
    { "spark.eventLog.dir", "^(spark[._-])?eventLog[._-]dir$", "spark-defaults.conf" },
    { "spark.eventLog.compress", "^(spark[._-])?eventLog[._-]compress$", "spark-defaults.conf" },
    { "spark.authenticate", "^(spark[._-])?authenticate$", "spark-defaults.conf" },
    { "spark.ssl.enabled", "^(spark[._-])?ssl[._-]enabled$", "spark-defaults.conf" },
    { "spark.yarn.am.memory", "^(spark[._-])?yarn[._-]am[._-]memory$", "spark-defaults.conf" },
    { "spark.yarn.executor.memoryOverhead", "^(spark[._-])?yarn[._-]executor[._-]memoryOverhead$", "spark-defaults.conf" },
    { "spark.yarn.driver.memoryOverhead", "^(spark[._-])?yarn[._-]driver[._-]memoryOverhead$", "spark-defaults.conf" },
    { "spark.rpc.message.maxSize", "^(spark[._-])?rpc[._-]message[._-]maxSize$", "spark-defaults.conf" },
    { "spark.blockManager.port", "^(spark[._-])?blockManager[._-]port$", "spark-defaults.conf" },
    { "spark.scheduler.mode", "^(spark[._-])?scheduler[._-]mode$", "spark-defaults.conf" },
    { "spark.checkpoint.compress", "^(spark[._-])?checkpoint[._-]compress$", "spark-defaults.conf" },
    { "spark.pyspark.python", "^(spark[._-])?pyspark[._-]python$", "spark-defaults.conf" }
};

static bool isValidSparkDuration(const char *value) {
    char *end;
    long num = strtol(value, &end, 10);
    if (num <= 0) return false;
    if (*end == '\0') return true; // Assume seconds
    if (strlen(end) > 2) return false;
    
    // Allow s/min/h/d suffixes
    return strcmp(end, "s") == 0 || strcmp(end, "min") == 0 || 
           strcmp(end, "h") == 0 || strcmp(end, "d") == 0 ||
           strcmp(end, "ms") == 0;
}

static bool isValidSparkMasterFormat(const char *value) {
    return strncmp(value, "local", 5) == 0 ||
           strncmp(value, "yarn", 4) == 0 ||
           strncmp(value, "spark://", 8) == 0 ||
           strncmp(value, "k8s://", 6) == 0;
}

ValidationResult validateSparkConfigParam(const char *param_name, const char *value) {
    // Check if parameter exists
    bool param_exists = false;
    for (size_t i = 0; i < sizeof(spark_param_configs)/sizeof(spark_param_configs[0]); i++) {
        if (strcmp(param_name, spark_param_configs[i].canonical_name) == 0) {
            param_exists = true;
            break;
        }
    }
    if (!param_exists) return ERROR_PARAM_NOT_FOUND;

    // Check value presence
    if (!value || strlen(value) == 0) return ERROR_VALUE_EMPTY;

    // Parameter-specific validation
    if (strcmp(param_name, "spark.executor.memory") == 0 ||
        strcmp(param_name, "spark.driver.memory") == 0 ||
        strcmp(param_name, "spark.memory.offHeap.size") == 0 ||
        strcmp(param_name, "spark.executor.memoryOverhead") == 0 ||
        strcmp(param_name, "spark.driver.memoryOverhead") == 0 ||
        strcmp(param_name, "spark.yarn.am.memory") == 0 ||
        strcmp(param_name, "spark.yarn.executor.memoryOverhead") == 0 ||
        strcmp(param_name, "spark.yarn.driver.memoryOverhead") == 0) {
        return isDataSizeWithUnit(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "spark.ui.port") == 0 ||
             strcmp(param_name, "spark.blockManager.port") == 0) {
        return isValidPort(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "spark.shuffle.service.enabled") == 0 ||
             strcmp(param_name, "spark.dynamicAllocation.enabled") == 0 ||
             strcmp(param_name, "spark.eventLog.enabled") == 0 ||
             strcmp(param_name, "spark.memory.offHeap.enabled") == 0 ||
             strcmp(param_name, "spark.shuffle.compress") == 0 ||
             strcmp(param_name, "spark.shuffle.spill.compress") == 0 ||
             strcmp(param_name, "spark.sql.adaptive.enabled") == 0 ||
             strcmp(param_name, "spark.sql.cbo.enabled") == 0 ||
             strcmp(param_name, "spark.streaming.backpressure.enabled") == 0 ||
             strcmp(param_name, "spark.eventLog.compress") == 0 ||
             strcmp(param_name, "spark.authenticate") == 0 ||
             strcmp(param_name, "spark.ssl.enabled") == 0 ||
             strcmp(param_name, "spark.checkpoint.compress") == 0 ||
             strcmp(param_name, "spark.ui.enabled") == 0) {
        return isValidBoolean(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "spark.network.timeout") == 0 ||
             strcmp(param_name, "spark.locality.wait") == 0 ||
             strcmp(param_name, "spark.dynamicAllocation.executorIdleTimeout") == 0) {
        return isValidSparkDuration(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "spark.executor.cores") == 0 ||
             strcmp(param_name, "spark.driver.cores") == 0 ||
             strcmp(param_name, "spark.sql.shuffle.partitions") == 0 ||
             strcmp(param_name, "spark.default.parallelism") == 0 ||
             strcmp(param_name, "spark.executor.instances") == 0 ||
             strcmp(param_name, "spark.dynamicAllocation.minExecutors") == 0 ||
             strcmp(param_name, "spark.dynamicAllocation.maxExecutors") == 0 ||
             strcmp(param_name, "spark.dynamicAllocation.initialExecutors") == 0 ||
             strcmp(param_name, "spark.reducer.maxSizeInFlight") == 0 ||
             strcmp(param_name, "spark.rpc.message.maxSize") == 0) {
        return isPositiveInteger(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "spark.master") == 0) {
        return isValidSparkMasterFormat(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "spark.serializer") == 0) {
        return strstr(value, "KryoSerializer") != NULL || 
               strstr(value, "JavaSerializer") != NULL
            ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "spark.submit.deployMode") == 0) {
        return strcmp(value, "client") == 0 || strcmp(value, "cluster") == 0
            ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "spark.io.compression.codec") == 0) {
        return strstr(value, "lz4") != NULL || strstr(value, "snappy") != NULL ||
               strstr(value, "zstd") != NULL || strstr(value, "lzf") != NULL
            ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "spark.sql.autoBroadcastJoinThreshold") == 0) {
        return isDataSizeWithUnit(value) || (atoi(value) == -1)
            ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "spark.memory.fraction") == 0) {
        char *end;
        float fraction = strtof(value, &end);
        return *end == '\0' && fraction >= 0.0f && fraction <= 1.0f
            ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "spark.scheduler.mode") == 0) {
        return strcmp(value, "FIFO") == 0 || strcmp(value, "FAIR") == 0
            ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "spark.sql.sources.partitionOverwriteMode") == 0) {
        return strcmp(value, "static") == 0 || strcmp(value, "dynamic") == 0
            ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "spark.pyspark.python") == 0) {
        return (strlen(value) > 0) ? VALIDATION_OK : ERROR_VALUE_EMPTY;
    }

    // Default validation for parameters without specific rules
    return VALIDATION_OK;
}


ConfigResult *get_spark_config(const char *param_name, const char *param_value) {
    regex_t regex;
    int reti;

    for (size_t i = 0; i < sizeof(spark_param_configs)/sizeof(spark_param_configs[0]); i++) {
        reti = regcomp(&regex, spark_param_configs[i].regex_pattern, REG_ICASE | REG_NOSUB | REG_EXTENDED);
        if (reti) {
            fprintf(stderr, "Failed to compile regex for %s\n", spark_param_configs[i].canonical_name);
            continue;
        }

        reti = regexec(&regex, param_name, 0, NULL, 0);
        regfree(&regex);

        if (reti == 0) {
            ConfigResult *result = malloc(sizeof(ConfigResult));
            if (!result) return NULL;

            result->canonical_name = strdup(spark_param_configs[i].canonical_name);
            result->value = strdup(param_value);
            result->config_file = strdup(spark_param_configs[i].config_file);

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

void free_spark_config_result(ConfigResult *result) {
    if (result) {
        free(result->canonical_name);
        free(result->value);
        free(result->config_file);
        free(result);
    }
}

/*
 * Update Spark configuration parameter in the configuration file.
 *
 * Searches for the given parameter and updates its value. If the parameter
 * does not exist, it is appended to the configuration file.
 *
 * Returns ConfigStatus indicating success or specific error.
 */
ConfigStatus
update_spark_config(const char *param, const char *value)
{
    char        config_path[PATH_MAX] = {0};
    FILE       *fp = NULL;
    FILE       *tmp_fp = NULL;
    int         found = 0;
    ConfigStatus status = INVALID_CONFIG_FILE;
    struct stat st;

    /* Validate input parameters */
    if (param == NULL || value == NULL)
    {
        return INVALID_CONFIG_FILE;
    }

    /* Determine the configuration file path */
    char *spark_home_env = getenv("SPARK_HOME");
    if (spark_home_env != NULL)
    {
        snprintf(config_path, sizeof(config_path), "%s/conf/%s", spark_home_env, SPARK_CONFIG_FILE_NAME);
    }
    else
    {
        const char *spark_home_candidates[] = {"/usr/local/spark", "/opt/spark", NULL};
        struct stat st;
        const char *selected_spark_home = NULL;
        for (int i = 0; spark_home_candidates[i] != NULL; i++)
        {
            if (stat(spark_home_candidates[i], &st) == 0 && S_ISDIR(st.st_mode))
            {
                selected_spark_home = spark_home_candidates[i];
                break;
            }
        }
        if (selected_spark_home == NULL)
        {
            /* Default to Red Hat path if none found */
            selected_spark_home = "/usr/local/spark";
        }
        snprintf(config_path, sizeof(config_path), "%s/conf/%s", selected_spark_home, SPARK_CONFIG_FILE_NAME);
    }

    /*
     * Ensure parent directory exists. If not, attempt to create it with
     * appropriate permissions.
     */
    char *last_slash = strrchr(config_path, '/');
    if (last_slash != NULL)
    {
        *last_slash = '\0';
        /* Check if directory exists; if not, create it */
        if (stat(config_path, &st) == -1)
        {
            if (mkdir(config_path, 0755) != 0)
            {
                status = FILE_WRITE_ERROR;
                goto cleanup;
            }
        }
        *last_slash = '/';
    }

    /*
     * Open existing configuration file for reading. If it doesn't exist,
     * proceed with an empty temporary file.
     */
    fp = fopen(config_path, "r");
    if (fp != NULL)
    {
        /* Create a temporary file to hold updated configuration */
        tmp_fp = tmpfile();
        if (tmp_fp == NULL)
        {
            fclose(fp);
            status = FILE_WRITE_ERROR;
            goto cleanup;
        }
    }
    else
    {
        /* Existing file doesn't exist; create a new temporary file */
        tmp_fp = tmpfile();
        if (tmp_fp == NULL)
        {
            status = FILE_WRITE_ERROR;
            goto cleanup;
        }
        fp = NULL; /* Ensure fp remains NULL */
    }

    /* Process each line in the existing configuration file */
    if (fp != NULL)
    {
        char        line[MAX_LINE_LENGTH];

        while (fgets(line, sizeof(line), fp) != NULL)
        {
            char       *key = line;
            char       *value_start;

            /* Trim leading whitespace */
            while (*key == ' ' || *key == '\t')
            {
                key++;
            }

            /* Skip comments and empty lines */
            if (*key == '#' || *key == '\n')
            {
                if (fputs(line, tmp_fp) == EOF)
                {
                    status = FILE_WRITE_ERROR;
                    goto cleanup;
                }
                continue;
            }

            /* Locate the value separator (space or tab) */
            value_start = strchr(key, ' ');
            if (value_start == NULL)
            {
                value_start = strchr(key, '\t');
            }

            if (value_start != NULL)
            {
                *value_start = '\0'; /* Terminate key string */
                value_start++;
                /* Trim leading whitespace from value */
                while (*value_start == ' ' || *value_start == '\t')
                {
                    value_start++;
                }

                /* Check if current line matches target parameter */
                if (strcmp(key, param) == 0)
                {
                    /* Replace with new value */
                    if (fprintf(tmp_fp, "%s %s\n", param, value) < 0)
                    {
                        status = FILE_WRITE_ERROR;
                        goto cleanup;
                    }
                    found = 1;
                    continue;
                }
            }

            /* Write the original line to temporary file */
            if (fputs(line, tmp_fp) == EOF)
            {
                status = FILE_WRITE_ERROR;
                goto cleanup;
            }
        }

        /* Check for read errors */
        if (ferror(fp))
        {
            status = FILE_READ_ERROR;
            goto cleanup;
        }

        fclose(fp);
        fp = NULL;
    }

    /* If parameter was not found, append it to the end */
    if (!found)
    {
        if (fprintf(tmp_fp, "%s %s\n", param, value) < 0)
        {
            status = FILE_WRITE_ERROR;
            goto cleanup;
        }
    }

    /* Write updated configuration back to the original file */
    rewind(tmp_fp);
    fp = fopen(config_path, "w");
    if (fp == NULL)
    {
        status = FILE_WRITE_ERROR;
        goto cleanup;
    }

    {
        char        buffer[MAX_LINE_LENGTH];

        while (fgets(buffer, sizeof(buffer), tmp_fp) != NULL)
        {
            if (fputs(buffer, fp) == EOF)
            {
                fclose(fp);
                status = FILE_WRITE_ERROR;
                goto cleanup;
            }
        }

        /* Check for write errors */
        if (ferror(tmp_fp))
        {
            fclose(fp);
            status = FILE_READ_ERROR;
            goto cleanup;
        }

        fclose(fp);
        fp = NULL;
    }

    status = SUCCESS;

cleanup:
    /* Cleanup resources */
    if (fp != NULL)
    {
        fclose(fp);
    }
    if (tmp_fp != NULL)
    {
        fclose(tmp_fp);
    }

    return status;
}
////////////////////////////////////kafka ////////////////////////////////////////////////////


typedef struct {
    const char *canonical_name;
    const char *normalized_name;
    const char *config_file;
} KafkaParamConfig;

KafkaParamConfig kafka_param_configs[] = {
    // Core Connection Settings
    { "bootstrap.servers", "^(kafka[._-])?bootstrap[._-]servers$", "producer.properties" },
    { "bootstrap.servers", "^(kafka[._-])?bootstrap[._-]servers$", "consumer.properties" },
    { "zookeeper.connect", "^(kafka[._-])?zookeeper[._-]connect$", "server.properties" },
    { "client.id", "^(kafka[._-])?client[._-]id$", "producer.properties" },
    { "client.id", "^(kafka[._-])?client[._-]id$", "consumer.properties" },
    { "listeners", "^(kafka[._-])?listeners$", "server.properties" },
    { "advertised.listeners", "^(kafka[._-])?advertised[._-]listeners$", "server.properties" },

    // Producer Configurations
    { "acks", "^(kafka[._-])?acks$", "producer.properties" },
    { "retries", "^(kafka[._-])?retries$", "producer.properties" },
    { "batch.size", "^(kafka[._-])?batch[._-]size$", "producer.properties" },
    { "linger.ms", "^(kafka[._-])?linger[._-]ms$", "producer.properties" },
    { "compression.type", "^(kafka[._-])?compression[._-]type$", "producer.properties" },
    { "max.request.size", "^(kafka[._-])?max[._-]request[._-]size$", "producer.properties" },
    { "enable.idempotence", "^(kafka[._-])?enable[._-]idempotence$", "producer.properties" },
    { "buffer.memory", "^(kafka[._-])?buffer[._-]memory$", "producer.properties" },
    { "max.block.ms", "^(kafka[._-])?max[._-]block[._-]ms$", "producer.properties" },
    { "delivery.timeout.ms", "^(kafka[._-])?delivery[._-]timeout[._-]ms$", "producer.properties" },
    { "request.timeout.ms", "^(kafka[._-])?request[._-]timeout[._-]ms$", "producer.properties" },
    { "max.in.flight.requests.per.connection", "^(kafka[._-])?max[._-]in[._-]flight[._-]requests[._-]per[._-]connection$", "producer.properties" },
    { "metadata.max.age.ms", "^(kafka[._-])?metadata[._-]max[._-]age[._-]ms$", "producer.properties" },
    { "send.buffer.bytes", "^(kafka[._-])?send[._-]buffer[._-]bytes$", "producer.properties" },
    { "transactional.id", "^(kafka[._-])?transactional[._-]id$", "producer.properties" },

    // Consumer Configurations
    { "group.id", "^(kafka[._-])?group[._-]id$", "consumer.properties" },
    { "auto.offset.reset", "^(kafka[._-])?auto[._-]offset[._-]reset$", "consumer.properties" },
    { "enable.auto.commit", "^(kafka[._-])?enable[._-]auto[._-]commit$", "consumer.properties" },
    { "max.poll.records", "^(kafka[._-])?max[._-]poll[._-]records$", "consumer.properties" },
    { "fetch.min.bytes", "^(kafka[._-])?fetch[._-]min[._-]bytes$", "consumer.properties" },
    { "fetch.max.bytes", "^(kafka[._-])?fetch[._-]max[._-]bytes$", "consumer.properties" },
    { "heartbeat.interval.ms", "^(kafka[._-])?heartbeat[._-]interval[._-]ms$", "consumer.properties" },
    { "max.partition.fetch.bytes", "^(kafka[._-])?max[._-]partition[._-]fetch[._-]bytes$", "consumer.properties" },
    { "receive.buffer.bytes", "^(kafka[._-])?receive[._-]buffer[._-]bytes$", "consumer.properties" },
    { "partition.assignment.strategy", "^(kafka[._-])?partition[._-]assignment[._-]strategy$", "consumer.properties" },
    { "fetch.max.wait.ms", "^(kafka[._-])?fetch[._-]max[._-]wait[._-]ms$", "consumer.properties" },
    { "max.poll.interval.ms", "^(kafka[._-])?max[._-]poll[._-]interval[._-]ms$", "consumer.properties" },

    // Broker Configurations
    { "log.dirs", "^(kafka[._-])?log[._-]dirs$", "server.properties" },
    { "num.partitions", "^(kafka[._-])?num[._-]partitions$", "server.properties" },
    { "default.replication.factor", "^(kafka[._-])?default[._-]replication[._-]factor$", "server.properties" },
    { "offsets.topic.replication.factor", "^(kafka[._-])?offsets[._-]topic[._-]replication[._-]factor$", "server.properties" },
    { "auto.create.topics.enable", "^(kafka[._-])?auto[._-]create[._-]topics[._-]enable$", "server.properties" },
    { "log.retention.ms", "^(kafka[._-])?log[._-]retention[._-]ms$", "server.properties" },
    { "log.segment.bytes", "^(kafka[._-])?log[._-]segment[._-]bytes$", "server.properties" },
    { "controlled.shutdown.enable", "^(kafka[._-])?controlled[._-]shutdown[._-]enable$", "server.properties" },
    { "unclean.leader.election.enable", "^(kafka[._-])?unclean[._-]leader[._-]election[._-]enable$", "server.properties" },
    { "socket.send.buffer.bytes", "^(kafka[._-])?socket[._-]send[._-]buffer[._-]bytes$", "server.properties" },
    { "socket.receive.buffer.bytes", "^(kafka[._-])?socket[._-]receive[._-]buffer[._-]bytes$", "server.properties" },
    { "num.recovery.threads.per.data.dir", "^(kafka[._-])?num[._-]recovery[._-]threads[._-]per[._-]data[._-]dir$", "server.properties" },
    { "log.flush.interval.messages", "^(kafka[._-])?log[._-]flush[._-]interval[._-]messages$", "server.properties" },
    { "log.flush.interval.ms", "^(kafka[._-])?log[._-]flush[._-]interval[._-]ms$", "server.properties" },
    { "message.max.bytes", "^(kafka[._-])?message[._-]max[._-]bytes$", "server.properties" },
    { "auto.leader.rebalance.enable", "^(kafka[._-])?auto[._-]leader[._-]rebalance[._-]enable$", "server.properties" },

    // Security (SSL/SASL)
    { "security.protocol", "^(kafka[._-])?security[._-]protocol$", "producer.properties" },
    { "security.protocol", "^(kafka[._-])?security[._-]protocol$", "consumer.properties" },
    { "security.protocol", "^(kafka[._-])?security[._-]protocol$", "server.properties" },
    { "ssl.keystore.location", "^(kafka[._-])?ssl[._-]keystore[._-]location$", "producer.properties" },
    { "ssl.keystore.location", "^(kafka[._-])?ssl[._-]keystore[._-]location$", "consumer.properties" },
    { "ssl.keystore.location", "^(kafka[._-])?ssl[._-]keystore[._-]location$", "server.properties" },
    { "ssl.truststore.location", "^(kafka[._-])?ssl[._-]truststore[._-]location$", "producer.properties" },
    { "ssl.truststore.location", "^(kafka[._-])?ssl[._-]truststore[._-]location$", "consumer.properties" },
    { "ssl.truststore.location", "^(kafka[._-])?ssl[._-]truststore[._-]location$", "server.properties" },
    { "ssl.keystore.password", "^(kafka[._-])?ssl[._-]keystore[._-]password$", "producer.properties" },
    { "ssl.keystore.password", "^(kafka[._-])?ssl[._-]keystore[._-]password$", "consumer.properties" },
    { "ssl.keystore.password", "^(kafka[._-])?ssl[._-]keystore[._-]password$", "server.properties" },
    { "ssl.truststore.password", "^(kafka[._-])?ssl[._-]truststore[._-]password$", "producer.properties" },
    { "ssl.truststore.password", "^(kafka[._-])?ssl[._-]truststore[._-]password$", "consumer.properties" },
    { "ssl.truststore.password", "^(kafka[._-])?ssl[._-]truststore[._-]password$", "server.properties" },
    { "ssl.key.password", "^(kafka[._-])?ssl[._-]key[._-]password$", "producer.properties" },
    { "ssl.key.password", "^(kafka[._-])?ssl[._-]key[._-]password$", "consumer.properties" },
    { "ssl.key.password", "^(kafka[._-])?ssl[._-]key[._-]password$", "server.properties" },
    { "ssl.endpoint.identification.algorithm", "^(kafka[._-])?ssl[._-]endpoint[._-]identification[._-]algorithm$", "producer.properties" },
    { "ssl.endpoint.identification.algorithm", "^(kafka[._-])?ssl[._-]endpoint[._-]identification[._-]algorithm$", "consumer.properties" },
    { "ssl.endpoint.identification.algorithm", "^(kafka[._-])?ssl[._-]endpoint[._-]identification[._-]algorithm$", "server.properties" },
    { "sasl.mechanism", "^(kafka[._-])?sasl[._-]mechanism$", "producer.properties" },
    { "sasl.mechanism", "^(kafka[._-])?sasl[._-]mechanism$", "consumer.properties" },
    { "sasl.mechanism", "^(kafka[._-])?sasl[._-]mechanism$", "server.properties" },
    { "sasl.jaas.config", "^(kafka[._-])?sasl[._-]jaas[._-]config$", "producer.properties" },
    { "sasl.jaas.config", "^(kafka[._-])?sasl[._-]jaas[._-]config$", "consumer.properties" },
    { "sasl.jaas.config", "^(kafka[._-])?sasl[._-]jaas[._-]config$", "server.properties" },

    // Performance Tuning
    { "log.retention.hours", "^(kafka[._-])?log[._-]retention[._-]hours$", "server.properties" },
    { "log.retention.bytes", "^(kafka[._-])?log[._-]retention[._-]bytes$", "server.properties" },
    { "num.io.threads", "^(kafka[._-])?num[._-]io[._-]threads$", "server.properties" },
    { "num.network.threads", "^(kafka[._-])?num[._-]network[._-]threads$", "server.properties" },
    { "log.retention.ms", "^(kafka[._-])?log[._-]retention[._-]ms$", "server.properties" },
    { "log.segment.bytes", "^(kafka[._-])?log[._-]segment[._-]bytes$", "server.properties" },
};

// Kafka-specific helper functions
static bool isHostPortPair(const char *value) {
    char *colon = strchr(value, ':');
    return colon && isValidPort(colon + 1);
}

static bool isValidCompressionType(const char *value) {
    const char *valid[] = {"none", "gzip", "snappy", "lz4", "zstd", NULL};
    for (int i = 0; valid[i]; i++)
        if (strcmp(value, valid[i]) == 0) return true;
    return false;
}

static bool isSecurityProtocolValid(const char *value) {
    const char *valid[] = {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL", NULL};
    for (int i = 0; valid[i]; i++)
        if (strcmp(value, valid[i]) == 0) return true;
    return false;
}

static bool isSaslMechanismValid(const char *value) {
    const char *valid[] = {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI", NULL};
    for (int i = 0; valid[i]; i++)
        if (strcmp(value, valid[i]) == 0) return true;
    return false;
}

static bool isAutoOffsetResetValid(const char *value) {
    const char *valid[] = {"earliest", "latest", "none", NULL};
    for (int i = 0; valid[i]; i++)
        if (strcmp(value, valid[i]) == 0) return true;
    return false;
}

ValidationResult validateKafkaConfigParam(const char *param_name, const char *value) {
    // Check if parameter exists
    bool param_exists = true;
 //   for (size_t i = 0; i < sizeof(kafka_param_configs)/sizeof(kafka_param_configs[0]); i++) {
   //     if (strcmp(param_name, kafka_param_configs[i].normalized_name) == 0) {
     //       param_exists = true;
       //     break;
       // }
    //}
    if (!param_exists) return ERROR_PARAM_NOT_FOUND;

    // Check value presence
    if (!value || strlen(value) == 0) return ERROR_VALUE_EMPTY;

    // Parameter-specific validation
    if (strcmp(param_name, "bootstrap.servers") == 0) {
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
    else if (strcmp(param_name, "acks") == 0) {
        if (strcmp(value, "all") != 0 && 
            strcmp(value, "0") != 0 && 
            strcmp(value, "1") != 0)
            return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "compression.type") == 0) {
        if (!isValidCompressionType(value))
            return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "security.protocol") == 0) {
        if (!isSecurityProtocolValid(value))
            return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "sasl.mechanism") == 0) {
        if (!isSaslMechanismValid(value))
            return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "auto.offset.reset") == 0) {
        if (!isAutoOffsetResetValid(value))
            return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "enable.idempotence") == 0 ||
             strcmp(param_name, "enable.auto.commit") == 0 ||
             strcmp(param_name, "auto.create.topics.enable") == 0 ||
             strcmp(param_name, "controlled.shutdown.enable") == 0 ||
             strcmp(param_name, "unclean.leader.election.enable") == 0) {
        if (!isValidBoolean(value))
            return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "retries") == 0 ||
             strcmp(param_name, "max.poll.records") == 0 ||
             strcmp(param_name, "num.partitions") == 0 ||
             strcmp(param_name, "default.replication.factor") == 0) {
        if (!isPositiveInteger(value))
            return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "batch.size") == 0 ||
             strcmp(param_name, "buffer.memory") == 0 ||
             strcmp(param_name, "max.request.size") == 0 ||
             strcmp(param_name, "fetch.max.bytes") == 0 ||
             strcmp(param_name, "message.max.bytes") == 0) {
        char *end;
        strtoll(value, &end, 10);
        if (*end != '\0' && !(end[0] == 'k' || end[0] == 'K' || 
                              end[0] == 'm' || end[0] == 'M' || 
                              end[0] == 'g' || end[0] == 'G'))
            return ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "linger.ms") == 0 ||
             strcmp(param_name, "max.block.ms") == 0 ||
             strcmp(param_name, "request.timeout.ms") == 0 ||
             strcmp(param_name, "max.poll.interval.ms") == 0) {
        char *end;
        long ms = strtol(value, &end, 10);
        if (*end != '\0' || ms < 0)
            return ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "ssl.keystore.location") == 0 ||
             strcmp(param_name, "ssl.truststore.location") == 0) {
        if (strlen(value) == 0)
            return ERROR_VALUE_EMPTY;
    }
    else if (strcmp(param_name, "partition.assignment.strategy") == 0) {
        const char *valid[] = {"range", "roundrobin", "sticky", NULL};
        bool valid_strategy = false;
        for (int i = 0; valid[i]; i++)
            if (strstr(value, valid[i])) valid_strategy = true;
        if (!valid_strategy)
            return ERROR_CONSTRAINT_VIOLATED;
    }

    return VALIDATION_OK;
}

#define NUM_CONFIGS (sizeof(kafka_param_configs) / sizeof(kafka_param_configs[0]))

char *normalize_kafka_param_name(const char *param) {
    if (!param) return NULL;
    size_t len = strlen(param);
    char *normalized = malloc(len + 1);
    if (!normalized) return NULL;

    size_t j = 0;
    for (size_t i = 0; i < len; i++) {
        unsigned char c = param[i];
        if (isalnum(c)) {
            normalized[j++] = tolower(c);
        }
    }
    normalized[j] = '\0';
    return normalized;
}

ConfigResult *validate_kafka_config_param(const char *param_name, const char *param_value) {
    if (!param_name || !param_value) return NULL;

    ConfigResult *result = NULL;

    for (size_t i = 0; i < NUM_CONFIGS; i++) {
        regex_t regex;
        int ret = regcomp(&regex, kafka_param_configs[i].normalized_name, REG_EXTENDED | REG_NOSUB | REG_ICASE);
        if (ret != 0) continue;

        ret = regexec(&regex, param_name, 0, NULL, 0);
        regfree(&regex);

        if (ret == 0) {
            result = malloc(sizeof(ConfigResult));
            if (!result) break;

            result->canonical_name = strdup(kafka_param_configs[i].canonical_name);
            result->value = strdup(param_value);
            result->config_file = strdup(kafka_param_configs[i].config_file);

            if (!result->canonical_name || !result->value || !result->config_file) {
                free(result->canonical_name);
                free(result->value);
                free(result->config_file);
                free(result);
                result = NULL;
            }
            break;
        }
    }

    return result;
}

void free_kafka_config_result(ConfigResult *result) {
    if (result) {
        free(result->canonical_name);
        free(result->value);
        free(result->config_file);
        free(result);
    }
}
ConfigStatus modify_kafka_config(const char *param, const char *value, const char *config_file) {
    // Validate the config_file parameter
    if (strcmp(config_file, "producer.properties") != 0 &&
        strcmp(config_file, "server.properties") != 0 &&
        strcmp(config_file, "consumer.properties") != 0) {
        return FILE_NOT_FOUND;
    }

    const char *kafka_home = getenv("KAFKA_HOME");
    char filepath[1024];
    bool found = false;

    // Check KAFKA_HOME/config directory first
    if (kafka_home != NULL) {
        snprintf(filepath, sizeof(filepath), "%s/config/%s", kafka_home, config_file);
        if (access(filepath, F_OK) == 0) {
            found = true;
        }
    }

    // If not found, check common configuration paths
    if (!found) {
        const char *common_paths[] = {
            "/etc/kafka/%s",               // Common default
            "/etc/kafka/conf/%s",          // Subdirectory for some installations
            "/opt/kafka/config/%s",  // Red Hat-based
            "/usr/local/kafka/config/%s",    // Debian-based
            NULL
        };

        for (int i = 0; common_paths[i] != NULL; i++) {
            snprintf(filepath, sizeof(filepath), common_paths[i], config_file);
            if (access(filepath, F_OK) == 0) {
                found = true;
                break;
            }
        }
    }

    if (!found) {
        return FILE_NOT_FOUND;
    }

    // Read the entire file into memory
    FILE *file = fopen(filepath, "r");
    if (!file) {
        return FILE_NOT_FOUND;
    }

    char **lines = NULL;
    size_t line_count = 0;
    char buffer[1024];
    bool param_found = false;

    while (fgets(buffer, sizeof(buffer), file) != NULL) {
        char *line = strdup(buffer);
        if (!line) {
            fclose(file);
            for (size_t i = 0; i < line_count; i++) free(lines[i]);
            free(lines);
            return SAVE_FAILED;
        }

        char *trimmed_line = line;
        trim_whitespace(trimmed_line);

        // Skip empty lines and comments
        if (trimmed_line[0] == '#' || trimmed_line[0] == '\0') {
            lines = realloc(lines, (line_count + 1) * sizeof(char *));
            if (!lines) {
                fclose(file);
                free(line);
                return SAVE_FAILED;
            }
            lines[line_count++] = line;
            continue;
        }

        char *equals = strchr(trimmed_line, '=');
        if (equals) {
            *equals = '\0';
            char *current_key = trimmed_line;
            trim_whitespace(current_key);

            if (strcmp(current_key, param) == 0) {
                // Replace this line with the new key=value
                free(line);
                size_t new_line_len = strlen(param) + strlen(value) + 2; // '=' and '\n'
                line = malloc(new_line_len + 1);
                if (!line) {
                    fclose(file);
                    for (size_t i = 0; i < line_count; i++) free(lines[i]);
                    free(lines);
                    return SAVE_FAILED;
                }
                snprintf(line, new_line_len + 1, "%s=%s\n", param, value);
                param_found = true;
            } else {
                // Restore the '=' character
                *equals = '=';
            }
        }

        lines = realloc(lines, (line_count + 1) * sizeof(char *));
        if (!lines) {
            fclose(file);
            free(line);
            return SAVE_FAILED;
        }
        lines[line_count++] = line;
    }

    fclose(file);

    // Add new parameter if not found
    if (!param_found) {
        char *new_line = malloc(strlen(param) + strlen(value) + 3); // '=', '\n', and null terminator
        if (!new_line) {
            for (size_t i = 0; i < line_count; i++) free(lines[i]);
            free(lines);
            return SAVE_FAILED;
        }
        snprintf(new_line, strlen(param) + strlen(value) + 3, "%s=%s\n", param, value);
        lines = realloc(lines, (line_count + 1) * sizeof(char *));
        if (!lines) {
            free(new_line);
            for (size_t i = 0; i < line_count; i++) free(lines[i]);
            free(lines);
            return SAVE_FAILED;
        }
        lines[line_count++] = new_line;
    }

    // Write back to the file
    file = fopen(filepath, "w");
    if (!file) {
        for (size_t i = 0; i < line_count; i++) free(lines[i]);
        free(lines);
        return SAVE_FAILED;
    }

    for (size_t i = 0; i < line_count; i++) {
        if (fputs(lines[i], file) == EOF) {
            fclose(file);
            for (size_t j = 0; j < line_count; j++) free(lines[j]);
            free(lines);
            return SAVE_FAILED;
        }
        free(lines[i]);
    }

    free(lines);
    fclose(file);

    return SUCCESS;
}



///////////////////////////flink ////////////////////////////////////////////

static const struct {
    const char *canonical;
    const char *normalized;
    const char *config_file;
} flink_params[] = {
    // Core Configuration
    { "jobmanager.rpc.address", "^jobmanager[._-]rpc[._-]address$", "config.yaml" },
    { "jobmanager.rpc.port", "^jobmanager[._-]rpc[._-]port$", "config.yaml" },
    { "jobmanager.heap.size", "^jobmanager[._-]heap[._-]size$", "config.yaml" },
    { "taskmanager.heap.size", "^taskmanager[._-]heap[._-]size$", "config.yaml" },
    { "taskmanager.numberOfTaskSlots", "^taskmanager[._-]numberOfTaskSlots$", "config.yaml" },
    { "parallelism.default", "^parallelism[._-]default$", "config.yaml" },
    { "io.tmp.dirs", "^io[._-]tmp[._-]dirs$", "config.yaml" },
    { "classloader.resolve-order", "^classloader[._-]resolve[._-]order$", "config.yaml" },

    // State Backend & Checkpointing
    { "state.backend", "^state[._-]backend$", "config.yaml" },
    { "state.checkpoints.dir", "^state[._-]checkpoints[._-]dir$", "config.yaml" },
    { "state.savepoints.dir", "^state[._-]savepoints[._-]dir$", "config.yaml" },
    { "checkpoint.interval", "^checkpoint[._-]interval$", "config.yaml" },
    { "execution.checkpointing.interval", "^execution[._-]checkpointing[._-]interval$", "config.yaml" },
    { "state.backend.incremental", "^state[._-]backend[._-]incremental$", "config.yaml" },
    { "state.backend.async", "^state[._-]backend[._-]async$", "config.yaml" },
    { "state.backend.rocksdb.ttl-compaction-filter.enabled", "^state[._-]backend[._-]rocksdb[._-]ttl[._-]compaction[._-]filter[._-]enabled$", "config.yaml" },

    // Rest & Web UI
    { "rest.port", "^rest[._-]port$", "config.yaml" },
    { "rest.address", "^rest[._-]address$", "config.yaml" },
    { "web.timeout", "^web[._-]timeout$", "config.yaml" },
    { "web.submit.enable", "^web[._-]submit[._-]enable$", "config.yaml" },
    { "web.upload.dir", "^web[._-]upload[._-]dir$", "config.yaml" },
    { "web.access-control-allow-origin", "^web[._-]access[._-]control[._-]allow[._-]origin$", "config.yaml" },

    // High Availability
    { "high-availability", "^high[._-]availability$", "config.yaml" },
    { "high-availability.storageDir", "^high[._-]availability[._-]storageDir$", "config.yaml" },
    { "high-availability.zookeeper.quorum", "^high[._-]availability[._-]zookeeper[._-]quorum$", "config.yaml" },
    { "high-availability.zookeeper.path.root", "^high[._-]availability[._-]zookeeper[._-]path[._-]root$", "config.yaml" },
    { "high-availability.cluster-id", "^high[._-]availability[._-]cluster[._-]id$", "config.yaml" },

    // Security
    { "security.ssl.enabled", "^security[._-]ssl[._-]enabled$", "config.yaml" },
    { "security.ssl.keystore", "^security[._-]ssl[._-]keystore$", "config.yaml" },
    { "security.ssl.keystore-password", "^security[._-]ssl[._-]keystore[._-]password$", "config.yaml" },
    { "security.ssl.key-password", "^security[._-]ssl[._-]key[._-]password$", "config.yaml" },
    { "security.ssl.truststore", "^security[._-]ssl[._-]truststore$", "config.yaml" },
    { "security.ssl.truststore-password", "^security[._-]ssl[._-]truststore[._-]password$", "config.yaml" },
    { "security.kerberos.login.keytab", "^security[._-]kerberos[._-]login[._-]keytab$", "config.yaml" },

    // Network & Communication
    { "taskmanager.data.port", "^taskmanager[._-]data[._-]port$", "config.yaml" },
    { "taskmanager.data.ssl.enabled", "^taskmanager[._-]data[._-]ssl[._-]enabled$", "config.yaml" },
    { "blob.server.port", "^blob[._-]server[._-]port$", "config.yaml" },
    { "queryable-state.proxy.ports", "^queryable[._-]state[._-]proxy[._-]ports$", "config.yaml" },
    { "akka.ask.timeout", "^akka[._-]ask[._-]timeout$", "config.yaml" },
    { "akka.framesize", "^akka[._-]framesize$", "config.yaml" },

    // Memory Management
    { "taskmanager.memory.framework.heap.size", "^taskmanager[._-]memory[._-]framework[._-]heap[._-]size$", "config.yaml" },
    { "taskmanager.memory.network.min", "^taskmanager[._-]memory[._-]network[._-]min$", "config.yaml" },
    { "taskmanager.memory.managed.size", "^taskmanager[._-]memory[._-]managed[._-]size$", "config.yaml" },
    { "taskmanager.memory.managed.fraction", "^taskmanager[._-]memory[._-]managed[._-]fraction$", "config.yaml" },
    { "jobmanager.memory.off-heap.size", "^jobmanager[._-]memory[._-]off[._-]heap[._-]size$", "config.yaml" },

    // YARN Deployment
    { "yarn.application.name", "^yarn[._-]application[._-]name$", "config.yaml" },
    { "yarn.application.queue", "^yarn[._-]application[._-]queue$", "config.yaml" },
    { "yarn.containers.vcores", "^yarn[._-]containers[._-]vcores$", "config.yaml" },
    { "yarn.containers.memory", "^yarn[._-]containers[._-]memory$", "config.yaml" },
    { "yarn.ship-files", "^yarn[._-]ship[._-]files$", "config.yaml" },

    // Kubernetes Deployment
    { "kubernetes.cluster-id", "^kubernetes[._-]cluster[._-]id$", "config.yaml" },
    { "kubernetes.namespace", "^kubernetes[._-]namespace$", "config.yaml" },
    { "kubernetes.service.account", "^kubernetes[._-]service[._-]account$", "config.yaml" },
    { "kubernetes.container.image", "^kubernetes[._-]container[._-]image$", "config.yaml" },

    // Mesos Deployment
    { "mesos.resourcemanager.tasks.cpus", "^mesos[._-]resourcemanager[._-]tasks[._-]cpus$", "config.yaml" },
    { "mesos.resourcemanager.tasks.mem", "^mesos[._-]resourcemanager[._-]tasks[._-]mem$", "config.yaml" },

    // Failover & Recovery
    { "jobmanager.execution.failover-strategy", "^jobmanager[._-]execution[._-]failover[._-]strategy$", "config.yaml" },
    { "restart-strategy", "^restart[._-]strategy$", "config.yaml" },
    { "restart-strategy.fixed-delay.attempts", "^restart[._-]strategy[._-]fixed[._-]delay[._-]attempts$", "config.yaml" },

    // Metrics & Monitoring
    { "metrics.reporter.prom.class", "^metrics[._-]reporter[._-]prom[._-]class$", "config.yaml" },
    { "metrics.reporter.prom.port", "^metrics[._-]reporter[._-]prom[._-]port$", "config.yaml" },
    { "metrics.system-resource", "^metrics[._-]system[._-]resource$", "config.yaml" },
};

// Flink-specific helper functions
static bool isMemorySize(const char *value) {
    char *end;
    strtol(value, &end, 10);
    if (*end == '\0') return true; // bytes
    if (end == value) return false;
    return tolower(*end) == 'k' || tolower(*end) == 'm' || 
           tolower(*end) == 'g' || tolower(*end) == 't';
}

static bool isValidURI(const char *value) {
    return strstr(value, "://") != NULL && strlen(value) > 5;
}

static bool isFraction(const char *value) {
    char *end;
    float num = strtof(value, &end);
    return *end == '\0' && num >= 0.0f && num <= 1.0f;
}

ValidationResult validateFlinkConfigParam(const char *param_name, const char *value) {
    // Check if parameter exists
    bool param_exists = false;
    for (size_t i = 0; i < sizeof(flink_params)/sizeof(flink_params[0]); i++) {
        if (strcmp(param_name, flink_params[i].canonical) == 0) {
            param_exists = true;
            break;
        }
    }
    if (!param_exists) return ERROR_PARAM_NOT_FOUND;

    // Check value presence
    if (!value || strlen(value) == 0) return ERROR_VALUE_EMPTY;

    // Parameter-specific validation
    if (strcmp(param_name, "jobmanager.rpc.port") == 0 ||
        strcmp(param_name, "rest.port") == 0 ||
        strcmp(param_name, "blob.server.port") == 0 ||
        strcmp(param_name, "metrics.reporter.prom.port") == 0) {
        return isValidPort(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "jobmanager.heap.size") == 0 ||
             strcmp(param_name, "taskmanager.heap.size") == 0 ||
             strcmp(param_name, "taskmanager.memory.framework.heap.size") == 0 ||
             strcmp(param_name, "taskmanager.memory.managed.size") == 0 ||
             strcmp(param_name, "jobmanager.memory.off-heap.size") == 0) {
        return isMemorySize(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "state.backend") == 0) {
        const char *valid[] = {"rocksdb", "filesystem", "hashmap", NULL};
        for (int i = 0; valid[i]; i++)
            if (strcmp(value, valid[i]) == 0) return VALIDATION_OK;
        return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "taskmanager.numberOfTaskSlots") == 0 ||
             strcmp(param_name, "parallelism.default") == 0 ||
             strcmp(param_name, "restart-strategy.fixed-delay.attempts") == 0 ||
             strcmp(param_name, "yarn.containers.vcores") == 0) {
        return isPositiveInteger(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "checkpoint.interval") == 0 ||
             strcmp(param_name, "execution.checkpointing.interval") == 0 ||
             strcmp(param_name, "web.timeout") == 0 ||
             strcmp(param_name, "akka.ask.timeout") == 0) {
        return isValidDuration(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "io.tmp.dirs") == 0 ||
             strcmp(param_name, "state.checkpoints.dir") == 0 ||
             strcmp(param_name, "state.savepoints.dir") == 0 ||
             strcmp(param_name, "web.upload.dir") == 0) {
        return (value[0] != '\0') ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "high-availability.zookeeper.quorum") == 0) {
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
    else if (strcmp(param_name, "taskmanager.memory.managed.fraction") == 0) {
        return isFraction(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "security.ssl.enabled") == 0 ||
             strcmp(param_name, "state.backend.incremental") == 0 ||
             strcmp(param_name, "state.backend.async") == 0 ||
             strcmp(param_name, "web.submit.enable") == 0 ||
             strcmp(param_name, "taskmanager.data.ssl.enabled") == 0) {
        return isValidBoolean(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "restart-strategy") == 0) {
        const char *valid[] = {"fixed-delay", "failure-rate", "none", NULL};
        for (int i = 0; valid[i]; i++)
            if (strcmp(value, valid[i]) == 0) return VALIDATION_OK;
        return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "high-availability") == 0) {
        return strcmp(value, "NONE") == 0 || strcmp(value, "ZOOKEEPER") == 0
            ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "akka.framesize") == 0) {
        char *end;
        strtol(value, &end, 10);
        if (*end != '\0' && !(end[0] == 'b' || end[0] == 'k' || end[0] == 'm' || end[0] == 'g'))
            return ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "security.ssl.keystore") == 0 ||
             strcmp(param_name, "security.ssl.truststore") == 0 ||
             strcmp(param_name, "security.kerberos.login.keytab") == 0) {
        return (strlen(value) > 0) ? VALIDATION_OK : ERROR_VALUE_EMPTY;
    }
    else if (strcmp(param_name, "kubernetes.namespace") == 0 ||
             strcmp(param_name, "yarn.application.queue") == 0) {
        return (strlen(value) > 0) ? VALIDATION_OK : ERROR_VALUE_EMPTY;
    }

    return VALIDATION_OK;
}


ConfigResult* set_flink_config(const char *param, const char *value) {
    if (!param || !value) return NULL;

    for (size_t i = 0; i < sizeof(flink_params)/sizeof(flink_params[0]); i++) {
        regex_t regex;
        int reti = regcomp(&regex, flink_params[i].normalized, REG_EXTENDED | REG_NOSUB);
        if (reti) {
            // Handle regex compilation error if needed
            continue;
        }

        reti = regexec(&regex, param, 0, NULL, 0);
        regfree(&regex); // Always free compiled regex

        if (reti == 0) { // Match found
            ConfigResult *result = malloc(sizeof(ConfigResult));
            if (!result) return NULL;

            result->canonical_name = strdup(flink_params[i].canonical);
            result->value = strdup(value);
            result->config_file = strdup(flink_params[i].config_file);

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
// Remember to free the returned FlinkConfigResult and its members after use.
void free_flink_config_result(ConfigResult *result) {
    if (result) {
        free(result->canonical_name);
        free(result->value);
        free(result->config_file);
        free(result);
    }
}

int find_flink_config(char *config_path) {
    // Check FLINK_HOME environment variable
    char *flink_home = getenv("FLINK_HOME");
    if (flink_home != NULL) {
        snprintf(config_path, PATH_MAX, "%s/conf/config.yaml", flink_home);
        if (access(config_path, F_OK) == 0) {
            return 1;
        }
    }

    // Check Bigtop default paths
    const char *bigtop_paths[] = {
        "/etc/flink/conf/config.yaml",
        "/usr/local/flink/conf/config.yaml",
        "/usr/share/flink/conf/config.yaml",
        "/opt/flink/conf/config.yaml",
        NULL
    };

    for (int i = 0; bigtop_paths[i] != NULL; i++) {
        if (access(bigtop_paths[i], F_OK) == 0) {
            strncpy(config_path, bigtop_paths[i], PATH_MAX);
            return 1;
        }
    }

    return 0;
}

ConfigStatus update_flink_config(const char *param, const char *value) {
    char config_path[PATH_MAX];
    if (!find_flink_config(config_path)) {
        return FILE_NOT_FOUND; // Config file not found
   }

    FILE *file = fopen(config_path, "r");
    if (!file) {
        return FILE_READ_ERROR; // Failed to open file for reading
    }

    char **lines = NULL;
    size_t line_count = 0;
    char buffer[1024];
    int param_found = 0;

    // Read and process lines
    while (fgets(buffer, sizeof(buffer), file) != NULL) {
        trim_whitespace(buffer);
        size_t param_len = strlen(param);

        // Check if line starts with the parameter and has a colon immediately after
        if (strncmp(buffer, param, param_len) == 0 && buffer[param_len] == ':') {
            char new_line[1024];
            snprintf(new_line, sizeof(new_line), "%s: %s\n", param, value);
            
            // Allocate space for new line
            char **temp = realloc(lines, (line_count + 1) * sizeof(char *));
            if (!temp) {
                fclose(file);
                return SAVE_FAILED; // Memory allocation failed
            }
            lines = temp;
            
            lines[line_count] = strdup(new_line);
            if (!lines[line_count]) {
                fclose(file);
                free(lines);
                return SAVE_FAILED;
            }
            line_count++;
            param_found = 1;
     } else {
            // Keep original line
            char **temp = realloc(lines, (line_count + 1) * sizeof(char *));
            if (!temp) {
                fclose(file);
                return SAVE_FAILED;
            }
            lines = temp;
            
            lines[line_count] = strdup(buffer);
            if (!lines[line_count]) {
                fclose(file);
                free(lines);
                return SAVE_FAILED;
            }
            line_count++;
        }
    }
    fclose(file);

    // Add new parameter if not found
    if (!param_found) {
        char new_line[1024];
        snprintf(new_line, sizeof(new_line), "%s: %s\n", param, value);
        
        char **temp = realloc(lines, (line_count + 1) * sizeof(char *));
        if (!temp) return SAVE_FAILED;
        lines = temp;
        
        lines[line_count] = strdup(new_line);
        if (!lines[line_count]) {
            for (size_t i = 0; i < line_count; i++) free(lines[i]);
            free(lines);
            return SAVE_FAILED;
        }
        line_count++;
    }

    // Write to file
    file = fopen(config_path, "w");
    if (!file) {
        for (size_t i = 0; i < line_count; i++) free(lines[i]);
        free(lines);
        return FILE_WRITE_ERROR; // Failed to open for writing
    }

    for (size_t i = 0; i < line_count; i++) {
        if (fputs(lines[i], file) == EOF) {
            fclose(file);
            for (size_t j = 0; j < line_count; j++) free(lines[j]);
            free(lines);
            return FILE_WRITE_ERROR; // Write error
        }
    }

    // Cleanup
    fclose(file);
    for (size_t i = 0; i < line_count; i++) free(lines[i]);
    free(lines);

    return SUCCESS; // Success/
}

/////////////////////////////zookeeper/conf////////////////////////////////////////////////

ConfigResult *parse_zookeeper_param(const char *param_name, const char *param_value) {
    // Predefined list of ZooKeeper parameters with canonical names and their normalized forms
static const struct {
    const char *canonical;
    const char *normalized;
} param_map[] = {
    // Original Parameters (Retained)
    { "clientPort", "clientport" },
    { "dataDir", "datadir" },
    { "tickTime", "ticktime" },
    { "initLimit", "initlimit" },
    { "syncLimit", "synclimit" },
    { "maxClientCnxns", "maxclientcnxns" },
    { "autopurge.snapRetainCount", "autopurgesnapretaincount" },
    { "autopurge.purgeInterval", "autopurgepurgeinterval" },
    { "minSessionTimeout", "minsessiontimeout" },
    { "maxSessionTimeout", "maxsessiontimeout" },
    { "electionPort", "electionport" },
    { "leaderServes", "leaderserves" },
    { "server.id", "serverid" },  // Placeholder for dynamic server entries (e.g., server.1)
    { "cnxTimeout", "cnxtimeout" },
    { "standaloneEnabled", "standaloneenabled" },
    { "reconfigEnabled", "reconfigenabled" },
    { "4lw.commands.whitelist", "4lwcommandswhitelist" },
    { "globalOutstandingLimit", "globaloutstandinglimit" },
    { "preAllocSize", "preallocsize" },
    { "snapCount", "snapcount" },

    // Security & Authentication
    { "clientPortAddress", "clientportaddress" },           // Bind address for clientPort
    { "secureClientPort", "secureclientport" },             // SSL port for secure client connections
    { "ssl.keyStore.location", "sslkeystorelocation" },     // Path to keystore
    { "ssl.keyStore.password", "sslkeystorepassword" },     // Keystore password
    { "ssl.trustStore.location", "ssltruststorelocation" }, // Path to truststore
    { "ssl.trustStore.password", "ssltruststorepassword" }, // Truststore password
    { "ssl.hostnameVerification", "sslhostnameverification" }, // Enable/disable hostname verification
    { "authProvider.sasl", "authprovidersasl" },            // SASL authentication provider class
    { "jaasLoginRenew", "jaasloginrenew" },                 // JAAS login context renewal interval
    { "sasl.client.id", "saslclientid" },                   // Client ID for SASL authentication
    { "kerberos.removeHostFromPrincipal", "kerberosremovehostfromprincipal" }, // Strip host from Kerberos principal
    { "kerberos.removeRealmFromPrincipal", "kerberosremoverealmfromprincipal" }, // Strip realm from principal
    { "ssl.clientAuth", "sslclientauth" },                   // Require client SSL certificate
    { "zookeeper.superUser", "zookeepersuperuser" },         // Superuser ACL (e.g., "super:password")

    // Quorum & Ensemble Management
    { "quorum.enableSasl", "quorumenablesasl" },            // Enable SASL for quorum communication
    { "quorum.auth.learnerRequireSasl", "quorumauthlearnerrequiresasl" }, // Require SASL for learners
    { "quorum.auth.serverRequireSasl", "quorumauthserverrequiresasl" }, // Require SASL for servers
    { "quorum.cnxTimeout", "quorumcnxtimeout" },            // Quorum connection timeout
    { "quorum.electionAlg", "quorumelectionalg" },          // Leader election algorithm (e.g., 3)
    { "quorum.portUnification", "quorumportunification" },  // Enable port unification for quorum

    // ACLs & Data Security
    { "skipACL", "skipacl" },                               // Bypass ACL checks (dangerous!)
    { "aclProvider", "aclprovider" },                       // Custom ACL provider class

    // Performance & Advanced Tuning
    { "jute.maxbuffer", "jutemaxbuffer" },                  // Max size of ZNode data buffer
    { "commitProcessor.numWorkerThreads", "commitprocessornumworkerthreads" }, // Worker threads for commits
    { "fsync.warningthresholdms", "fsyncwarningthresholdms" }, // Warn if fsync takes longer than this
    { "forceSync", "forcesync" },                           // Disable fsync (for testing only)
    { "syncEnabled", "syncenabled" },                       // Enable syncing to disk
    { "connectTimeout", "connecttimeout" },                 // Client connection timeout
    { "readTimeout", "readtimeout" },                       // Client read timeout

    // Dynamic Configuration & Admin
    { "dynamicConfigFile", "dynamicconfigfile" },            // Path to dynamic ensemble config
    { "admin.enableServer", "adminenableserver" },          // Enable admin server
    { "admin.serverPort", "adminserverport" },              // Admin server port
    { "admin.serverAddress", "adminserveraddress" },        // Admin server bind address

    // Metrics & Monitoring
    { "metricsProvider.className", "metricsproviderclassname" }, // Metrics provider implementation

    // Network & Client Settings
    { "clientCnxnSocket", "clientcnxnsocket" },             // Custom client connection socket class
    { "client.secure", "clientsecure" },                    // Force client to use secure channel

    // Additional 4LW Controls
    { "4lw.commands.enabled", "4lwcommandsenabled" },       // Enable/disable 4-letter commands

    // Advanced Throttling and NIO
    { "zookeeper.request_throttler.shutdownTimeout", "zookeeperrequestthrottlershutdowntimeout" },
    { "zookeeper.nio.numSelectorThreads", "zookeepernionumselectorthreads" },
    { "zookeeper.nio.numWorkerThreads", "zookeepernionumworkerthreads" },
    { "zookeeper.nio.directBufferBytes", "zookeeperniodirectbufferbytes" }
};  


const size_t num_params = sizeof(param_map) / sizeof(param_map[0]);

    // Normalize the input parameter name
    char normalized[256] = {0};
    size_t j = 0;
    for (size_t i = 0; param_name[i] && j < sizeof(normalized) - 1; ++i) {
        if (isalnum((unsigned char)param_name[i])) {
            normalized[j++] = tolower((unsigned char)param_name[i]);
        }
    }
    normalized[j] = '\0';

    // Search for a matching parameter
    for (size_t i = 0; i < num_params; ++i) {
        if (strcmp(normalized, param_map[i].normalized) == 0) {
            // Allocate and populate the configuration entry
            ConfigResult *entry = malloc(sizeof(ConfigResult));
            if (!entry) return NULL;

            entry->canonical_name = strdup(param_map[i].canonical);
            entry->value = strdup(param_value);

            if (!entry->canonical_name || !entry->value) {
                free(entry->canonical_name);
                free(entry->value);
                free(entry);
                return NULL;
            }

            return entry;
        }
    }

    // No matching parameter found
    return NULL;
}

// ZooKeeper-specific helper functions
bool isTimeMillis(const char *value) {
    char *end;
    long ms = strtol(value, &end, 10);
    if (*end == '\0') return ms > 0;
    if (strcmp(end, "ms") == 0) return ms > 0;
    return false;
}

bool isSizeWithUnit(const char *value) {
    char *end;
    strtol(value, &end, 10);
    if (*end == '\0') return true;
    return end[0] == 'K' || end[0] == 'M' || end[0] == 'G';
}

bool isCommaSeparatedList(const char *value) {
    if (strlen(value) == 0) return false;
    char *copy = strdup(value);
    char *token = strtok(copy, ",");
    while (token != NULL) {
        if (strlen(token) == 0) {
            free(copy);
            return false;
        }
        token = strtok(NULL, ",");
    }
    free(copy);
    return true;
}

ValidationResult validateZooKeeperConfigParam(const char *param_name, const char *value) {
    // Check if parameter exists
    bool param_exists = true;
    if (!param_exists) return ERROR_PARAM_NOT_FOUND;

    // Check value presence
    if (!value || strlen(value) == 0) return ERROR_VALUE_EMPTY;

    // Parameter-specific validation
    if (strcmp(param_name, "clientPort") == 0 ||
        strcmp(param_name, "secureClientPort") == 0 ||
        strcmp(param_name, "electionPort") == 0 ||
        strcmp(param_name, "admin.serverPort") == 0) {
        return isValidPort(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "tickTime") == 0 ||
             strcmp(param_name, "initLimit") == 0 ||
             strcmp(param_name, "syncLimit") == 0 ||
             strcmp(param_name, "cnxTimeout") == 0 ||
             strcmp(param_name, "minSessionTimeout") == 0 ||
             strcmp(param_name, "maxSessionTimeout") == 0) {
        return isTimeMillis(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "leaderServes") == 0 ||
             strcmp(param_name, "standaloneEnabled") == 0 ||
             strcmp(param_name, "reconfigEnabled") == 0 ||
             strcmp(param_name, "ssl.hostnameVerification") == 0 ||
             strcmp(param_name, "kerberos.removeHostFromPrincipal") == 0 ||
             strcmp(param_name, "kerberos.removeRealmFromPrincipal") == 0 ||
             strcmp(param_name, "ssl.clientAuth") == 0 ||
             strcmp(param_name, "quorum.enableSasl") == 0 ||
             strcmp(param_name, "quorum.auth.learnerRequireSasl") == 0 ||
             strcmp(param_name, "quorum.auth.serverRequireSasl") == 0) {
        return isValidBoolean(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "maxClientCnxns") == 0 ||
             strcmp(param_name, "autopurge.snapRetainCount") == 0 ||
             strcmp(param_name, "commitProcessor.numWorkerThreads") == 0 ||
             strcmp(param_name, "zookeeper.nio.numSelectorThreads") == 0 ||
             strcmp(param_name, "zookeeper.nio.numWorkerThreads") == 0) {
        return isPositiveInteger(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "jute.maxbuffer") == 0 ||
             strcmp(param_name, "preAllocSize") == 0 ||
             strcmp(param_name, "snapCount") == 0 ||
             strcmp(param_name, "zookeeper.nio.directBufferBytes") == 0) {
        return isSizeWithUnit(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "4lw.commands.whitelist") == 0) {
        const char *allowed[] = {"conf", "cons", "crst", "dump", "envi", 
                                "ruok", "srst", "srvr", "stat", "wchc", 
                                "wchp", "wchs", "mntr", NULL};
        char *copy = strdup(value);
        char *token = strtok(copy, ",");
        while (token) {
            bool valid = false;
            for (int i = 0; allowed[i]; i++) {
                if (strcmp(token, allowed[i])) valid = true;
            }
            if (!valid) {
                free(copy);
                return ERROR_CONSTRAINT_VIOLATED;
            }
            token = strtok(NULL, ",");
        }
        free(copy);
    }
    else if (strcmp(param_name, "quorum.electionAlg") == 0) {
        int alg = atoi(value);
        if (alg < 0 || alg > 3) return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "ssl.keyStore.location") == 0 ||
             strcmp(param_name, "ssl.trustStore.location") == 0 ||
             strcmp(param_name, "authProvider.sasl") == 0 ||
             strcmp(param_name, "dynamicConfigFile") == 0) {
        return (strlen(value) > 0) ? VALIDATION_OK : ERROR_VALUE_EMPTY;
    }
    else if (strcmp(param_name, "clientPortAddress") == 0 ||
             strcmp(param_name, "admin.serverAddress") == 0) {
        // Basic IP/hostname validation
        return (strlen(value) > 0) ? VALIDATION_OK : ERROR_VALUE_EMPTY;
    }
    else if (strcmp(param_name, "zookeeper.superUser") == 0) {
        return (strchr(value, ':') != NULL ? VALIDATION_OK : ERROR_INVALID_FORMAT);
    }

    return VALIDATION_OK;
}


ConfigStatus modify_zookeeper_config(const char* config_param, const char* value) {
    const char *filename = get_filename_zoo(); // Assume this returns "zoo.cfg" or similar
    const char *zookeeper_home = getenv("ZOOKEEPER_HOME");
    char zk_home_path[PATH_MAX];
    char *file_path = NULL;

    // Check ZOOKEEPER_HOME directory first
    if (zookeeper_home != NULL) {
        snprintf(zk_home_path, sizeof(zk_home_path), "%s/conf/%s", zookeeper_home, filename);
        if (access(zk_home_path, F_OK) == 0) {
            file_path = strdup(zk_home_path);
        }
    }

    // Fallback to default directories if not found in ZOOKEEPER_HOME
    if (!file_path) {
        const char *default_dirs[] = {
            "/user/local/zookeeper",          // Debian
            "/opt/zookeeper/conf",     // Red Hat
            // Add other possible paths if needed
        };
        size_t num_dirs = sizeof(default_dirs) / sizeof(default_dirs[0]);
        for (size_t i = 0; i < num_dirs; i++) {
            char path[PATH_MAX];
            snprintf(path, sizeof(path), "%s/%s", default_dirs[i], filename);
            if (access(path, F_OK) == 0) {
                file_path = strdup(path);
                break;
            }
        }
        if (!file_path) {
            return FILE_NOT_FOUND;
        }
    }

    // Open the original file for reading
    FILE *file = fopen(file_path, "r");
    if (!file) {
        free(file_path);
        return FILE_READ_ERROR;
    }

    // Create temp file path
    char temp_path[PATH_MAX];
    snprintf(temp_path, sizeof(temp_path), "%s.tmp", file_path);
    FILE *temp_file = fopen(temp_path, "w");
    if (!temp_file) {
        fclose(file);
        free(file_path);
        return FILE_WRITE_ERROR;
    }

    char buffer[1024];
    bool found = false;
    size_t param_len = strlen(config_param);

    while (fgets(buffer, sizeof(buffer), file)) {
        char *line = buffer;
        // Trim leading whitespace
        while (isspace((unsigned char)*line)) line++;

        // Skip comment lines
        if (*line == '#') {
            fputs(buffer, temp_file);
            continue;
        }

        char *eq = strchr(line, '=');
        if (eq) {
            // Extract key part (before '=')
            char *key_end = eq;
            // Trim trailing whitespace from key
            while (key_end > line && isspace((unsigned char)*(key_end - 1))) {
                key_end--;
            }
            size_t key_length = key_end - line;

            // Check if key matches config_param
            if (key_length == param_len && strncmp(line, config_param, param_len) == 0) {
                // Replace line with new key=value
                fprintf(temp_file, "%s=%s\n", config_param, value);
                found = true;
            } else {
                // Write original line
                fputs(buffer, temp_file);
            }
        } else {
            // Write non-key-value lines as-is
            fputs(buffer, temp_file);
        }
    }

    // Check for read errors
    if (ferror(file)) {
        fclose(file);
        fclose(temp_file);
        remove(temp_path);
        free(file_path);
        return FILE_READ_ERROR;
    }

    // Append the parameter if not found
    if (!found) {
        fprintf(temp_file, "%s=%s\n", config_param, value);
    }

    // Close files
    fclose(file);
    if (fclose(temp_file) != 0) {
        remove(temp_path);
        free(file_path);
        return FILE_WRITE_ERROR;
    }

    // Replace original file with temp file
    if (remove(file_path) != 0) {
        remove(temp_path);
        free(file_path);
        return FILE_WRITE_ERROR;
    }
    if (rename(temp_path, file_path) != 0) {
        remove(temp_path);
        free(file_path);
        return FILE_WRITE_ERROR;
    }

    free(file_path);
    return SUCCESS;
}

/////////////////////storm//////////////////////////////////////////////////////////


typedef struct {
    const char* canonical_name;
    const char* config_file;
} StormConfigParam;

StormConfigParam storm_predefined_params[] = {
    // Cluster Infrastructure
    {"storm.zookeeper.servers", "storm.yaml"},
    {"storm.zookeeper.port", "storm.yaml"},
    {"storm.zookeeper.root", "storm.yaml"},
    {"storm.zookeeper.session.timeout", "storm.yaml"},
    {"storm.zookeeper.connection.timeout", "storm.yaml"},
    {"storm.local.dir", "storm.yaml"},
    {"storm.cluster.mode", "storm.yaml"},
    
    // Nimbus Configuration
    {"nimbus.seeds", "storm.yaml"},
    {"nimbus.host", "storm.yaml"},
    {"nimbus.thrift.port", "storm.yaml"},
    {"nimbus.task.launch.secs", "storm.yaml"},
    {"nimbus.task.timeout.secs", "storm.yaml"},
    {"nimbus.supervisor.timeout.secs", "storm.yaml"},
    {"nimbus.code.sync.freq.secs", "storm.yaml"},
    {"nimbus.blobstore.class", "storm.yaml"},
    
    // Supervisor Configuration
    {"supervisor.slots.ports", "storm.yaml"},
    {"supervisor.worker.timeout.secs", "storm.yaml"},
    {"supervisor.cpu.capacity", "storm.yaml"},
    {"supervisor.memory.capacity.mb", "storm.yaml"},
    {"supervisor.heartbeat.frequency.secs", "storm.yaml"},
    {"supervisor.monitor.frequency.secs", "storm.yaml"},
    {"supervisor.enable", "storm.yaml"},
    {"supervisor.worker.port", "storm.yaml"},
    
    // Worker Configuration
    {"worker.childopts", "storm.yaml"},
    {"worker.heap.memory.mb", "storm.yaml"},
    {"worker.gc.childopts", "storm.yaml"},
    {"worker.log.level.reset.interval.secs", "storm.yaml"},
    {"worker.profiler.enabled", "storm.yaml"},
    
    // Network and Messaging
    {"storm.messaging.transport", "storm.yaml"},
    {"storm.messaging.netty.buffer_size", "storm.yaml"},
    {"storm.network.topography.plugin", "storm.yaml"},
    {"storm.thrift.socket.timeout.ms", "storm.yaml"},
    
    // UI and Logging
    {"ui.port", "storm.yaml"},
    {"ui.host", "storm.yaml"},
    {"ui.http.x-frame-options", "storm.yaml"},
    {"logviewer.port", "storm.yaml"},
    {"logviewer.max.per.worker.logs.mb", "storm.yaml"},
    {"storm.log4j2.conf.dir", "storm.yaml"},
    
    // Security
    {"storm.kerberos.principal", "storm.yaml"},
    {"storm.kerberos.keytab", "storm.yaml"},
    {"java.security.auth.login.config", "storm.yaml"},
    {"supervisor.run.worker.as.user", "storm.yaml"},
    
    // DRPC Configuration
    {"drpc.servers", "storm.yaml"},
    {"drpc.port", "storm.yaml"},
    {"drpc.worker.threads", "storm.yaml"},
    {"drpc.queue.size", "storm.yaml"},
    
    // Resource Management
    {"topology.priority", "defaults.yaml"},
    {"topology.scheduler.strategy", "defaults.yaml"},
    {"topology.component.resources.onheap.memory.mb", "defaults.yaml"},
    {"topology.component.resources.offheap.memory.mb", "defaults.yaml"},
    {"topology.component.cpu.pcore.percent", "defaults.yaml"},
    
    // Topology Execution
    {"topology.workers", "defaults.yaml"},
    {"topology.acker.executors", "defaults.yaml"},
    {"topology.max.spout.pending", "defaults.yaml"},
    {"topology.message.timeout.secs", "defaults.yaml"},
    {"topology.debug", "defaults.yaml"},
    {"topology.tasks", "defaults.yaml"},
    {"topology.state.checkpoint.interval.ms", "defaults.yaml"},
    {"topology.enable.message.timeouts", "defaults.yaml"},
    
    // Fault Tolerance
    {"topology.state.synchronization.timeout.secs", "defaults.yaml"},
    {"topology.max.task.parallelism", "defaults.yaml"},
    {"topology.worker.gc.ratio", "defaults.yaml"},
    
    // Serialization
    {"topology.multilang.serializer", "defaults.yaml"},
    {"topology.skip.missing.kryo.registrations", "defaults.yaml"},
    {"topology.fall.back.on.java.serialization", "defaults.yaml"},
    
    // Metrics and Monitoring
    {"topology.builtin.metrics.bucket.size.secs", "defaults.yaml"},
    {"topology.stats.sample.rate", "defaults.yaml"},
    {"topology.metrics.consumer.register", "defaults.yaml"},
    
    // Advanced Configuration
    {"storm.blobstore.replication.factor", "storm.yaml"},
    {"storm.health.check.timeout.ms", "storm.yaml"},
    {"topology.auto-credentials", "defaults.yaml"},
    {"topology.enable.classloader", "defaults.yaml"},
    {"topology.testing.always.try.serialize", "defaults.yaml"},
    
    // Transactional Topologies
    {"topology.transactional.id.seed", "defaults.yaml"},
    {"topology.state.provider", "defaults.yaml"},
    
    // Add new parameters here
};
#define NUM_PARAMS sizeof(storm_predefined_params) / sizeof(storm_predefined_params[0])

// Storm-specific helper functions
static bool isMemorySizeMB(const char *value) {
    char *end;
     (void)strtol(value, &end, 10);
    if (*end == '\0') return true;
    if (strcasecmp(end, "mb") == 0) return true;
    return false;
}

static bool isTimeSeconds(const char *value) {
    char *end;
    long seconds = strtol(value, &end, 10);
    return *end == '\0' && seconds > 0;
}

static bool isPercentage(const char *value) {
    char *end;
    float pct = strtof(value, &end);
    return *end == '\0' && pct >= 0.0f && pct <= 100.0f;
}

ValidationResult validateStormConfigParam(const char *param_name, const char *value) {
    // Check if parameter exists
    bool param_exists = false;
    for (size_t i = 0; i < sizeof(storm_predefined_params)/sizeof(storm_predefined_params[0]); i++) {
        if (strcmp(param_name, storm_predefined_params[i].canonical_name) == 0) {
            param_exists = true;
            break;
        }
    }
    if (!param_exists) return ERROR_PARAM_NOT_FOUND;

    // Check value presence
    if (!value || strlen(value) == 0) return ERROR_VALUE_EMPTY;

    // Parameter-specific validation
    if (strcmp(param_name, "nimbus.thrift.port") == 0 ||
        strcmp(param_name, "ui.port") == 0 ||
        strcmp(param_name, "logviewer.port") == 0 ||
        strcmp(param_name, "drpc.port") == 0) {
        return isValidPort(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "storm.zookeeper.port") == 0) {
        return isValidPort(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "supervisor.slots.ports") == 0) {
        char *copy = strdup(value);
        char *token = strtok(copy, ",");
        while (token) {
            if (!isValidPort(token)) {
                free(copy);
                return ERROR_INVALID_FORMAT;
            }
            token = strtok(NULL, ",");
        }
        free(copy);
    }
    else if (strcmp(param_name, "worker.heap.memory.mb") == 0 ||
             strcmp(param_name, "supervisor.memory.capacity.mb") == 0 ||
             strcmp(param_name, "logviewer.max.per.worker.logs.mb") == 0) {
        return isMemorySizeMB(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "supervisor.enable") == 0 ||
             strcmp(param_name, "worker.profiler.enabled") == 0 ||
             strcmp(param_name, "topology.debug") == 0 ||
             strcmp(param_name, "supervisor.run.worker.as.user") == 0) {
        return isValidBoolean(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "storm.zookeeper.session.timeout") == 0 ||
             strcmp(param_name, "storm.zookeeper.connection.timeout") == 0 ||
             strcmp(param_name, "nimbus.task.launch.secs") == 0 ||
             strcmp(param_name, "nimbus.task.timeout.secs") == 0) {
        return isTimeSeconds(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "storm.zookeeper.servers") == 0 ||
             strcmp(param_name, "nimbus.seeds") == 0 ||
             strcmp(param_name, "drpc.servers") == 0) {
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
    else if (strcmp(param_name, "supervisor.cpu.capacity") == 0 ||
             strcmp(param_name, "topology.component.cpu.pcore.percent") == 0) {
        return isPercentage(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "topology.workers") == 0 ||
             strcmp(param_name, "topology.acker.executors") == 0 ||
             strcmp(param_name, "topology.tasks") == 0) {
        return isPositiveInteger(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "storm.cluster.mode") == 0) {
        return (strcmp(value, "local") == 0 || strcmp(value, "distributed") == 0)
            ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "storm.messaging.transport") == 0) {
        return (strstr(value, "netty") != NULL || strstr(value, "zmq") != NULL)
            ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "topology.scheduler.strategy") == 0) {
        return (strstr(value, "default") != NULL || strstr(value, "resource") != NULL)
            ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "storm.kerberos.keytab") == 0 ||
             strcmp(param_name, "java.security.auth.login.config") == 0) {
        return (strlen(value) > 0) ? VALIDATION_OK : ERROR_VALUE_EMPTY;
    }

    return VALIDATION_OK;
}

char* normalize_storm_param_name(const char* param_name) {
    if (!param_name) return NULL;
    size_t len = strlen(param_name);
    if (len == 0) return strdup("");

    // First pass: handle camelCase and convert to lowercase
    char* buffer = malloc(2 * len + 1);
    if (!buffer) return NULL;

    size_t j = 0;
    for (size_t i = 0; i < len; i++) {
        char c = param_name[i];
        if (i > 0 && isupper(c)) {
            char prev = param_name[i - 1];
            if (islower(prev) || isdigit(prev)) {
                buffer[j++] = '.';
            }
        }
        buffer[j++] = tolower(c);
    }
    buffer[j] = '\0';

    // Second pass: replace non-alnum with dots and handle sequences
    char* normalized = malloc(j + 1);
    if (!normalized) {
        free(buffer);
        return NULL;
    }

    size_t k = 0;
    char prev = '\0';
    for (size_t i = 0; i < j; i++) {
        char c = buffer[i];
        if (isalnum(c)) {
            normalized[k++] = c;
            prev = c;
        } else {
            if (prev != '.') {
                normalized[k++] = '.';
                prev = '.';
            }
        }
    }
    normalized[k] = '\0';
    free(buffer);

    // Collapse consecutive dots
    char* collapsed = malloc(k + 1);
    if (!collapsed) {
        free(normalized);
        return NULL;
    }

    size_t c_idx = 0;
    prev = '\0';
    for (size_t i = 0; i < k; i++) {
        char current = normalized[i];
        if (current == '.') {
            if (prev != '.') {
                collapsed[c_idx++] = current;
            }
        } else {
            collapsed[c_idx++] = current;
        }
        prev = current;
    }
    collapsed[c_idx] = '\0';
    free(normalized);

    // Trim leading and trailing dots
    size_t start = 0;
    while (start < c_idx && collapsed[start] == '.') {
        start++;
    }

    size_t end = c_idx;
    while (end > start && collapsed[end - 1] == '.') {
        end--;
    }

    char* trimmed = malloc(end - start + 1);
    if (!trimmed) {
        free(collapsed);
        return NULL;
    }
    strncpy(trimmed, collapsed + start, end - start);
    trimmed[end - start] = '\0';
    free(collapsed);

    return trimmed;
}

ConfigResult* validate_storm_config_param(const char* param_name, const char* param_value) {
    if (!param_name || !param_value) return NULL;

    char* normalized = normalize_storm_param_name(param_name);
    if (!normalized) return NULL;

    for (size_t i = 0; i < NUM_PARAMS; i++) {
        if (strcmp(normalized, storm_predefined_params[i].canonical_name) == 0) {
            ConfigResult* result = malloc(sizeof(ConfigResult));
            if (!result) {
                free(normalized);
                return NULL;
            }

            result->canonical_name = strdup(storm_predefined_params[i].canonical_name);
            result->value = strdup(param_value);
            result->config_file = strdup(storm_predefined_params[i].config_file);

            free(normalized);

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

    free(normalized);
    return NULL;
}

// Example usage and memory cleanup
void free_config_result(ConfigResult* result) {
    if (result) {
        free(result->canonical_name);
        free(result->value);
        free(result->config_file);
        free(result);
    }
}


ConfigStatus modify_storm_config(const char *param, const char *value, const char *filename) {
    // Validate configuration file name
    if (strcmp(filename, "storm.yaml") != 0 && strcmp(filename, "defaults.yaml") != 0) {
        return SAVE_FAILED;
    }

    const char *storm_home = getenv("STORM_HOME");
    char filepath[1024];
    bool found = false;
    size_t line_count = 0;
    char      **lines = NULL;

    // Check STORM_HOME configuration path
    if (storm_home != NULL) {
        snprintf(filepath, sizeof(filepath), "%s/conf/%s", storm_home, filename);
        if (access(filepath, F_OK) == 0) {
            found = true;
        }
    }

    // Check Bigtop default paths if not found in STORM_HOME
    if (!found) {
        snprintf(filepath, sizeof(filepath), "/opt/storm/conf/%s", filename);
        if (access(filepath, F_OK) == 0) {
            found = true;
        } else {
            snprintf(filepath, sizeof(filepath), "/usr/local/storm/conf/%s", filename);
            if (access(filepath, F_OK) == 0) {
                found = true;
            } else {
                return FILE_NOT_FOUND;
            }
        }
    }

    // Open the configuration file for reading
    FILE *file = fopen(filepath, "r");
    if (!file) {
        return FILE_NOT_FOUND;
    }

    char buffer[1024];

    while (fgets(buffer, sizeof(buffer), file)) {
        char *original_line = strdup(buffer);
        if (!original_line) {
            fclose(file);
            for (size_t i = 0; i < line_count; i++) free(lines[i]);
            free(lines);
            return SAVE_FAILED;
        }

        char *line_copy = strdup(buffer);
        if (!line_copy) {
            free(original_line);
            fclose(file);
            for (size_t i = 0; i < line_count; i++) free(lines[i]);
            free(lines);
            return SAVE_FAILED;
        }

        trim_whitespace(line_copy);

        // Skip comment lines and empty lines
        if (line_copy[0] == '#' || line_copy[0] == '\0') {
            free(line_copy);
            lines = realloc(lines, (line_count + 1) * sizeof(char *));
            if (!lines) {
                free(original_line);
                fclose(file);
                return SAVE_FAILED;
            }
            lines[line_count++] = original_line;
            continue;
        }

        char *colon = strchr(line_copy, ':');
        if (colon) {
            *colon = '\0';
            char *key_part = line_copy;
            trim_whitespace(key_part);

            if (strcmp(key_part, param) == 0) {
                // Replace the line with the new key-value pair
                free(original_line);
                free(line_copy);
                size_t new_line_len = strlen(param) + strlen(value) + 3; // ": " and "\n"
                char *new_line = malloc(new_line_len);
                if (!new_line) {
                    fclose(file);
                    for (size_t i = 0; i < line_count; i++) free(lines[i]);
                    free(lines);
                    return SAVE_FAILED;
                }
                snprintf(new_line, new_line_len, "%s: %s\n", param, value);
                lines = realloc(lines, (line_count + 1) * sizeof(char *));
                if (!lines) {
                    free(new_line);
                    fclose(file);
                    return SAVE_FAILED;
                }
                lines[line_count++] = new_line;
             //   param_found = true;
            } else {
                // Not the target parameter, keep original line
                free(line_copy);
                lines = realloc(lines, (line_count + 1) * sizeof(char *));
                if (!lines) {
                    free(original_line);
                    fclose(file);
                    return SAVE_FAILED;
                }
                lines[line_count++] = original_line;
            }
        } else {
            // Not a key-value line, preserve original
            free(line_copy);
            lines = realloc(lines, (line_count + 1) * sizeof(char *));
            if (!lines) {
                free(original_line);
                fclose(file);
                return SAVE_FAILED;
            }
            lines[line_count++] = original_line;
        }
    }

    fclose(file);

    // Add new parameter if not found
    if (found) {
        size_t new_line_len = strlen(param) + strlen(value) + 3;
        char *new_line = malloc(new_line_len);
        if (!new_line) {
            for (size_t i = 0; i < line_count; i++) free(lines[i]);
            free(lines);
            return SAVE_FAILED;
        }
        snprintf(new_line, new_line_len, "%s: %s\n", param, value);
        lines = realloc(lines, (line_count + 1) * sizeof(char *));
        if (!lines) {
            free(new_line);
            for (size_t i = 0; i < line_count; i++) free(lines[i]);
            free(lines);
            return SAVE_FAILED;
        }
        lines[line_count++] = new_line;
    }

    // Write the updated configuration back to the filer
    file = fopen(filepath, "w");
    if (!file) {
        for (size_t i = 0; i < line_count; i++) free(lines[i]);
        free(lines);
        return SAVE_FAILED;
    }

    for (size_t i = 0; i < line_count; i++) {
        if (fputs(lines[i], file) == EOF) {
            fclose(file);
            for (size_t j = 0; j < line_count; j++) free(lines[j]);
            free(lines);
            return SAVE_FAILED;
        }
        free(lines[i]);
    }

    free(lines);
    fclose(file);

    return SUCCESS;
}


////////hive/////////////////////////////////////////////////////////////////


typedef struct {
    const char* canonical_name;
    const char* config_file;
} PredefinedHiveParameter;

char* generate_regex_pattern(const char* canonical_name) {
    char* copy = strdup(canonical_name);
    if (!copy) return NULL;

    int num_parts = 1;
    for (char* p = copy; *p; p++) {
        if (*p == '.') num_parts++;
    }

    char** parts = malloc(num_parts * sizeof(char*));
    if (!parts) {
        free(copy);
        return NULL;
    }

    int i = 0;
    char* token = strtok(copy, ".");
    while (token != NULL) {
        parts[i++] = token;
        token = strtok(NULL, ".");
    }

    size_t regex_len = 2; // For ^ and $
    regex_len += (i - 1) * strlen("[._-]+");
    for (int j = 0; j < i; j++) {
        regex_len += strlen(parts[j]);
    }

    // Check for potential overflow when adding 1 to regex_len
    if (regex_len > SIZE_MAX - 1) {
        fprintf(stderr, "Error: regex_len is too large\n");
        free(copy);
        free(parts);
        return NULL;
    }

    size_t needed = regex_len + 1;
    // Check if the needed size exceeds system's maximum allowed allocation size
    if (needed > (size_t)SSIZE_MAX) {
        fprintf(stderr, "Error: regex pattern exceeds maximum allowed size\n");
        free(copy);
        free(parts);
        return NULL;
    }

    char* regex_pattern = malloc(needed);
    if (!regex_pattern) {
        free(copy);
        free(parts);
        return NULL;
    }
    regex_pattern[0] = '\0';

    strcat(regex_pattern, "^");
    for (int j = 0; j < i; j++) {
        strcat(regex_pattern, parts[j]);
        if (j < i - 1) {
            strcat(regex_pattern, "[._-]+");
        }
    }
    strcat(regex_pattern, "$");

    free(copy);
    free(parts);
    return regex_pattern;
}

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
    { "hive.support.sql11.reserved.keywords", "hive-site.xml" }
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

static bool isJDBCURL(const char *value) {
    return strstr(value, "jdbc:") != NULL && strlen(value) > 10;
}

static bool isValidCompressionCodec(const char *value) {
    const char *valid[] = {"NONE", "SNAPPY", "GZIP", "LZO", "ZSTD", NULL};
    for (int i = 0; valid[i]; i++)
        if (strcmp(value, valid[i]) == 0) return true;
    return false;
}

ValidationResult validateHiveConfigParam(const char *param_name, const char *value) {
    // Check if parameter exists
    bool param_exists = true;
    if (!param_exists) return ERROR_PARAM_NOT_FOUND;

    // Check value presence
    if (!value || strlen(value) == 0) return ERROR_VALUE_EMPTY;

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

/////////////////////////hue//////////////////////////////////////////////

/*
 * Locates Hue configuration file using environment variables or common paths.
 *
 * Populates config_path buffer with found configuration file path.
 * Returns:
 *   - SUCCESS if configuration file exists
 *   - FILE_NOT_FOUND if no valid configuration found
 */
ConfigStatus
find_hue_config(char *config_path)
{
    char       *hue_home;

    /* First check HUE_HOME environment variable */
    hue_home = getenv("HUE_HOME");
    if (hue_home != NULL)
    {
        snprintf(config_path, PATH_MAX, "%s/conf/%s", hue_home, HUE_CONFIG_FILE);
        if (access(config_path, F_OK) == 0)
        {
            return SUCCESS;
        }
    }

    /* Check common Bigtop installation paths */
    const char *bigtop_paths[] = {
        "/etc/hue/conf/hue.ini",
        "/usr/lib/hue/conf/hue.ini",
        "/usr/share/hue/conf/hue.ini",
        "/opt/hue/conf/hue.ini",
        NULL
    };

    for (int i = 0; bigtop_paths[i] != NULL; i++)
    {
        if (access(bigtop_paths[i], F_OK) == 0)
        {
            strncpy(config_path, bigtop_paths[i], PATH_MAX);
            return SUCCESS;
        }
    }

    return FILE_NOT_FOUND;
}

/*
 * Updates Hue configuration parameter in INI-style configuration file.
 *
 * Parameter must be formatted as "section.key". Creates new sections/keys
 * if they don't exist.
 *
 * Returns:
 *   ConfigStatus indicating operation success or failure reason
 */
ConfigStatus
set_hue_config(const char *param, const char *value)
{
    char        config_path[PATH_MAX];
    ConfigStatus status = SAVE_FAILED; /* Default to generic error */
    char       *section = NULL;
    char       *key = NULL;
    FILE       *file = NULL;
    char      **lines = NULL;
    size_t      line_count = 0;
    int         in_target_section = 0;
    int         key_found = 0;
    size_t      section_pos = 0;
    size_t      key_pos = 0;

    /* Validate input parameters */
    if (param == NULL || value == NULL)
    {
        return INVALID_CONFIG_FILE;
    }

    /* Locate configuration file */
    status = find_hue_config(config_path);
    if (status != SUCCESS)
    {
        return status;
    }

    /* Split parameter into section and key (format: "section.key") */
    char       *last_dot = strrchr(param, '.');
    if (last_dot == NULL || last_dot == param)
    {
        return INVALID_CONFIG_FILE; /* Invalid parameter format */
    }

    size_t      section_len = last_dot - param;
    section = malloc(section_len + 1);
    if (section == NULL)
    {
        return SAVE_FAILED;
    }
    strncpy(section, param, section_len);
    section[section_len] = '\0';
    key = last_dot + 1;

    /* Open configuration file for reading */
    file = fopen(config_path, "r");
    if (file == NULL)
    {
        status = FILE_READ_ERROR;
        goto cleanup;
    }

    /* Read all lines into memory */
    char        buffer[MAX_LINE_LENGTH];
    while (fgets(buffer, sizeof(buffer), file) != NULL)
    {
        char       *line = strdup(buffer);
        char      **temp;

        if (line == NULL)
        {
            status = SAVE_FAILED;
            goto cleanup;
        }

        /* Resize lines array */
        temp = realloc(lines, (line_count + 1) * sizeof(char *));
        if (temp == NULL)
        {
            free(line);
            status = SAVE_FAILED;
            goto cleanup;
        }
        lines = temp;
        lines[line_count++] = line;

        /* Identify sections and keys */
        char       *trimmed = trim(strdup(line));
        if (trimmed[0] == '[')
        {
            /* Section header detected */
            char       *end = strchr(trimmed, ']');
            if (end != NULL)
            {
                *end = '\0';
                char       *current_section = trim(trimmed + 1);

                if (strcmp(current_section, section) == 0)
                {
                    in_target_section = 1;
                    section_pos = line_count - 1; /* Track section position */
                }
                else
                {
                    in_target_section = 0;
                }
            }
            free(trimmed);
            continue;
        }

        /* Check for key in target section */
        if (in_target_section)
        {
            char       *eq = strchr(trimmed, '=');
            if (eq != NULL)
            {
                *eq = '\0';
                char       *current_key = trim(trimmed);
                if (strcmp(current_key, key) == 0)
                {
                    key_found = 1;
                    key_pos = line_count - 1; /* Track key position */
                }
            }
        }
        free(trimmed);
    }

    /* Check for read errors */
    if (ferror(file))
    {
        status = FILE_READ_ERROR;
        goto cleanup;
    }
    fclose(file);
    file = NULL;

    /* Prepare new configuration line */
    char        new_line[MAX_LINE_LENGTH];
    snprintf(new_line, sizeof(new_line), "%s = %s\n", key, value);

    /* Update or add configuration */
    if (key_found)
    {
        /* Replace existing key */
        free(lines[key_pos]);
        lines[key_pos] = strdup(new_line);
        if (lines[key_pos] == NULL)
        {
            status = SAVE_FAILED;
            goto cleanup;
        }
    }
    else if (section_pos != 0 || in_target_section)
    {
        /* Insert after section header */
        char      **new_lines = realloc(lines, (line_count + 1) * sizeof(char *));
        if (new_lines == NULL)
        {
            status = SAVE_FAILED;
            goto cleanup;
        }
        lines = new_lines;
        memmove(&lines[section_pos + 2], &lines[section_pos + 1],
                (line_count - section_pos - 1) * sizeof(char *));
        lines[section_pos + 1] = strdup(new_line);
        if (lines[section_pos + 1] == NULL)
        {
            status = SAVE_FAILED;
            goto cleanup;
        }
        line_count++;
    }
    else
    {
        /* Add new section and key */
        char      **new_lines = realloc(lines, (line_count + 2) * sizeof(char *));
        if (new_lines == NULL)
        {
            status = SAVE_FAILED;
            goto cleanup;
        }
        lines = new_lines;
        lines[line_count] = strdup("\n");
        lines[line_count + 1] = malloc(MAX_LINE_LENGTH);
        if (lines[line_count] == NULL || lines[line_count + 1] == NULL)
        {
            status = SAVE_FAILED;
            goto cleanup;
        }
        int written = snprintf(lines[line_count + 1], MAX_LINE_LENGTH, "[%s]\n%s", section, new_line);

if (written >= MAX_LINE_LENGTH) {
    fprintf(stderr, "Warning: Truncated output in set_hue_config\n");
}


        line_count += 2;
    }

    /* Write updated configuration back to file */
    file = fopen(config_path, "w");
    if (file == NULL)
    {
        status = FILE_WRITE_ERROR;
        goto cleanup;
    }

    for (size_t i = 0; i < line_count; i++)
    {
        if (fputs(lines[i], file) == EOF)
        {
            status = FILE_WRITE_ERROR;
            goto cleanup;
        }
    }

    status = SUCCESS;

cleanup:
    /* Release allocated resources */
    if (section != NULL)
    {
        free(section);
    }
    if (file != NULL)
    {
        fclose(file);
    }
    if (lines != NULL)
    {
        for (size_t i = 0; i < line_count; i++)
        {
            free(lines[i]);
        }
        free(lines);
    }

    return status;
}///////////////////////////////////oozie////////////////////////////////


ConfigStatus modify_oozie_config(const char* config_param, const char* value) {
    const char *filename = get_filename_oozie();
    const char *oozie_home = getenv("OOZIE_HOME");
    const char *bigtop_dir = "/etc/oozie/conf"; // Adjust if necessary
    char path_buffer[PATH_MAX];
    char *file_path = NULL;

    // Check OOZIE_HOME/conf directory
    if (oozie_home != NULL) {
        snprintf(path_buffer, sizeof(path_buffer), "%s/conf/%s", oozie_home, filename);
        if (access(path_buffer, F_OK) == 0) {
            file_path = strdup(path_buffer);
        }
    }

    // Fallback to Bigtop directory if not found
    if (!file_path) {
        snprintf(path_buffer, sizeof(path_buffer), "%s/%s", bigtop_dir, filename);
        if (access(path_buffer, F_OK) == 0) {
            file_path = strdup(path_buffer);
        } else {
            return FILE_NOT_FOUND;
        }
    }

    // Parse XML document
    xmlDoc *doc = xmlReadFile(file_path, NULL, 0);
    if (!doc) {
        free(file_path);
        return XML_PARSE_ERROR;
    }

    xmlNode *root = xmlDocGetRootElement(doc);
    if (!root || xmlStrcmp(root->name, BAD_CAST "configuration") != 0) {
        xmlFreeDoc(doc);
        free(file_path);
        return XML_INVALID_ROOT;
    }

    xmlNode *target_prop = NULL;
    // Search for existing property
    for (xmlNode *node = root->children; node; node = node->next) {
        if (node->type == XML_ELEMENT_NODE && xmlStrcmp(node->name, BAD_CAST "property") == 0) {
            xmlNode *name_node = NULL;
            for (xmlNode *child = node->children; child; child = child->next) {
                if (child->type == XML_ELEMENT_NODE && xmlStrcmp(child->name, BAD_CAST "name") == 0) {
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
            if (child->type == XML_ELEMENT_NODE && xmlStrcmp(child->name, BAD_CAST "value") == 0) {
                value_node = child;
                break;
            }
        }
        if (value_node) {
            xmlNodeSetContent(value_node, BAD_CAST value);
        } else {
            // Create new value node if missing
            if (!xmlNewTextChild(target_prop, NULL, BAD_CAST "value", BAD_CAST value)) {
                xmlFreeDoc(doc);
                free(file_path);
                return XML_UPDATE_ERROR;
            }
        }
    } else {
        // Add new property
        xmlNode *new_prop = xmlNewNode(NULL, BAD_CAST "property");
        if (!new_prop) {
            xmlFreeDoc(doc);
            free(file_path);
            return XML_UPDATE_ERROR;
        }
        if (!xmlNewTextChild(new_prop, NULL, BAD_CAST "name", BAD_CAST config_param)) {
            xmlFreeNode(new_prop);
            xmlFreeDoc(doc);
            free(file_path);
            return XML_UPDATE_ERROR;
        }
        if (!xmlNewTextChild(new_prop, NULL, BAD_CAST "value", BAD_CAST value)) {
            xmlFreeNode(new_prop);
            xmlFreeDoc(doc);
            free(file_path);
            return XML_UPDATE_ERROR;
        }
        xmlAddChild(root, new_prop);
    }

    // Save changes to file
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
////////////////////////////////livy//////////////////////////////////////////

static const char *canonical_params[] = {
    /* Core Server Configuration */
    "livy.server.port",
    "livy.server.host",
    "livy.server.session.timeout",
    "livy.server.session.state-retain.sec",
    "livy.server.session.factory",
    "livy.server.recovery.mode",
    "livy.server.recovery.state-store",
    "livy.server.recovery.state-store.url",

    /* Security & Authentication */
    "livy.server.auth.type",
    "livy.keystore",
    "livy.keystore.password",
    "livy.truststore",
    "livy.truststore.password",
    "livy.server.auth.kerberos.principal",
    "livy.server.auth.kerberos.keytab",
    "livy.server.auth.ldap.url",
    "livy.server.auth.ldap.baseDN",
    "livy.server.auth.ldap.userDNPattern",
    "livy.server.auth.ldap.groupDNPattern",
    "livy.server.auth.jwt.public-key",
    "livy.server.auth.jwt.issuer",
    "livy.server.auth.jwt.audience",
    "livy.server.impersonation.enabled",
    "livy.server.impersonation.allowed.users",
    "livy.server.access-control.enabled",
    "livy.server.access-control.users",
    "livy.server.access-control.groups",
    "livy.server.launch.kerberos.principal",
    "livy.server.launch.kerberos.keytab",
    "livy.server.superusers",

    /* Spark Configuration */
    "livy.spark.master",
    "livy.spark.deploy-mode",
    "livy.spark.home",
    "livy.spark.submit.deployMode",
    "livy.spark.submit.proxyUser",
    "livy.spark.driver.cores",
    "livy.spark.driver.memory",
    "livy.spark.executor.cores",
    "livy.spark.executor.memory",
    "livy.spark.dynamicAllocation.enabled",
    "livy.spark.dynamicAllocation.minExecutors",
    "livy.spark.dynamicAllocation.maxExecutors",
    "livy.spark.dynamicAllocation.initialExecutors",

    /* Resource Management */
    "livy.spark.yarn.queue",
    "livy.spark.yarn.archives",
    "livy.spark.yarn.dist.files",
    "livy.spark.yarn.maxAppAttempts",
    "livy.spark.kubernetes.namespace",
    "livy.spark.kubernetes.container.image",
    "livy.spark.kubernetes.authenticate.driver.serviceAccountName",
    "livy.spark.kubernetes.driver.podTemplateFile",
    "livy.spark.kubernetes.executor.podTemplateFile",

    /* Session Management */
    "livy.file.local-dir",
    "livy.file.local-dir-whitelist",
    "livy.server.session.max_creation_time",
    "livy.server.session.heartbeat.timeout",
    "livy.server.session.max_sessions_per_user",
    "livy.rsc.server-address",
    "livy.rsc.jvm.opts",
    "livy.rsc.sparkr.package",
    "livy.rsc.livy-jars",

    /* Interactive & Batch Processing */
    "livy.repl.enableHiveContext",
    "livy.batch.retained",

    /* UI & Monitoring */
    "livy.ui.enabled",
    "livy.ui.session-list.max",
    "livy.metrics.enabled",
    "livy.metrics.reporters",
    "livy.metrics.jmx.domain",
    "livy.server.request-log.enabled",
    "livy.server.access-log.enabled",

    /* Network & Security Protocols */
    "livy.server.csrf-protection.enabled",
    "livy.server.cors.enabled",
    "livy.server.cors.allowed-origins",
    "livy.server.cors.allowed-methods",
    "livy.server.cors.allowed-headers",
    "livy.server.cors.exposed-headers",

    /* YARN/Kubernetes Specific */
    "livy.yarn.app-name",
    "livy.yarn.config-file",
    "livy.yarn.jar",
    "livy.yarn.poll-interval",
    "livy.kubernetes.truststore.secret",
    "livy.kubernetes.truststore.password.secret",
    "livy.kubernetes.keystore.secret",
    "livy.kubernetes.keystore.password.secret"
};

static bool isValidAuthType(const char *value) {
    const char *valid[] = {"kerberos", "ldap", "jwt", "basic", "none", NULL};
    for (int i = 0; valid[i]; i++)
        if (strcmp(value, valid[i]) == 0) return true;
    return false;
}

static bool isValidSparkMaster(const char *value) {
    return strncmp(value, "local", 5) == 0 ||
           strncmp(value, "yarn", 4) == 0 ||
           strncmp(value, "k8s://", 5) == 0 ||
           strstr(value, "spark://") != NULL;
}

ValidationResult validateLivyConfigParam(const char *param_name, const char *value) {
    // Check if parameter exists
    bool param_exists = false;
    for (size_t i = 0; i < sizeof(canonical_params)/sizeof(canonical_params[0]); i++) {
        if (strcmp(param_name, canonical_params[i]) == 0) {
            param_exists = true;
            break;
        }
    }
    if (!param_exists) return ERROR_PARAM_NOT_FOUND;

    // Check value presence
    if (!value || strlen(value) == 0) return ERROR_VALUE_EMPTY;

    // Parameter-specific validation
    if (strcmp(param_name, "livy.server.port") == 0) {
        return isValidPort(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "livy.spark.master") == 0) {
        return isValidSparkMaster(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "livy.server.auth.type") == 0) {
        return isValidAuthType(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "livy.spark.driver.memory") == 0 ||
             strcmp(param_name, "livy.spark.executor.memory") == 0) {
        return isMemorySize(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "livy.spark.driver.cores") == 0 ||
             strcmp(param_name, "livy.spark.executor.cores") == 0 ||
             strcmp(param_name, "livy.spark.dynamicAllocation.minExecutors") == 0 ||
             strcmp(param_name, "livy.spark.dynamicAllocation.maxExecutors") == 0) {
        return isPositiveInteger(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "livy.server.session.timeout") == 0 ||
             strcmp(param_name, "livy.server.session.state-retain.sec") == 0) {
        return isTimeSeconds(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "livy.server.impersonation.enabled") == 0 ||
             strcmp(param_name, "livy.ui.enabled") == 0 ||
             strcmp(param_name, "livy.server.csrf-protection.enabled") == 0) {
        return isValidBoolean(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "livy.server.auth.ldap.url") == 0) {
        return strstr(value, "ldap://") != NULL || strstr(value, "ldaps://") != NULL
            ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "livy.spark.deploy-mode") == 0) {
        return strcmp(value, "client") == 0 || strcmp(value, "cluster") == 0
            ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "livy.server.access-control.users") == 0 ||
             strcmp(param_name, "livy.server.access-control.groups") == 0) {
        return (strlen(value) > 0) ? VALIDATION_OK : ERROR_VALUE_EMPTY;
    }
    else if (strcmp(param_name, "livy.spark.yarn.queue") == 0) {
        return (strlen(value) > 0) ? VALIDATION_OK : ERROR_VALUE_EMPTY;
    }
    else if (strcmp(param_name, "livy.server.recovery.mode") == 0) {
        return strcmp(value, "off") == 0 || strcmp(value, "recovery") == 0
            ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }

    return VALIDATION_OK;
}

#define NUM_CANONICAL_PARAMS (sizeof(canonical_params)/sizeof(canonical_params[0]))


ConfigResult* parse_livy_config_param(const char *input_key, const char *input_value) {

ConfigResult* result = malloc(sizeof(ConfigResult));

    for (size_t i = 0; i < NUM_CANONICAL_PARAMS; i++) {
        const char *canonical = canonical_params[i];
        char *pattern = generate_regex_pattern(canonical);
        if (!pattern) continue;

        regex_t regex;
        int ret = regcomp(&regex, pattern, REG_ICASE | REG_EXTENDED);
        if (ret != 0) {
            free(pattern);
            continue;
        }

        ret = regexec(&regex, input_key, 0, NULL, 0);
        regfree(&regex);
        free(pattern);

        if (ret == 0) {
            result->canonical_name = strdup(canonical);
            result->value = strdup(input_value);
            return result;
        }
    }

    return result;
}

/*
 * Update Livy configuration parameter in properties-style configuration file
 *
 * Parameters:
 *   param - Configuration parameter name to set/update
 *   value - New value for the parameter
 *
 * Returns:
 *   SUCCESS if operation completed successfully
 *   FILE_NOT_FOUND if configuration file not found
 *   FILE_READ_ERROR if file access issues occur
 *   FILE_WRITE_ERROR if write operations fail
 *   SAVE_FAILED if final file replacement fails
 */
ConfigStatus set_livy_config(const char *param, const char *value) {
    const char *livy_home = getenv("LIVY_HOME");
    char config_path[MAX_PATH_LEN] = {0};
    char temp_path[MAX_PATH_LEN] = {0};
    bool found_config = false;
    ConfigStatus status = SUCCESS;
    FILE *file = NULL;
    FILE *temp_file = NULL;
    int fd = -1;

    /* Phase 1: Determine configuration file location */
    const char *search_paths[] = {
        livy_home ? livy_home : "",
        "/opt/livy",
        "/usr/local/livy"
    };

    for (int i = 0; i < 3; i++) {
        if (search_paths[i][0] == '\0') continue;

        size_t written = snprintf(config_path, sizeof(config_path), "%s/conf/livy.conf", search_paths[i]);
        if (written >= sizeof(config_path)) {
            status = FILE_NOT_FOUND;
            goto cleanup;
        }

        if (access(config_path, F_OK) == 0) {
            found_config = true;
            break;
        }
    }

    if (!found_config) {
        status = FILE_NOT_FOUND;
        goto cleanup;
    }

    /* Phase 2: Open existing configuration file */
    if (!(file = fopen(config_path, "r"))) {
        status = FILE_READ_ERROR;
        goto cleanup;
    }

    /* Phase 3: Temporary file creation */
    size_t written = snprintf(temp_path, sizeof(temp_path), "%s.XXXXXX", config_path);
    if (written >= sizeof(temp_path)) {
        status = FILE_WRITE_ERROR;
        goto cleanup;
    }

    if ((fd = mkstemp(temp_path)) == -1) {
        status = FILE_WRITE_ERROR;
        goto cleanup;
    }

    if (!(temp_file = fdopen(fd, "w"))) {
        status = FILE_WRITE_ERROR;
        goto cleanup;
    }

    /* Phase 4: Process existing configuration */
    char line[4096];
    bool param_found = false;
    size_t param_len = strlen(param);

    while (fgets(line, sizeof(line), file)) {
        char *trimmed = line;
        while (isspace(*trimmed)) trimmed++;

        // Preserve comments and empty lines
        if (*trimmed == '#' || *trimmed == '\0') {
            fputs(line, temp_file);
            continue;
        }

        // Check parameter match
        if (strncmp(trimmed, param, param_len) == 0) {
            char *after_param = trimmed + param_len;
            char *separator = after_param;

            // Skip whitespace after parameter
            while (isspace(*separator)) separator++;

            // Determine separator type
            bool use_equals = (*separator == '=');
            if (use_equals) {
                separator++;
                while (isspace(*separator)) separator++;
            }

            // Find comment part
            char *comment = strchr(after_param, '#');

            // Write updated parameter
            if (use_equals) {
                fprintf(temp_file, "%s=%s", param, value);
            } else {
                fprintf(temp_file, "%s %s", param, value);
            }

            // Preserve comment if exists
            if (comment) {
                fprintf(temp_file, " %s", comment);
            } else {
                fputc('\n', temp_file);
            }

            param_found = true;
            continue;
        }

        fputs(line, temp_file);
    }

    /* Phase 5: Add new parameter if missing */
    if (!param_found) {
        fprintf(temp_file, "%s=%s\n", param, value);
    }

    /* Phase 6: Finalize file replacement */
    if (fflush(temp_file) != 0 || fclose(temp_file) != 0) {
        temp_file = NULL;
        status = FILE_WRITE_ERROR;
        goto cleanup;
    }
    temp_file = NULL;

    if (rename(temp_path, config_path) != 0) {
        status = SAVE_FAILED;
        goto cleanup;
    }

cleanup:
    if (file) fclose(file);
    if (temp_file) fclose(temp_file);
    else if (fd != -1) close(fd);

    if (status != SUCCESS && temp_path[0] != '\0') {
        unlink(temp_path);
    }

    return status;
}

//////////////////phoenix///////////////////////////////

/*
 * Update Phoenix configuration parameter in XML configuration file
 *
 * Parameters:
 *   param - Configuration parameter name to update
 *   value - New value for the parameter
 *
 * Returns:
 *   ConfigStatus indicating operation outcome
 */
ConfigStatus
update_phoenix_config(const char *param, const char *value)
{
    const char *phoenix_home = getenv("PHOENIX_HOME");
    char        file_path[MAX_PATH_LEN] = {0};
    char        conf_dir[MAX_PATH_LEN] = {0};
    struct stat st;
    ConfigStatus status = SUCCESS;
    xmlDocPtr    doc = NULL;
    xmlNodePtr   root = NULL;
    bool         found = false;

    /* Determine configuration file location */
    const char *search_paths[3]; // Priority order: PHOENIX_HOME, Red Hat, Debian
    int num_search_paths = 0;
    bool found_config = false;

    // Build search paths based on priority
    if (phoenix_home != NULL) {
        search_paths[num_search_paths++] = phoenix_home;
    }
    search_paths[num_search_paths++] = "/opt/phoenix";
    search_paths[num_search_paths++] = "/usr/local/phoenix";

    // Check existing configuration files in order
    for (int i = 0; i < num_search_paths; i++) {
        const char *base = search_paths[i];
        if (snprintf(file_path, (int)sizeof(file_path), "%s/conf/%s", base, PHOENIX_CONFIG_FILE) >= (int)sizeof(file_path)) {
            // Path truncated, consider as not found
            continue;
        }
        if (stat(file_path, &st) == 0) {
            found_config = true;
            break;
        }
    }

    // If no existing config found, create in the highest priority base directory
    if (!found_config) {
        const char *base = search_paths[0];
        if (snprintf(conf_dir, (int)sizeof(conf_dir), "%s/conf", base) >= (int)sizeof(conf_dir)) {
            status = FILE_WRITE_ERROR;
            goto cleanup;
        }
        if (mkdir_p(conf_dir) != 0) {
            status = FILE_WRITE_ERROR;
            goto cleanup;
        }
        if (snprintf(file_path, (int)sizeof(file_path), "%s/%s", conf_dir, PHOENIX_CONFIG_FILE) >= (int)sizeof(file_path)) {
            status = FILE_WRITE_ERROR;
            goto cleanup;
        }
    }

    /* Load or initialize XML document */
    if (stat(file_path, &st) == 0) {
        /* Parse existing configuration file */
        doc = xmlReadFile(file_path, NULL, XML_PARSE_NOBLANKS | XML_PARSE_NOERROR | XML_PARSE_NOWARNING);
        if (!doc) {
            status = XML_PARSE_ERROR;
            goto cleanup;
        }

        /* Validate document structure */
        root = xmlDocGetRootElement(doc);
        if (!root || xmlStrcmp(root->name, BAD_CAST "configuration") != 0) {
            status = XML_INVALID_ROOT;
            goto cleanup;
        }
    } else {
        /* Create new XML document */
        doc = xmlNewDoc(BAD_CAST "1.0");
        root = xmlNewNode(NULL, BAD_CAST "configuration");
        if (!doc || !root) {
            status = XML_UPDATE_ERROR;
            goto cleanup;
        }
        xmlDocSetRootElement(doc, root);
    }

    /* Search for existing parameter */
    for (xmlNodePtr prop = root->children; prop != NULL; prop = prop->next) {
        /* Validate property node */
        if (prop->type != XML_ELEMENT_NODE || 
            xmlStrcmp(prop->name, BAD_CAST "property") != 0) {
            continue;
        }

        /* Find name element */
        xmlChar *name = NULL;
        for (xmlNodePtr child = prop->children; child != NULL; child = child->next) {
            if (child->type == XML_ELEMENT_NODE &&
                xmlStrcmp(child->name, BAD_CAST "name") == 0) {
                name = xmlNodeGetContent(child);
                break;
            }
        }

        /* Check parameter match */
        if (name && xmlStrcmp(name, BAD_CAST param) == 0) {
            /* Update existing value */
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
                xmlNewTextChild(prop, NULL, BAD_CAST "value", BAD_CAST value);
            }
            
            found = true;
            xmlFree(name);
            break;
        }
        xmlFree(name);
    }

    /* Add new property if not found */
    if (!found) {
        xmlNodePtr new_prop = xmlNewNode(NULL, BAD_CAST "property");
        xmlNewTextChild(new_prop, NULL, BAD_CAST "name", BAD_CAST param);
        xmlNewTextChild(new_prop, NULL, BAD_CAST "value", BAD_CAST value);
        xmlAddChild(root, new_prop);
    }

    /* Save configuration changes */
    if (xmlSaveFormatFileEnc(file_path, doc, "UTF-8", 1) < 0) {
        status = SAVE_FAILED;
    }

cleanup:
    /* Cleanup XML resources */
    if (doc) {
        xmlFreeDoc(doc);
    }
    
    return status;
}
////////////////////////////solar////////////////////////

typedef struct {
    const char *pattern;
    const char *canonical_name;
    const char *config_file;
} SolrParamConfig;

static const SolrParamConfig solr_param_configs[] = {
    // Core Configuration (solr.xml)
    {.pattern = "^coreRootDirectory$", .canonical_name = "coreRootDirectory", .config_file = "solr.xml"},

    // SolrCloud/ZooKeeper (solr.xml)
    {.pattern = "^zkHost$", .canonical_name = "zkHost", .config_file = "solr.xml"},
    {.pattern = "^zkClientTimeout$", .canonical_name = "zkClientTimeout", .config_file = "solr.xml"},
    {.pattern = "^cloud\\.collection\\.configName$", .canonical_name = "cloud.collection.configName", .config_file = "solr.xml"},
    {.pattern = "^numShards$", .canonical_name = "numShards", .config_file = "solr.xml"},

    // Replication/Sharding (solr.xml)
    {.pattern = "^shardHandlerFactory\\.socketTimeout$", .canonical_name = "shardHandlerFactory.socketTimeout", .config_file = "solr.xml"},
    {.pattern = "^replication\\.factor$", .canonical_name = "replication.factor", .config_file = "solr.xml"},

    // Monitoring/Logging (solr.xml)
    {.pattern = "^metrics\\.reporter\\.jmx$", .canonical_name = "metrics.reporter.jmx", .config_file = "solr.xml"},
    {.pattern = "^logging\\.watcher\\.threshold$", .canonical_name = "logging.watcher.threshold", .config_file = "solr.xml"},

    // HTTP/Network Settings (solr.xml)
    {.pattern = "^hostContext$", .canonical_name = "hostContext", .config_file = "solr.xml"},
    {.pattern = "^http\\.maxConnections$", .canonical_name = "http.maxConnections", .config_file = "solr.xml"},

    // Legacy Parameters (solr.xml only)
    {.pattern = "^transientCacheSize$", .canonical_name = "transientCacheSize", .config_file = "solr.xml"}
};

// Solr-specific helper functions
static bool isValidZKHostList(const char *value) {
    char *copy = strdup(value);
    char *token = strtok(copy, ",");
    bool valid = true;
    
    while (token != NULL) {
        char *colon = strchr(token, ':');
        if (!colon || !isValidPort(colon + 1)) {
            valid = false;
            break;
        }
        token = strtok(NULL, ",");
    }
    
    free(copy);
    return valid;
}

static bool isValidLogLevel(const char *value) {
    const char *levels[] = {"TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", NULL};
    for (int i = 0; levels[i]; i++) {
        if (strcmp(value, levels[i]) == 0) return true;
    }
    return false;
}

static bool isValidContextPath(const char *value) {
    return value[0] == '/' && strchr(value, ' ') == NULL && 
           strchr(value, ';') == NULL && strchr(value, '\\') == NULL;
}

ValidationResult validateSolrConfigParam(const char *param_name, const char *value) {
    // Check if parameter exists
    bool param_exists = false;
    for (size_t i = 0; i < sizeof(solr_param_configs)/sizeof(solr_param_configs[0]); i++) {
        if (strcmp(param_name, solr_param_configs[i].canonical_name) == 0) {
            param_exists = true;
            break;
        }
    }
    if (!param_exists) return ERROR_PARAM_NOT_FOUND;

    // Check value presence
    if (!value || strlen(value) == 0) return ERROR_VALUE_EMPTY;

    // Parameter-specific validation
    if (strcmp(param_name, "coreRootDirectory") == 0) {
        if (strlen(value) == 0 || strstr(value, "..") != NULL)
            return ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "zkHost") == 0) {
        if (!isValidZKHostList(value))
            return ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "zkClientTimeout") == 0 ||
             strcmp(param_name, "shardHandlerFactory.socketTimeout") == 0) {
        if (!isPositiveInteger(value))
            return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "cloud.collection.configName") == 0) {
        if (strlen(value) == 0 || strchr(value, '/') != NULL)
            return ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "numShards") == 0 ||
             strcmp(param_name, "replication.factor") == 0 ||
             strcmp(param_name, "http.maxConnections") == 0 ||
             strcmp(param_name, "transientCacheSize") == 0) {
        if (!isPositiveInteger(value))
            return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "metrics.reporter.jmx") == 0) {
        if (strcmp(value, "true") != 0 && strcmp(value, "false") != 0)
            return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "logging.watcher.threshold") == 0) {
        if (!isValidLogLevel(value))
            return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "hostContext") == 0) {
        if (!isValidContextPath(value))
            return ERROR_INVALID_FORMAT;
    }

    return VALIDATION_OK;
}

ConfigResult* validate_solr_parameter(const char *param_name, const char *param_value) {
    regex_t regex;
    ConfigResult* result = malloc(sizeof(ConfigResult));

    for (size_t i = 0; i < sizeof(solr_param_configs)/sizeof(solr_param_configs[0]); ++i) {
        int reti = regcomp(&regex, solr_param_configs[i].pattern, REG_EXTENDED | REG_ICASE);
        if (reti) {
            char error_message[1024];
            regerror(reti, &regex, error_message, sizeof(error_message));
            fprintf(stderr, "Regex compilation failed: %s\n", error_message);
            continue;
        }

        reti = regexec(&regex, param_name, 0, NULL, 0);
        regfree(&regex);
        if (reti == 0) {
            result->canonical_name = (char *)solr_param_configs[i].canonical_name;
            result->value = (char *)param_value;
            result->config_file = (char *)solr_param_configs[i].config_file;
            return result;
        }
    }

    return result;
}

/*
 * Update Solr configuration parameter in XML/properties file
 *
 * Parameters:
 *   param_name - Name of the configuration parameter to update
 *   param_value - New value for the configuration parameter
 *   config_file - Configuration file name (solrconfig.xml, solr.xml, core.properties)
 *
 * Returns:
 *   ConfigStatus indicating operation outcome
 */
ConfigStatus
update_solr_config(const char *param_name, const char *param_value, const char *config_file)
{
    char *config_path = NULL;
    bool file_exists = false;
    ConfigStatus status = SUCCESS;
    const char *solr_home = getenv("SOLR_HOME");
    const char *search_paths[] = {
        solr_home,
        "/opt/solr",
        "/usr/local/solr",
        NULL
    };

    /* Validate config_file parameter */
    if (config_file == NULL ||
        (strcmp(config_file, "solrconfig.xml") != 0 &&
        strcmp(config_file, "solr.xml") != 0 &&
        strcmp(config_file, "core.properties") != 0)) {
        return INVALID_CONFIG_FILE;
    }

    /* Search for existing configuration file in SOLR_HOME/server/solr */
    for (int i = 0; search_paths[i]; i++) {
        if (!search_paths[i]) continue;
        
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/server/solr/%s", search_paths[i], config_file);
        if (access(path, F_OK) == 0) {
            config_path = strdup(path);
            file_exists = true;
            break;
        }
    }

    /* Determine location for new file in SOLR_HOME/server/solr */
    if (!config_path) {
        const char *base = solr_home ? solr_home : "/opt/solr";
        config_path = malloc(PATH_MAX);
        if (!config_path) return FILE_READ_ERROR;
        snprintf(config_path, PATH_MAX, "%s/server/solr/%s", base, config_file);
    }


    /* XML Processing */
    xmlDocPtr doc = NULL;
    xmlNodePtr root = NULL;
    const char *expected_root = strcmp(config_file, "solr.xml") == 0 ? "solr" : "config";

    if (file_exists) {
        doc = xmlReadFile(config_path, NULL, 0);
        if (!doc) {
            status = XML_PARSE_ERROR;
            goto cleanup;
        }
    } else {
        doc = xmlNewDoc(BAD_CAST "1.0");
        if (!doc) {
            status = XML_UPDATE_ERROR;
            goto cleanup;
        }
        root = xmlNewNode(NULL, BAD_CAST expected_root);
        if (!root) {
            status = XML_UPDATE_ERROR;
            goto cleanup;
        }
        xmlDocSetRootElement(doc, root);
    }

    /* Validate document structure */
    root = xmlDocGetRootElement(doc);
    if (!root || xmlStrcmp(root->name, BAD_CAST expected_root) != 0) {
        status = XML_INVALID_ROOT;
        goto cleanup;
    }

    /* Find existing parameter node */
    xmlNodePtr param_node = NULL;
    for (xmlNodePtr cur = root->children; cur; cur = cur->next) {
        if (cur->type == XML_ELEMENT_NODE && 
            xmlStrcmp(cur->name, BAD_CAST param_name) == 0) {
            param_node = cur;
            break;
        }
    }

    /* Update or create parameter */
    if (param_node) {
        xmlNodeSetContent(param_node, BAD_CAST param_value);
    } else {
        if (!xmlNewTextChild(root, NULL, BAD_CAST param_name, BAD_CAST param_value)) {
            status = XML_UPDATE_ERROR;
            goto cleanup;
        }
    }

    /* Save changes */
    if (xmlSaveFileEnc(config_path, doc, "UTF-8") <= 0) {
        status = SAVE_FAILED;
    }

cleanup:
    if (doc) xmlFreeDoc(doc);
    free(config_path);
    return status;
}
//////////////////////////////////////ZEPPELIN//////////////////////

typedef struct {
    char *canonical_name;
    char *value;
    char *config_file;
} ZeppelinConfigParam;

ZeppelinConfigParam zeppelin_predefined_params[] = {
    // Core Server Configuration
    {"zeppelin.server.port", "zeppelin-server-port", "zeppelin-site.xml"},
    {"zeppelin.server.addr", "zeppelin-server-addr", "zeppelin-site.xml"},
    {"zeppelin.server.context.path", "zeppelin-server-context-path", "zeppelin-site.xml"},
    {"zeppelin.ssl.enabled", "zeppelin-ssl-enabled", "zeppelin-site.xml"},
    {"zeppelin.ssl.keystore.path", "zeppelin-ssl-keystore-path", "zeppelin-site.xml"},
    {"zeppelin.ssl.truststore.path", "zeppelin-ssl-truststore-path", "zeppelin-site.xml"},

    // Notebook Management
    {"zeppelin.notebook.storage", "zeppelin-notebook-storage", "zeppelin-site.xml"},
    {"zeppelin.notebook.dir", "zeppelin-notebook-dir", "zeppelin-site.xml"},
    {"zeppelin.notebook.git.remote.url", "zeppelin-notebook-git-remote-url", "zeppelin-site.xml"},
    {"zeppelin.notebook.git.username", "zeppelin-notebook-git-username", "zeppelin-site.xml"},
    {"zeppelin.notebook.auto.commit", "zeppelin-notebook-auto-commit", "zeppelin-site.xml"},

    // Interpreter Configuration
    {"zeppelin.interpreter.localRepo", "zeppelin-interpreter-local-repo", "zeppelin-site.xml"},
    {"zeppelin.interpreter.group", "zeppelin-interpreter-group", "zeppelin-site.xml"},
    {"zeppelin.interpreter.connect.timeout", "zeppelin-interpreter-connect-timeout", "zeppelin-site.xml"},
    {"zeppelin.interpreter.isolation", "zeppelin-interpreter-isolation", "zeppelin-site.xml"},
    {"zeppelin.interpreter.process.max_threads", "zeppelin-interpreter-process-max-threads", "zeppelin-site.xml"},

    // Resource Management
    {"zeppelin.executor.memory", "zeppelin-executor-memory", "zeppelin-site.xml"},
    {"zeppelin.resource.pool.size", "zeppelin-resource-pool-size", "zeppelin-site.xml"},
    {"zeppelin.memory.allocator.max", "zeppelin-memory-allocator-max", "zeppelin-site.xml"},

    // Backend Integration
    {"zeppelin.spark.master", "zeppelin-spark-master", "zeppelin-site.xml"},
    {"zeppelin.spark.executor.cores", "zeppelin-spark-executor-cores", "zeppelin-site.xml"},
    {"zeppelin.flink.jobmanager.url", "zeppelin-flink-jobmanager-url", "zeppelin-site.xml"},
    {"zeppelin.hive.hiveserver2.url", "zeppelin-hive-hiveserver2-url", "zeppelin-site.xml"},
    {"zeppelin.jdbc.drivers", "zeppelin-jdbc-drivers", "zeppelin-site.xml"},

    // Security & Authentication
    {"shiro.realm", "shiro-realm", "shiro.ini"},
    {"shiro.ldap.contextFactory.url", "shiro-ldap-context-factory-url", "shiro.ini"},
    {"shiro.ldap.userDnTemplate", "shiro-ldap-user-dn-template", "shiro.ini"},
    {"shiro.activeDirectoryRealm.domain", "shiro-active-directory-realm-domain", "shiro.ini"},
    {"shiro.oauth2.clientId", "shiro-oauth2-client-id", "shiro.ini"},
    {"shiro.oauth2.callbackUrl", "shiro-oauth2-callback-url", "shiro.ini"},

    // High Availability & Clustering
    {"zeppelin.ha.enabled", "zeppelin-ha-enabled", "zeppelin-site.xml"},
    {"zeppelin.ha.zookeeper.quorum", "zeppelin-ha-zookeeper-quorum", "zeppelin-site.xml"},
    {"zeppelin.cluster.addr", "zeppelin-cluster-addr", "zeppelin-site.xml"},

    // REST API & Monitoring
    {"zeppelin.server.rest.api.port", "zeppelin-server-rest-api-port", "zeppelin-site.xml"},
    {"zeppelin.monitoring.enabled", "zeppelin-monitoring-enabled", "zeppelin-site.xml"},

    // Logging & Diagnostics
    {"zeppelin.log.dir", "zeppelin-log-dir", "log4j.properties"},
    {"zeppelin.log.level", "zeppelin-log-level", "log4j.properties"},

    // Dependency Management
    {"zeppelin.dep.additionalRemoteRepository", "zeppelin-dep-additional-remote-repository", "zeppelin-site.xml"},

    // User Interface
    {"zeppelin.helium.registry", "zeppelin-helium-registry", "zeppelin-site.xml"},
    {"zeppelin.notebook.collaborative.mode", "zeppelin-notebook-collaborative-mode", "zeppelin-site.xml"},

    // Session Management
    {"zeppelin.session.timeout", "zeppelin-session-timeout", "zeppelin-site.xml"},
    {"shiro.sessionTimeout", "shiro-session-timeout", "shiro.ini"},

    // External Systems Integration
    {"zeppelin.config.fs.dir", "zeppelin-config-fs-dir", "zeppelin-site.xml"},
    {"zeppelin.credentials.file", "zeppelin-credentials-file", "zeppelin-site.xml"}
};


static bool isZKQuorum(const char *value) {
    char *copy = strdup(value);
    char *token = strtok(copy, ",");
    bool valid = true;
    
    while (token != NULL) {
        char *colon = strchr(token, ':');
        if (!colon || !isValidPort(colon + 1)) {
            valid = false;
            break;
        }
        token = strtok(NULL, ",");
    }
    
    free(copy);
    return valid;
}

ValidationResult validateZeppelinConfigParam(const char *param_name, const char *value) {
    // Check if parameter exists
    bool param_exists = false;
    for (size_t i = 0; i < sizeof(zeppelin_predefined_params)/sizeof(zeppelin_predefined_params[0]); i++) {
        if (strcmp(param_name, zeppelin_predefined_params[i].canonical_name) == 0) {
            param_exists = true;
            break;
        }
    }
    if (!param_exists) return ERROR_PARAM_NOT_FOUND;

    // Check value presence
    if (!value || strlen(value) == 0) return ERROR_VALUE_EMPTY;

    // Parameter-specific validation
    if (strcmp(param_name, "zeppelin.server.port") == 0 ||
        strcmp(param_name, "zeppelin.server.rest.api.port") == 0) {
        return isValidPort(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "zeppelin.ssl.enabled") == 0 ||
             strcmp(param_name, "zeppelin.notebook.auto.commit") == 0 ||
             strcmp(param_name, "zeppelin.ha.enabled") == 0) {
        return isValidBoolean(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "zeppelin.executor.memory") == 0) {
        return isMemorySize(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "zeppelin.ha.zookeeper.quorum") == 0) {
        return isZKQuorum(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "zeppelin.flink.jobmanager.url") == 0 ||
             strcmp(param_name, "zeppelin.hive.hiveserver2.url") == 0) {
        return isValidURI(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "zeppelin.log.level") == 0) {
        return isValidLogLevel(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "zeppelin.interpreter.connect.timeout") == 0 ||
             strcmp(param_name, "shiro.sessionTimeout") == 0) {
        return isPositiveInteger(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "zeppelin.notebook.git.remote.url") == 0) {
        return strstr(value, "git@") != NULL || strstr(value, "http") != NULL
            ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "shiro.ldap.contextFactory.url") == 0) {
        return strstr(value, "ldap://") != NULL || strstr(value, "ldaps://") != NULL
            ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "zeppelin.spark.master") == 0) {
        return strstr(value, "local") != NULL || strstr(value, "spark://") != NULL ||
               strstr(value, "yarn") != NULL || strstr(value, "mesos") != NULL
            ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }

    return VALIDATION_OK;
}

#define NUM_ZEPPELINE (sizeof(zeppelin_predefined_params) / sizeof(zeppelin_predefined_params[0]))

void preprocess_param_name(const char *input, char *output) {
    int j = 0;
    char prev_char = '\0';
    for (int i = 0; input[i] != '\0'; i++) {
        char c = input[i];
        if (i > 0 && isupper((unsigned char)c)) {
            if (j > 0 && output[j-1] != '-') {
                output[j++] = '-';
            }
        }
        c = tolower((unsigned char)c);
        if (!isalnum((unsigned char)c)) {
            c = '-';
        }
        if (c == '-') {
            if (prev_char != '-') {
                output[j++] = c;
            }
        } else {
            output[j++] = c;
        }
        prev_char = c;
    }
    output[j] = '\0';
}

ConfigResult *process_zeppelin_config_param(const char *param_name, const char *param_value) {
    char processed_name[256];
    preprocess_param_name(param_name, processed_name);

    for (size_t i = 0; i < NUM_ZEPPELINE; i++) {
        // Fix: Compare against 'value' field instead of 'canonical_name'
        if (strcmp(processed_name, zeppelin_predefined_params[i].value) == 0) {
            ConfigResult *result = malloc(sizeof(ConfigResult));
            if (!result) return NULL;

            result->canonical_name = strdup(zeppelin_predefined_params[i].canonical_name);
            result->value = strdup(param_value);
            result->config_file = strdup(zeppelin_predefined_params[i].config_file);

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

/* Assume necessary constants and helper functions (e.g., mkdir_p) are defined elsewhere */

ConfigStatus set_zeppelin_config(const char *config_file, const char *param, const char *value) {
    char config_path[MAX_PATH_LEN] = {0};
    char conf_dir[MAX_PATH_LEN] = {0};
    ConfigStatus status = SAVE_FAILED;
    bool config_exists = false;

    /* Validate input parameters */
    if (config_file == NULL || param == NULL || value == NULL) {
        return INVALID_CONFIG_FILE;
    }

    /* Check if config_file is allowed */
    if (strcmp(config_file, "zeppelin-site.xml") != 0 && strcmp(config_file, "shiro.ini") != 0) {
        return INVALID_CONFIG_FILE;
    }

    /* Determine configuration file location */
    const char *zeppelin_home = getenv("ZEPPELIN_HOME");
    const char *base_dirs[] = {
        zeppelin_home,
        "/opt/solr",
        "/usr/local/zeppelin"
    };
    const int num_base_dirs = sizeof(base_dirs) / sizeof(base_dirs[0]);

    /* Check existing configuration files in priority order */
    for (int i = 0; i < num_base_dirs; ++i) {
        const char *base = base_dirs[i];
        if (base == NULL) continue;

        snprintf(config_path, sizeof(config_path), "%s/conf/%s", base, config_file);
        if (access(config_path, F_OK) == 0) {
            config_exists = true;
            break;
        }
    }

    /* If not found, determine where to create the configuration */
    if (!config_exists) {
        bool dir_created = false;
        for (int i = 0; i < num_base_dirs; ++i) {
            const char *base = base_dirs[i];
            if (base == NULL) continue;

            snprintf(conf_dir, sizeof(conf_dir), "%s/conf", base);
            if (access(conf_dir, F_OK) == 0) {
                snprintf(config_path, sizeof(config_path), "%s/%s", conf_dir, config_file);
                dir_created = true;
                break;
            } else {
                status = mkdir_p(conf_dir);
                if (status == SUCCESS) {
                    snprintf(config_path, sizeof(config_path), "%s/%s", conf_dir, config_file);
                    dir_created = true;
                    break;
                }
            }
        }
        if (!dir_created) {
            return status; /* Return last error status */
        }
    }

    /* Determine if handling XML or INI */
    bool is_xml = strcmp(config_file, "zeppelin-site.xml") == 0;

    if (is_xml) {
        /* Existing XML handling logic */
        xmlDocPtr doc = NULL;
        xmlNodePtr root = NULL;
        bool property_found = false;

        if (config_exists) {
            doc = xmlReadFile(config_path, NULL, 0);
            if (!doc) return XML_PARSE_ERROR;

            root = xmlDocGetRootElement(doc);
            if (!root || xmlStrcmp(root->name, BAD_CAST "configuration") != 0) {
                xmlFreeDoc(doc);
                return XML_INVALID_ROOT;
            }
        } else {
            doc = xmlNewDoc(BAD_CAST "1.0");
            if (!doc) return XML_PARSE_ERROR;

            root = xmlNewNode(NULL, BAD_CAST "configuration");
            if (!root) {
                xmlFreeDoc(doc);
                return XML_PARSE_ERROR;
            }
            xmlDocSetRootElement(doc, root);
        }

        /* Search and update XML property */
        for (xmlNodePtr node = root->children; node; node = node->next) {
            if (node->type != XML_ELEMENT_NODE || xmlStrcmp(node->name, BAD_CAST "property") != 0)
                continue;

            xmlNodePtr name_node = NULL, value_node = NULL;
            xmlChar *name_content = NULL;

            for (xmlNodePtr child = node->children; child; child = child->next) {
                if (child->type == XML_ELEMENT_NODE) {
                    if (xmlStrcmp(child->name, BAD_CAST "name") == 0) {
                        name_node = child;
                    } else if (xmlStrcmp(child->name, BAD_CAST "value") == 0) {
                        value_node = child;
                    }
                }
            }

            if (name_node && value_node) {
                name_content = xmlNodeGetContent(name_node);
                if (xmlStrcmp(name_content, BAD_CAST param) == 0) {
                    xmlNodeSetContent(value_node, BAD_CAST value);
                    property_found = true;
                    xmlFree(name_content);
                    break;
                }
                xmlFree(name_content);
            }
        }

        /* Add new property if not found */
        if (!property_found) {
            xmlNodePtr new_prop = xmlNewNode(NULL, BAD_CAST "property");
            if (!new_prop) {
                xmlFreeDoc(doc);
                return XML_PARSE_ERROR;
            }

            xmlNodePtr name = xmlNewNode(NULL, BAD_CAST "name");
            xmlNodePtr value_node = xmlNewNode(NULL, BAD_CAST "value");
            if (!name || !value_node) {
                if (name) xmlFreeNode(name);
                if (value_node) xmlFreeNode(value_node);
                xmlFreeNode(new_prop);
                xmlFreeDoc(doc);
                return XML_PARSE_ERROR;
            }

            xmlNodeAddContent(name, BAD_CAST param);
            xmlNodeAddContent(value_node, BAD_CAST value);
            xmlAddChild(new_prop, name);
            xmlAddChild(new_prop, value_node);
            xmlAddChild(root, new_prop);
        }

        /* Save XML file */
        if (xmlSaveFile(config_path, doc) < 0) {
            status = SAVE_FAILED;
        } else {
            status = SUCCESS;
        }

        xmlFreeDoc(doc);
    } else {
        /* INI file handling */
        FILE *file = fopen(config_path, "r");
        char **lines = NULL;
        size_t line_count = 0;
        bool found = false;

        /* Read existing lines */
        if (file) {
            char buffer[1024];
            while (fgets(buffer, sizeof(buffer), file)) {
                char *line = strdup(buffer);
                if (!line) {
                    fclose(file);
                    for (size_t j = 0; j < line_count; ++j) free(lines[j]);
                    free(lines);
                    return SAVE_FAILED;
                }

                char **new_lines = realloc(lines, (line_count + 1) * sizeof(char *));
                if (!new_lines) {
                    free(line);
                    fclose(file);
                    for (size_t j = 0; j < line_count; ++j) free(lines[j]);
                    free(lines);
                    return SAVE_FAILED;
                }
                lines = new_lines;
                lines[line_count++] = line;

                char *trimmed = line;
                while (isspace(*trimmed)) trimmed++;

                /* Skip comments and empty lines */
                if (*trimmed == '#' || *trimmed == ';' || *trimmed == '\0') continue;

                char *equals = strchr(trimmed, '=');
                if (!equals) continue;

                size_t key_len = equals - trimmed;
                while (key_len > 0 && isspace(trimmed[key_len - 1])) key_len--;

                if (key_len == strlen(param) && strncmp(trimmed, param, key_len) == 0) {
                    /* Replace the line */
                    free(line);
                    size_t new_line_len = strlen(param) + strlen(value) + 2;
                    lines[line_count - 1] = malloc(new_line_len + 1);
                    if (!lines[line_count - 1]) {
                        fclose(file);
                        for (size_t j = 0; j < line_count; ++j) free(lines[j]);
                        free(lines);
                        return SAVE_FAILED;
                    }
                    snprintf(lines[line_count - 1], new_line_len + 1, "%s=%s\n", param, value);
                    found = true;
                }
            }
            fclose(file);
        }

        /* Add new parameter if not found */
        if (!found) {
            size_t new_line_len = strlen(param) + strlen(value) + 2;
            char **new_lines = realloc(lines, (line_count + 1) * sizeof(char *));
            if (!new_lines) {
                for (size_t j = 0; j < line_count; ++j) free(lines[j]);
                free(lines);
                return SAVE_FAILED;
            }
            lines = new_lines;
            lines[line_count] = malloc(new_line_len + 1);
            if (!lines[line_count]) {
                for (size_t j = 0; j < line_count; ++j) free(lines[j]);
                free(lines);
                return SAVE_FAILED;
            }
            snprintf(lines[line_count], new_line_len + 1, "%s=%s\n", param, value);
            line_count++;
        }

        /* Write back to file */
        FILE *out = fopen(config_path, "w");
        if (!out) {
            status = SAVE_FAILED;
        } else {
            for (size_t i = 0; i < line_count; ++i) {
                if (fputs(lines[i], out) == EOF) {
                    status = SAVE_FAILED;
                    break;
                }
                free(lines[i]);
            }
            fclose(out);
            if (status != SAVE_FAILED) status = SUCCESS;
        }
        free(lines);
    }

    return status;
}

/////////////////////////ranger//////////////////////////////////////////

typedef struct {
    const char *pattern;
    const char *canonical_name;
    const char *config_file;
} RangerConfigEntry;

static const RangerConfigEntry predefined_entries[] = {
    // Core Administration
    { "^ranger[._-]+admin[._-]+(https?|server)([._-]port)?$", "ranger.admin.${1}.port", "ranger-admin-site.xml" },
    { "^ranger[._-]+policy[._-]+rest[._-]+url$", "ranger.policy.rest.url", "ranger-admin-site.xml" },
    { "^ranger[._-]+service[._-]+(https_|)storepass$", "ranger.service.${1}storepass", "ranger-admin-site.xml" },
    
    // Database Configuration
    { "^ranger[._-]+db[._-](host|port|name|user|password)$", "ranger.db.${1}", "ranger-admin-site.xml" },
    { "^ranger[._-]+jpa[._-]+(jdbc|audit\\.jdbc)\\.(url|driver|user|password)$", "ranger.jpa.${1}.${2}", "ranger-admin-site.xml" },
    
    // Security & Authentication
    { "^ranger[._-]+(kerberos|spnego)\\.(keytab|principal)$", "ranger.${1}.${2}", "ranger-admin-site.xml"},
    { "^ranger[._-]+ldap[._-]+(base\\.dn|bind\\.dn|bind\\.password|url|referral)$", "ranger.ldap.${1}", "ranger-admin-site.xml"},
    { "^ranger[._-]+sso[._-]+(provider|enabled|cookie\\.name|public\\.key)$", "ranger.sso.${1}", "ranger-admin-site.xml"},
    
    // Plugin Framework
    { "^ranger[._-]+plugin[._-]+(hdfs|hive|hbase|kafka|yarn|storm|atlas|sqoop)\\.([\\w\\.]+)$", 
      "ranger.${1}.${2}", "ranger-${1}-security.xml"},
    { "^ranger[._-]+plugin[._-]+(enable|policy\\.pollIntervalMs|cache\\.dir|audit\\.enabled)$", 
      "ranger.plugin.${1}", "ranger-plugins-common.xml"},
    
    // Service-Specific Configurations
    { "^ranger[._-]+(hive|hdfs|hbase|kafka)[._-]+(service|repository)[._-]+name$", 
      "ranger.${1}.service.name", "ranger-${1}-security.xml"},
    { "^ranger[._-]+(s3|atlas)[._-]+audit[._-]+(path|enable)$", 
      "ranger.${1}.audit.${2}", "ranger-${1}-security.xml"},
    
    // Tag-Based Authorization
    { "^ranger[._-]+tag[._-]+(service|store|download\\.interval\\.ms|policy\\.evaluator\\.class)$", 
      "ranger.tag.${1}", "ranger-tagsync-site.xml"},
    { "^ranger[._-]+tagsync[._-]+(source|atlas\\.endpoints|retry\\.interval)$", 
      "ranger.tagsync.${1}", "ranger-tagsync-site.xml"},
    
    // User/Group Synchronization
    { "^ranger[._-]+usersync[._-]+(source|ldap|interval|retry|batch\\.size)$", 
      "ranger.usersync.${1}", "ranger-ugsync-site.xml"},
    { "^ranger[._-]+unix[._-]+(user|group)[._-]+(name|mapping)$", 
      "ranger.unix.${1}.${2}", "ranger-ugsync-site.xml"},
    
    // Advanced Authorization Features
    { "^ranger[._-]+(abac|attribute)[._-]+(enable|evaluator|context\\.enricher)$", 
      "ranger.abac.${2}", "ranger-policymgr.xml"},
    { "^ranger[._-]+policy[._-]+(condition|resource\\.matcher)\\.class\\.([\\w]+)$", 
      "ranger.policy.${1}.${2}", "ranger-policymgr.xml"},
    
    // Audit Configuration
    { "^ranger[._-]+audit[._-]+(solr|hdfs|db|cloud)\\.([\\w\\.]+)$", 
      "ranger.audit.${1}.${2}", "ranger-admin-site.xml"},
    { "^ranger[._-]+audit[._-]+(encrypt|filter|queue|buffer|ssl)\\.([\\w]+)$", 
      "ranger.audit.${1}.${2}", "ranger-admin-site.xml"},
    
    // SSL/TLS Configuration
    { "^ranger[._-]+ssl[._-]+(keystore|truststore)\\.(file|password|type)$", 
      "ranger.ssl.${1}.${2}", "ranger-security.xml"},
    { "^ranger[._-]+ssl[._-]+(enabledProtocols|cipherSuites|requireClientAuth)$", 
      "ranger.ssl.${1}", "ranger-security.xml"},
    
    // Policy Management
    { "^ranger[._-]+policy[._-]+(engine|update|cache)\\.([\\w]+)$", 
      "ranger.policy.${1}.${2}", "ranger-policymgr.xml"},
    { "^ranger[._-]+rest[._-]+(api|client)\\.([\\w]+)$", 
      "ranger.rest.${1}.${2}", "ranger-admin-site.xml"},
    
    // Advanced Features
    { "^ranger[._-]+(zone|resource)\\.([\\w]+)\\.([\\w]+)$", 
      "ranger.${1}.${2}.${3}", "ranger-admin-site.xml"},
    { "^ranger[._-]+(admin|user)\\.([\\w]+)\\.([\\w]+)$", 
      "ranger.${1}.${2}.${3}", "ranger-admin-site.xml"},
    
    // Plugin Lifecycle Management
    { "^ranger[._-]+plugin[._-]+(init|shutdown)[._-]+(timeout|retry)$", 
      "ranger.plugin.${1}.${2}", "ranger-plugins-common.xml"},
    { "^ranger[._-]+service[._-]+(register|refresh)[._-]+interval$", 
      "ranger.service.${1}.interval", "ranger-admin-site.xml"},
    
    // External Integration
    { "^ranger[._-]+(keycloak|okta|azuread)\\.([\\w]+)\\.([\\w]+)$", 
      "ranger.${1}.${2}.${3}", "ranger-admin-site.xml"},
    { "^ranger[._-]+(saml|oauth2)\\.([\\w]+)\\.([\\w]+)$", 
      "ranger.${1}.${2}.${3}", "ranger-admin-site.xml"}
};

// Ranger-specific helper functions
static bool isSSLProtocolValid(const char *value) {
    const char *valid[] = {"TLSv1.2", "TLSv1.3", NULL};
    for (int i = 0; valid[i]; i++)
        if (strstr(value, valid[i]) != NULL) return true;
    return false;
}

static bool isComponentValid(const char *component) {
    const char *valid[] = {"hdfs", "hive", "hbase", "kafka", "yarn", NULL};
    for (int i = 0; valid[i]; i++)
        if (strcmp(component, valid[i]) == 0) return true;
    return false;
}

static bool isURLValid(const char *value) {
    return strstr(value, "://") != NULL && strlen(value) > 8;
}

ValidationResult validateRangerConfigParam(const char *param_name, const char *value) {
    // Check if parameter exists
    bool param_exists = false;
    regex_t regex;
    regmatch_t matches[4];
    char pattern[512];
    
    for (size_t i = 0; i < sizeof(predefined_entries)/sizeof(predefined_entries[0]); i++) {
        // Convert pattern to valid regex
        snprintf(pattern, sizeof(pattern), "%s", predefined_entries[i].pattern);
        // Replace ${x} with capture groups
        char *ptr = strstr(pattern, "${");
        while (ptr) {
            *ptr = '(';
            ptr = strchr(ptr, '}');
            if (ptr) *ptr = ')';
        }
        
        if (regcomp(&regex, pattern, REG_EXTENDED) != 0) continue;
        if (regexec(&regex, param_name, 4, matches, 0) == 0) {
            param_exists = true;
            regfree(&regex);
            break;
        }
        regfree(&regex);
    }
    
    if (!param_exists) return ERROR_PARAM_NOT_FOUND;

    // Check value presence
    if (!value || strlen(value) == 0) return ERROR_VALUE_EMPTY;

    // Parameter-specific validation
    if (strstr(param_name, ".port") != NULL) {
        return isValidPort(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strstr(param_name, ".enabled") != NULL ||
             strstr(param_name, ".ssl.enabled") != NULL) {
        return isValidBoolean(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strstr(param_name, "ranger.db.") != NULL) {
        if (strstr(param_name, ".password") != NULL)
            return (strlen(value) >= 8) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
        return (strlen(value) > 0) ? VALIDATION_OK : ERROR_VALUE_EMPTY;
    }
    else if (strstr(param_name, "ranger.ldap.url") != NULL) {
        return strstr(value, "ldap://") != NULL || strstr(value, "ldaps://") != NULL
            ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strstr(param_name, "ranger.ssl.") != NULL) {
        if (strstr(param_name, "keystore.file") != NULL ||
            strstr(param_name, "truststore.file") != NULL) {
            return (access(value, R_OK) == 0) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
        }
        else if (strstr(param_name, "enabledProtocols") != NULL) {
            return isSSLProtocolValid(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
        }
    }
    else if (strstr(param_name, "ranger.plugin.") != NULL) {
        char component[50];
        sscanf(param_name, "ranger.plugin.%49[^.].", component);
        if (!isComponentValid(component))
            return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strstr(param_name, "ranger.policy.rest.url") != NULL) {
        return isURLValid(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }

    return VALIDATION_OK;
}

ConfigResult* get_ranger_config(const char *param_name, const char *param_value) {
               ConfigResult *result = malloc(sizeof(ConfigResult));
            if (!result) return NULL;

    for (size_t i = 0; i < sizeof(predefined_entries)/sizeof(predefined_entries[0]); ++i) {
        const RangerConfigEntry *entry = &predefined_entries[i];
        regex_t regex;
        int reti = regcomp(&regex, entry->pattern, REG_ICASE | REG_NOSUB);
        if (reti != 0) continue;

        reti = regexec(&regex, param_name, 0, NULL, 0);
        regfree(&regex);

        if (reti == 0) {
           result->canonical_name = strdup(entry->canonical_name);
            result->value = strdup(param_value);
            result->config_file = strdup(entry->config_file);
            
            // Properly null-terminate
            break;
        }
    }

    return result;
}


static char *determine_config_path(const char *config_file) {
    const char *ranger_home = getenv("RANGER_HOME");
    char path[MAX_PATH_LEN];
    int len;

    // Check RANGER_HOME subdirectories
    if (ranger_home != NULL) {
        const char *subdirs[] = {"conf", "etc", NULL};
        for (int i = 0; subdirs[i] != NULL; i++) {
            len = snprintf(path, sizeof(path), "%s/%s/%s", ranger_home, subdirs[i], config_file);
            if (len < 0 || len >= (int)sizeof(path)) continue;
            if (access(path, F_OK) == 0) {
                char *result = strdup(path);
                return result ? result : NULL; // Return NULL if strdup fails
            }
        }
    }

    // Check standard Red Hat and Debian base directories
    const char *standard_bases[] = {"/opt/ranger", "/usr/local/ranger", NULL};
    const char *subdirs[] = {"conf", "etc", NULL};
    
    for (int j = 0; standard_bases[j] != NULL; j++) {
        for (int i = 0; subdirs[i] != NULL; i++) {
            len = snprintf(path, sizeof(path), "%s/%s/%s", standard_bases[j], subdirs[i], config_file);
            if (len < 0 || len >= (int)sizeof(path)) continue;
            if (access(path, F_OK) == 0) {
                char *result = strdup(path);
                return result ? result : NULL;
            }
        }
    }

    // Configuration file not found in any standard location
    return NULL;
}
ConfigStatus set_ranger_config(const char *param, const char *value, const char *config_file) {
    if (!param || !value || !config_file) return INVALID_CONFIG_FILE;

    char *file_path = determine_config_path(config_file);
    if (!file_path) return FILE_NOT_FOUND;

    xmlDocPtr doc = NULL;
    xmlNodePtr root_node = NULL;
    ConfigStatus rc = XML_PARSE_ERROR;
    struct stat st;

    int file_exists = (stat(file_path, &st) == 0);

    if (file_exists) {
        doc = xmlReadFile(file_path, NULL, 0);
        if (!doc) {
            rc = FILE_NOT_FOUND;
            goto cleanup;
        }
        root_node = xmlDocGetRootElement(doc);
        if (!root_node || xmlStrcmp(root_node->name, BAD_CAST "configuration")) {
            rc = XML_INVALID_ROOT;
            goto cleanup;
        }
    } else {
        doc = xmlNewDoc(BAD_CAST "1.0");
        root_node = xmlNewNode(NULL, BAD_CAST "configuration");
        xmlDocSetRootElement(doc, root_node);
    }

    xmlXPathContextPtr xpathCtx = xmlXPathNewContext(doc);
    if (!xpathCtx) {
        rc = XML_PARSE_ERROR;
        goto cleanup;
    }

    char xpath_expr[256];
    snprintf(xpath_expr, sizeof(xpath_expr), "//property/name[text()='%s']/..", param);
    xmlXPathObjectPtr xpathObj = xmlXPathEvalExpression(BAD_CAST xpath_expr, xpathCtx);
    if (!xpathObj) {
        rc = XML_PARSE_ERROR;
        goto xpath_cleanup;
    }

    xmlNodeSetPtr nodes = xpathObj->nodesetval;
    if (nodes && nodes->nodeNr > 0) {
        xmlNodePtr prop_node = nodes->nodeTab[0];
        xmlNodePtr value_node = NULL;
        for (xmlNodePtr child = prop_node->children; child; child = child->next) {
            if (child->type == XML_ELEMENT_NODE && !xmlStrcmp(child->name, BAD_CAST "value")) {
                value_node = child;
                break;
            }
        }
        if (value_node) {
            xmlNodeSetContent(value_node, BAD_CAST value);
        } else {
            xmlNewChild(prop_node, NULL, BAD_CAST "value", BAD_CAST value);
        }
    } else {
        xmlNodePtr prop_node = xmlNewNode(NULL, BAD_CAST "property");
        xmlNewChild(prop_node, NULL, BAD_CAST "name", BAD_CAST param);
        xmlNewChild(prop_node, NULL, BAD_CAST "value", BAD_CAST value);
        xmlAddChild(root_node, prop_node);
    }

    if (xmlSaveFormatFileEnc(file_path, doc, "UTF-8", 1) == -1) {
        rc = FILE_WRITE_ERROR;
    } else {
        rc = SUCCESS;
    }

xpath_cleanup:
    xmlXPathFreeObject(xpathObj);
    xmlXPathFreeContext(xpathCtx);
cleanup:
    if (doc) xmlFreeDoc(doc);
    free(file_path);
    return rc;
}

///////////////////////////////////////////////tez////////////////////////////////////////

typedef struct {
    char *name;
    char *value;
} TezConfigParam;

const char *tez_canonical_params[] = {
    // Resource Allocation
    "tez.am.resource.memory.mb",
    "tez.task.resource.memory.mb",
    "tez.am.resource.cpu.vcores",
    "tez.task.resource.cpu.vcores",
    "tez.am.container.heap.memory-mb.ratio",
    "tez.am.container.java.opts",
    "tez.am.launch.cmd-opts",
    
    // Queuing and Scheduling
    "tez.queue.name",
    "tez.am.node-blacklisting.enabled",
    "tez.am.node-blacklisting.ignore-threshold.node-percent",
    
    // Execution Control
    "tez.am.container.reuse.enabled",
    "tez.am.container.reuse.rack-fallback.enabled",
    "tez.am.container.idle.release-timeout-min.millis",
    "tez.am.container.idle.release-timeout-max.millis",
    
    // Shuffle and Sorting
    "tez.runtime.io.sort.mb",
    "tez.runtime.io.sort.factor",
    "tez.runtime.unordered.output.buffer.size.mb",
    "tez.runtime.shuffle.parallel.copies",
    "tez.runtime.shuffle.fetch.buffer.percent",
    "tez.runtime.shuffle.merge.percent",
    "tez.runtime.sort.spill.percent",
    
    // Compression
    "tez.runtime.compress",
    "tez.runtime.compress.codec",
    "tez.runtime.shuffle.enable.ssl",
    
    // Grouping and Parallelism
    "tez.grouping.split-count",
    "tez.grouping.max-size",
    "tez.grouping.min-size",
    "tez.grouping.shuffle.enabled",
    "tez.vertex.max.output.consumers",
    
    // Fault Tolerance
    "tez.am.task.max.failed.attempts",
    "tez.task.skip.enable",
    "tez.am.task.preemption.wait.timeout.millis",
    
    // Logging and Monitoring
    "tez.staging-dir",
    "tez.am.application.tag",
    "tez.am.log.level",
    "tez.task.log.level",
    "tez.task.profiling.enabled",
    "tez.task.profiling.interval.millis",
    
    // Counters and Limits
    "tez.counters.max",
    "tez.counters.groups.max",
    "tez.task.max.output.limit",
    
    // Session Management
    "tez.session.mode",
    "tez.session.client.timeout.sec",
    
    // Advanced Runtime
    "tez.runtime.transfer.data-via-events.enabled",
    "tez.runtime.pipelined-shuffle.enabled",
    "tez.runtime.optimize.local.fetch",
    "tez.runtime.ifile.readahead",
    "tez.runtime.ifile.readahead.bytes",
    
    // Security
    "tez.am.view-acls",
    "tez.am.modify-acls",
    "tez.am.acls.enabled",
    
    // Speculation
    "tez.am.speculation.enabled",
    "tez.am.speculation.speculative-capacity-factor",
    
    // Recovery
    "tez.am.dag.recovery.enabled",
    "tez.am.dag.recovery.timeout.sec",
    
    // Advanced Configuration
    "tez.task.get.task.sleep.interval-ms.max",
    "tez.am.heartbeat.interval-ms.max",
    "tez.runtime.key.class",
    "tez.runtime.value.class",
    "tez.runtime.key.comparator.class",
    
    // Add more parameters as needed from official documentation
    // (This list covers core parameters but may require updates)
};


static bool isRatio(const char *value) {
    char *end;
    float ratio = strtof(value, &end);
    return *end == '\0' && ratio >= 0.0f && ratio <= 1.0f;
}

ValidationResult validateTezConfigParam(const char *param_name, const char *value) {
    // Check if parameter exists
    bool param_exists = false;
    for (size_t i = 0; i < sizeof(tez_canonical_params)/sizeof(tez_canonical_params[0]); i++) {
        if (strcmp(param_name, tez_canonical_params[i]) == 0) {
            param_exists = true;
            break;
        }
    }
    if (!param_exists) return ERROR_PARAM_NOT_FOUND;

    // Check value presence
    if (!value || strlen(value) == 0) return ERROR_VALUE_EMPTY;

    // Parameter-specific validation
    if (strstr(param_name, ".memory.mb") != NULL) {
        return isMemorySize(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strstr(param_name, ".vcores") != NULL) {
        return isPositiveInteger(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "tez.am.container.heap.memory-mb.ratio") == 0) {
        return isRatio(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strstr(param_name, ".enabled") != NULL) {
        return isValidBoolean(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "tez.runtime.compress.codec") == 0) {
        return isValidCompressionCodec(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strstr(param_name, ".log.level") != NULL) {
        return isValidLogLevel(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strstr(param_name, ".percent") != NULL) {
        char *end;
        float pct = strtof(value, &end);
        return *end == '\0' && pct >= 0.0f && pct <= 100.0f;
    }
    else if (strstr(param_name, ".timeout") != NULL ||
             strstr(param_name, ".interval") != NULL) {
        return isPositiveInteger(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "tez.session.mode") == 0) {
        return strcmp(value, "none") == 0 || strcmp(value, "session") == 0
            ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "tez.am.node-blacklisting.ignore-threshold.node-percent") == 0) {
        char *end;
        float pct = strtof(value, &end);
        return *end == '\0' && pct >= 0.0f && pct <= 100.0f;
    }
    else if (strcmp(param_name, "tez.staging-dir") == 0) {
        return strlen(value) > 0 ? VALIDATION_OK : ERROR_VALUE_EMPTY;
    }
    else if (strcmp(param_name, "tez.am.speculation.speculative-capacity-factor") == 0) {
        char *end;
        float factor = strtof(value, &end);
        return *end == '\0' && factor >= 0.0f;
    }

    return VALIDATION_OK;
}

#define NUM_TEZ_CANONICAL_PARAMS (sizeof(tez_canonical_params) / sizeof(tez_canonical_params[0]))

char *normalize_tez_param_name(const char *param_name) {
    size_t len = strlen(param_name);
    char *normalized = malloc(len + 1);
    if (!normalized) return NULL;

    size_t j = 0;
    char prev = '\0';
    for (size_t i = 0; i < len; i++) {
        char c = tolower(param_name[i]);
        // Replace underscores with dots, leave hyphens as-is
        if (c == '_') {
            c = '.';
        }
        // Replace other non-alphanumeric characters with dots
        if (c != '.' && c != '-' && !isalnum(c)) {
            c = '.';
        }
        // Collapse consecutive dots
        if (c == '.' && prev == '.') {
            continue;
        }
        normalized[j++] = c;
        prev = c;
    }
    normalized[j] = '\0';

    // Trim trailing dots
    while (j > 0 && normalized[j - 1] == '.') {
        normalized[--j] = '\0';
    }

    // Trim leading dots
    size_t start = 0;
    while (start < j && normalized[start] == '.') {
        start++;
    }
    if (start > 0) {
        memmove(normalized, normalized + start, j - start + 1); // +1 for null terminator
        j -= start;
    }

    // Reallocate to save memory
    char *trimmed = realloc(normalized, j + 1);
    return trimmed ? trimmed : normalized;
}

TezConfigParam *parse_tez_config_param(const char *param_name, const char *param_value) {
    char *normalized = normalize_tez_param_name(param_name);
    if (!normalized) return NULL;

    for (size_t i = 0; i < NUM_TEZ_CANONICAL_PARAMS; i++) {
        if (strcmp(normalized, tez_canonical_params[i]) == 0) {
            TezConfigParam *config = malloc(sizeof(TezConfigParam));
            if (!config) {
                free(normalized);
                return NULL;
            }
            config->name = strdup(tez_canonical_params[i]);
            config->value = param_value ? strdup(param_value) : NULL;

            if (!config->name || (param_value && !config->value)) {
                free(config->name);
                free(config->value);
                free(config);
                free(normalized);
                return NULL;
            }

            free(normalized);
            return config;
        }
    }

    free(normalized);
    return NULL;
}




ConfigStatus modify_tez_config(const char *param, const char *value, const char *config_file) {
    char *file_path = NULL;
    const char *tez_home = getenv("TEZ_HOME");
    char *possible_paths[3] = {NULL, NULL, NULL};
    int num_paths = 0;

    // Construct possible file paths
    if (tez_home != NULL) {
        size_t len = strlen(tez_home) + strlen("/conf/") + strlen(config_file) + 1;
        possible_paths[num_paths] = malloc(len);
        snprintf(possible_paths[num_paths], len, "%s/conf/%s", tez_home, config_file);
        num_paths++;
    }

    // Bigtop paths
    const char *bigtop_paths[] = {"/opt/tez/conf/", "/usr/local/tez/conf/"};
    for (int i = 0; i < 2; i++) {
        size_t len = strlen(bigtop_paths[i]) + strlen(config_file) + 1;
        possible_paths[num_paths] = malloc(len);
        snprintf(possible_paths[num_paths], len, "%s%s", bigtop_paths[i], config_file);
        num_paths++;
    }

    // Check if any of the possible paths exist
    for (int i = 0; i < num_paths; i++) {
        struct stat st;
        if (stat(possible_paths[i], &st) == 0) {
            file_path = strdup(possible_paths[i]);
            break;
        }
    }

    // Free possible_paths
    for (int i = 0; i < num_paths; i++) {
        free(possible_paths[i]);
    }

    // If not found, determine where to create the file
    if (file_path == NULL) {
        // Check TEZ_HOME/conf directory
        if (tez_home != NULL) {
            char tez_conf_dir[PATH_MAX];
            snprintf(tez_conf_dir, sizeof(tez_conf_dir), "%s/conf", tez_home);
            struct stat st;
            if (stat(tez_conf_dir, &st) == 0 && S_ISDIR(st.st_mode)) {
                size_t len = strlen(tez_conf_dir) + strlen(config_file) + 2;
                file_path = malloc(len);
                snprintf(file_path, len, "%s/%s", tez_conf_dir, config_file);
            }
        }
        // Check Bigtop directories
        if (file_path == NULL) {
            const char *bigtop_dirs[] = {"/opt/tez/conf", "/usr/local/tez/conf"};
            for (int i = 0; i < 2; i++) {
                struct stat st;
                if (stat(bigtop_dirs[i], &st) == 0 && S_ISDIR(st.st_mode)) {
                    size_t len = strlen(bigtop_dirs[i]) + strlen(config_file) + 2;
                    file_path = malloc(len);
                    snprintf(file_path, len, "%s/%s", bigtop_dirs[i], config_file);
                    break;
                }
            }
        }
        if (file_path == NULL) {
            return -1;
        }
    }

    // Proceed to parse or create XML
    xmlDocPtr doc = NULL;
    xmlNodePtr root_node = NULL;

    // Check if file exists
    FILE *file = fopen(file_path, "r");
    if (file != NULL) {
        fclose(file);
        doc = xmlReadFile(file_path, NULL, 0);
    } else {
        doc = xmlNewDoc(BAD_CAST "1.0");
        root_node = xmlNewNode(NULL, BAD_CAST "configuration");
        xmlDocSetRootElement(doc, root_node);
    }

    if (doc == NULL) {
        free(file_path);
        return -1;
    }

    root_node = xmlDocGetRootElement(doc);
    if (root_node == NULL) {
        root_node = xmlNewNode(NULL, BAD_CAST "configuration");
        xmlDocSetRootElement(doc, root_node);
    } else if (xmlStrcmp(root_node->name, BAD_CAST "configuration") != 0) {
        xmlFreeDoc(doc);
        free(file_path);
        return -1;
    }

    int found = 0;
    xmlNodePtr cur = NULL;
    for (cur = root_node->children; cur != NULL; cur = cur->next) {
        if (cur->type == XML_ELEMENT_NODE && xmlStrcmp(cur->name, BAD_CAST "property") == 0) {
            xmlChar *name = NULL;
            xmlNodePtr value_node = NULL;
            xmlNodePtr child = cur->children;
            while (child != NULL) {
                if (child->type == XML_ELEMENT_NODE) {
                    if (xmlStrcmp(child->name, BAD_CAST "name") == 0) {
                        name = xmlNodeGetContent(child);
                    } else if (xmlStrcmp(child->name, BAD_CAST "value") == 0) {
                        value_node = child;
                    }
                }
                child = child->next;
            }
            if (name != NULL) {
                if (xmlStrcmp(name, (const xmlChar *)param) == 0) {
                    found = 1;
                    if (value_node != NULL) {
                        xmlNodeSetContent(value_node, (const xmlChar *)value);
                    } else {
                        xmlNewTextChild(cur, NULL, BAD_CAST "value", (const xmlChar *)value);
                    }
                    xmlFree(name);
                    break;
                }
                xmlFree(name);
            }
        }
    }

    if (!found) {
        xmlNodePtr prop = xmlNewNode(NULL, BAD_CAST "property");
        xmlNewTextChild(prop, NULL, BAD_CAST "name", (const xmlChar *)param);
        xmlNewTextChild(prop, NULL, BAD_CAST "value", (const xmlChar *)value);
        xmlAddChild(root_node, prop);
    }

    int save_result = xmlSaveFormatFileEnc(file_path, doc, "UTF-8", 1);
    xmlFreeDoc(doc);
    xmlCleanupParser();
    free(file_path);

    return save_result != -1 ? 0 : -1;
}


////////////////////////////////////pig///////////////////////////////////////////

typedef struct PigConfigParam {
    const char *canonical_param;
    char *value;
} PigConfigParam;

static const struct {
    const char *canonical;
    const char *normalized;
} predefined_parameters[] = {
    // Original parameters
    { "pig.exec.mapPartAgg", "pig.exec.mappartagg" },
    { "pig.skewedjoin.reduce.memusage", "pig.skewedjoin.reduce.memusage" },
    { "pig.cachedbag.memusage", "pig.cachedbag.memusage" },
    { "pig.maxCombinedSplitSize", "pig.maxcombinedsplitsize" },
    { "pig.optimizer.multiquery", "pig.optimizer.multiquery" },
    { "pig.tmpFileCompression", "pig.tmpfilecompression" },
    { "pig.exec.nocombiner", "pig.exec.nocombiner" },
    { "pig.user.cache.location", "pig.user.cache.location" },
    { "pig.exec.reducers.bytes.per.reducer", "pig.exec.reducers.bytes.per.reducer" },
    { "pig.exec.reducers.max", "pig.exec.reducers.max" },
    { "pig.exec.mapPartAgg.minFraction", "pig.exec.mappartagg.minfraction" },
    { "pig.join.optimized", "pig.join.optimized" },
    { "pig.skewedjoin.minimizeDataSkew", "pig.skewedjoin.minimizedataskew" },
    { "pig.auto.local.enabled", "pig.auto.local.enabled" },
    { "pig.auto.local.input.maxbytes", "pig.auto.local.input.maxbytes" },
    { "pig.script.allow.udf.import", "pig.script.allow.udf.import" },
    { "pig.logfile", "pig.logfile" },
    { "pig.stats.logging.level", "pig.stats.logging.level" },
    { "pig.job.priority", "pig.job.priority" },
    { "pig.script.udf.import.path", "pig.script.udf.import.path" },

    // New parameters covering Pig's full configuration capabilities
    { "pig.default.parallel", "pig.default.parallel" },
    { "pig.splitCombination", "pig.splitcombination" },
    { "pig.exec.mapPartition", "pig.exec.mappartition" },
    { "pig.broadcast.join.threshold", "pig.broadcast.join.threshold" },
    { "pig.join.tuples.batch.size", "pig.join.tuples.batch.size" },
    { "pig.mergeCombinedSplitSize", "pig.mergecombinedsplitsize" },
    { "pig.output.lzo.enabled", "pig.output.lzo.enabled" },
    { "pig.optimizer.list", "pig.optimizer.list" },
    { "pig.jar", "pig.jar" },
    { "pig.udf.profiles", "pig.udf.profiles" },
    { "pig.task.agg.memusage", "pig.task.agg.memusage" },
    { "pig.spill.size.threshold", "pig.spill.size.threshold" },
    { "pig.optimizer.rules.disabled", "pig.optimizer.rules.disabled" },
    { "pig.hadoop.version", "pig.hadoop.version" },
    { "pig.execution.mode", "pig.execution.mode" },
    { "pig.schema.tuple.enable", "pig.schema.tuple.enable" },
    { "pig.datetime.default.tz", "pig.datetime.default.tz" },
    { "pig.optimizer.uniqueKey", "pig.optimizer.uniquekey" },
    { "pig.stats.reliability", "pig.stats.reliability" },
    { "pig.optimizer.disable.splitcombiner", "pig.optimizer.disable.splitcombiner" },
    { "pig.udf.import.list", "pig.udf.import.list" },
    { "pig.jobcontrol.statement.retry.max", "pig.jobcontrol.statement.retry.max" },
    { "pig.jobcontrol.statement.retry.interval", "pig.jobcontrol.statement.retry.interval" },
    { "pig.output.compression.enabled", "pig.output.compression.enabled" },
    { "pig.output.compression.codec", "pig.output.compression.codec" },
    { "pig.relocation.jars", "pig.relocation.jars" },
    { "pig.script.auto.progress", "pig.script.auto.progress" },
    { "pig.tez.jvm.args", "pig.tez.jvm.args" },
    { "pig.tez.container.reuse", "pig.tez.container.reuse" }
};

static bool isCompressionCodec(const char *value) {
    const char *valid[] = {"none", "gzip", "snappy", "lzo", "bzip2", "zstd", NULL};
    for (int i = 0; valid[i]; i++)
        if (strcasecmp(value, valid[i]) == 0) return true;
    return false;
}

static bool isExecutionModeValid(const char *value) {
    return strcasecmp(value, "mapreduce") == 0 || strcasecmp(value, "tez") == 0;
}

static bool isTimezoneValid(const char *value) {
    // Simple check for format (more comprehensive validation would require TZ database lookup)
    return strchr(value, '/') != NULL && strlen(value) > 3;
}

ValidationResult validatePigConfigParam(const char *param_name, const char *value) {
    // Check if parameter exists
    bool param_exists = true;
 //   for (size_t i = 0; i < sizeof(predefined_parameters)/sizeof(predefined_parameters[0]); i++) {
   //     if (strcmp(param_name, predefined_parameters[i].canonical) == 0) {
     //       param_exists = true;
       //     break;
        //}
    //}
    if (!param_exists) return ERROR_PARAM_NOT_FOUND;

    // Check value presence
    if (!value || strlen(value) == 0) return ERROR_VALUE_EMPTY;

    // Parameter-specific validation
    if (strstr(param_name, ".memusage") != NULL ||
        strstr(param_name, ".threshold") != NULL) {
        return isMemorySize(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strstr(param_name, ".enabled") != NULL ||
             strstr(param_name, ".optimized") != NULL ||
             strstr(param_name, ".allow.") != NULL) {
        return isValidBoolean(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "pig.default.parallel") == 0 ||
             strcmp(param_name, "pig.exec.reducers.max") == 0) {
        return isPositiveInteger(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "pig.output.compression.codec") == 0) {
        return isCompressionCodec(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "pig.execution.mode") == 0) {
        return isExecutionModeValid(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "pig.datetime.default.tz") == 0) {
        return isTimezoneValid(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "pig.exec.mapPartAgg.minFraction") == 0) {
        char *end;
        float fraction = strtof(value, &end);
        return *end == '\0' && fraction >= 0.0f && fraction <= 1.0f;
    }
    else if (strcmp(param_name, "pig.auto.local.input.maxbytes") == 0) {
        return isMemorySize(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "pig.job.priority") == 0) {
        const char *valid[] = {"VERY_HIGH", "HIGH", "NORMAL", "LOW", "VERY_LOW", NULL};
        for (int i = 0; valid[i]; i++)
            if (strcmp(value, valid[i]) == 0) return VALIDATION_OK;
        return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "pig.tez.jvm.args") == 0) {
        return strstr(value, "-Xmx") != NULL && strstr(value, "-Xms") != NULL
            ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }

    return VALIDATION_OK;
}


#define NUM_PIG_PARAMS (sizeof(predefined_parameters) / sizeof(predefined_parameters[0]))

void normalize_pig_param_name(const char *input, char *output) {
    int i, j = 0;
    char prev = '\0';
    for (i = 0; input[i] != '\0' && j < 255; i++) {
        char c = tolower(input[i]);
        if (c == '_' || c == '.') {
            c = '.';
        }
        if (c == '.' && prev == '.') {
            continue;
        }
        output[j++] = c;
        prev = c;
    }
    if (j > 0 && output[j-1] == '.') {
        j--;
    }
    output[j] = '\0';
}

ConfigResult *validate_pig_config_param(const char *param_name, const char *param_value) {
    char normalized_input[256];
    normalize_pig_param_name(param_name, normalized_input);

    for (size_t i = 0; i < NUM_PIG_PARAMS; i++) {
        if (strcmp(normalized_input, predefined_parameters[i].normalized) == 0) {
            ConfigResult *result = (ConfigResult *)malloc(sizeof(ConfigResult));
            if (!result) {
                return NULL;
            }
            result->canonical_name = (char *)predefined_parameters[i].canonical;
            result->value = strdup(param_value);
            if (!result->value) {
                free(result);
                return NULL;
            }
            return result;
        }
    }
    return NULL;
}

ConfigStatus update_pig_config(char *param, char *value) {
    const char *pig_home = getenv("PIG_HOME");
    char *paths[3] = {NULL, NULL, NULL};
    int num_paths = 0;
    FILE *config_file = NULL;
    char *selected_path = NULL;
    ConfigStatus rc = XML_PARSE_ERROR;

    // Build potential configuration paths
    if (pig_home) {
        size_t conf_len = strlen(pig_home) + strlen("/conf/pig.conf") + 1;
        char *pig_conf = malloc(conf_len);
        if (!pig_conf) return FILE_NOT_FOUND;
        snprintf(pig_conf, conf_len, "%s/conf/pig.conf", pig_home);
        paths[num_paths++] = pig_conf;
    }

    // Add Red Hat default path
    char *redhat_conf = strdup("/opt/pig/conf/pig.conf");
    if (!redhat_conf) {
        for (int i = 0; i < num_paths; i++) free(paths[i]);
        return FILE_NOT_FOUND;
    }
    paths[num_paths++] = redhat_conf;

    // Add Debian default path
    char *debian_conf = strdup("/usr/local/pig/conf/pig.conf");
    if (!debian_conf) {
        for (int i = 0; i < num_paths; i++) free(paths[i]);
        return FILE_NOT_FOUND;
    }
    paths[num_paths++] = debian_conf;

    // Try to find existing config file
    for (int i = 0; i < num_paths; i++) {
        if (access(paths[i], F_OK) == 0) {
            selected_path = strdup(paths[i]);
            break;
        }
    }

    // If no existing file, find first writable directory
    if (!selected_path) {
        for (int i = 0; i < num_paths; i++) {
            char *dir = strdup(paths[i]);
            if (!dir) continue;

            char *slash = strrchr(dir, '/');
            if (slash) {
                *slash = '\0';
                if (access(dir, W_OK) == 0) {
                    selected_path = strdup(paths[i]);
                    free(dir);
                    break;
                }
            }
            free(dir);
        }
    }

    if (!selected_path) {
        for (int i = 0; i < num_paths; i++) free(paths[i]);
        return FILE_NOT_FOUND;
    }

    // Open or create config file
    config_file = fopen(selected_path, "r+");
    if (!config_file && errno == ENOENT) {
        config_file = fopen(selected_path, "w");
    }
    if (!config_file) {
        free(selected_path);
        for (int i = 0; i < num_paths; i++) free(paths[i]);
        return FILE_NOT_FOUND;
    }

    // Process file contents
    char line[MAX_LINE_LENGTH];
    char **lines = NULL;
    size_t line_count = 0;
    int param_found = 0;

    // Read existing lines
    if (fseek(config_file, 0, SEEK_SET) == 0) {
        while (fgets(line, sizeof(line), config_file)) {
            char **new_lines = realloc(lines, (line_count + 1) * sizeof(char *));
            if (!new_lines) goto cleanup;
            lines = new_lines;

            lines[line_count] = strdup(line);
            if (!lines[line_count]) goto cleanup;

            // Check for parameter match
            char *line_copy = strdup(line);
            if (line_copy) {
                char *key = strtok(line_copy, " =\t\n");
                if (key && strcmp(key, param) == 0) {
                    free(lines[line_count]);
                    int needed = snprintf(NULL, 0, "%s=%s\n", param, value);
                    lines[line_count] = malloc(needed + 1);
                    if (!lines[line_count]) {
                        free(line_copy);
                        goto cleanup;
                    }
                    snprintf(lines[line_count], needed + 1, "%s=%s\n", param, value);
                    param_found = 1;
                }
                free(line_copy);
            }
            line_count++;
        }
    }

    // Add new parameter if not found
    if (!param_found) {
        char **new_lines = realloc(lines, (line_count + 1) * sizeof(char *));
        if (!new_lines) goto cleanup;
        lines = new_lines;
        int needed = snprintf(NULL, 0, "%s=%s\n", param, value);
        lines[line_count] = malloc(needed + 1);
        if (!lines[line_count]) goto cleanup;
        snprintf(lines[line_count], needed + 1, "%s=%s\n", param, value);
        line_count++;
    }

    // Write back to file
    if (freopen(selected_path, "w", config_file) == NULL) {
        perror("freopen failed");
        rc = FILE_WRITE_ERROR;
        goto cleanup;
    }

    for (size_t i = 0; i < line_count; i++) {
        if (fputs(lines[i], config_file) == EOF) {
            rc = FILE_WRITE_ERROR;
            break;
        }
        // Removed free() here to prevent double-free
    }

    // Final error check
    if (ferror(config_file)) {
        rc = FILE_WRITE_ERROR;
        clearerr(config_file);
    } else {
        rc = SUCCESS;
    }

cleanup:
    // Single cleanup point for all allocations
    if (lines) {
        for (size_t i = 0; i < line_count; i++) {
            free(lines[i]);
        }
        free(lines);
    }
    if (config_file) fclose(config_file);
    free(selected_path);
    for (int i = 0; i < num_paths; i++) free(paths[i]);
    return rc;
}
///////////////////////////////presto////////////////////////////////////////////////////

typedef struct {
    const char *canonical_name;
    const char *config_file;
    const char *regex_pattern;
} PrestoConfigParam;

ConfigResult *get_presto_config_setting(const char *param_name, const char *param_value) {
    // Predefined list of the most impactful Presto configuration parameters
PrestoConfigParam presto_predefined_params[] = {
    // node.properties parameters
    {"node.id", "node.properties", "^node[._-]id$"},
    {"node.environment", "node.properties", "^node[._-]environment$"},
    {"node.data-dir", "node.properties", "^node[._-]data[._-]dir$"},
    {"node.launcher-log-file", "node.properties", "^node[._-]launcher[._-]log[._-]file$"},
    {"node.server-log-file", "node.properties", "^node[._-]server[._-]log[._-]file$"},
    {"node.presto-version", "node.properties", "^node[._-]presto[._-]version$"},
    {"node.allow-version-mismatch", "node.properties", "^node[._-]allow[._-]version[._-]mismatch$"},

    // config.properties - Coordinator & Discovery
    {"coordinator", "config.properties", "^coordinator$"},
    {"discovery-server.enabled", "config.properties", "^discovery[._-]server[._-]enabled$"},
    {"discovery.uri", "config.properties", "^discovery[._-]uri$"},

    // HTTP Server
    {"http-server.http.port", "config.properties", "^http[._-]server[._-]http[._-]port$"},
    {"http-server.https.port", "config.properties", "^http[._-]server[._-]https[._-]port$"},
    {"http-server.https.enabled", "config.properties", "^http[._-]server[._-]https[._-]enabled$"},
    {"http-server.https.keystore.path", "config.properties", "^http[._-]server[._-]https[._-]keystore[._-]path$"},
    {"http-server.https.keystore.key", "config.properties", "^http[._-]server[._-]https[._-]keystore[._-]key$"},
    {"http-server.https.truststore.path", "config.properties", "^http[._-]server[._-]https[._-]truststore[._-]path$"},
    {"http-server.log.path", "config.properties", "^http[._-]server[._-]log[._-]path$"},
    {"http-server.log.enabled", "config.properties", "^http[._-]server[._-]log[._-]enabled$"},
    {"http-server.authentication.type", "config.properties", "^http[._-]server[._-]authentication[._-]type$"},
    {"http-server.process-forwarded", "config.properties", "^http[._-]server[._-]process[._-]forwarded$"},

    // Query Management
    {"query.max-memory", "config.properties", "^query[._-]max[._-]memory$"},
    {"query.max-memory-per-node", "config.properties", "^query[._-]max[._-]memory[._-]per[._-]node$"},
    {"query.max-total-memory-per-node", "config.properties", "^query[._-]max[._-]total[._-]memory[._-]per[._-]node$"},
    {"query.max-execution-time", "config.properties", "^query[._-]max[._-]execution[._-]time$"},
    {"query.max-run-time", "config.properties", "^query[._-]max[._-]run[._-]time$"},
    {"query.client.timeout", "config.properties", "^query[._-]client[._-]timeout$"},
    {"query.min-expire-age", "config.properties", "^query[._-]min[._-]expire[._-]age$"},

    // Memory Management
    {"memory.heap-headroom-per-node", "config.properties", "^memory[._-]heap[._-]headroom[._-]per[._-]node$"},
    {"memory.max-revokable-memory-per-node", "config.properties", "^memory[._-]max[._-]revokable[._-]memory[._-]per[._-]node$"},

    // Task & Scheduler
    {"task.concurrency", "config.properties", "^task[._-]concurrency$"},
    {"task.http-response-threads", "config.properties", "^task[._-]http[._-]response[._-]threads$"},
    {"task.info-update-interval", "config.properties", "^task[._-]info[._-]update[._-]interval$"},
    {"scheduler.http-client.max-connections", "config.properties", "^scheduler[._-]http[._-]client[._-]max[._-]connections$"},
    {"scheduler.http-client.max-connections-per-server", "config.properties", "^scheduler[._-]http[._-]client[._-]max[._-]connections[._-]per[._-]server$"},
    {"scheduler.include-coordinator", "config.properties", "^scheduler[._-]include[._-]coordinator$"},
    {"node-scheduler.network-topology", "config.properties", "^node[._-]scheduler[._-]network[._-]topology$"},

    // Exchange
    {"exchange.client-threads", "config.properties", "^exchange[._-]client[._-]threads$"},
    {"exchange.max-buffer-size", "config.properties", "^exchange[._-]max[._-]buffer[._-]size$"},

    // Optimizer
    {"optimizer.dictionary-aggregation", "config.properties", "^optimizer[._-]dictionary[._-]aggregation$"},
    {"optimizer.optimize-hash-generation", "config.properties", "^optimizer[._-]optimize[._-]hash[._-]generation$"},
    {"redistribute-writes", "config.properties", "^redistribute[._-]writes$"},

    // JMX
    {"jmx.base-name", "config.properties", "^jmx[._-]base[._-]name$"},

    // Security
    {"internal-communication.https.required", "config.properties", "^internal[._-]communication[._-]https[._-]required$"},

    // Experimental/Spilling
    {"experimental.spiller-spill-path", "config.properties", "^experimental[._-]spiller[._-]spill[._-]path$"},
    {"spill-enabled", "config.properties", "^spill[._-]enabled$"},

    // Resource Management
    {"resource-manager", "config.properties", "^resource[._-]manager$"},
    {"resource-group-manager", "config.properties", "^resource[._-]group[._-]manager$"},

    // Additional parameters
    {"join-distribution-type", "config.properties", "^join[._-]distribution[._-]type$"},
    {"task.writer-count", "config.properties", "^task[._-]writer[._-]count$"},
    {"http-server.https.sni-host-check", "config.properties", "^http[._-]server[._-]https[._-]sni[._-]host[._-]check$"},
    {"query.max-stage-count", "config.properties", "^query[._-]max[._-]stage[._-]count$"}
};

    int num_params = sizeof(presto_predefined_params) / sizeof(presto_predefined_params[0]);

    for (int i = 0; i < num_params; i++) {
        regex_t regex;
        int ret;

        // Compile the regex pattern with case-insensitive matching
        ret = regcomp(&regex, presto_predefined_params[i].regex_pattern, REG_ICASE | REG_NOSUB);
        if (ret != 0) {
            // Handle regex compilation error (unlikely for static patterns)
            continue;
        }

        // Execute the regex match
        ret = regexec(&regex, param_name, 0, NULL, 0);
        regfree(&regex); // Free compiled regex

        if (ret == 0) {
            // Match found, prepare the result
            ConfigResult *result = malloc(sizeof(ConfigResult));
            if (!result) return NULL;

            result->canonical_name = strdup(presto_predefined_params[i].canonical_name);
            result->value = strdup(param_value);
            result->config_file = strdup(presto_predefined_params[i].config_file);

            if (!result->canonical_name || !result->value || !result->config_file) {
                // Free allocated memory in case of strdup failure
                free(result->canonical_name);
                free(result->value);
                free(result->config_file);
                free(result);
                return NULL;
            }

            return result;
        }
    }

    // No matching parameter found
    return NULL;
}


ValidationResult validatePrestoConfigParam(const char *param_name, const char *value) {
    // Check if parameter exists
    bool param_exists = true;
    
    
    if (!param_exists) return ERROR_PARAM_NOT_FOUND;

    // Check value presence
    if (!value || strlen(value) == 0) return ERROR_VALUE_EMPTY;

    // Parameter-specific validation
    if (strstr(param_name, ".port") != NULL) {
        return isValidPort(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strstr(param_name, ".enabled") != NULL ||
             strstr(param_name, ".required") != NULL ||
             strstr(param_name, "coordinator") == param_name) {
        return isValidBoolean(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strstr(param_name, ".memory") != NULL ||
             strstr(param_name, ".buffer-size") != NULL) {
        return isMemorySize(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "discovery.uri") == 0) {
        return isValidURI(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strstr(param_name, ".timeout") != NULL ||
             strstr(param_name, ".interval") != NULL ||
             strstr(param_name, ".age") != NULL) {
        return isValidDuration(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strstr(param_name, ".concurrency") != NULL ||
             strstr(param_name, ".threads") != NULL ||
             strstr(param_name, ".count") != NULL) {
        return isPositiveInteger(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "node-scheduler.network-topology") == 0) {
        return strcmp(value, "flat") == 0 || strcmp(value, "legacy") == 0
            ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strstr(param_name, ".path") != NULL) {
        return strlen(value) > 0 ? VALIDATION_OK : ERROR_VALUE_EMPTY;
    }
    else if (strcmp(param_name, "http-server.authentication.type") == 0) {
        const char *valid[] = {"password", "certificate", "jwt", "kerberos", "oauth2", NULL};
        for (int i = 0; valid[i]; i++)
            if (strcmp(value, valid[i]) == 0) return VALIDATION_OK;
        return ERROR_CONSTRAINT_VIOLATED;
    }

    return VALIDATION_OK;
}

ConfigStatus  set_presto_config(const char *param, const char *value, const char *config_file) {
    char *presto_home = getenv("PRESTO_HOME");
    char path[MAX_PATH_LEN];
    char *found_path = NULL;

    // Check PRESTO_HOME/etc/config_file
    if (presto_home != NULL) {
        snprintf(path, MAX_PATH_LEN, "%s/etc/%s", presto_home, config_file);
        if (access(path, F_OK) == 0) {
            found_path = strdup(path);
        }
    }

    // Check Bigtop default directories if not found yet
    if (!found_path) {
        const char *bigtop_dirs[] = {
            "/opt/presto/conf",
            "usr/local/presto",
            "/etc/presto",
            "/usr/local/presto/etc"
        };
        for (size_t i = 0; i < sizeof(bigtop_dirs)/sizeof(bigtop_dirs[0]); i++) {
            snprintf(path, MAX_PATH_LEN, "%s/%s", bigtop_dirs[i], config_file);
            if (access(path, F_OK) == 0) {
                found_path = strdup(path);
                break;
            }
        }
    }

    // If not found, create in first possible directory
    if (!found_path) {
        // Try PRESTO_HOME/etc first
        if (presto_home != NULL) {
            snprintf(path, MAX_PATH_LEN, "%s/etc", presto_home);
            if (mkdir(path, 0755) != 0 && errno != EEXIST) {
                fprintf(stderr, "Failed to create directory %s: %s\n", path, strerror(errno));
            } else {
                snprintf(path, MAX_PATH_LEN, "%s/etc/%s", presto_home, config_file);
                FILE *fp = fopen(path, "w");
                if (fp) {
                    fclose(fp);
                    found_path = strdup(path);
                } else {
                    fprintf(stderr, "Failed to create file %s: %s\n", path, strerror(errno));
                }
            }
        }

        // If still not found, try Bigtop directories
        if (!found_path) {
            const char *bigtop_create_dirs[] = {
                "/etc/presto/conf",
                "/etc/presto",
                "/usr/lib/presto/etc"
            };
            for (size_t i = 0; i < sizeof(bigtop_create_dirs)/sizeof(bigtop_create_dirs[0]); i++) {
                if (mkdir(bigtop_create_dirs[i], 0755) != 0 && errno != EEXIST) {
                    fprintf(stderr, "Failed to create directory %s: %s\n", bigtop_create_dirs[i], strerror(errno));
                    continue;
                }
                snprintf(path, MAX_PATH_LEN, "%s/%s", bigtop_create_dirs[i], config_file);
                FILE *fp = fopen(path, "w");
                if (fp) {
                    fclose(fp);
                    found_path = strdup(path);
                    break;
                } else {
                    fprintf(stderr, "Failed to create file %s: %s\n", path, strerror(errno));
                }
            }
        }
    }

    if (!found_path) {
        fprintf(stderr, "Failed to find or create config file\n");
        return FILE_NOT_FOUND;
    }

    // Prepare temp file path
    char temp_path[MAX_PATH_LEN + 4];
    snprintf(temp_path, sizeof(temp_path), "%s.tmp", found_path);

    FILE *src = fopen(found_path, "r");
    FILE *dst = fopen(temp_path, "w");
    if (!dst) {
        fprintf(stderr, "Failed to create temp file %s: %s\n", temp_path, strerror(errno));
        free(found_path);
        return FILE_NOT_FOUND;
    }

    int found = 0;
    if (src) {
        char line[1024];
        while (fgets(line, sizeof(line), src) != NULL) {
            char *original_line = line;
            char line_copy[1024];
            strncpy(line_copy, line, sizeof(line_copy));
            line_copy[sizeof(line_copy) - 1] = '\0'; // Ensure null-terminated

            char *trimmed = trim(line_copy);
            if (trimmed[0] == '#') {
                fputs(original_line, dst);
                continue;
            }

            char *equals = strchr(trimmed, '=');
            if (!equals) {
                fputs(original_line, dst);
                continue;
            }

            *equals = '\0';
            char *key = trim(trimmed);
            if (strcmp(key, param) == 0) {
                fprintf(dst, "%s=%s\n", param, value);
                found = 1;
            } else {
                fputs(original_line, dst);
            }
        }
        fclose(src);
    }

    if (!found) {
        fprintf(dst, "%s=%s\n", param, value);
    }

    fclose(dst);

    // Replace the original file with the temp file
    if (rename(temp_path, found_path) != 0) {
        fprintf(stderr, "Failed to replace file: %s\n", strerror(errno));
        remove(temp_path);
        free(found_path);
        return -1;
    }

    free(found_path);
    return SUCCESS;
}



/////////////////////////////////atlas//////////////////////////////////////

typedef struct {
    const char *canonicalName;
    const char *normalizedName;
    const char *configFile;
} AtlasConfigParam;

static const AtlasConfigParam predefinedParams[] = {
    // Server Configuration
    {"atlas.server.http.port", "atlas.server.http.port", "atlas-application.properties"},
    {"atlas.server.https.port", "atlas.server.https.port", "atlas-application.properties"},
    {"atlas.server.bind.address", "atlas.server.bind.address", "atlas-application.properties"},
    {"atlas.server.admin.port", "atlas.server.admin.port", "atlas-application.properties"},
    {"atlas.rest.address", "atlas.rest.address", "atlas-application.properties"},
    {"atlas.server.data", "atlas.server.data", "atlas-application.properties"},
    {"atlas.server.ha.enabled", "atlas.server.ha.enabled", "atlas-application.properties"},

    // Security & Authentication
    {"atlas.enableTLS", "atlas.enabletls", "atlas-application.properties"},
    {"atlas.ssl.keystore.file", "atlas.ssl.keystore.file", "atlas-application.properties"},
    {"atlas.ssl.keystore.password", "atlas.ssl.keystore.password", "atlas-application.properties"},
    {"atlas.ssl.truststore.file", "atlas.ssl.truststore.file", "atlas-application.properties"},
    {"atlas.ssl.truststore.password", "atlas.ssl.truststore.password", "atlas-application.properties"},
    {"atlas.authentication.method.ldap.url", "atlas.authentication.method.ldap.url", "atlas-application.properties"},
    {"atlas.authentication.method.ldap.userDNpattern", "atlas.authentication.method.ldap.userdnpattern", "atlas-application.properties"},
    {"atlas.authentication.method.kerberos.keytab", "atlas.authentication.method.kerberos.keytab", "atlas-application.properties"},
    {"atlas.authentication.method.oidc.issuer.url", "atlas.authentication.method.oidc.issuer.url", "atlas-application.properties"},
    {"atlas.authorization.simple.authz.policy.file", "atlas.authorization.simple.authz.policy.file", "atlas-application.properties"},

    // Storage & Backend
    {"atlas.graph.storage.backend", "atlas.graph.storage.backend", "atlas-graph.properties"},
    {"atlas.graph.storage.hbase.table", "atlas.graph.storage.hbase.table", "atlas-graph.properties"},
    {"atlas.graph.storage.cassandra.keyspace", "atlas.graph.storage.cassandra.keyspace", "atlas-graph.properties"},
    {"atlas.graph.index.search.backend", "atlas.graph.index.search.backend", "atlas-graph.properties"},
    {"atlas.graph.index.search.solr.zookeeper-url", "atlas.graph.index.search.solr.zookeeper-url", "atlas-graph.properties"},
    {"atlas.graph.index.search.elasticsearch.hosts", "atlas.graph.index.search.elasticsearch.hosts", "atlas-graph.properties"},

    // Metadata & Governance
    {"atlas.metadata.namespace", "atlas.metadata.namespace", "atlas-application.properties"},
    {"atlas.entity.audit.export", "atlas.entity.audit.export", "atlas-application.properties"},
    {"atlas.entity.audit.retention.days", "atlas.entity.audit.retention.days", "atlas-application.properties"},
    {"atlas.glossary.import.file", "atlas.glossary.import.file", "atlas-application.properties"},
    {"atlas.tag.policy.file", "atlas.tag.policy.file", "atlas-application.properties"},

    // Notification & Messaging
    {"atlas.notification.embedded", "atlas.notification.embedded", "atlas-application.properties"},
    {"atlas.kafka.zookeeper.connect", "atlas.kafka.zookeeper.connect", "atlas-application.properties"},
    {"atlas.notification.create.topics", "atlas.notification.create.topics", "atlas-application.properties"},
    {"atlas.notification.max.retries", "atlas.notification.max.retries", "atlas-application.properties"},

    // High Availability
    {"atlas.server.ha.zookeeper.connect", "atlas.server.ha.zookeeper.connect", "atlas-application.properties"},
    {"atlas.server.ha.zookeeper.session.timeout", "atlas.server.ha.zookeeper.session.timeout", "atlas-application.properties"},
    {"atlas.server.ha.id", "atlas.server.ha.id", "atlas-application.properties"},

    // Performance & Monitoring
    {"atlas.metrics.enabled", "atlas.metrics.enabled", "atlas-application.properties"},
    {"atlas.metrics.reporters", "atlas.metrics.reporters", "atlas-application.properties"},
    {"atlas.performance.cache.size", "atlas.performance.cache.size", "atlas-application.properties"},

    // Data Governance
    {"atlas.data.quality.validator.class", "atlas.data.quality.validator.class", "atlas-application.properties"},
    {"atlas.lineage.audit.enabled", "atlas.lineage.audit.enabled", "atlas-application.properties"},
    {"atlas.policy.evaluation.enabled", "atlas.policy.evaluation.enabled", "atlas-application.properties"},

    // UI Configuration
    {"atlas.ui.default.namespace", "atlas.ui.default.namespace", "atlas-application.properties"},
    {"atlas.ui.search.result.limit", "atlas.ui.search.result.limit", "atlas-application.properties"},

    // Advanced Features
    {"atlas.titan.attribute.ids.enabled", "atlas.titan.attribute.ids.enabled", "atlas-graph.properties"},
    {"atlas.fulltext.search.enabled", "atlas.fulltext.search.enabled", "atlas-application.properties"},
    {"atlas.entity.relationships.enabled", "atlas.entity.relationships.enabled", "atlas-application.properties"}
};


// Atlas-specific helper functions
static bool isHostPortList(const char *value) {
    char *copy = strdup(value);
    char *token = strtok(copy, ",");
    bool valid = true;
    
    while (token != NULL) {
        char *colon = strchr(token, ':');
        if (!colon || !isValidPort(colon + 1)) {
            valid = false;
            break;
        }
        token = strtok(NULL, ",");
    }
    
    free(copy);
    return valid;
}

static bool isURL(const char *value) {
    return strstr(value, "://") != NULL && strlen(value) > 8;
}

static bool fileExists(const char *path) {
    return access(path, R_OK) == 0;
}

ValidationResult validateAtlasConfigParam(const char *param_name, const char *value) {
    // Check if parameter exists
    bool param_exists = false;
    regex_t regex;
    regmatch_t matches[3];
    
    for (size_t i = 0; i < sizeof(predefinedParams)/sizeof(predefinedParams[0]); i++) {
        if (regcomp(&regex, predefinedParams[i].canonicalName, REG_EXTENDED|REG_ICASE) != 0) continue;
        if (regexec(&regex, param_name, 3, matches, 0) == 0) {
            param_exists = true;
            regfree(&regex);
            break;
        }
        regfree(&regex);
    }
    
    if (!param_exists) return ERROR_PARAM_NOT_FOUND;

    // Check value presence
    if (!value || strlen(value) == 0) return ERROR_VALUE_EMPTY;

    // Parameter-specific validation
    if (strstr(param_name, ".port") != NULL) {
        return isValidPort(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strstr(param_name, ".enabled") != NULL ||
             strstr(param_name, ".embedded") != NULL ||
             strstr(param_name, ".create.topics") != NULL) {
        return isValidBoolean(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strstr(param_name, ".file") != NULL ||
             strstr(param_name, ".keytab") != NULL) {
        if (!fileExists(value)) return ERROR_PARAM_NOT_FOUND;
    }
    else if (strstr(param_name, ".url") != NULL ||
             strstr(param_name, ".rest.address") != NULL) {
        return isURL(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strstr(param_name, "zookeeper.connect") != NULL ||
             strstr(param_name, "zookeeper-url") != NULL) {
        return isHostPortList(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strstr(param_name, ".storage.backend") != NULL) {
        const char *valid[] = {"hbase", "cassandra", "berkeleyje", NULL};
        for (int i = 0; valid[i]; i++)
            if (strcmp(value, valid[i]) == 0) return VALIDATION_OK;
        return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strstr(param_name, ".index.search.backend") != NULL) {
        const char *valid[] = {"solr", "elasticsearch", "lucene", NULL};
        for (int i = 0; valid[i]; i++)
            if (strcmp(value, valid[i]) == 0) return VALIDATION_OK;
        return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strstr(param_name, ".retention.days") != NULL ||
             strstr(param_name, ".max.retries") != NULL ||
             strstr(param_name, ".cache.size") != NULL) {
        return isPositiveInteger(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strstr(param_name, ".authentication.method") != NULL) {
        const char *valid[] = {"ldap", "kerberos", "file", "oidc", NULL};
        for (int i = 0; valid[i]; i++)
            if (strstr(value, valid[i]) != NULL) return VALIDATION_OK;
        return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strstr(param_name, ".hosts") != NULL) {
        char *copy = strdup(value);
        char *token = strtok(copy, ",");
        bool valid = true;
        while (token) {
            if (!strchr(token, ':') || !isValidPort(strchr(token, ':') + 1)) {
                valid = false;
                break;
            }
            token = strtok(NULL, ",");
        }
        free(copy);
        return valid ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }

    return VALIDATION_OK;
}

char *normalize_atlas_param_name(const char *paramName) {
    if (paramName == NULL) return NULL;

    size_t len = strlen(paramName);
    char *normalized = malloc(len + 1);
    if (normalized == NULL) return NULL;

    for (size_t i = 0; i < len; i++) {
        char c = paramName[i];
        if (c == '_') {
            normalized[i] = '.';
        } else {
            normalized[i] = tolower((unsigned char)c);
        }
    }
    normalized[len] = '\0';
    return normalized;
}

ConfigResult *validate_config_param(const char *paramName, const char *paramValue) {
    if (paramName == NULL || paramValue == NULL) return NULL;

    char *normalizedName = normalize_atlas_param_name(paramName);
    if (normalizedName == NULL) return NULL;

    for (size_t i = 0; i < sizeof(predefinedParams)/sizeof(predefinedParams[0]); i++) {
        if (strcmp(normalizedName, predefinedParams[i].normalizedName) == 0) {
            ConfigResult *result = malloc(sizeof(ConfigResult));
            if (result == NULL) {
                free(normalizedName);
                return NULL;
            }
            result->canonical_name = strdup(predefinedParams[i].canonicalName);
            result->value = strdup(paramValue);
            result->config_file = strdup(predefinedParams[i].configFile);

            free(normalizedName);

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

    free(normalizedName);
    return NULL;
}

void free_atlas_config_result(ConfigResult *result) {
    if (result) {
        free(result->canonical_name);
        free(result->value);
        free(result->config_file);
        free(result);
    }
}

static int is_directory(const char *path) {
    struct stat statbuf;
    return (stat(path, &statbuf) == 0) && S_ISDIR(statbuf.st_mode);
}

static char* determine_file_path(const char *filename) {
    char *atlas_home = getenv("ATLAS_HOME");
    char *paths[] = {
        atlas_home,          // First priority: ATLAS_HOME
        "/opt/atlas",        // Red Hat fallback
        "/usr/local/atlas"   // Debian fallback
    };
    const char *conf_subdir = "conf";
    static char selected_path[MAX_PATH_LEN];

    for (size_t i = 0; i < sizeof(paths)/sizeof(paths[0]); i++) {
        if (!paths[i]) continue;

        char conf_dir[MAX_PATH_LEN];
        int rc = snprintf(conf_dir, sizeof(conf_dir), "%s/%s", paths[i], conf_subdir);
        if (rc < 0 || (size_t)rc >= sizeof(conf_dir)) continue;

        if (!is_directory(conf_dir)) continue;

        rc = snprintf(selected_path, sizeof(selected_path), "%s/%s/%s", 
                     paths[i], conf_subdir, filename);
        if (rc < 0 || (size_t)rc >= sizeof(selected_path)) continue;

        if (access(selected_path, F_OK) == 0) {
            return selected_path;
        }
    }

    return NULL;  // File not found in any valid location
}

static void trim_trailing_whitespace(char *str) {
    char *end = str + strlen(str) - 1;
    while (end >= str && isspace((unsigned char)*end)) {
        end--;
    }
    *(end + 1) = '\0';
}

ConfigStatus update_atlas_config(const char *param, const char *value, const char *filename) {
    char *file_path = determine_file_path(filename);
    if (!file_path) {
        return FILE_NOT_FOUND; // No suitable directory found
    }

    FILE *fp = fopen(file_path, "r");
    int file_exists = (fp != NULL);
    char **lines = NULL;
    size_t num_lines = 0;
    int param_found = 0;
    char buffer[1024];

    if (file_exists) {
        while (fgets(buffer, sizeof(buffer), fp) != NULL) {
            char *line = buffer;
            while (isspace((unsigned char)*line)) line++; // Trim leading whitespace

            if (*line == '#') {
                // Comment line, add as-is
                lines = realloc(lines, (num_lines + 1) * sizeof(char *));
                if (!lines) goto error;
                lines[num_lines] = strdup(buffer);
                if (!lines[num_lines]) goto error;
                num_lines++;
                continue;
            }

            char *equals = strchr(line, '=');
            if (equals) {
                *equals = '\0';
                char *key = line;
                trim_trailing_whitespace(key);
                if (strcmp(key, param) == 0) {
                    // Replace this line
                    char new_line[1024];
                    snprintf(new_line, sizeof(new_line), "%s=%s\n", param, value);
                    lines = realloc(lines, (num_lines + 1) * sizeof(char *));
                    if (!lines) goto error;
                    lines[num_lines] = strdup(new_line);
                    if (!lines[num_lines]) goto error;
                    num_lines++;
                    param_found = 1;
                    *equals = '=';
                    continue;
                }
                *equals = '=';
            }

            // Add the line as-is
            lines = realloc(lines, (num_lines + 1) * sizeof(char *));
            if (!lines) goto error;
            lines[num_lines] = strdup(buffer);
            if (!lines[num_lines]) goto error;
            num_lines++;
        }
        fclose(fp);
    }

    if (!param_found) {
        // Append new parameter
        lines = realloc(lines, (num_lines + 1) * sizeof(char *));
        if (!lines) goto error;
        char new_line[1024];
        snprintf(new_line, sizeof(new_line), "%s=%s\n", param, value);
        lines[num_lines] = strdup(new_line);
        if (!lines[num_lines]) goto error;
        num_lines++;
    }

    // Write to file
    FILE *out_fp = fopen(file_path, "w");
    if (!out_fp) goto error;

    for (size_t i = 0; i < num_lines; i++) {
        fputs(lines[i], out_fp);
        free(lines[i]);
    }
    free(lines);
    fclose(out_fp);

    return SUCCESS;

error:
    if (fp) fclose(fp);
    if (lines) {
        for (size_t i = 0; i < num_lines; i++) free(lines[i]);
        free(lines);
    }
    return SAVE_FAILED;
}
