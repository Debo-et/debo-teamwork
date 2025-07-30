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

ConfigResult *parse_zookeeper_param(const char *param_name, const char *param_value) {
    // Predefined list of ZooKeeper parameters with canonical names and their normalized forms
    ConfigParam param_map[] = {
        // Original Parameters (Retained)
        { "clientPort", "clientport", "zoo.cfg" },
        { "dataDir", "datadir", "zoo.cfg" },
        { "tickTime", "ticktime", "zoo.cfg" },
        { "initLimit", "initlimit", "zoo.cfg" },
        { "syncLimit", "synclimit", "zoo.cfg" },
        { "maxClientCnxns", "maxclientcnxns", "zoo.cfg" },
        { "autopurge.snapRetainCount", "autopurgesnapretaincount", "zoo.cfg" },
        { "autopurge.purgeInterval", "autopurgepurgeinterval", "zoo.cfg" },
        { "minSessionTimeout", "minsessiontimeout", "zoo.cfg" },
        { "maxSessionTimeout", "maxsessiontimeout", "zoo.cfg" },
        { "electionPort", "electionport", "zoo.cfg" },
        { "leaderServes", "leaderserves", "zoo.cfg" },
        { "server.id", "serverid", "zoo.cfg" },  // Dynamic server entries
        { "cnxTimeout", "cnxtimeout", "zoo.cfg" },
        { "standaloneEnabled", "standaloneenabled", "zoo.cfg" },
        { "reconfigEnabled", "reconfigenabled", "zoo.cfg" },
        { "4lw.commands.whitelist", "4lwcommandswhitelist", "zoo.cfg" },
        { "globalOutstandingLimit", "globaloutstandinglimit", "zoo.cfg" },
        { "preAllocSize", "preallocsize", "zoo.cfg" },
        { "snapCount", "snapcount", "zoo.cfg" },

        // Security & Authentication
        { "clientPortAddress", "clientportaddress", "zoo.cfg" },
        { "secureClientPort", "secureclientport", "zoo.cfg" },
        { "ssl.keyStore.location", "sslkeystorelocation", "zoo.cfg" },
        { "ssl.keyStore.password", "sslkeystorepassword", "zoo.cfg" },
        { "ssl.trustStore.location", "ssltruststorelocation", "zoo.cfg" },
        { "ssl.trustStore.password", "ssltruststorepassword", "zoo.cfg" },
        { "ssl.hostnameVerification", "sslhostnameverification", "zoo.cfg" },
        { "authProvider.sasl", "authprovidersasl", "zoo.cfg" },
        { "jaasLoginRenew", "jaasloginrenew", "zoo.cfg" },
        { "sasl.client.id", "saslclientid", "zoo.cfg" },
        { "kerberos.removeHostFromPrincipal", "kerberosremovehostfromprincipal", "zoo.cfg" },
        { "kerberos.removeRealmFromPrincipal", "kerberosremoverealmfromprincipal", "zoo.cfg" },
        { "ssl.clientAuth", "sslclientauth", "zoo.cfg" },
        { "zookeeper.superUser", "zookeepersuperuser", "zoo.cfg" },

        // Quorum & Ensemble Management
        { "quorum.enableSasl", "quorumenablesasl", "zoo.cfg" },
        { "quorum.auth.learnerRequireSasl", "quorumauthlearnerrequiresasl", "zoo.cfg" },
        { "quorum.auth.serverRequireSasl", "quorumauthserverrequiresasl", "zoo.cfg" },
        { "quorum.cnxTimeout", "quorumcnxtimeout", "zoo.cfg" },
        { "quorum.electionAlg", "quorumelectionalg", "zoo.cfg" },
        { "quorum.portUnification", "quorumportunification", "zoo.cfg" },

        // ACLs & Data Security
        { "skipACL", "skipacl", "zoo.cfg" },
        { "aclProvider", "aclprovider", "zoo.cfg" },

        // Performance & Advanced Tuning
        { "jute.maxbuffer", "jutemaxbuffer", "zoo.cfg" },
        { "commitProcessor.numWorkerThreads", "commitprocessornumworkerthreads", "zoo.cfg" },
        { "fsync.warningthresholdms", "fsyncwarningthresholdms", "zoo.cfg" },
        { "forceSync", "forcesync", "zoo.cfg" },
        { "syncEnabled", "syncenabled", "zoo.cfg" },
        { "connectTimeout", "connecttimeout", "zoo.cfg" },
        { "readTimeout", "readtimeout", "zoo.cfg" },

        // Dynamic Configuration & Admin
        { "dynamicConfigFile", "dynamicconfigfile", "zoo.cfg" },
        { "admin.enableServer", "adminenableserver", "zoo.cfg" },
        { "admin.serverPort", "adminserverport", "zoo.cfg" },
        { "admin.serverAddress", "adminserveraddress", "zoo.cfg" },

        // Metrics & Monitoring
        { "metricsProvider.className", "metricsproviderclassname", "zoo.cfg" },

        // Network & Client Settings
        { "clientCnxnSocket", "clientcnxnsocket", "zoo.cfg" },
        { "client.secure", "clientsecure", "zoo.cfg" },

        // Additional 4LW Controls
        { "4lw.commands.enabled", "4lwcommandsenabled", "zoo.cfg" },

        // Advanced Throttling and NIO
        { "zookeeper.request_throttler.shutdownTimeout", "zookeeperrequestthrottlershutdowntimeout", "zoo.cfg" },
        { "zookeeper.nio.numSelectorThreads", "zookeepernionumselectorthreads", "zoo.cfg" },
        { "zookeeper.nio.numWorkerThreads", "zookeepernionumworkerthreads", "zoo.cfg" },
        { "zookeeper.nio.directBufferBytes", "zookeeperniodirectbufferbytes", "zoo.cfg" },
        // New Parameters from log4j.properties
        { "log4j.rootLogger", "log4jrootlogger", "log4j.properties" },
        { "log4j.appender.CONSOLE", "log4jappenderconsole", "log4j.properties" },
        { "log4j.appender.CONSOLE.Threshold", "log4jappenderconsolethreshold", "log4j.properties" },
        { "log4j.appender.CONSOLE.layout", "log4jappenderconsolelayout", "log4j.properties" },
        { "log4j.appender.CONSOLE.layout.ConversionPattern", "log4jappenderconsolelayoutconversionpattern", "log4j.properties" },
        { "log4j.appender.ROLLINGFILE", "log4jappenderrollingfile", "log4j.properties" },
        { "log4j.appender.ROLLINGFILE.Threshold", "log4jappenderrollingfilethreshold", "log4j.properties" },
        { "log4j.appender.ROLLINGFILE.File", "log4jappenderrollingfilefile", "log4j.properties" },
        { "log4j.appender.ROLLINGFILE.MaxFileSize", "log4jappenderrollingfilemaxfilesize", "log4j.properties" },
        { "log4j.appender.ROLLINGFILE.layout", "log4jappenderrollingfilelayout", "log4j.properties" },
        { "log4j.appender.ROLLINGFILE.layout.ConversionPattern", "log4jappenderrollingfilelayoutconversionpattern", "log4j.properties" },
        { "log4j.appender.TRACEFILE", "log4jappendertracefile", "log4j.properties" },
        { "log4j.appender.TRACEFILE.Threshold", "log4jappendertracefilethreshold", "log4j.properties" },
        { "log4j.appender.TRACEFILE.File", "log4jappendertracefilefile", "log4j.properties" },
        { "log4j.appender.TRACEFILE.layout", "log4jappendertracefilelayout", "log4j.properties" },
        { "log4j.appender.TRACEFILE.layout.ConversionPattern", "log4jappendertracefilelayoutconversionpattern", "log4j.properties" },
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
        if (strcmp(normalized, param_map[i].normalizedName) == 0) {
            // Allocate and populate the configuration entry
            ConfigResult *entry = malloc(sizeof(ConfigResult));
            if (!entry) return NULL;

            entry->canonical_name = strdup(param_map[i].canonicalName);
            entry->value = strdup(param_value);
            entry->config_file = strdup(param_map[i].configFile);

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


ConfigStatus modify_zookeeper_config(const char* config_param, const char* value, const char *filename) {
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
            "/usr/local/zookeeper/conf",          // Debian
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

    if (strcmp(filename, "log4j.properties") == 0 ) {
        configure_hadoop_property(file_path, config_param, value);
        return SUCCESS;
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

