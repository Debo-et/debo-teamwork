#include "utiles.h"

ConfigResult *get_presto_config_setting(const char *param_name, const char *param_value) {
    // Predefined list of the most impactful Presto configuration parameters
ConfigParam presto_predefined_params[] = {
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
        ret = regcomp(&regex, presto_predefined_params[i].configFile, REG_ICASE | REG_NOSUB);
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

            result->canonical_name = strdup(presto_predefined_params[i].canonicalName);
            result->value = strdup(param_value);
            result->config_file = strdup(presto_predefined_params[i].normalizedName);

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



