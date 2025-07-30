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

static const ConfigParam predefinedParams[] = {
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
