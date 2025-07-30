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


ConfigParam zeppelin_predefined_params[] = {
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
    {"zeppelin.credentials.file", "zeppelin-credentials-file", "zeppelin-site.xml"},
    // New Configuration Parameters (from log4j.properties)
    {"log4j.rootLogger", "log4j-rootLogger", "log4j.properties"},
    {"log4j.appender.stdout", "log4j-appender-stdout", "log4j.properties"},
    {"log4j.appender.stdout.layout", "log4j-appender-stdout-layout", "log4j.properties"},
    {"log4j.appender.stdout.layout.ConversionPattern", "log4j-appender-stdout-layout-ConversionPattern", "log4j.properties"},
    {"log4j.appender.dailyfile.DatePattern", "log4j-appender-dailyfile-DatePattern", "log4j.properties"},
    {"log4j.appender.dailyfile.DEBUG", "log4j-appender-dailyfile-DEBUG", "log4j.properties"},
    {"log4j.appender.dailyfile", "log4j-appender-dailyfile", "log4j.properties"},
    {"log4j.appender.dailyfile.File", "log4j-appender-dailyfile-File", "log4j.properties"},
    {"log4j.appender.dailyfile.layout", "log4j-appender-dailyfile-layout", "log4j.properties"},
    {"log4j.appender.dailyfile.layout.ConversionPattern", "log4j-appender-dailyfile-layout-ConversionPattern", "log4j.properties"},
    {"log4j.logger.org.apache.zeppelin.python", "log4j-logger-org-apache-zeppelin-python", "log4j.properties"},
    {"log4j.logger.org.apache.zeppelin.spark", "log4j-logger-org-apache-zeppelin-spark", "log4j.properties"},
    // New entries from shiro.ini
    // [users] section
    {"shiro.user.user1", "password2, role1, role2", "shiro.ini"},
    {"shiro.user.user2", "password3, role3", "shiro.ini"},
    {"shiro.user.user3", "password4, role2", "shiro.ini"},

    // [main] section
    {"shiro.main.sessionManager", "org.apache.shiro.web.session.mgt.DefaultWebSessionManager", "shiro.ini"},
    {"shiro.main.cookie", "org.apache.shiro.web.servlet.SimpleCookie", "shiro.ini"},
    {"shiro.main.cookie.name", "JSESSIONID", "shiro.ini"},
    {"shiro.main.cookie.httpOnly", "true", "shiro.ini"},
    {"shiro.main.sessionManager.sessionIdCookie", "$cookie", "shiro.ini"},
    {"shiro.main.securityManager.sessionManager", "$sessionManager", "shiro.ini"},
    {"shiro.main.securityManager.sessionManager.globalSessionTimeout", "86400000", "shiro.ini"},
    {"shiro.main.shiro.loginUrl", "/api/login", "shiro.ini"},

    // [roles] section
    {"shiro.role.role1", "*", "shiro.ini"},
    {"shiro.role.role2", "*", "shiro.ini"},
    {"shiro.role.role3", "*", "shiro.ini"},
    {"shiro.role.admin", "*", "shiro.ini"},

    // [urls] section
    {"shiro.url./api/version", "anon", "shiro.ini"},
    {"shiro.url./api/cluster/address", "anon", "shiro.ini"},
    {"shiro.url./api/interpreter/setting/restart/**", "authc", "shiro.ini"},
    {"shiro.url./api/interpreter/**", "authc, roles[admin]", "shiro.ini"},
    {"shiro.url./api/notebook-repositories/**", "authc, roles[admin]", "shiro.ini"},
    {"shiro.url./api/configurations/**", "authc, roles[admin]", "shiro.ini"},
    {"shiro.url./api/credential/**", "authc, roles[admin]", "shiro.ini"},
    {"shiro.url./api/admin/**", "authc, roles[admin]", "shiro.ini"},
    {"shiro.url./**", "authc", "shiro.ini"},
};


ValidationResult validateZeppelinConfigParam(const char *param_name, const char *value) {
    // Check if parameter exists
    bool param_exists = false;
    for (size_t i = 0; i < sizeof(zeppelin_predefined_params)/sizeof(zeppelin_predefined_params[0]); i++) {
        if (strcmp(param_name, zeppelin_predefined_params[i].canonicalName) == 0) {
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
        if (strcmp(processed_name, zeppelin_predefined_params[i].normalizedName) == 0) {
            ConfigResult *result = malloc(sizeof(ConfigResult));
            if (!result) return NULL;

            result->canonical_name = strdup(zeppelin_predefined_params[i].canonicalName);
            result->value = strdup(param_value);
            result->config_file = strdup(zeppelin_predefined_params[i].configFile);

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
    if (strcmp(config_file, "zeppelin-site.xml") != 0 && strcmp(config_file, "shiro.ini") != 0
        && strcmp(config_file, "log4j.properties") != 0) {
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

    if (strcmp(config_file, "log4j.properties") == 0) {
        configure_hadoop_property(config_path, param, value);
        return SUCCESS;
    }

    if (strcmp(config_file, "zeppelin-shiro.ini") == 0 ) {
        configure_hadoop_property(config_path, param, value);
        return SUCCESS;
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

