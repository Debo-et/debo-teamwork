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

static const struct {
    const char *canonical;
    const char *normalized;
    const char *config_file;
} livy_params[] = {
    /* Core Server Configuration */
    { "livy.server.port", "^livy[._-]server[._-]port$", "livy.conf" },
    { "livy.server.host", "^livy[._-]server[._-]host$", "livy.conf" },
    { "livy.server.session.timeout", "^livy[._-]server[._-]session[._-]timeout$", "livy.conf" },
    { "livy.server.session-state-retain.sec", "^livy[._-]server[._-]session[._-]state[._-]retain[._-]sec$", "livy.conf" },
    { "livy.server.session.factory", "^livy[._-]server[._-]session[._-]factory$", "livy.conf" },
    { "livy.server.recovery.mode", "^livy[._-]server[._-]recovery[._-]mode$", "livy.conf" },
    { "livy.server.recovery.state-store", "^livy[._-]server[._-]recovery[._-]state[._-]store$", "livy.conf" },
    { "livy.server.recovery.state-store.url", "^livy[._-]server[._-]recovery[._-]state[._-]store[._-]url$", "livy.conf" },

    /* Security & Authentication */
    { "livy.server.auth.type", "^livy[._-]server[._-]auth[._-]type$", "livy.conf" },
    { "livy.keystore", "^livy[._-]keystore$", "livy.conf" },
    { "livy.keystore.password", "^livy[._-]keystore[._-]password$", "livy.conf" },
    { "livy.truststore", "^livy[._-]truststore$", "livy.conf" },
    { "livy.truststore.password", "^livy[._-]truststore[._-]password$", "livy.conf" },
    { "livy.server.auth.kerberos.principal", "^livy[._-]server[._-]auth[._-]kerberos[._-]principal$", "livy.conf" },
    { "livy.server.auth.kerberos.keytab", "^livy[._-]server[._-]auth[._-]kerberos[._-]keytab$", "livy.conf" },
    { "livy.server.auth.ldap.url", "^livy[._-]server[._-]auth[._-]ldap[._-]url$", "livy.conf" },
    { "livy.server.auth.ldap.baseDN", "^livy[._-]server[._-]auth[._-]ldap[._-]baseDN$", "livy.conf" },
    { "livy.server.auth.ldap.userDNPattern", "^livy[._-]server[._-]auth[._-]ldap[._-]userDNPattern$", "livy.conf" },
    { "livy.server.auth.ldap.groupDNPattern", "^livy[._-]server[._-]auth[._-]ldap[._-]groupDNPattern$", "livy.conf" },
    { "livy.server.auth.jwt.public-key", "^livy[._-]server[._-]auth[._-]jwt[._-]public[._-]key$", "livy.conf" },
    { "livy.server.auth.jwt.issuer", "^livy[._-]server[._-]auth[._-]jwt[._-]issuer$", "livy.conf" },
    { "livy.server.auth.jwt.audience", "^livy[._-]server[._-]auth[._-]jwt[._-]audience$", "livy.conf" },
    { "livy.server.impersonation.enabled", "^livy[._-]server[._-]impersonation[._-]enabled$", "livy.conf" },
    { "livy.server.impersonation.allowed.users", "^livy[._-]server[._-]impersonation[._-]allowed[._-]users$", "livy.conf" },
    { "livy.server.access-control.enabled", "^livy[._-]server[._-]access[._-]control[._-]enabled$", "livy.conf" },
    { "livy.server.access-control.users", "^livy[._-]server[._-]access[._-]control[._-]users$", "livy.conf" },
    { "livy.server.access-control.groups", "^livy[._-]server[._-]access[._-]control[._-]groups$", "livy.conf" },
    { "livy.server.launch.kerberos.principal", "^livy[._-]server[._-]launch[._-]kerberos[._-]principal$", "livy.conf" },
    { "livy.server.launch.kerberos.keytab", "^livy[._-]server[._-]launch[._-]kerberos[._-]keytab$", "livy.conf" },
    { "livy.server.superusers", "^livy[._-]server[._-]superusers$", "livy.conf" },

    /* Spark Configuration */
    { "livy.spark.master", "^livy[._-]spark[._-]master$", "livy.conf" },
    { "livy.spark.deploy-mode", "^livy[._-]spark[._-]deploy[._-]mode$", "livy.conf" },
    { "livy.spark.home", "^livy[._-]spark[._-]home$", "livy.conf" },
    { "livy.spark.submit.deployMode", "^livy[._-]spark[._-]submit[._-]deployMode$", "livy.conf" },
    { "livy.spark.submit.proxyUser", "^livy[._-]spark[._-]submit[._-]proxyUser$", "livy.conf" },
    { "livy.spark.driver.cores", "^livy[._-]spark[._-]driver[._-]cores$", "livy.conf" },
    { "livy.spark.driver.memory", "^livy[._-]spark[._-]driver[._-]memory$", "livy.conf" },
    { "livy.spark.executor.cores", "^livy[._-]spark[._-]executor[._-]cores$", "livy.conf" },
    { "livy.spark.executor.memory", "^livy[._-]spark[._-]executor[._-]memory$", "livy.conf" },
    { "livy.spark.dynamicAllocation.enabled", "^livy[._-]spark[._-]dynamicAllocation[._-]enabled$", "livy.conf" },
    { "livy.spark.dynamicAllocation.minExecutors", "^livy[._-]spark[._-]dynamicAllocation[._-]minExecutors$", "livy.conf" },
    { "livy.spark.dynamicAllocation.maxExecutors", "^livy[._-]spark[._-]dynamicAllocation[._-]maxExecutors$", "livy.conf" },
    { "livy.spark.dynamicAllocation.initialExecutors", "^livy[._-]spark[._-]dynamicAllocation[._-]initialExecutors$", "livy.conf" },

    /* Resource Management */
    { "livy.spark.yarn.queue", "^livy[._-]spark[._-]yarn[._-]queue$", "livy.conf" },
    { "livy.spark.yarn.archives", "^livy[._-]spark[._-]yarn[._-]archives$", "livy.conf" },
    { "livy.spark.yarn.dist.files", "^livy[._-]spark[._-]yarn[._-]dist[._-]files$", "livy.conf" },
    { "livy.spark.yarn.maxAppAttempts", "^livy[._-]spark[._-]yarn[._-]maxAppAttempts$", "livy.conf" },
    { "livy.spark.kubernetes.namespace", "^livy[._-]spark[._-]kubernetes[._-]namespace$", "livy.conf" },
    { "livy.spark.kubernetes.container.image", "^livy[._-]spark[._-]kubernetes[._-]container[._-]image$", "livy.conf" },
    { "livy.spark.kubernetes.authenticate.driver.serviceAccountName", "^livy[._-]spark[._-]kubernetes[._-]authenticate[._-]driver[._-]serviceAccountName$", "livy.conf" },
    { "livy.spark.kubernetes.driver.podTemplateFile", "^livy[._-]spark[._-]kubernetes[._-]driver[._-]podTemplateFile$", "livy.conf" },
    { "livy.spark.kubernetes.executor.podTemplateFile", "^livy[._-]spark[._-]kubernetes[._-]executor[._-]podTemplateFile$", "livy.conf" },

    /* Session Management */
    { "livy.file.local-dir", "^livy[._-]file[._-]local[._-]dir$", "livy.conf" },
    { "livy.file.local-dir-whitelist", "^livy[._-]file[._-]local[._-]dir[._-]whitelist$", "livy.conf" },
    { "livy.server.session.max_creation_time", "^livy[._-]server[._-]session[._-]max_creation_time$", "livy.conf" },
    { "livy.server.session.heartbeat.timeout", "^livy[._-]server[._-]session[._-]heartbeat[._-]timeout$", "livy.conf" },
    { "livy.server.session.max_sessions_per_user", "^livy[._-]server[._-]session[._-]max_sessions_per_user$", "livy.conf" },
    { "livy.rsc.server-address", "^livy[._-]rsc[._-]server[._-]address$", "livy.conf" },
    { "livy.rsc.jvm.opts", "^livy[._-]rsc[._-]jvm[._-]opts$", "livy.conf" },
    { "livy.rsc.sparkr.package", "^livy[._-]rsc[._-]sparkr[._-]package$", "livy.conf" },
    { "livy.rsc.livy-jars", "^livy[._-]rsc[._-]livy[._-]jars$", "livy.conf" },

    /* Interactive & Batch Processing */
    { "livy.repl.enableHiveContext", "^livy[._-]repl[._-]enableHiveContext$", "livy.conf" },
    { "livy.batch.retained", "^livy[._-]batch[._-]retained$", "livy.conf" },

    /* UI & Monitoring */
    { "livy.ui.enabled", "^livy[._-]ui[._-]enabled$", "livy.conf" },
    { "livy.ui.session-list.max", "^livy[._-]ui[._-]session[._-]list[._-]max$", "livy.conf" },
    { "livy.metrics.enabled", "^livy[._-]metrics[._-]enabled$", "livy.conf" },
    { "livy.metrics.reporters", "^livy[._-]metrics[._-]reporters$", "livy.conf" },
    { "livy.metrics.jmx.domain", "^livy[._-]metrics[._-]jmx[._-]domain$", "livy.conf" },
    { "livy.server.request-log.enabled", "^livy[._-]server[._-]request[._-]log[._-]enabled$", "livy.conf" },
    { "livy.server.access-log.enabled", "^livy[._-]server[._-]access[._-]log[._-]enabled$", "livy.conf" },

    /* Network & Security Protocols */
    { "livy.server.csrf-protection.enabled", "^livy[._-]server[._-]csrf[._-]protection[._-]enabled$", "livy.conf" },
    { "livy.server.cors.enabled", "^livy[._-]server[._-]cors[._-]enabled$", "livy.conf" },
    { "livy.server.cors.allowed-origins", "^livy[._-]server[._-]cors[._-]allowed[._-]origins$", "livy.conf" },
    { "livy.server.cors.allowed-methods", "^livy[._-]server[._-]cors[._-]allowed[._-]methods$", "livy.conf" },
    { "livy.server.cors.allowed-headers", "^livy[._-]server[._-]cors[._-]allowed[._-]headers$", "livy.conf" },
    { "livy.server.cors.exposed-headers", "^livy[._-]server[._-]cors[._-]exposed[._-]headers$", "livy.conf" },

    /* YARN/Kubernetes Specific */
    { "livy.yarn.app-name", "^livy[._-]yarn[._-]app[._-]name$", "livy.conf" },
    { "livy.yarn.config-file", "^livy[._-]yarn[._-]config[._-]file$", "livy.conf" },
    { "livy.yarn.jar", "^livy[._-]yarn[._-]jar$", "livy.conf" },
    { "livy.yarn.poll-interval", "^livy[._-]yarn[._-]poll[._-]interval$", "livy.conf" },
    { "livy.kubernetes.truststore.secret", "^livy[._-]kubernetes[._-]truststore[._-]secret$", "livy.conf" },
    { "livy.kubernetes.truststore.password.secret", "^livy[._-]kubernetes[._-]truststore[._-]password[._-]secret$", "livy.conf" },
    { "livy.kubernetes.keystore.secret", "^livy[._-]kubernetes[._-]keystore[._-]secret$", "livy.conf" },
    { "livy.kubernetes.keystore.password.secret", "^livy[._-]kubernetes[._-]keystore[._-]password[._-]secret$", "livy.conf" },
    { "log4j.rootCategory", "^log4j[._-]rootCategory$", "livy-log4j.properties" },
    { "log4j.appender.console", "^log4j[._-]appender[._-]console$", "livy-log4j.properties" },
    { "log4j.appender.console.target", "^log4j[._-]appender[._-]console[._-]target$", "livy-log4j.properties" },
    { "log4j.appender.console.layout", "^log4j[._-]appender[._-]console[._-]layout$", "livy-log4j.properties" },
    { "log4j.appender.console.layout.ConversionPattern", "^log4j[._-]appender[._-]console[._-]layout[._-]ConversionPattern$", "livy-log4j.properties" },
    { "log4j.logger.org.eclipse.jetty", "^log4j[._-]logger[._-]org[._-]eclipse[._-]jetty$", "livy-log4j.properties" },
    { "livy.client.http.connection.timeout", "^livy[._-]client[._-]http[._-]connection[._-]timeout$", "livy-client.conf" },
    { "livy.client.http.connection.socket.timeout", "^livy[._-]client[._-]http[._-]connection[._-]socket[._-]timeout$", "livy-client.conf" },
    { "livy.client.http.content.compress.enable", "^livy[._-]client[._-]http[._-]content[._-]compress[._-]enable$", "livy-client.conf" },
    { "livy.client.http.connection.idle.timeout", "^livy[._-]client[._-]http[._-]connection[._-]idle[._-]timeout$", "livy-client.conf" },
    { "livy.client.http.job.initial-poll-interval", "^livy[._-]client[._-]http[._-]job[._-]initial[._-]poll[._-]interval$", "livy-client.conf" },
    { "livy.client.http.job.max-poll-interval", "^livy[._-]client[._-]http[._-]job[._-]max[._-]poll[._-]interval$", "livy-client.conf" },
    { "livy.rsc.client.auth.id", "^livy[._-]rsc[._-]client[._-]auth[._-]id$", "livy-client.conf" },
    { "livy.rsc.client.auth.secret", "^livy[._-]rsc[._-]client[._-]auth[._-]secret$", "livy-client.conf" },
    { "livy.rsc.client.shutdown-timeout", "^livy[._-]rsc[._-]client[._-]shutdown[._-]timeout$", "livy-client.conf" },
    { "livy.rsc.driver-class", "^livy[._-]rsc[._-]driver[._-]class$", "livy-client.conf" },
    { "livy.rsc.session.kind", "^livy[._-]rsc[._-]session[._-]kind$", "livy-client.conf" },
    { "livy.rsc.jars", "^livy[._-]rsc[._-]jars$", "livy-client.conf" },
    { "livy.rsc.sparkr.package", "^livy[._-]rsc[._-]sparkr[._-]package$", "livy-client.conf" },
    { "livy.rsc.pyspark.archives", "^livy[._-]rsc[._-]pyspark[._-]archives$", "livy-client.conf" },
    { "livy.rsc.launcher.address", "^livy[._-]rsc[._-]launcher[._-]address$", "livy-client.conf" },
    { "livy.rsc.launcher.port.range", "^livy[._-]rsc[._-]launcher[._-]port[._-]range$", "livy-client.conf" },
    { "livy.rsc.server.idle-timeout", "^livy[._-]rsc[._-]server[._-]idle[._-]timeout$", "livy-client.conf" },
    { "livy.rsc.proxy-user", "^livy[._-]rsc[._-]proxy[._-]user$", "livy-client.conf" },
    { "livy.rsc.rpc.server.address", "^livy[._-]rsc[._-]rpc[._-]server[._-]address$", "livy-client.conf" },
    { "livy.rsc.server.connect.timeout", "^livy[._-]rsc[._-]server[._-]connect[._-]timeout$", "livy-client.conf" },
    { "livy.rsc.channel.log.level", "^livy[._-]rsc[._-]channel[._-]log[._-]level$", "livy-client.conf" },
    { "livy.rsc.rpc.sasl.mechanisms", "^livy[._-]rsc[._-]rpc[._-]sasl[._-]mechanisms$", "livy-client.conf" },
    { "livy.rsc.rpc.sasl.qop", "^livy[._-]rsc[._-]rpc[._-]sasl[._-]qop$", "livy-client.conf" },
    { "livy.rsc.job-cancel.trigger-interval", "^livy[._-]rsc[._-]job[._-]cancel[._-]trigger[._-]interval$", "livy-client.conf" },
    { "livy.rsc.job-cancel.timeout", "^livy[._-]rsc[._-]job[._-]cancel[._-]timeout$", "livy-client.conf" },
    { "livy.rsc.retained-statements", "^livy[._-]rsc[._-]retained[._-]statements$", "livy-client.conf" },

    /* Spark Blacklist Configuration */
    { "spark.master", "^spark[._-]master$", "spark-blacklist.conf" },
    { "spark.submit.deployMode", "^spark[._-]submit[._-]deployMode$", "spark-blacklist.conf" },
    { "spark.yarn.jar", "^spark[._-]yarn[._-]jar$", "spark-blacklist.conf" },
    { "spark.yarn.jars", "^spark[._-]yarn[._-]jars$", "spark-blacklist.conf" },
    { "spark.yarn.archive", "^spark[._-]yarn[._-]archive$", "spark-blacklist.conf" },
    { "livy.rsc.server.idle-timeout", "^livy[._-]rsc[._-]server[._-]idle[._-]timeout$", "spark-blacklist.conf" },
};


ValidationResult validateLivyConfigParam(const char *param_name, const char *value) {
    // Check if parameter exists
    bool param_exists = false;
    for (size_t i = 0; i < sizeof(livy_params)/sizeof(livy_params[0]); i++) {
        if (strcmp(param_name, livy_params[i].canonical) == 0) {
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

#define NUM_CANONICAL_PARAMS (sizeof(livy_params)/sizeof(livy_params[0]))


ConfigResult* parse_livy_config_param(const char *input_key, const char *input_value) {

    ConfigResult* result = malloc(sizeof(ConfigResult));

    for (size_t i = 0; i < NUM_CANONICAL_PARAMS; i++) {
        const char *canonical = livy_params[i].canonical;
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

