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


