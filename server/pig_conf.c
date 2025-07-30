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

