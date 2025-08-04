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


