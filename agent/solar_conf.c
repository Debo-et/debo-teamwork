
#include <libxml/parser.h>
#include <libxml/tree.h>
#include <libxml/xpath.h>

#include "utiles.h"

static const ConfigParam solr_param_configs[] = {
    // Core Configuration (solr.xml)
    {.canonicalName = "^coreRootDirectory$", .normalizedName  =  "coreRootDirectory", .configFile  = "solr.xml"},

    // SolrCloud/ZooKeeper (solr.xml)
    {.canonicalName = "^zkHost$", .normalizedName  =  "zkHost", .configFile  = "solr.xml"},
    {.canonicalName = "^zkClientTimeout$", .normalizedName  =  "zkClientTimeout", .configFile  = "solr.xml"},
    {.canonicalName = "^cloud\\.collection\\.configName$", .normalizedName  =  "cloud.collection.configName", .configFile  = "solr.xml"},
    {.canonicalName = "^numShards$", .normalizedName  =  "numShards", .configFile  = "solr.xml"},

    // Replication/Sharding (solr.xml)
    {.canonicalName = "^shardHandlerFactory\\.socketTimeout$", .normalizedName  =  "shardHandlerFactory.socketTimeout", .configFile  = "solr.xml"},
    {.canonicalName = "^replication\\.factor$", .normalizedName  =  "replication.factor", .configFile  = "solr.xml"},

    // Monitoring/Logging (solr.xml)
    {.canonicalName = "^metrics\\.reporter\\.jmx$", .normalizedName  =  "metrics.reporter.jmx", .configFile  = "solr.xml"},
    {.canonicalName = "^logging\\.watcher\\.threshold$", .normalizedName  =  "logging.watcher.threshold", .configFile  = "solr.xml"},

    // HTTP/Network Settings (solr.xml)
    {.canonicalName = "^hostContext$", .normalizedName  =  "hostContext", .configFile  = "solr.xml"},
    {.canonicalName = "^http\\.maxConnections$", .normalizedName  =  "http.maxConnections", .configFile  = "solr.xml"},

    // Legacy Parameters (solr.xml only)
    {.canonicalName = "^transientCacheSize$", .normalizedName  =  "transientCacheSize", .configFile  = "solr.xml"},
        // solr-log4j.properties parameters
    {.canonicalName = "^solr\\.log$", .normalizedName  =  "solr.log", .configFile  = "solr-log4j.properties"},
    {.canonicalName = "^log4j\\.rootLogger$", .normalizedName  =  "log4j.rootLogger", .configFile  = "solr-log4j.properties"},
    {.canonicalName = "^log4j\\.appender\\.CONSOLE$", .normalizedName  =  "log4j.appender.CONSOLE", .configFile  = "solr-log4j.properties"},
    {.canonicalName = "^log4j\\.appender\\.CONSOLE\\.layout$", .normalizedName  =  "log4j.appender.CONSOLE.layout", .configFile  = "solr-log4j.properties"},
    {.canonicalName = "^log4j\\.appender\\.CONSOLE\\.layout\\.ConversionPattern$", .normalizedName  =  "log4j.appender.CONSOLE.layout.ConversionPattern", .configFile  = "solr-log4j.properties"},
    {.canonicalName = "^log4j\\.logger\\.org\\.apache\\.zookeeper$", .normalizedName  =  "log4j.logger.org.apache.zookeeper", .configFile  = "solr-log4j.properties"},
    {.canonicalName = "^log4j\\.logger\\.org\\.apache\\.hadoop$", .normalizedName  =  "log4j.logger.org.apache.hadoop", .configFile  = "solr-log4j.properties"},
    {.canonicalName = "^log4j\\.logger\\.org\\.eclipse\\.jetty$", .normalizedName  =  "log4j.logger.org.eclipse.jetty", .configFile  = "solr-log4j.properties"},
    {.canonicalName = "^log4j\\.logger\\.org\\.eclipse\\.jetty\\.server\\.Server$", .normalizedName  =  "log4j.logger.org.eclipse.jetty.server.Server", .configFile  = "solr-log4j.properties"},
    {.canonicalName = "^log4j\\.logger\\.org\\.eclipse\\.jetty\\.server\\.ServerConnector$", .normalizedName  =  "log4j.logger.org.eclipse.jetty.server.ServerConnector", .configFile  = "solr-log4j.properties"},
    {.canonicalName = "^log4j\\.logger\\.org\\.apache\\.solr\\.update\\.LoggingInfoStream$", .normalizedName  =  "log4j.logger.org.apache.solr.update.LoggingInfoStream", .configFile  = "solr-log4j.properties"},


};


ValidationResult validateSolrConfigParam(const char *param_name, const char *value) {
    // Check if parameter exists
    bool param_exists = false;
    for (size_t i = 0; i < sizeof(solr_param_configs)/sizeof(solr_param_configs[0]); i++) {
        if (strcmp(param_name, solr_param_configs[i].normalizedName) == 0) {
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
        int reti = regcomp(&regex, solr_param_configs[i].canonicalName, REG_EXTENDED | REG_ICASE);
        if (reti) {
            char error_message[1024];
            regerror(reti, &regex, error_message, sizeof(error_message));
            FPRINTF(global_client_socket, "Regex compilation failed: %s\n", error_message);
            continue;
        }

        reti = regexec(&regex, param_name, 0, NULL, 0);
        regfree(&regex);
        if (reti == 0) {
            result->canonical_name = (char *)solr_param_configs[i].normalizedName;
            result->value = (char *)param_value;
            result->config_file = (char *)solr_param_configs[i].configFile ;
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
        strcmp(config_file, "solr-log4j.properties") != 0 &&
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

    if (strcmp(config_file, "solr-log4j.properties") == 0 ) {
        configure_hadoop_property(config_path, param_name, param_value);
        return SUCCESS;
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

