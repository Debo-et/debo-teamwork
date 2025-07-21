
#include <libxml/parser.h>
#include <libxml/tree.h>
#include <libxml/xpath.h>

#include "utiles.h"

static const ConfigParam predefined_entries[] = {
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

ValidationResult validateRangerConfigParam(const char *param_name, const char *value) {
    // Check if parameter exists
    bool param_exists = false;
    regex_t regex;
    regmatch_t matches[4];
    char pattern[512];

    for (size_t i = 0; i < sizeof(predefined_entries)/sizeof(predefined_entries[0]); i++) {
        // Convert pattern to valid regex
        snprintf(pattern, sizeof(pattern), "%s", predefined_entries[i].canonicalName);
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
        const ConfigParam *entry = &predefined_entries[i];
        regex_t regex;
        int reti = regcomp(&regex, entry->canonicalName, REG_ICASE | REG_NOSUB);
        if (reti != 0) continue;

        reti = regexec(&regex, param_name, 0, NULL, 0);
        regfree(&regex);

        if (reti == 0) {
            result->canonical_name = strdup(entry->normalizedName);
            result->value = strdup(param_value);
            result->config_file = strdup(entry->configFile);

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

