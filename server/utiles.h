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


#ifndef UTILES_H
#define UTILES_H
#include <signal.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <regex.h>
#include <sys/stat.h>
#include <errno.h>
#include <sys/stat.h>
#include <ctype.h>
#include <stdbool.h>

#include "connect.h"

#define RESET   "\033[0m"
#define BOLD    "\033[1m"
#define GREEN   "\033[32m"
#define CYAN    "\033[36m"
#define YELLOW  "\033[33m"
#define BOX_WIDTH 70

#define MAX_PATH_LEN 1024
#define MAX_LINE_LENGTH 8096

extern const char *port;
extern const char       *host;
extern const char       *user;
extern char *password ;

#define STATUS_OK                               (0)
#define STATUS_ERROR                    (-1)
#define STATUS_EOF                              (-2)


typedef enum {
    // Flink components
    NONE,
    FLINK,

    // Hadoop components
    HDFS,
    YARN,
    // HBase components
    HBASE,
    // Hive components
    HIVE,
    // Kafka components
    KAFKA,
    // Livy
    LIVY,

    // Phoenix
    PHOENIX,
    STORM,
    HUE,
    PIG,
    OOZIE,
    PRESTO,
    ATLAS,
    // Ranger components
    RANGER,
    // Solr components
    SOLR,
    // Spark components
    SPARK,
    // Tez
    TEZ,
    HIVE_METASTORE,
    // Zeppelin
    ZEPPELIN,

    // Zookeeper components
    ZOOKEEPER,
} Component;

typedef enum {
    SUCCESS = 0,
    INVALID_FILE_TYPE,
    FILE_NOT_FOUND,
    XML_PARSE_ERROR,
    INVALID_CONFIG_FILE,
    XML_UPDATE_ERROR,
    FILE_WRITE_ERROR,
    FILE_READ_ERROR,
    XML_INVALID_ROOT,
    SAVE_FAILED
} ConfigStatus;

typedef enum {
    VALIDATION_OK,
    ERROR_PARAM_NOT_FOUND,
    ERROR_VALUE_EMPTY,
    ERROR_INVALID_FORMAT,
    ERROR_CONSTRAINT_VIOLATED
} ValidationResult;

typedef struct {
    char *canonical_name;
    char *value;
    char *config_file;
} ConfigResult;

typedef enum {
    OS_DEBIAN,
    OS_REDHAT,
    OS_MACOS,
    OS_UNSUPPORTED
} OS_TYPE;

typedef enum {
    CORE_SITE,
    HDFS_SITE,
    YARN_SITE
        // Extend with other file types as necessary
} ConfigFileType;

typedef struct {
    const char *canonicalName;
    const char *normalizedName;
    const char *configFile;
} ConfigParam;

typedef enum {NO_ACTION, START, STOP, RESTART, INSTALL, VERSION_SWITCH,
    UNINSTALL, REPORT ,CONFIGURE} Action;

typedef struct PromptInterruptContext
{
    /* To avoid including <setjmp.h> here, jmpbuf is declared "void *" */
    void       *jmpbuf;                     /* existing longjmp buffer */
    volatile sig_atomic_t *enabled; /* flag that enables longjmp-on-interrupt */
    bool            canceled;               /* indicates whether cancellation occurred */
} PromptInterruptContext;




typedef struct {
    Component component;
    const char **services;
    size_t count;
} ComponentServices;


// Component service definitions
static const char *HDFS_SERVICES[] = {
    "hadoop-hdfs-namenode",
    "hadoop-hdfs-datanode",
    "hadoop-hdfs-journalnode",
    "hadoop-hdfs-zkfc"
};

static const char *YARN_SERVICES[] = {
    "hadoop-yarn-resourcemanager",
    "hadoop-yarn-nodemanager",
    "hadoop-yarn-proxyserver"
};

static const ComponentServices COMPONENT_SERVICES_MAP[] = {
    {HDFS,          HDFS_SERVICES,        sizeof(HDFS_SERVICES)/sizeof(HDFS_SERVICES[0])},
    {YARN,          YARN_SERVICES,        sizeof(YARN_SERVICES)/sizeof(YARN_SERVICES[0])},
    {HBASE,         (const char*[]){"hbase-master", "hbase-regionserver"}, 2},
    {SPARK,         (const char*[]){"spark-master", "spark-worker"}, 2},
    {KAFKA,         (const char*[]){"kafka"}, 1},
    {ZOOKEEPER,     (const char*[]){"zookeeper-server"}, 1},
    {FLINK,         (const char*[]){"flink"}, 1},
    {STORM,         (const char*[]){"storm-nimbus", "storm-supervisor"}, 2},
    {HIVE_METASTORE,(const char*[]){"hive-metastore"}, 1},
    {HUE,           (const char*[]){"hue"}, 1},
    {OOZIE,         (const char*[]){"oozie"}, 1},
    {PRESTO,        (const char*[]){"presto-coordinator", "presto-worker"}, 2},
    {ATLAS,         (const char*[]){"atlas"}, 1},
    {RANGER,        (const char*[]){"ranger-admin"}, 1},
    {ZEPPELIN,      (const char*[]){"zeppelin"}, 1},
    {LIVY,          (const char*[]){"livy-server"}, 1},
    {PHOENIX,       (const char*[]){"phoenix"}, 1},
    {SOLR,          (const char*[]){"solr"}, 1}
};

char *simple_prompt(const char *prompt, bool echo);
void execute_and_print(const char *command);
char * apache_strdup(const char *in);
bool is_version_format(const char *version);
void printBorder(const char *start, const char *end, const char *color);
void printTextBlock(const char *text, const char *textColor, const char *borderColor);
const char * component_to_string(Component comp);
const char * action_to_string(Action action);
char* execute_remote_script(const char* script, const char* username, const char* host, const char* password);
int execute_remote_command(const char* host,
                           const char* username, const char* password,
                           const char* cmd);
//Component* get_dependencies(Component comp);
Component string_to_component(const char* name);
unsigned char get_protocol_code(Component comp, Action action, const char* version);
char *concatenate_strings(const char *s1, const char *s2);
int validate_file_path(const char* path);
bool executeSystemCommand(const char *cmd);
bool isComponentVersionSupported(Component component, const char *version);
Conn* connect_to_debo(const char* host, const char* port);
void  reset_connection_buffers(Conn *conn);
void handle_result(ConfigStatus status, const char *config_param, const char *config_value, const char *config_file);
bool handleValidationResult(ValidationResult result);
bool isPositiveInteger(const char *value);
bool isValidPort(const char *value);
bool isValidBoolean(const char *value);
bool isValidHostPort(const char *value);
bool isDataSize(const char *value);
bool isValidDuration(const char *value);
bool isValidCommaSeparatedList(const char *value);
bool isNonNegativeInteger(const char *value);
bool isValidHostPortList(const char *value);
bool isValidUrl(const char *value);
bool isValidPath(const char *value);
bool isValidPath(const char *value);
bool isValidHBaseDuration(const char *value);
bool isValidPrincipalFormat(const char *value);
bool isDataSizeWithUnit(const char *value);
bool isValidEncoding(const char *value);
bool isValidSparkDuration(const char *value);
bool isValidSparkMasterFormat(const char *value);
bool isValidCompressionType(const char *value);
bool isSecurityProtocolValid(const char *value);
bool isSaslMechanismValid(const char *value);
bool isAutoOffsetResetValid(const char *value);
bool isValidCredentialFile(const char *value);
bool isHostPortPair(const char *value);
bool isMemorySize(const char *value);
bool isValidURI(const char *value);
bool isFraction(const char *value);
bool isTimeMillis(const char *value);
bool isSizeWithUnit(const char *value);
bool isCommaSeparatedList(const char *value);
bool isMemorySizeMB(const char *value);
bool isTimeSeconds(const char *value);
bool isPercentage(const char *value);
bool isJDBCURL(const char *value);
bool isValidCompressionCodec(const char *value);
bool isURL(const char *value);
bool isJCEKSPath(const char *value);
bool isValidSparkMaster(const char *value);
bool isValidAuthType(const char *value);
bool isValidZKHostList(const char *value);
bool isValidLogLevel(const char *value);
bool isValidContextPath(const char *value);
bool isZKQuorum(const char *value);
bool isSSLProtocolValid(const char *value);
bool isComponentValid(const char *component);
bool isURLValid(const char *value);
bool isRatio(const char *value);
bool isCompressionCodec(const char *value);
bool isExecutionModeValid(const char *value);
bool isTimezoneValid(const char *value);
bool isHostPortList(const char *value);
bool fileExists(const char *path);
int is_redhat();
int is_debian();
void trim_whitespace(char *str);
char *trim(char *str);
int mkdir_p(const char *path);
char* generate_regex_pattern(const char* canonical_name);
int configure_hadoop_property(const char *file_path, const char *key, const char *value);
int updateHadoopConfigXML(const char *filePath, const char *parameterName, const char *parameterValue);
void SendComponentActionCommand(Component component, Action action,
                                const char* version,
                                const char* param_name, const char* param_value,
                                Conn* conn);
Component* get_dependencies(Component comp, int *count);
int update_config(const char *param, const char *value, const char *file_path);
int create_xml_file(const char *directory_path, const char *xml_file_name);
int create_properties_file(const char *directory_path, const char *properties_file_name);
int create_conf_file(const char *directory_path, const char *conf_file_name);
int start_stdout_capture(Component comp);
int stop_stdout_capture(void);
char* get_component_config_path(Component comp, const char* config_filename);
bool isComponentInstalled(Component comp);
bool check_installed_message(const char* str);
#endif
