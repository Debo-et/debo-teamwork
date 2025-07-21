#ifndef UTILES_H
#define UTILES_H
#include <signal.h>
#include <stdbool.h>
#include <stddef.h>
#include "connutil.h"
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

#define RESET   "\033[0m"
#define BOLD    "\033[1m"
#define GREEN   "\033[32m"
#define CYAN    "\033[36m"
#define YELLOW  "\033[33m"
#define BOX_WIDTH 70

#ifndef SIG_ATOMIC_T_DEFINED
typedef int sig_atomic_t;
#endif

extern const char *port;
extern const char       *host;
extern const char       *user;
extern char *password ;

#define MAX_PATH_LEN 1024
#define MAX_LINE_LENGTH 8096

typedef enum {
    NONE,
    FLINK,

    HDFS,
    YARN,
    HBASE,
    HIVE,
    KAFKA,
    LIVY,
    PHOENIX,
    STORM,
    HUE,
    PIG,
    OOZIE,
    PRESTO,
    ATLAS,
    RANGER,
    SOLR,
    SPARK,
    TEZ,
    ZEPPELIN,
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

typedef enum {NO_ACTION, START, STOP, RESTART, INSTALL, UPGRADE,
    UNINSTALL, CONFIGURE} Action;

typedef struct PromptInterruptContext
{
    /* To avoid including <setjmp.h> here, jmpbuf is declared "void *" */
    void       *jmpbuf;                     /* existing longjmp buffer */
    volatile sig_atomic_t *enabled; /* flag that enables longjmp-on-interrupt */
    bool            canceled;               /* indicates whether cancellation occurred */
} PromptInterruptContext;

typedef struct {
    const char *canonicalName;
    const char *normalizedName;
    const char *configFile;
} ConfigParam;


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

int update_component_version(const char* component, const char* new_version);
bool executeSystemCommand(const char *cmd);
char **split_string(char *input);
bool isComponentVersionSupported(Component component, const char *version);
void buffer_intermediary_function(ClientSocket *client_sock, char *buf);
void PERROR(ClientSocket *client_sock, const char *s);
int PRINTF(ClientSocket *client_sock, const char *format, ...);
int FPRINTF(ClientSocket *client_sock, const char *format, ...);
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
int create_xml_file(const char *directory_path, const char *xml_file_name);
int update_config(const char *param, const char *value, const char *file_path);
int create_properties_file(const char *directory_path, const char *properties_file_name);
int create_conf_file(const char *directory_path, const char *conf_file_name);
char* get_component_config_path(Component comp, const char* config_filename);
bool isComponentInstalled(Component comp);
#endif
