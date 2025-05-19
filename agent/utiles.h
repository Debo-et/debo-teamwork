#ifndef UTILES_H
#define UTILES_H
#include <signal.h>
#include <stdbool.h>
#include <stddef.h>
#include "connutil.h"


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
void handle_result(ConfigStatus status);
bool handleValidationResult(ValidationResult result);
#endif
