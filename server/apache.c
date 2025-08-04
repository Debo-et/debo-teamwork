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

#include "getopt_long.h"
#include "utiles.h"
#include "configuration.h"
#include "install.h"
#include "uninstall.h"
#include "action.h"
#include "report.h"
#include "protocol.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <libintl.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/wait.h>
#ifdef HAVE_TERMIOS_H
#include <termios.h>
#endif
#include <ctype.h>
#include <limits.h>

#ifdef _WIN32
#define popen win32_popen
#define pclose _pclose
#endif

#define ntoh32(x) (x)
#define MAX_DEPTH 20

OS_TYPE detect_os() {
    struct stat buffer;
#ifdef __APPLE__
    return OS_MACOS;
#else
    if (stat("/etc/debian_version", &buffer) == 0) return OS_DEBIAN;
    if (stat("/etc/redhat-release", &buffer) == 0) return OS_REDHAT;
    return OS_UNSUPPORTED;
#endif
}


static bool all = false;
bool dependency = false;


const char *port = NULL;
const char       *host = NULL;
char       *value = NULL;

typedef struct {
    Component component;
    const char *deb_pkg;
    const char *rhel_pkg;
    const char *mac_pkg;
} ComponentPackage;

typedef struct {
    Component component;
    const char *version_cmd;
    const char *deb_pkg;
    const char *rhel_pkg;
    const char *mac_pkg;
} ComponentInfo;

const ComponentPackage COMPONENT_PACKAGE_MAP[] = {
    {HDFS,          "hadoop-hdfs",          "hadoop-hdfs",          "hadoop"},
    {HBASE,         "hbase",                "hbase",                "hbase"},
    {SPARK,         "spark-core",           "spark",                "apache-spark"},
    {KAFKA,         "kafka",                "kafka",                "kafka"},
    {ZOOKEEPER,     "zookeeper",            "zookeeper",            "zookeeper"},
    {FLINK,         "flink",                "flink",                "apache-flink"},
    {STORM,         "storm",                "storm",                "apache-storm"},
    {HIVE,"hive-metastore",       "hive-metastore",       "hive"},
    {PIG,           "pig",                  "pig",                  "pig"},
    {PRESTO,        "presto",               "presto",               "presto"},
    {TEZ,           "tez",                  "tez",                  "tez"},
    {ATLAS,         "atlas",                "atlas",                "atlas"},
    {RANGER,        "ranger",               "ranger",               "ranger"},
    {ZEPPELIN,      "zeppelin",             "zeppelin",             "zeppelin"},
    {LIVY,          "livy",                 "livy-server",          "livy"},
    {PHOENIX,       "phoenix",              "phoenix",              "phoenix"},
    {SOLR,          "solr",                 "solr",                 "solr"}
};

const ComponentInfo COMPONENT_INFO_MAP[] = {
    {HDFS,          "hadoop version 2>&1 | grep '^Hadoop' | awk '{print $2}'",
        "hadoop-hdfs", "hadoop-hdfs", "hadoop"},
    {HBASE,         "hbase version 2>&1 | grep '^HBase' | awk '{print $2}'",
        "hbase", "hbase", "hbase"},
    {SPARK,         "spark-submit --version 2>&1 | grep '^version' | awk '{print $2}'",
        "spark-core", "spark", "apache-spark"},
    {KAFKA,         "grep -oP 'version=\\K\\d+\\.\\d+\\.\\d+' $KAFKA_HOME/bin/kafka-run-class.sh",
        "kafka", "kafka", "kafka"},
    {ZOOKEEPER,     "echo stat | nc localhost 2181 2>/dev/null | grep 'Zookeeper version:' | awk '{print $3}'",
        "zookeeper", "zookeeper", "zookeeper"},
    {FLINK,         "flink --version 2>&1 | awk '{print $2}' | head -1",
        "flink", "flink", "apache-flink"},
    {STORM,         "storm version 2>&1 | grep 'Storm' | awk '{print $2}'",
        "storm", "storm", "apache-storm"},
    {HIVE,"hive --version 2>&1 | grep 'Hive' | awk '{print $2}'",
        "hive-metastore", "hive-metastore", "hive"},
    {PIG,           "pig --version 2>&1 | grep 'Apache Pig' | awk '{print $3}'",
        "pig", "pig", "pig"},
    {PRESTO,        "presto --version 2>&1 | awk '{print $2}'",
        "presto", "presto", "presto"},
    {TEZ,           "hcat -h 2>&1 | grep 'Tez' | awk '{print $4}' | tr -d ','",
        "tez", "tez", "tez"},
    {ATLAS,         "atlas_admin.py -status 2>&1 | grep 'version' | awk '{print $3}'",
        "atlas", "atlas", "atlas"},
    {RANGER,        "ranger-admin version 2>&1 | awk '{print $2}'",
        "ranger", "ranger", "ranger"},
    {ZEPPELIN,      "zeppelin-daemon.sh version 2>&1 | grep 'Zeppelin version' | awk '{print $3}'",
        "zeppelin", "zeppelin", "zeppelin"},
    {LIVY,          "livy-server --version 2>&1 | head -1 | awk '{print $2}'",
        "livy", "livy-server", "livy"},
    {PHOENIX,       "hbase org.apache.phoenix.util.PhoenixRuntime -version 2>&1 | grep 'Phoenix' | awk '{print $3}'",
        "phoenix", "phoenix", "phoenix"},
    {SOLR,          "solr version 2>&1 | tail -1",
        "solr", "solr", "solr"}
};

static const char *progname = "apache";


static void help(const char *progname);
static void install_component(Component comp, char *version);
static bool component_action(Component comp, Action action);
static bool uninstall_component(Component comp);
ConfigStatus configure(Component comp, char *config_param, char *value);
static void handle_local_components(bool ALL, Component component, Action action,
                                    char *version ,char *config_param, char *value);
static void handle_remote_components(bool ALL, Component component, Action action,
                                     char *version ,char *config_param, char *value);


void validate_options(Action action, Component component, bool all, bool dependency) {
    switch (action) {
    case START:
    case STOP:
    case RESTART:
    case REPORT:
    case INSTALL:
    case UNINSTALL:
        if (all) {
            if (component != NONE) {
                fprintf(stderr, "Error: Cannot combine --all with individual components\n");
                exit(EXIT_FAILURE);
            }
            if (dependency) {
                fprintf(stderr, "Error: Cannot use --all with a dependency\n");
                exit(EXIT_FAILURE);
            }
        } else {
            if (component == NONE) {
                fprintf(stderr, "Error: Must specify a component when not using --all\n");
                exit(EXIT_FAILURE);
            }
            //   if (!dependency) {
            //     fprintf(stderr, "Error: Must specify a dependency when using a component for this action\n");
            //   exit(EXIT_FAILURE);
            //}
        }
        break;

    case CONFIGURE:
    case VERSION_SWITCH:
        if (all) {
            fprintf(stderr, "Error: Cannot use --all with this action\n");
            exit(EXIT_FAILURE);
        }
        if (dependency) {
            fprintf(stderr, "Error: Cannot use dependency with this action\n");
            exit(EXIT_FAILURE);
        }
        if (component == NONE) {
            fprintf(stderr, "Error: Must specify a component for this action\n");
            exit(EXIT_FAILURE);
        }
        break;

    default:
        //   fprintf(stderr, "Error: Unsupported action type\n");
        // exit(EXIT_FAILURE);
    }
}

int
main(int argc, char *argv[])
{

    static struct option long_options[] = {
        {"version", required_argument, NULL, 'V'},
        {"port", required_argument, NULL, 'P'},
        {"host", required_argument, NULL, 'H'},
        {"all", no_argument, NULL, 'A'},
        {"install", no_argument, NULL, 'l'},
        {"start", no_argument, NULL, 'T'},
        {"stop", no_argument, NULL, 'O'},
        {"restart", no_argument, NULL, 'R'},
        {"verswitch", required_argument, NULL, 'U'},
        {"uninstall", no_argument, NULL, 'u'},
        {"configure", required_argument, NULL, 'c'},
        {"hdfs", no_argument, NULL, 'h'},
        //  {"yarn", no_argument, NULL, 'y'},
        {"hbase", no_argument, NULL, 'b'},
        {"spark", no_argument, NULL, 's'},
        {"kafka", no_argument, NULL, 'k'},
        {"zookeeper", no_argument, NULL, 'z'},
        {"flink", no_argument, NULL, 'f'},
        {"storm", no_argument, NULL, 'S'},
        {"hive", no_argument, NULL, 'm'},
        //	{"hue", no_argument, NULL, 'E'},
        {"pig", no_argument, NULL, 'p'},
        //	{"oozie", no_argument, NULL, 'o'},
        {"presto", no_argument, NULL, 'e'},
        {"tez", no_argument, NULL, 't'},
        {"atlas", no_argument, NULL, 'a'},
        {"ranger", no_argument, NULL, 'r'},
        {"livy", no_argument, NULL, 'L'},
        {"phoenix", no_argument, NULL, 'x'},
        {"value", required_argument, NULL, 'v'},
        {"solr", no_argument, NULL, 'X'},
        {"zeppelin", no_argument, NULL, 'Z'},
        {"with-dependency", no_argument, NULL, 'd'},
        {NULL, 0, NULL, 0}
    };
    Component component = NONE;
    Action action = NO_ACTION;
    int			optindex;
    int			c;

    char *config_param = NULL;
    char *version = NULL;

    if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
    {
        help(progname);
        exit(0);
    }
    /* process command-line options */
    while ((c = getopt_long(argc, argv, "P:H:n:c:WAlITORUudaphyeLbzZSskfmtrtrxXvV:",
                            long_options, &optindex)) != -1)
    {

        switch (c)
        {
        case 'A':
            all = true;
            break;
        case 'a':
            component = ATLAS;
            break;
        case 'r':
            component = RANGER;
            break;
        case 'b':
            component = HBASE;
            break;
        case 'h':
            component = HDFS;
            break;
        case 'P':
            port = apache_strdup(optarg);
            break;
        case 'V':
            version = apache_strdup(optarg);
            break;
        case 'm':
            component = HIVE;
            break;
        case 'e':
            component = PRESTO;
            break;
        case 'f':
            component = FLINK;
            break;
        case 's':
            component = SPARK;
            break;
        case 'S':
            component = STORM;
            break;
        case 'k':
            component = KAFKA;
            break;
            //	case 'o':
            //		component = OOZIE;
            //		break;
        case 'p':
            component = PIG;
            break;
        case 't':
            component = TEZ;
            break;
            //	case 'E':
            //		component = HUE;
            //		break;
        case 'z':
            component = ZOOKEEPER;
            break;
        case 'l':
            action = INSTALL;
            break;
        case 'T':
            action = START;
            break;
        case 'O':
            action = STOP;
            break;
        case 'R':
            action = RESTART;
            break;
        case 'U':
            action = VERSION_SWITCH;
            version = apache_strdup(optarg);
            break;
        case 'u':
            action = UNINSTALL;
            break;
        case 'L':
            component = LIVY;
            break;
        case 'x':
            component = PHOENIX;
            break;
        case 'X':
            component = SOLR;
            break;
        case 'Z':
            component = ZEPPELIN;
            break;
        case 'c':
            action = CONFIGURE;
            config_param = apache_strdup(optarg);
            break;
        case 'v':
            value = apache_strdup(optarg);
            break;
        case 'H':
            host = apache_strdup(optarg);
            break;
        case 'd':
            dependency = true;
            break;
        default:
            /* getopt_long already emitted a complaint */
            fprintf(stderr, "Try %s --help for more information.", progname);
            exit(EXIT_FAILURE);
        }
    }
    validate_options(action, component, all, dependency);
    // Validate connection options group
    if (port || host) {
        // Check all connection parameters are present
        if (!(port && host)) {
            fprintf(stderr, "Error: --port and  --host, must be used together\n");
            exit(EXIT_FAILURE);
        }

        // Check connection options are used with component/--all
        if (component == NONE && !all) {
            fprintf(stderr, "Error: Connection options require --all or a component\n");
            exit(EXIT_FAILURE);
        }
    }

    // Validate mutual exclusivity between --all and components
    if (all && component != NONE) {
        fprintf(stderr, "Error: Cannot combine --all with individual components\n");
        exit(EXIT_FAILURE);
    }

    // Validate mutual exclusivity between --all and components
    if (component == TEZ && (action == START
                             || action == STOP || action == RESTART)) {
        fprintf(stderr, "Error: Tez did't have its service to run \n" );
        exit(EXIT_FAILURE);
    }

    // Validate mutual exclusivity between --all and components
    if (all && action == CONFIGURE) {
        fprintf(stderr, "Error: Cannot combine --all with --configure\n");
        exit(EXIT_FAILURE);
    }

    // Validate component/--all specification
    if (component == NONE && !all) {
        fprintf(stderr, "Error: Must specify either --all or at least one component\n");
        exit(EXIT_FAILURE);
    }

    if (version && !is_version_format(version)) {
        fprintf(stderr, "Error: Invalid version format: %s\n", version);
        exit(EXIT_FAILURE);
    }

    if (version && !isComponentVersionSupported(component, version)){
        fprintf(stderr, "Error: unsupported  version : %s\n", version);
        exit(EXIT_FAILURE);
    }

    if (optind < argc)
    {
        fprintf(stderr, "too many command-line arguments (first is %s",
                argv[optind]);
        exit(EXIT_FAILURE);
    }
    if (port || host )
        handle_remote_components(all , component , action, version , config_param, value);
    else
        handle_local_components(all , component , action, version , config_param, value);

}

/*
 * help
 *
 * Prints help page for the program
 *
 * progname: the name of the executed program
 */
static void
help(const char *progname)
{
    printf("%s manages Hadoop ecosystem components and checks their status.\n\n", progname);
    printf("Usage:\n");
    printf("  %s [ACTION] [COMPONENT] [OPTION]...\n", progname);
    printf("  %s [OPTION]...\n\n", progname);

    printf("Action options (perform operations on components):\n");
    printf("  --start             Start the specified component/service\n");
    printf("  --stop              Stop the specified component/service\n");
    printf("  --restart           Restart the specified component/service\n");
    printf("  --install           Install the component\n");
    printf("  --verswitch           switch between version\n");
    printf("  --uninstall         Remove the component\n");
    printf("  --configure         Apply configuration changes\n\n");

    printf("Target components (use with action options):\n");
    printf("  --all               Apply action to all components\n");
    printf("  --hdfs              HDFS distributed filesystem\n");
    printf("  --hbase             HBase database\n");
    printf("  --spark             Spark processing engine\n");
    printf("  --kafka             Kafka messaging system\n");
    printf("  --zookeeper         ZooKeeper coordination service\n");
    printf("  --flink             Flink stream processing\n");
    printf("  --storm             Storm real-time computation\n");
    printf("  --hive              Hive data warehouse\n");
    printf("  --pig               Pig scripting platform\n");
    printf("  --presto            Presto distributed SQL\n");
    printf("  --tez               Tez execution framework\n");
    printf("  --atlas             Atlas metadata service\n");
    printf("  --ranger            Ranger security manager\n");
    printf("  --zeppelin          Zeppelin notebook\n");
    printf("  --livy              Livy REST service\n");
    printf("  --phoenix           Phoenix SQL layer\n");
    printf("  --solr              Solr search platform\n\n");

    printf("General options:\n");
    printf("  -h, --host=HOSTNAME   Target server hostname\n");
    printf("  -p, --port=PORT       Connection port number\n");
    printf("  -V, --version         Show component version\n");
    printf("  --help                Display this help message\n\n");

    printf("Examples:\n");
    printf("  Check HDFS status:       %s --hdfs\n", progname);
    printf("  Start Zookeeper:         %s --start --zookeeper\n", progname);
    printf("  Restart all components:  %s --restart --all\n", progname);
    printf("  Install Kafka:           %s --install --kafka\n", progname);
}
///////////////////////////////////instalation//////////////////////////////
/*
 * Retrieve the package name for a given component on a specific OS.
 */
const char *
get_package_name(OS_TYPE os, Component comp)
{
    for (size_t i = 0; i < sizeof(COMPONENT_PACKAGE_MAP)/sizeof(COMPONENT_PACKAGE_MAP[0]); i++) {
        if (COMPONENT_PACKAGE_MAP[i].component == comp) {
            switch (os) {
            case OS_DEBIAN:
                return COMPONENT_PACKAGE_MAP[i].deb_pkg;
            case OS_REDHAT:
                return COMPONENT_PACKAGE_MAP[i].rhel_pkg;
            case OS_MACOS:
                return COMPONENT_PACKAGE_MAP[i].mac_pkg;
            default:
                return NULL;
            }
        }
    }
    return NULL;
}


const char* get_env_variable(Component comp) {
    switch (comp) {
    case HDFS: return "HADOOP_HOME";
    case HBASE: return "HBASE_HOME";
    case SPARK: return "SPARK_HOME";
    case KAFKA: return "KAFKA_HOME";
    case FLINK: return "FLINK_HOME";
    case ZOOKEEPER: return "ZOOKEEPER_HOME";
    case STORM: return "STORM_HOME";
    case HIVE: return "HIVE_HOME";
    case LIVY: return "LIVY_HOME";
    case PHOENIX: return "PHOENIX_HOME";
    case SOLR: return "SOLR_HOME";
    case ZEPPELIN: return "ZEPPELIN_HOME";
    case RANGER: return "RANGER_HOME";
    case ATLAS: return "ATLAS_HOME";
    case TEZ: return "TEZ_HOME";
    default: return NULL;
    }
}



static void install_component(Component comp, char *version) {

    if (isComponentInstalled(comp)) {
        fprintf(stderr,"%s is already installed.\n", component_to_string(comp));
        exit(EXIT_FAILURE);
    }

    int dep_count;

    if (dependency)
    {
        Component *deps = get_dependencies(comp, &dep_count);
        for (int i = 0; i < dep_count; i++) {
            char buffer[256];
            const char* compStr = component_to_string(deps[i]);
            snprintf(buffer, sizeof(buffer),
                     "Installing dependency %s [%d of %d]\n",
                     compStr, i+1, dep_count);
            printTextBlock(buffer, CYAN, YELLOW);
            install_component(deps[i], version);
            configure_target_component(comp);
        }
    }
    switch (comp) {
    case HDFS:
        install_hadoop(version , NULL);
        break;
    case HBASE:
        install_HBase(version, NULL);
        break;
    case SPARK:
        install_spark(version, NULL);
        break;
    case KAFKA:
        install_kafka(version, NULL);
        break;
    case FLINK:
        install_flink(version, NULL);
        break;
    case ZOOKEEPER:
        install_zookeeper(version, NULL);
        break;
    case STORM:
        install_Storm(version, NULL);
        break;
    case HIVE:
        install_hive(version, NULL);
        break;
    case LIVY:
        install_Livy(version, NULL);
        break;
    case PHOENIX:
        install_phoenix(version, NULL);
        break;
    case SOLR:
        install_Solr(version, NULL);
        break;
    case ZEPPELIN:
        install_Zeppelin(version, NULL);
        break;
    case RANGER:
        install_Ranger(version, NULL);
        break;
    case ATLAS:
        install_Atlas(version, NULL);
        break;
    case TEZ:
        install_Tez(version, NULL);
        break;
    case PIG:
        install_pig(version, NULL);
        break;
    case PRESTO:
        install_Presto(version, NULL);
        break;
    default:
        fprintf(stderr, "Error: Unknown component %s.\n", component_to_string(comp));
        return;
    }
    configure_target_component(comp);
} ////////////////////////////////////////// action start stop restart ///////////////



/**
 * Performs an action (start, stop, restart) on all services of a given component.
 *
 * @param comp The component whose services should be acted upon.
 * @param action The action to perform.
 * @return True if all actions succeed, otherwise false.
 */
static bool component_action(Component comp, Action action) {
    switch (comp) {
    case HDFS:
        hadoop_action(action);
        return true;
        //case YARN:
        //   modify_yarn_config(param, value);
    case HBASE:
        HBase_action(action);
        return true;
    case SPARK:
        spark_action(action);
        return true;
    case KAFKA:
        kafka_action(action);
        return true;
    case FLINK:
        flink_action(action);
        return true;
    case ZOOKEEPER:
        zookeeper_action(action);
        return true;
    case STORM:
        storm_action(action);
        return true;
    case HIVE:
        hive_action(action);
        return true;
        //case HUE:
        //   set_hue_config(version);
    case LIVY:
        livy_action(action);
        return true;
    case PHOENIX:
        phoenix_action(action);
        return true;
    case SOLR:
        Solr_action(action);
        return true;
    case ZEPPELIN:
        Zeppelin_action(action);
        return true;
    case ATLAS:
        atlas_action(action);
        return true;
    case TEZ:
        tez_action(action);
        return true;
    case PIG:
        pig_action(action);
        return true;
    case PRESTO:
        Presto_action(action);
        return true;
    default:
        fprintf(stderr, "Error: Unknown Hadoop component provided.\n");
    }
    return false;
}

/////////////////switch //////////////////////////////////////////////////////


/* Function to switch a component to a specified version */
static bool switch_component(Component comp, char *version) {
    OS_TYPE os = detect_os();
    if (os == OS_UNSUPPORTED) return "Unsupported operating system";


    uninstall_component(comp);

    install_component(comp, version);
    return true;


}

//////////////////////////////////uninstall////////////////////////////////
static bool uninstall_component(Component comp) {

    int dep_count;

    if (dependency)
    {
        Component *deps = get_dependencies(comp, &dep_count);
        for (int i = 0; i < dep_count; i++) {
            char buffer[256];
            const char* compStr = component_to_string(deps[i]);
            snprintf(buffer, sizeof(buffer),
                     "Uninstalling dependency %s [%d of %d]\n",
                     compStr, i+1, dep_count);
            printTextBlock(buffer, CYAN, YELLOW);
            uninstall_component(deps[i]);
        }
    }
    switch (comp) {
    case HDFS:
        uninstall_hadoop();
        return true;
        //case YARN:
        //   modify_yarn_config(param, value);
    case HBASE:
        uninstall_HBase();
        return true;
    case SPARK:
        uninstall_spark();
        return true;
    case KAFKA:
        uninstall_kafka();
        return true;
    case FLINK:
        uninstall_flink();
        return true;
    case ZOOKEEPER:
        uninstall_zookeeper();
        return true;
    case STORM:
        uninstall_Storm();
        return true;
    case HIVE:
        uninstall_hive();
        return true;
        //case HUE:
        //   set_hue_config(version);
    case LIVY:
        uninstall_livy();
        return true;
    case PHOENIX:
        uninstall_phoenix();
        return true;
    case SOLR:
        uninstall_Solr();
        return true;
    case ZEPPELIN:
        uninstall_Zeppelin();
        return true;
    case ATLAS:
        uninstall_Atlas();
        return true;
    case RANGER:
        uninstall_ranger();
        return true;
    case TEZ:
        uninstall_Tez();
        return true;
    case PIG:
        uninstall_pig();
        return true;
    case PRESTO:
        uninstall_Presto();
        return true;
    default:
        fprintf(stderr, "Error: Unknown Hadoop component provided.\n");
    }
    return false;
}

/////////////////////////configure //////////////////////////////////////////////////

ConfigStatus configure(Component component, char* param, char* value) {
    switch (component) {
    case HDFS:
        ValidationResult validationresult = validateHdfsConfigParam(param, value);
        if (!handleValidationResult(validationresult))
            break;
        ConfigResult *hdfsResult= find_hdfs_config(param);
        if (hdfsResult == NULL)
            fprintf(stderr,"configuration parameter not supported yet");
        ConfigStatus hdfsStatus =  modify_hdfs_config(hdfsResult->canonical_name,value,hdfsResult->config_file);
        handle_result(hdfsStatus, hdfsResult->canonical_name,value,hdfsResult->config_file);
        break;
    case HBASE:
        ValidationResult validationHbase = validateHBaseConfigParam(param, value);
        if (!handleValidationResult(validationHbase))
            break;
        ConfigResult *hbaseResult= process_hbase_config(param,value);
        if (hbaseResult == NULL)
            fprintf(stderr,"configuration parameter not supported yet");
        ConfigStatus hbaseStatus =  update_hbase_config(hbaseResult->canonical_name, hbaseResult->value, hbaseResult->config_file);
        handle_result(hbaseStatus, hbaseResult->canonical_name, hbaseResult->value, hbaseResult->config_file);
        break;
    case SPARK:
        ValidationResult validationSpark = validateSparkConfigParam(param, value);
        if (!handleValidationResult(validationSpark))
            break;
        ConfigResult *sparkResult= get_spark_config(param,value);
        if (sparkResult == NULL)
            fprintf(stderr,"configuration parameter not supported yet");
        ConfigStatus sparkStatus = update_spark_config(sparkResult->canonical_name, sparkResult->value, sparkResult->config_file);
        handle_result(sparkStatus, sparkResult->canonical_name, sparkResult->value, sparkResult->config_file);
        break;
    case KAFKA:
        ValidationResult validationKafka = validateKafkaConfigParam(param, value);
        if (!handleValidationResult(validationKafka))
            break;
        ConfigResult *kafkaResult = validate_kafka_config_param(param,value);
        if (kafkaResult == NULL)
            fprintf(stderr,"configuration parameter not supported yet");
        ConfigStatus kafkaStatus = modify_kafka_config(kafkaResult->canonical_name,kafkaResult->value,kafkaResult->config_file);
        handle_result(kafkaStatus, kafkaResult->canonical_name,kafkaResult->value,kafkaResult->config_file);
        break;
    case FLINK:
        ValidationResult validationflink = validateFlinkConfigParam(param, value);
        if (!handleValidationResult(validationflink))
            break;
        ConfigResult *flinkResult = set_flink_config(param,value);
        if (flinkResult ==NULL)
            fprintf(stderr,"configuration parameter not supported yet");
        ConfigStatus flinkStatus = update_flink_config(flinkResult->canonical_name,flinkResult->value , flinkResult->config_file);
        handle_result(flinkStatus, flinkResult->canonical_name,flinkResult->value, flinkResult->config_file);
        break;
    case ZOOKEEPER:
        ValidationResult validationZookeeper = validateZooKeeperConfigParam(param, value);
        if (!handleValidationResult(validationZookeeper))
            break;
        ConfigResult *zookeperResult = parse_zookeeper_param(param,value);
        if (zookeperResult == NULL)
            fprintf(stderr,"configuration parameter not supported yet");
        ConfigStatus zookeeperStatus = modify_zookeeper_config(zookeperResult->canonical_name, zookeperResult->value, zookeperResult->config_file);
        handle_result(zookeeperStatus, zookeperResult->canonical_name, zookeperResult->value, zookeperResult->config_file);
        break;
    case STORM:
        ValidationResult validationStorm = validateStormConfigParam(param, value);
        if (!handleValidationResult(validationStorm))
            break;
        ConfigResult *conf = validate_storm_config_param(param,value);
        if (conf == NULL)
            fprintf(stderr,"configuration parameter not supported yet");
        ConfigStatus stormStatus = modify_storm_config(conf->canonical_name, conf->value, conf->config_file);
        handle_result(stormStatus, conf->canonical_name, conf->value, conf->config_file);
        break;
    case HIVE:
        ValidationResult validationHive = validateHiveConfigParam(param, value);
        if (!handleValidationResult(validationHive))
            break;
        ConfigResult *hiveConf = process_hive_parameter(param,value);
        if (hiveConf == NULL)
            fprintf(stderr,"configuration parameter not supported yet");
        ConfigStatus hiveStatus = modify_hive_config(hiveConf->canonical_name, hiveConf->value, hiveConf->config_file);
        handle_result(hiveStatus, hiveConf->canonical_name, hiveConf->value, hiveConf->config_file);
        break;
    case PIG:
        ValidationResult validationPig = validatePigConfigParam(param, value);
        if (!handleValidationResult(validationPig))
            break;
        ConfigResult *pigConf = validate_pig_config_param(param,value);
        if (pigConf == NULL)
            fprintf(stderr,"configuration parameter not supported yet");
        ConfigStatus pigStatus = update_pig_config(pigConf->canonical_name, pigConf->value);
        handle_result(pigStatus, pigConf->canonical_name, pigConf->value, pigConf->config_file);
        break;
        // case PRESTO:
        //   return modify_oozie_config(param,value);
        //case ATLAS:
        //  return modify_oozie_config(param,value);
    case RANGER:
        ConfigResult *rangerConf = process_zeppelin_config_param(param,value);
        if (rangerConf == NULL)
            fprintf(stderr,"configuration parameter not supported yet");
        ConfigStatus rangerStatus =  set_ranger_config(rangerConf->canonical_name, rangerConf->value, rangerConf->config_file);
        handle_result(rangerStatus, rangerConf->canonical_name, rangerConf->value, rangerConf->config_file);
        break;
    case LIVY:
        ValidationResult validationLivy = validateLivyConfigParam(param, value);
        if (!handleValidationResult(validationLivy))
            break;
        ConfigResult *livyConf = parse_livy_config_param(param,value);
        if (livyConf == NULL)
            fprintf(stderr,"configuration parameter not supported yet");
        ConfigStatus livyStatus = set_livy_config(livyConf->canonical_name, livyConf->value, livyConf->config_file);
        handle_result(livyStatus , livyConf->canonical_name, livyConf->value, livyConf->config_file);
        break;
    case PHOENIX:
        //      ConfigStatus phoenixStatus = update_phoenix_config(param,value);
        //  handle_result(phoenixStatus);
        break;
    case SOLR:
        ValidationResult validationSolr = validateSolrConfigParam(param, value);
        if (!handleValidationResult(validationSolr))
            break;
        ConfigResult *solrConf = validate_solr_parameter(param,value);
        if (solrConf == NULL)
            fprintf(stderr,"configuration parameter not supported yet");
        ConfigStatus solrStatus =  update_solr_config(solrConf->canonical_name, solrConf->value, solrConf->config_file);
        handle_result(solrStatus, solrConf->canonical_name, solrConf->value, solrConf->config_file);
        break;
    case ZEPPELIN:
        ValidationResult validationZeppelin = validateZeppelinConfigParam(param, value);
        if (!handleValidationResult(validationZeppelin))
            break;
        ConfigResult *zeppelinConf = process_zeppelin_config_param(param,value);
        if (zeppelinConf == NULL)
            fprintf(stderr,"configuration parameter not supported yet");
        ConfigStatus zeppStatus =  set_zeppelin_config(zeppelinConf->config_file , zeppelinConf->canonical_name, zeppelinConf->value);
        handle_result(zeppStatus, zeppelinConf->config_file , zeppelinConf->canonical_name, zeppelinConf->value);
        break;
    case TEZ:
        ValidationResult validationTez = validateTezConfigParam(param, value);
        if (!handleValidationResult(validationTez))
            break;
        ConfigResult *tezConf = parse_tez_config_param(param,value);
        if (tezConf == NULL)
            fprintf(stderr,"configuration parameter not supported yet");
        ConfigStatus tezStatus =   modify_tez_config(tezConf->canonical_name, tezConf->value, "tez-site.xml");
        handle_result(tezStatus, tezConf->canonical_name, tezConf->value, "tez-site.xml");
        break;
    default:
        fprintf(stderr, "Error: Unknown Hadoop component provided.\n");
        return INVALID_CONFIG_FILE;
    }
    return SUCCESS;
}


void report(Component comp) {
    if (!isComponentInstalled(comp)) {
        fprintf(stderr, "%s is not installed.\n", component_to_string(comp));
        return;
    }
    printBorder("┌", "┐", YELLOW);
    switch(comp) {
    case HDFS:
        printTextBlock("HDFS status", BOLD GREEN, YELLOW);
        printBorder("├", "┤", YELLOW);
        printTextBlock(report_hdfs(), CYAN, YELLOW);
        printBorder("└", "┘", YELLOW);
        break;
    case HBASE:
        printTextBlock("HBASE status", BOLD GREEN, YELLOW);
        printBorder("├", "┤", YELLOW);
        printTextBlock(report_hbase(), CYAN, YELLOW);
        printBorder("└", "┘", YELLOW);
        break;
    case SPARK:
        printTextBlock("SPARK status", BOLD GREEN, YELLOW);
        printBorder("├", "┤", YELLOW);
        printTextBlock(report_spark(), CYAN, YELLOW);
        printBorder("└", "┘", YELLOW);
        break;
    case KAFKA:
        printTextBlock("KAFKA status", BOLD GREEN, YELLOW);
        printBorder("├", "┤", YELLOW);
        printTextBlock(report_kafka(), CYAN, YELLOW);
        printBorder("└", "┘", YELLOW);
        break;
    case ZOOKEEPER:
        printTextBlock("ZOOKEEPER status", BOLD GREEN, YELLOW);
        printBorder("├", "┤", YELLOW);
        printTextBlock(report_zookeeper(), CYAN, YELLOW);
        printBorder("└", "┘", YELLOW);
        break;
    case FLINK:
        printTextBlock("FLINK status", BOLD GREEN, YELLOW);
        printBorder("├", "┤", YELLOW);
        printTextBlock(report_flink(), CYAN, YELLOW);
        printBorder("└", "┘", YELLOW);
        break;
    case STORM:
        printTextBlock("STORM status", BOLD GREEN, YELLOW);
        printBorder("├", "┤", YELLOW);
        printTextBlock(report_storm(), CYAN, YELLOW);
        printBorder("└", "┘", YELLOW);
        break;
    case HIVE:
        printTextBlock("HIVE status", BOLD GREEN, YELLOW);
        printBorder("├", "┤", YELLOW);
        printTextBlock(report_hive(), CYAN, YELLOW);
        printBorder("└", "┘", YELLOW);
        break;
    case PIG:
        printTextBlock("PIG status", BOLD GREEN, YELLOW);
        printBorder("├", "┤", YELLOW);
        printTextBlock(report_pig(), CYAN, YELLOW);
        printBorder("└", "┘", YELLOW);
        break;
    case PRESTO:
        printTextBlock("PRESTO status", BOLD GREEN, YELLOW);
        printBorder("├", "┤", YELLOW);
        printTextBlock(report_presto(), CYAN, YELLOW);
        printBorder("└", "┘", YELLOW);
        break;
    case TEZ:
        printTextBlock("PRESTO status", BOLD GREEN, YELLOW);
        printBorder("├", "┤", YELLOW);
        printTextBlock(report_tez(), CYAN, YELLOW);
        printBorder("└", "┘", YELLOW);
        break;
    case ATLAS:
        printTextBlock("ATLAS status", BOLD GREEN, YELLOW);
        printBorder("├", "┤", YELLOW);
        printTextBlock(report_atlas(), CYAN, YELLOW);
        printBorder("└", "┘", YELLOW);
        break;
    case RANGER:
        printTextBlock("RANGER status", BOLD GREEN, YELLOW);
        printBorder("├", "┤", YELLOW);
        printTextBlock(report_ranger(), CYAN, YELLOW);
        printBorder("└", "┘", YELLOW);
        break;
    case LIVY:
        printTextBlock("LIVY status", BOLD GREEN, YELLOW);
        printBorder("├", "┤", YELLOW);
        printTextBlock(report_livy(), CYAN, YELLOW);
        printBorder("└", "┘", YELLOW);
        break;
    case SOLR:
        printTextBlock("SOLR status", BOLD GREEN, YELLOW);
        printBorder("├", "┤", YELLOW);
        printTextBlock(report_solr(), CYAN, YELLOW);
        printBorder("└", "┘", YELLOW);
        break;
    case ZEPPELIN:
        printTextBlock("LIVY status", BOLD GREEN, YELLOW);
        printBorder("├", "┤", YELLOW);
        printTextBlock(report_zeppelin(), CYAN, YELLOW);
        printBorder("└", "┘", YELLOW);
        break;
        /* Add cases for all components */
    default: fprintf(stderr, "Unknown component\n"); break;
    }
}

void perform(Component comp, Action action, char *version , char *config_param ,  char *value) {

    // Define the two input strings
    const char* act;
    char* message;
    const char* compStr = component_to_string(comp);
    switch(action) {
    case START:
    case STOP:
    case RESTART:
        if (component_action(comp, action))
        {
            if (action == START)
            {
                act = " started";
                message = (char*)malloc(strlen(compStr) + strlen(act) + 1);
                if (message == NULL)
                    printf("Memory allocation failed\n");
                // Copy the first string into the result
                strcpy(message, compStr);

                // Concatenate the second string to the result
                strcat(message, act);
                printTextBlock(message, CYAN, YELLOW);
                printBorder("└", "┘", YELLOW);

            }
            else if (action == STOP)
            {
                act = " stoped";
                message = (char*)malloc(strlen(compStr) + strlen(act) + 1);
                if (message == NULL)
                    printf("Memory allocation failed\n");
                // Copy the first string into the result
                strcpy(message, compStr);

                // Concatenate the second string to the result
                strcat(message, act);
                printTextBlock(message, CYAN, YELLOW);
                printBorder("└", "┘", YELLOW);

            }
            else if (action == RESTART)
            {
                act = " restarted";
                message = (char*)malloc(strlen(compStr) + strlen(act) + 1);
                if (message == NULL)
                    printf("Memory allocation failed\n");
                // Copy the first string into the result
                strcpy(message, compStr);

                // Concatenate the second string to the result
                strcat(message, act);
                printTextBlock(message, CYAN, YELLOW);
                printBorder("└", "┘", YELLOW);

            }
        }
        else
        {
            if (action == START)
            {
                act = " can't started";
                message = (char*)malloc(strlen(compStr) + strlen(act) + 1);
                if (message == NULL)
                    printf("Memory allocation failed\n");
                // Copy the first string into the result
                strcpy(message, compStr);

                // Concatenate the second string to the result
                strcat(message, act);
                printTextBlock(message, CYAN, YELLOW);
                printBorder("└", "┘", YELLOW);

            }
            else if (action == STOP)
            {
                act = " can't stoped";
                message = (char*)malloc(strlen(compStr) + strlen(act) + 1);
                if (message == NULL)
                    printf("Memory allocation failed\n");
                // Copy the first string into the result
                strcpy(message, compStr);

                // Concatenate the second string to the result
                strcat(message, act);
                printTextBlock(message, CYAN, YELLOW);
                printBorder("└", "┘", YELLOW);

            }
            else if (action == RESTART)
            {
                act = " can't restarted";
                message = (char*)malloc(strlen(compStr) + strlen(act) + 1);
                if (message == NULL)
                    printf("Memory allocation failed\n");
                // Copy the first string into the result
                strcpy(message, compStr);

                // Concatenate the second string to the result
                strcat(message, act);
                printTextBlock(message, CYAN, YELLOW);
                printBorder("└", "┘", YELLOW);

            }
        }
        break;
    case INSTALL:
        if (isComponentInstalled(comp)) {
            fprintf(stderr, "%s is  installed.\n", component_to_string(comp));
            return;
        }
        install_component(comp, version);
        break;
    case VERSION_SWITCH:
        if (switch_component(comp, version))
        {
            act = "  switching version";
            message = (char*)malloc(strlen(compStr) + strlen(act) + 1);
            if (message == NULL)
                printf("Memory allocation failed\n");
            // Copy the first string into the result
            strcpy(message, compStr);

            // Concatenate the second string to the result
            strcat(message, act);
            printTextBlock(message, CYAN, YELLOW);
            printBorder("└", "┘", YELLOW);

        }
        else
        {
            act = "  can't be switching";
            message = (char*)malloc(strlen(compStr) + strlen(act) + 1);
            if (message == NULL)
                printf("Memory allocation failed\n");
            // Copy the first string into the result
            strcpy(message, compStr);

            // Concatenate the second string to the result
            strcat(message, act);
            printTextBlock(message, CYAN, YELLOW);
            printBorder("└", "┘", YELLOW);

        }
        break;
    case UNINSTALL:
        if (uninstall_component(comp))
        {
            act = "  uninstalled";
            message = (char*)malloc(strlen(compStr) + strlen(act) + 1);
            if (message == NULL)
                printf("Memory allocation failed\n");
            // Copy the first string into the result
            strcpy(message, compStr);

            // Concatenate the second string to the result
            strcat(message, act);
            printTextBlock(message, CYAN, YELLOW);
            printBorder("└", "┘", YELLOW);

        }
        else
        {
            act = "  can't be uninstalled";
            message = (char*)malloc(strlen(compStr) + strlen(act) + 1);
            if (message == NULL)
                printf("Memory allocation failed\n");
            // Copy the first string into the result
            strcpy(message, compStr);

            // Concatenate the second string to the result
            strcat(message, act);
            printTextBlock(message, CYAN, YELLOW);
            printBorder("└", "┘", YELLOW);

        }
        break;
    case CONFIGURE:
        if (configure(comp,config_param, value))
        {
            act = "  configured";
            message = (char*)malloc(strlen(compStr) + strlen(act) + 1);
            if (message == NULL)
                printf("Memory allocation failed\n");
            // Copy the first string into the result
            strcpy(message, compStr);

            // Concatenate the second string to the result
            strcat(message, act);
            printTextBlock(message, CYAN, YELLOW);
            printBorder("└", "┘", YELLOW);

        }
        else
        {
            act = "  can't be configured";
            message = (char*)malloc(strlen(compStr) + strlen(act) + 1);
            if (message == NULL)
                printf("Memory allocation failed\n");
            // Copy the first string into the result
            strcpy(message, compStr);

            // Concatenate the second string to the result
            strcat(message, act);
            printTextBlock(message, CYAN, YELLOW);
            printBorder("└", "┘", YELLOW);

        }
        break;
        /* Add cases for all actions */
    default: fprintf(stderr, "Unknown action\n"); break;
    }
}


static void handle_local_components(bool ALL, Component component, Action action,
                                    char *version , char *config_param , char *value) {
    if (ALL) {
        // Handle all components (skip NONE)
        for (Component c = HDFS; c <= RANGER; c++) {
            const char *comp_str = component_to_string(c);
            if (!comp_str) continue; // Skip if component string is NULL

            if (action != NO_ACTION) {
                printBorder("┌", "┐", YELLOW);
                printTextBlock(comp_str, BOLD GREEN, YELLOW); // Use validated string
                printBorder("├", "┤", YELLOW);
                if (strcmp(action_to_string(action), "Installing...") == 0)
                    printTextBlock("Installing might take several minutes", BOLD GREEN, YELLOW);
                printTextBlock(action_to_string(action), CYAN, YELLOW);
                perform(c, action, version, config_param, value);
            } else {
                report(c);
            }
        }
    } else {
        // Handle single component
        if (component == NONE) {
            fprintf(stderr, "Component must be specified when ALL=false\n");
            return;
        }

        if (action != NO_ACTION) {
            printBorder("┌", "┐", YELLOW);
            printTextBlock(component_to_string(component), BOLD GREEN, YELLOW);
            printBorder("├", "┤", YELLOW);
            if (strcmp(action_to_string(action), "Installing...") == 0)
                printTextBlock("Installing might take several minutes", BOLD GREEN, YELLOW);
            printTextBlock(action_to_string(action), CYAN, YELLOW);
            perform(component, action, version, config_param, value);
        } else {
            printBorder("┌", "┐", YELLOW);
            printTextBlock(component_to_string(component), BOLD GREEN, YELLOW);
            printBorder("├", "┤", YELLOW);
            printTextBlock(action_to_string(action), CYAN, YELLOW);
            report(component);
        }
    }
}



void
print_inBuffer(const Conn *conn)
{
    if (conn == NULL) {
        fprintf(stderr, "Error: Connection pointer is NULL\n");
        return;
    }
    if (conn->inBuffer == NULL) {
        fprintf(stderr, "Error: inBuffer is NULL\n");
        return;
    }

    size_t data_len = conn->inEnd - conn->inStart;
    if (data_len == 0) {
        printf("inBuffer is empty\n");
        return;
    }

    const unsigned char *data = (const unsigned char *) conn->inBuffer + conn->inStart;

    printf("inBuffer contents (%zu bytes):\n", data_len);

    // Print hexadecimal and ASCII representation
    for (size_t i = 0; i < data_len; i++) {
        // Print hexadecimal
        printf("%02X ", data[i]);

        // Print ASCII or '.' for non-printable characters
        if (isprint(data[i])) {
            putchar(data[i]);
        } else {
            putchar('.');
        }

        // Formatting for readability (16 bytes per line)
        if ((i + 1) % 16 == 0) {
            printf("\n");
        } else {
            printf("   "); // Spacing between hex and ASCII
        }
    }

    printf("\n");
}


// Helper function to get required reads for INSTALL based on component
static int get_required_reads_for_install(Component comp) {
    const char *comp_str = component_to_string(comp);
    if (strcasecmp(comp_str, "HDFS") == 0) return 465;
    if (strcasecmp(comp_str, "HBASE") == 0) return 111;
    if (strcasecmp(comp_str, "HIVE") == 0) return 508;
    if (strcasecmp(comp_str, "KAFKA") == 0) return 213;
    if (strcasecmp(comp_str, "LIVY") == 0) return 19;
    if (strcasecmp(comp_str, "STORM") == 0) return 4;
    if (strcasecmp(comp_str, "PIG") == 0) return 4;
    if (strcasecmp(comp_str, "PRESTO") == 0) return 14;
    if (strcasecmp(comp_str, "ATLAS") == 0) return 7;
    if (strcasecmp(comp_str, "RANGER") == 0) return 4;
    if (strcasecmp(comp_str, "SOLR") == 0) return 5;
    if (strcasecmp(comp_str, "SPARK") == 0) return 66;
    if (strcasecmp(comp_str, "TEZ") == 0) return 46;
    if (strcasecmp(comp_str, "ZEPPELIN") == 0) return 55;
    if (strcasecmp(comp_str, "ZOOKEEPER") == 0) return 30;
    if (strcasecmp(comp_str, "FLINK") == 0) return 129;
    return 1; // Default for unknown components
}

// Helper function to perform required reads
static void do_read(Conn *conn, int num_reads, Component comp, Action action) {
    bool break_on_success = (action == INSTALL);
    for (int i = 0; i < num_reads; i++) {
        reset_connection_buffers(conn);
        int dataResult;
        do {
            dataResult = ReadData(conn);
            if (dataResult < 0) {
                fprintf(stderr, "Failed to read from socket for %s\n", component_to_string(comp));
                return;
            }
        } while (dataResult <= 0);

        printTextBlock(conn->inBuffer + conn->inStart, BOLD GREEN, YELLOW);

        if (break_on_success && check_installed_message(conn->inBuffer + conn->inStart)) {
            break;
        }
    }
}

// Helper function to handle dependency operations
static void process_dependencies(Component component, Action action, Conn *conn, bool include_dependencies) {
    if (!include_dependencies) return;

    int dep_count = 0;
    Component* dependencies = get_dependencies(component, &dep_count);

    if (dep_count > 0) {
        printTextBlock("Processing dependencies...", BOLD CYAN, YELLOW);
    }

    for (int i = 0; i < dep_count; i++) {
        Component dep = dependencies[i];
        const char* dep_name = component_to_string(dep);

        // Print dependency header
        char dep_header[128];
        snprintf(dep_header, sizeof(dep_header), "Dependency: %s", dep_name);
        printBorder("├", "┤", YELLOW);
        printTextBlock(dep_header, CYAN, YELLOW);
        printTextBlock(action_to_string(action), CYAN, YELLOW);

        // Send command and handle reads based on action
        SendComponentActionCommand(dep, action, NULL, NULL, NULL, conn);

        if (action == INSTALL) {
            if (start_stdout_capture(dep) != 0) {
                fprintf(stderr, "Failed to capture stdout for %s\n", dep_name);
                continue;
            }
            do_read(conn, get_required_reads_for_install(dep), dep, action);
            stop_stdout_capture();
        } else if (action == VERSION_SWITCH) {
            // Special handling for version switch in dependencies
            SendComponentActionCommand(dep, UNINSTALL, NULL, NULL, NULL, conn);
            do_read(conn, 1, dep, UNINSTALL);

            SendComponentActionCommand(dep, INSTALL, NULL, NULL, NULL, conn);
            do_read(conn, get_required_reads_for_install(dep), dep, INSTALL);
        } else {
            // START/STOP/RESTART/REPORT/UNINSTALL
            do_read(conn, 1, dep, action);
        }
    }
}




static void handle_remote_components(bool ALL, Component component, Action action,
                                     char *version , char *config_param , char *value) {
    Conn* conn = connect_to_debo(host, port);
    if (conn == NULL)
        fprintf(stderr, "Failed to connect to Debo\n");

    if (ALL) {
        // Handle all components (skip NONE)
        for (Component c = HDFS; c <= RANGER; c++) {
            const char *comp_str = component_to_string(c);
            if (!comp_str) continue; // Skip if component string is NULL

            printBorder("┌", "┐", YELLOW);
            printTextBlock(component_to_string(c), BOLD GREEN, YELLOW);
            printBorder("├", "┤", YELLOW);
            if (strcmp(action_to_string(action), "Installing...") == 0)
                printTextBlock("Installing might take several minutes", BOLD GREEN, YELLOW);
            printTextBlock(action_to_string(action), CYAN, YELLOW);
            SendComponentActionCommand(c, action, version , config_param, value, conn);

            // Determine read strategy based on action type
            int num_reads;
            int check_early_exit = 0;  // Flag for install/version-switch early exit

            if (action == INSTALL) {
                num_reads = get_required_reads_for_install(c);
                check_early_exit = 1;
                if (start_stdout_capture(c) != 0) exit(EXIT_FAILURE);
            }
            else if (action == VERSION_SWITCH) {
                num_reads = get_required_reads_for_install(c) + 1;
                check_early_exit = 1;
                if (start_stdout_capture(c) != 0) exit(EXIT_FAILURE);
            }
            else {
                num_reads = 1;  // Single read for other actions
            }

            // Unified read handling for all actions
            for (int i = 0; i < num_reads; i++) {
                int dataResult;
                reset_connection_buffers(conn);
                do {
                    dataResult = ReadData(conn);
                    if (dataResult < 0) {
                        fprintf(stderr, "Failed to read from socket\n");
                        break;
                    }
                } while (dataResult <= 0);

                printTextBlock(conn->inBuffer + conn->inStart, BOLD GREEN, YELLOW);

                // Early exit check for install/version-switch
                if (check_early_exit && check_installed_message(conn->inBuffer + conn->inStart)) {
                    break;
                }
            }

            // Only stop capture if we started it
            if (action == INSTALL || action == VERSION_SWITCH) {
                stop_stdout_capture();
            }
        }
        // Fixed component handling code
    } else {
        if (component == NONE) {
            fprintf(stderr, "Component must be specified when ALL=false\n");
            return;
        }

        const char* comp_name = component_to_string(component);
        int dep_count = 0;

        // Print main component header
        printBorder("┌", "┐", YELLOW);
        printTextBlock(comp_name, BOLD GREEN, YELLOW);
        printBorder("├", "┤", YELLOW);

        if (action == INSTALL) {
            printTextBlock("Installation might take several minutes", BOLD GREEN, YELLOW);
        }

        printTextBlock(action_to_string(action), CYAN, YELLOW);

        // Handle VERSION_SWITCH first (special case)
        if (action == VERSION_SWITCH) {
            // Process dependencies for version switch
            process_dependencies(component, VERSION_SWITCH, conn, dependency);

            // Handle main component version switch
            SendComponentActionCommand(component, UNINSTALL, NULL, NULL, NULL, conn);
            do_read(conn, 1, component, UNINSTALL);

            SendComponentActionCommand(component, INSTALL, version, config_param, value, conn);
            if (start_stdout_capture(component) != 0) {
                fprintf(stderr, "Failed to capture stdout for %s\n", comp_name);
                exit(EXIT_FAILURE);
            }
            do_read(conn, get_required_reads_for_install(component) + 1, component, INSTALL);
            stop_stdout_capture();
        }
        // Handle all other actions
        else {
            // Process dependencies first (if specified)
            process_dependencies(component, action, conn, dependency);

            // Now handle the main component
            SendComponentActionCommand(component, action, version, config_param, value, conn);

            switch (action) {
            case INSTALL:
                if (start_stdout_capture(component) != 0) {
                    fprintf(stderr, "Failed to capture stdout for %s\n", comp_name);
                    exit(EXIT_FAILURE);
                }
                do_read(conn, get_required_reads_for_install(component), component, INSTALL);
                stop_stdout_capture();
                break;

            case START:
            case STOP:
            case RESTART:
            case REPORT:
            case UNINSTALL:
                // Single read for these actions
                do_read(conn, 1, component, action);
                break;

            default:
                // Handle other actions with single read
                do_read(conn, 1, component, action);
                break;
            }
        }

        // Send finish message
        if (PutMsgStart(CliMsg_Finish, conn) < 0) {
            fprintf(stderr, "Failed to send finish message\n");
            return;
        }

        PutMsgEnd(conn);
        (void)Flush(conn);

        printBorder("└", "┘", YELLOW);
        printf("\n");
    }
}
