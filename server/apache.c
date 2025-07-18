/*-------------------------------------------------------------------------
 *
 * apache.c
 *		command-line interface for the Hadoop ecosystem..
 *
 * Copyright (c) 2025 Surafel Temesgen
 *-------------------------------------------------------------------------
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
    {YARN,          "hadoop-yarn",          "hadoop-yarn",          "hadoop"},
    {HBASE,         "hbase",                "hbase",                "hbase"},
    {SPARK,         "spark-core",           "spark",                "apache-spark"},
    {KAFKA,         "kafka",                "kafka",                "kafka"},
    {ZOOKEEPER,     "zookeeper",            "zookeeper",            "zookeeper"},
    {FLINK,         "flink",                "flink",                "apache-flink"},
    {STORM,         "storm",                "storm",                "apache-storm"},
    {HIVE,"hive-metastore",       "hive-metastore",       "hive"},
    {HUE,           "hue",                  "hue",                  "hue"},
    {PIG,           "pig",                  "pig",                  "pig"},
    {OOZIE,         "oozie",                "oozie",                "oozie"},
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
    {YARN,          "hadoop version 2>&1 | grep '^Hadoop' | awk '{print $2}'",
                    "hadoop-yarn", "hadoop-yarn", "hadoop"},
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
    {HUE,           "hue version 2>&1 | awk '{print $3}'",
                    "hue", "hue", "hue"},
    {PIG,           "pig --version 2>&1 | grep 'Apache Pig' | awk '{print $3}'",
                    "pig", "pig", "pig"},
    {OOZIE,         "oozie version 2>&1 | grep 'Oozie' | awk '{print $2}'",
                    "oozie", "oozie", "oozie"},
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
		{"verswitch", no_argument, NULL, 'U'},
		{"uninstall", no_argument, NULL, 'u'},
		{"configure", required_argument, NULL, 'c'},
		{"hdfs", no_argument, NULL, 'h'},
		{"yarn", no_argument, NULL, 'y'},
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
		{"dependency", no_argument, NULL, 'd'},
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
			case 'y':
				component = YARN;
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
        case YARN: return "HADOOP_HOME";
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

bool is_component_installed(Component comp) {
    const char *env_var = get_env_variable(comp);
    if (!env_var) return false;

    char *home = getenv("HOME");
    if (!home) {
        fprintf(stderr, "HOME environment variable not set.\n");
        return false;
    }

    char bashrc_path[PATH_MAX];
    snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home);

    FILE *bashrc = fopen(bashrc_path, "r");
    if (!bashrc) {
        perror("Error opening .bashrc");
        return false;
    }

    char line[1024];
    char search_str[256];
    snprintf(search_str, sizeof(search_str), "export %s=", env_var);

    bool found = false;
    while (fgets(line, sizeof(line), bashrc)) {
        if (strstr(line, search_str)) {
            found = true;
            break;
        }
    }

    fclose(bashrc);
    return found;
}


void configure_dependency_for_component(Component target, Component dependency) {
    ConfigStatus status;
    
    switch(target) {
        case HBASE:
            switch(dependency) {
                case HDFS:
                    status = modify_hdfs_config("dfs.datanode.handler.count", "30", "hdfs-site.xml");
                    handle_result(status, "dfs.datanode.handler.count", "30", "hdfs-site.xml");
                    status = modify_hdfs_config("dfs.replication", "3", "hdfs-site.xml");
                    handle_result(status, "dfs.replication", "3", "hdfs-site.xml");
                    status = modify_hdfs_config("dfs.socket.timeout", "180000", "hdfs-site.xml");
                    handle_result(status, "dfs.socket.timeout", "180000", "hdfs-site.xml");
                    break;
                    
                case ZOOKEEPER:
                    status = modify_zookeeper_config("maxClientCnxns", "200", "zoo.cfg");
                    handle_result(status, "maxClientCnxns", "200", "zoo.cfg");
                    status = modify_zookeeper_config("tickTime", "2000", "zoo.cfg");
                    handle_result(status, "tickTime", "2000", "zoo.cfg");
                    status = modify_zookeeper_config("initLimit", "10", "zoo.cfg");
                    handle_result(status, "initLimit", "10", "zoo.cfg");
                    break;
                default: break;
            }
            break;
            
        case KAFKA:
            if (dependency == ZOOKEEPER) {
                status = modify_zookeeper_config("maxSessionTimeout", "60000", "zoo.cfg");
                handle_result(status, "maxSessionTimeout", "60000", "zoo.cfg");
                status = modify_zookeeper_config("minSessionTimeout", "6000", "zoo.cfg");
                handle_result(status, "minSessionTimeout", "6000", "zoo.cfg");
                status = modify_zookeeper_config("jute.maxbuffer", "4194304", "zoo.cfg");
                handle_result(status, "jute.maxbuffer", "4194304", "zoo.cfg");
            }
            break;
            
        case HIVE:
            switch(dependency) {
                case HDFS:
                    status = modify_hdfs_config("dfs.blocksize", "268435456", "hdfs-site.xml");
                    handle_result(status, "dfs.blocksize", "268435456", "hdfs-site.xml");
                    status = modify_hdfs_config("dfs.client.read.shortcircuit", "true", "hdfs-site.xml");
                    handle_result(status, "dfs.client.read.shortcircuit", "true", "hdfs-site.xml");
                    status = modify_hdfs_config("yarn.scheduler.maximum-allocation-mb", "16384", "yarn-site.xml");
                    handle_result(status, "yarn.scheduler.maximum-allocation-mb", "16384", "yarn-site.xml");
                    status = modify_hdfs_config("yarn.nodemanager.resource.memory-mb", "16384", "yarn-site.xml");
                    handle_result(status, "yarn.nodemanager.resource.memory-mb", "16384", "yarn-site.xml");
                    break;
                    
                case TEZ:
                    status = modify_tez_config("tez.am.resource.memory.mb", "4096", "tez-site.xml");
                    handle_result(status, "tez.am.resource.memory.mb", "4096", "tez-site.xml");
                    status = modify_tez_config("tez.task.resource.memory.mb", "2048", "tez-site.xml");
                    handle_result(status, "tez.task.resource.memory.mb", "2048", "tez-site.xml");
                    break;
              default: break;
            }
            break;
            
        case PHOENIX:
            if (dependency == HBASE) {
                status = update_hbase_config("hbase.regionserver.wal.codec", 
                                           "org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec", 
                                           "hbase-site.xml");
                handle_result(status, "hbase.regionserver.wal.codec", 
                           "org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec", 
                           "hbase-site.xml");
                status = update_hbase_config("phoenix.schema.isNamespaceMappingEnabled", 
                                           "true", 
                                           "hbase-site.xml");
                handle_result(status, "phoenix.schema.isNamespaceMappingEnabled", "true", "hbase-site.xml");
            }
            break;
            
        case STORM:
            if (dependency == ZOOKEEPER) {
                status = modify_zookeeper_config("autopurge.snapRetainCount", "10", "zoo.cfg");
                handle_result(status, "autopurge.snapRetainCount", "10", "zoo.cfg");
                status = modify_zookeeper_config("autopurge.purgeInterval", "24", "zoo.cfg");
                handle_result(status, "autopurge.purgeInterval", "24", "zoo.cfg");
            }
            break;
            
        case SPARK:
            switch(dependency) {
                case HDFS:
                    status = modify_hdfs_config("dfs.datanode.max.transfer.threads", "4096", "hdfs-site.xml");
                    handle_result(status, "dfs.datanode.max.transfer.threads", "4096", "hdfs-site.xml");
                    status = modify_hdfs_config("dfs.client.read.shortcircuit", "true", "hdfs-site.xml");
                    handle_result(status, "dfs.client.read.shortcircuit", "true", "hdfs-site.xml");
                    status = modify_hdfs_config("yarn.scheduler.maximum-allocation-mb", "16384", "yarn-site.xml");
                    handle_result(status, "yarn.scheduler.maximum-allocation-mb", "16384", "yarn-site.xml");
                    status = modify_hdfs_config("yarn.nodemanager.resource.memory-mb", "16384", "yarn-site.xml");
                    handle_result(status, "yarn.nodemanager.resource.memory-mb", "16384", "yarn-site.xml");
                    break;
            default: break;
            }
            break;
            
        case TEZ:
            switch(dependency) {
                case HDFS:
                    status = modify_hdfs_config("dfs.replication", "2", "hdfs-site.xml");
                    handle_result(status, "dfs.replication", "2", "hdfs-site.xml");
                    status = modify_hdfs_config("dfs.blocksize", "268435456", "hdfs-site.xml");
                    handle_result(status, "dfs.blocksize", "268435456", "hdfs-site.xml");
                    status = modify_hdfs_config("yarn.nodemanager.resource.cpu-vcores", "8", "yarn-site.xml");
                    handle_result(status, "yarn.nodemanager.resource.cpu-vcores", "8", "yarn-site.xml");
                    status = modify_hdfs_config("yarn.scheduler.minimum-allocation-mb", "1024", "yarn-site.xml");
                    handle_result(status, "yarn.scheduler.minimum-allocation-mb", "1024", "yarn-site.xml");
                    break;
              default: break;
            }
            break;
            
        case LIVY:
            if (dependency == SPARK) {
                status = update_spark_config("spark.sql.shuffle.partitions", "100", "spark-defaults.conf");
                handle_result(status, "spark.sql.shuffle.partitions", "100", "spark-defaults.conf");
                status = update_spark_config("spark.driver.memory", "4g", "spark-defaults.conf");
                handle_result(status, "spark.driver.memory", "4g", "spark-defaults.conf");
            }
            break;
            
        case RANGER:
            switch(dependency) {
                case SOLR:
                    status = update_solr_config("solr.autoSoftCommit.maxTime", "1000", "solrconfig.xml");
                    handle_result(status, "solr.autoSoftCommit.maxTime", "1000", "solrconfig.xml");
                    status = update_solr_config("solr.autoCommit.maxTime", "15000", "solrconfig.xml");
                    handle_result(status, "solr.autoCommit.maxTime", "15000", "solrconfig.xml");
                    break;
                    
                case HDFS:
                    status = modify_hdfs_config("dfs.permissions.enabled", "true", "hdfs-site.xml");
                    handle_result(status, "dfs.permissions.enabled", "true", "hdfs-site.xml");
                    status = modify_hdfs_config("dfs.namenode.acls.enabled", "true", "hdfs-site.xml");
                    handle_result(status, "dfs.namenode.acls.enabled", "true", "hdfs-site.xml");
                    break;
              default: break;
            }
            break;
            
        case ATLAS:
            switch(dependency) {
                case HBASE:
                    status = update_hbase_config("hbase.regionserver.wal.codec", 
                                               "org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec", 
                                               "hbase-site.xml");
                    handle_result(status, "hbase.regionserver.wal.codec", 
                               "org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec", 
                               "hbase-site.xml");
                    status = update_hbase_config("hbase.regionserver.handler.count", "60", "hbase-site.xml");
                    handle_result(status, "hbase.regionserver.handler.count", "60", "hbase-site.xml");
                    break;
                    
                case KAFKA:
                    status = modify_kafka_config("delete.topic.enable", "true", "server.properties");
                    handle_result(status, "delete.topic.enable", "true", "server.properties");
                    status = modify_kafka_config("log.retention.hours", "8760", "server.properties");
                    handle_result(status, "log.retention.hours", "8760", "server.properties");
                    break;
                    
                case SOLR:
                    status = update_solr_config("solr.autoCommit.maxDocs", "10000", "solrconfig.xml");
                    handle_result(status, "solr.autoCommit.maxDocs", "10000", "solrconfig.xml");
                    status = update_solr_config("solr.autoCommit.maxTime", "15000", "solrconfig.xml");
                    handle_result(status, "solr.autoCommit.maxTime", "15000", "solrconfig.xml");
                    break;
              default: break;
            }
            break;
            
        case PIG:
            switch(dependency) {
                case HDFS:
                    status = modify_hdfs_config("dfs.blocksize", "268435456", "hdfs-site.xml");
                    handle_result(status, "dfs.blocksize", "268435456", "hdfs-site.xml");
                    status = modify_hdfs_config("dfs.replication", "2", "hdfs-site.xml");
                    handle_result(status, "dfs.replication", "2", "hdfs-site.xml");
                    status = modify_hdfs_config("mapreduce.reduce.memory.mb", "4096" , "yarn-site.xml");
                    handle_result(status, "mapreduce.reduce.memory.mb", "4096", "yarn-site.xml");
                    status = modify_hdfs_config("mapreduce.map.memory.mb", "2048", "yarn-site.xml");
                    handle_result(status, "mapreduce.map.memory.mb", "2048", "yarn-site.xml");
                    break;
              default: break;
            }
            break;
            
        case SOLR:
            if (dependency == ZOOKEEPER) {
                status = modify_zookeeper_config("maxClientCnxns", "100", "zoo.cfg");
                handle_result(status, "maxClientCnxns", "100", "zoo.cfg");
                status = modify_zookeeper_config("tickTime", "2000", "zoo.cfg");
                handle_result(status, "tickTime", "2000", "zoo.cfg");
            }
            break;
            
        case YARN:
            if (dependency == HDFS) {
                status = modify_hdfs_config("dfs.datanode.data.dir.perm", "750", "hdfs-site.xml");
                handle_result(status, "dfs.datanode.data.dir.perm", "750", "hdfs-site.xml");
                status = modify_hdfs_config("dfs.namenode.name.dir.perm", "700", "hdfs-site.xml");
                handle_result(status, "dfs.namenode.name.dir.perm", "700", "hdfs-site.xml");
            }
            break;
            
        case FLINK:
            switch(dependency) {
                case HDFS:
                    status = modify_hdfs_config("dfs.stream-buffer-size", "4096", "hdfs-site.xml");
                    handle_result(status, "dfs.stream-buffer-size", "4096", "hdfs-site.xml");
                    status = modify_hdfs_config("dfs.client-write-packet-size", "65536", "hdfs-site.xml");
                    handle_result(status, "dfs.client-write-packet-size", "65536", "hdfs-site.xml");
                    status = modify_hdfs_config("yarn.application.classpath", 
                                              "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,...", 
                                              "yarn-site.xml");
                    handle_result(status, "yarn.application.classpath", 
                               "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,...", 
                               "yarn-site.xml");
                    status = modify_hdfs_config("yarn.nodemanager.vmem-check-enabled", "false", "yarn-site.xml");
                    handle_result(status, "yarn.nodemanager.vmem-check-enabled", "false", "yarn-site.xml");
                    break;
              default: break;
            }
            break;
            
        case PRESTO:
            switch(dependency) {
                case HIVE:
                    status = modify_hive_config("hive.metastore.uri", "thrift://localhost:9083", "hive-site.xml");
                    handle_result(status, "hive.metastore.uri", "thrift://localhost:9083", "hive-site.xml");
                    status = modify_hive_config("hive.allow-drop-table", "true", "hive-site.xml");
                    handle_result(status, "hive.allow-drop-table", "true", "hive-site.xml");
                    break;
                    
                case HDFS:
                    status = modify_hdfs_config("dfs.client.use.datanode.hostname", "true", "hdfs-site.xml");
                    handle_result(status, "dfs.client.use.datanode.hostname", "true", "hdfs-site.xml");
                    status = modify_hdfs_config("dfs.datanode.use.datanode.hostname", "true", "hdfs-site.xml");
                    handle_result(status, "dfs.datanode.use.datanode.hostname", "true", "hdfs-site.xml");
                    break;
                default: break;
            }
            break;
        default: break;
    }
}



static void install_component(Component comp, char *version) {

    if (is_component_installed(comp)) {
        printf("%s %s is already installed.\n", component_to_string(comp), version);
        return;
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
            configure_dependency_for_component(comp, deps[i]);
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

printBorder("┌", "┐", YELLOW);
    switch(comp) {
        case HDFS:
                printTextBlock("HDFS status", BOLD GREEN, YELLOW);
                printBorder("├", "┤", YELLOW);
                printTextBlock(report_hdfs(), CYAN, YELLOW);
                printBorder("└", "┘", YELLOW);
            break;
        case YARN:
            if (is_component_installed(comp)) 
            {
                printTextBlock("YARN status", BOLD GREEN, YELLOW);
                execute_and_print("yarn node -list");
            }
            else
            {
                printTextBlock("YARN status", BOLD GREEN, YELLOW);
                printTextBlock("YARN is not installed or started", BOLD GREEN, YELLOW);
            } 
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
        case HUE:
            if (is_component_installed(comp)) 
            {
                printTextBlock("HUE status", BOLD GREEN, YELLOW);
                execute_and_print("sudo systemctl status hue");
            }
            else
            {
                printTextBlock("HUE status", BOLD GREEN, YELLOW);
                printTextBlock("HUE is not installed or started", BOLD GREEN, YELLOW);
            }
            break;
        case PIG:
                printTextBlock("PIG status", BOLD GREEN, YELLOW);
                printBorder("├", "┤", YELLOW);
                printTextBlock(report_pig(), CYAN, YELLOW);
                printBorder("└", "┘", YELLOW);
            break;
        case OOZIE:
            if (is_component_installed(comp)) 
            {
                printTextBlock("OOZIE status", BOLD GREEN, YELLOW);
                execute_and_print("oozie admin -status");
            }
            else
            {
                printTextBlock("OOZIE status", BOLD GREEN, YELLOW);
                printTextBlock("OOZIE is not installed or started", BOLD GREEN, YELLOW);
            }
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
            if (action != NO_ACTION) {
                printBorder("┌", "┐", YELLOW);
                printTextBlock(component_to_string(c), BOLD GREEN, YELLOW);
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
    if (strcasecmp(comp_str, "HDFS") == 0) return 534;
    if (strcasecmp(comp_str, "HBASE") == 0) return 133;
    if (strcasecmp(comp_str, "HIVE") == 0) return 566;
    if (strcasecmp(comp_str, "KAFKA") == 0) return 246;
    if (strcasecmp(comp_str, "LIVY") == 0) return 37;
    if (strcasecmp(comp_str, "STORM") == 0) return 4;
    if (strcasecmp(comp_str, "PIG") == 0) return 4;
    if (strcasecmp(comp_str, "PRESTO") == 0) return 4;
    if (strcasecmp(comp_str, "ATLAS") == 0) return 7;
    if (strcasecmp(comp_str, "RANGER") == 0) return 4;
    if (strcasecmp(comp_str, "SOLR") == 0) return 14;
    if (strcasecmp(comp_str, "SPARK") == 0) return 72;
    if (strcasecmp(comp_str, "TEZ") == 0) return 53;
    if (strcasecmp(comp_str, "ZEPPELIN") == 0) return 62;
    if (strcasecmp(comp_str, "ZOOKEEPER") == 0) return 32;
    if (strcasecmp(comp_str, "FLINK") == 0) return 147;
    return 1; // Default for unknown components
}
static void handle_remote_components(bool ALL, Component component, Action action,
char *version , char *config_param , char *value) {
    Conn* conn = connect_to_debo(host, port);
    if (conn == NULL)
        fprintf(stderr, "Failed to connect to Debo\n");

    if (ALL) {
        // Handle all components (skip NONE)
        for (Component c = HDFS; c <= RANGER; c++) {
            printBorder("┌", "┐", YELLOW);
            printTextBlock(component_to_string(c), BOLD GREEN, YELLOW);
            printBorder("├", "┤", YELLOW);
            if (strcmp(action_to_string(action), "Installing...") == 0)
                printTextBlock("Installing might take several minutes", BOLD GREEN, YELLOW);
            printTextBlock(action_to_string(action), CYAN, YELLOW);
            SendComponentActionCommand(c, action, version , config_param, value, conn);
            //  PutMsgEnd(conn);
        }
    } else {
        // Handle single component
        if (component == NONE) {
            fprintf(stderr, "Component must be specified when ALL=false\n");
            return;
        }
        int dep_count = 0;
        
        // Get dependencies if applicable
        if (dependency && (action == INSTALL) && (action == UNINSTALL)) {
            (void) get_dependencies(component, &dep_count);
        }

        
        printBorder("┌", "┐", YELLOW);
        printTextBlock(component_to_string(component), BOLD GREEN, YELLOW);
        printBorder("├", "┤", YELLOW);
        if (strcmp(action_to_string(action), "Installing...") == 0)
            printTextBlock("Installing might take several minutes", BOLD GREEN, YELLOW);
        printTextBlock(action_to_string(action), CYAN, YELLOW);
        SendComponentActionCommand(component, action, version, config_param, value, conn);
          // Single read for non-INSTALL actions
            reset_connection_buffers(conn);
        if (action == INSTALL && !dependency) {
              if (start_stdout_capture() != 0) {
                        exit(EXIT_FAILURE);
               }
            int num_reads = get_required_reads_for_install(component);
            for (int i = 0; i < num_reads; i++) {
                int dataResult;
            do {
                dataResult = ReadData(conn);
                if (dataResult < 0) {
                    fprintf(stderr, "Failed to read from socket\n");
                    break;
                }
            } while (dataResult <= 0);
            printTextBlock(conn->inBuffer + conn->inStart, BOLD GREEN, YELLOW);
            }
            stop_stdout_capture();
        } else if (!dependency){
            int dataResult;
            do {
                dataResult = ReadData(conn);
                if (dataResult < 0) {
                    fprintf(stderr, "Failed to read from socket\n");
                    break;
                }
            } while (dataResult <= 0);
              printTextBlock(conn->inBuffer + conn->inStart, BOLD GREEN, YELLOW);
        }
        else 
        {
        // Calculate total iterations = (sum of reads for all dependencies) + (dependency count)
       int dep_count = 0;
       Component* dependencies = get_dependencies(component, &dep_count);
       int total_iterations = dep_count;  // Start with count of dependencies

// Sum required reads for each dependency
       for (int i = 0; i < dep_count; i++) {
           total_iterations += get_required_reads_for_install(dependencies[i]);
       }

       // Execute reading loop for total_iterations
       int has_error = 0;
       for (int i = 0; i < total_iterations && !has_error; i++) {
           int dataResult;
           do {
               dataResult = ReadData(conn);
               if (dataResult < 0) {
                   fprintf(stderr, "Failed to read from socket\n");
                   has_error = 1;
                   break;
               }
           } while (dataResult <= 0);

           if (!has_error) {
               printTextBlock(conn->inBuffer + conn->inStart, BOLD GREEN, YELLOW);
  
           }
         }
       }

 
        if (PutMsgStart(CliMsg_Finish, conn) < 0) {
            fprintf(stderr, "Failed to send finish message\n");
            return;
        }

        PutMsgEnd(conn);
        (void) Flush(conn);
    }
}
