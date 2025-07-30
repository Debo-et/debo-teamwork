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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <glob.h>
#include <errno.h>
#include <pwd.h>

#include "utiles.h"

void hadoop_action(Action a) {
    const char *hadoop_home = NULL;
    const char *candidates[] = {
        getenv("HADOOP_HOME"),  // Primary candidate
        "/opt/hadoop",          // Red Hat path
        "/usr/local/hadoop"     // Debian path
    };

    // Search for valid Hadoop installation
    for (size_t i = 0; i < sizeof(candidates)/sizeof(candidates[0]); i++) {
        const char *candidate = candidates[i];
        if (!candidate) continue;

        char start_script[PATH_MAX];
        char stop_script[PATH_MAX];
        struct stat st;

        // Check for both start and stop scripts
        snprintf(start_script, sizeof(start_script), "%s/sbin/start-all.sh", candidate);
        snprintf(stop_script, sizeof(stop_script), "%s/sbin/stop-all.sh", candidate);

        if (stat(start_script, &st) == 0 && S_ISREG(st.st_mode) &&
            stat(stop_script, &st) == 0 && S_ISREG(st.st_mode)) {
            hadoop_home = candidate;
            break;
        }
    }

    if (!hadoop_home) {
        FPRINTF(global_client_socket,  "Hadoop installation not found. Searched:\n"
                "- HADOOP_HOME environment variable\n"
                "- /opt/hadoop (Red Hat)\n"
                "- /usr/local/hadoop (Debian)\n");
        return;
    }

    char cmd[4096];
    int ret, len;

    switch (a) {
    case START:
        len = snprintf(cmd, sizeof(cmd), "\"%s/sbin/start-all.sh\"", hadoop_home);
        if (len < 0 || (size_t)len >= sizeof(cmd)) {
            FPRINTF(global_client_socket,  "Command buffer overflow detected\n");
            return;
        }
        ret = executeSystemCommand(cmd);
        if (ret == -1) {
            FPRINTF(global_client_socket,  "Start failed (Code: %d). Verify:\n"
                    "- User permissions\n"
                    "- Hadoop configuration\n"
                    "- Cluster status\n", ret);
        } else {
            PRINTF(global_client_socket, "All Hadoop services started successfully\n");
        }
        break;

    case STOP:
        len = snprintf(cmd, sizeof(cmd), "\"%s/sbin/stop-all.sh\"", hadoop_home);
        if (len < 0 || (size_t)len >= sizeof(cmd)) {
            FPRINTF(global_client_socket,  "Command buffer overflow detected\n");
            return;
        }
        ret = executeSystemCommand(cmd);
        if (ret == -1) {
            FPRINTF(global_client_socket,  "Stop failed (Code: %d). Possible causes:\n"
                    "- Services already stopped\n"
                    "- Permission issues\n"
                    "- Node connectivity problems\n", ret);
        } else {
            PRINTF(global_client_socket, "All Hadoop services stopped successfully\n");
        }
        break;

    case RESTART: {
        // Stop phase
        len = snprintf(cmd, sizeof(cmd), "\"%s/sbin/stop-all.sh\"", hadoop_home);
        if (len < 0 || (size_t)len >= sizeof(cmd)) {
            FPRINTF(global_client_socket,  "Command buffer overflow detected\n");
            return;
        }
        ret = executeSystemCommand(cmd);
        if (ret == -1) {
            FPRINTF(global_client_socket,  "Restart aborted - stop phase failed (Code: %d)\n", ret);
            return;
        }
        //            PRINTF(global_client_socket, "Services stopped successfully, initiating restart...\n");

        // Add delay for service shutdown (adjust as needed)
        sleep(5);

        // Start phase
        len = snprintf(cmd, sizeof(cmd), "\"%s/sbin/start-all.sh\"", hadoop_home);
        if (len < 0 || (size_t)len >= sizeof(cmd)) {
            FPRINTF(global_client_socket,  "Command buffer overflow detected\n");
            return;
        }
        ret = executeSystemCommand(cmd);
        if (ret == -1) {
            FPRINTF(global_client_socket,  "Restart incomplete - start phase failed (Code: %d)\n"
                    "System may be in inconsistent state!\n", ret);
        } else {
            PRINTF(global_client_socket, "All services restarted successfully\n");
        }
        break;
    }

    default:
        FPRINTF(global_client_socket,  "Invalid action. Valid options:\n"
                "0 - START\n1 - STOP\n2 - RESTART\n");
        break;
    }
}

static char* find_launcher(const char* base_dir) {
    if (!base_dir) return NULL;

    char candidate[PATH_MAX];
    struct stat st;
    
    // Check direct path first (new installation structure)
    snprintf(candidate, sizeof(candidate), "%s/bin/launcher", base_dir);
    if (stat(candidate, &st) == 0 && S_ISREG(st.st_mode)) {
        return strdup(candidate);
    }

    // Fallback to old pattern (versioned directory)
    char pattern[PATH_MAX];
    snprintf(pattern, sizeof(pattern), "%s/presto-server-*/bin/launcher", base_dir);
    
    glob_t glob_result;
    if (glob(pattern, GLOB_ERR, NULL, &glob_result) != 0 || glob_result.gl_pathc == 0) {
        globfree(&glob_result);
        return NULL;
    }

    // Use first match
    char* path = strdup(glob_result.gl_pathv[0]);
    globfree(&glob_result);
    return path;
}

void Presto_action(Action action) {
    char* launcher_path = NULL;
    char command[PATH_MAX * 2];
    const char* action_name = "";
    int ret;

    // Check installation locations in priority order
    const char* candidates[] = {
        getenv("PRESTO_HOME"),  // Environment variable
        "/opt/presto",          // RedHat/CentOS
        "/usr/local/presto",    // Debian/Ubuntu
        NULL
    };

    for (int i = 0; candidates[i] != NULL; i++) {
        launcher_path = find_launcher(candidates[i]);
        if (launcher_path) break;
    }

    if (!launcher_path) {
        FPRINTF(global_client_socket, "Error: Presto installation not found\n");
        exit(EXIT_FAILURE);
    }

    switch(action) {
        case START:
            action_name = "start";
            snprintf(command, sizeof(command), "%s start", launcher_path);
            break;

        case STOP:
            action_name = "stop";
            snprintf(command, sizeof(command), "%s stop", launcher_path);
            break;

        case RESTART:
            action_name = "restart";
            snprintf(command, sizeof(command), "%s restart", launcher_path);
            break;

        default:
            FPRINTF(global_client_socket, "Error: Invalid action\n");
            free(launcher_path);
            exit(EXIT_FAILURE);
    }

    // Execute command with output redirection
    snprintf(command + strlen(command), sizeof(command) - strlen(command), " >/dev/null 2>&1");
    
    if ((ret = executeSystemCommand(command)) != 0) {
        FPRINTF(global_client_socket, "Error: Failed to %s Presto (%d)\n", action_name, ret);
        free(launcher_path);
        exit(EXIT_FAILURE);
    }

    PRINTF(global_client_socket, "Presto service %s completed successfully\n", action_name);
    free(launcher_path);
}

void spark_action(Action a) {
    const char* install_path = NULL;

    // Detect OS distribution
    if (access("/etc/debian_version", F_OK) == 0) {
        install_path = "/usr/local/spark";
    } else if (access("/etc/redhat-release", F_OK) == 0 ||
               access("/etc/system-release", F_OK) == 0) {
        install_path = "/opt/spark";
    } else {
        FPRINTF(global_client_socket,  "Error: Unsupported Linux distribution\n");
        return;
    }

    // Construct base commands with environment configuration
    char start_cmd[512];
    char stop_cmd[512];
    long unsigned int cmd_len;

    cmd_len = snprintf(start_cmd, sizeof(start_cmd),
                       "export SPARK_HOME=%s && %s/sbin/start-all.sh" ,
                       install_path, install_path);
    if (cmd_len >= sizeof(start_cmd)) {
        FPRINTF(global_client_socket,  "Error: Start command buffer overflow\n");
        return;
    }

    cmd_len = snprintf(stop_cmd, sizeof(stop_cmd),
                       "export SPARK_HOME=%s && %s/sbin/stop-all.sh",
                       install_path, install_path);
    if (cmd_len >= sizeof(stop_cmd)) {
        FPRINTF(global_client_socket,  "Error: Stop command buffer overflow\n");
        return;
    }

    // Execute command with error handling
    int execute_service_command(const char* command) {
        //  PRINTF(global_client_socket, "Attempting to %s Spark service...\n", action);
        int status = executeSystemCommand(command);

        if (status == -1) {
            PERROR(global_client_socket, "System command execution failed");
            return -1;
        }
        return 0;
    }

    // Handle different actions
    switch(a) {
    case START:
        if (execute_service_command(start_cmd) == -1) {
            FPRINTF(global_client_socket,  "Spark service startup aborted\n");
        }
        PRINTF(global_client_socket, "Spark service Start successfully\n");
        break;

    case STOP:
        if (execute_service_command(stop_cmd) == -1) {
            FPRINTF(global_client_socket,  "Spark service shutdown aborted\n");
        }
        PRINTF(global_client_socket, "Spark service Stop successfully\n");
        break;

    case RESTART:
        // Stop phase
        if (execute_service_command(stop_cmd) == -1) {
            FPRINTF(global_client_socket,  "Restart aborted due to stop failure\n");
            return;
        }

        // Add delay for service shutdown
        //  PRINTF(global_client_socket, "Waiting for services to stop...\n");
        sleep(3);

        // Start phase
        if (execute_service_command(start_cmd) == -1) {
            FPRINTF(global_client_socket,  "Spark service restart aborted\n");
        }
        PRINTF(global_client_socket, "Spark service restart successfully\n");
        break;

    default:
        FPRINTF(global_client_socket,  "Error: Invalid action specified\n");
        break;
    }
}

void hive_action(Action a) {
    const char *hive_path = NULL;
    const char *hive_home = getenv("HIVE_HOME");
    const char *default_paths[] = { "/opt/hive", "/usr/local/hive" };

    // Try HIVE_HOME first
    if (hive_home != NULL) {
        if (access(hive_home, F_OK) == 0) {
            hive_path = hive_home;
        } else {
            FPRINTF(global_client_socket,  "Warning: HIVE_HOME directory '%s' not found. Checking defaults...\n", hive_home);
        }
    }

    // Check default paths if needed
    if (hive_path == NULL) {
        for (size_t i = 0; i < sizeof(default_paths)/sizeof(default_paths[0]); i++) {
            if (access(default_paths[i], F_OK) == 0) {
                hive_path = default_paths[i];
                break;
            }
        }
    }

    if (hive_path == NULL) {
        FPRINTF(global_client_socket,  "Error: Hive installation not found. Checked:\n"
                "• HIVE_HOME environment variable\n"
                "• /opt/hive (Red Hat)\n"
                "• /usr/local/hive (Debian)\n");
        exit(EXIT_FAILURE);
    }

    // Validate Hive binary exists
    char hive_bin[PATH_MAX];
    snprintf(hive_bin, sizeof(hive_bin), "%s/bin/hive", hive_path);
    if (access(hive_bin, X_OK) != 0) {
        FPRINTF(global_client_socket,  "Error: Hive executable not found or not executable: %s\n", hive_bin);
        exit(EXIT_FAILURE);
    }

    char start_cmd[PATH_MAX * 2];
    char stop_cmd[PATH_MAX * 2];
    int ret;

    // Construct commands with proper shell escaping
    snprintf(start_cmd, sizeof(start_cmd),
             "\"%s/bin/hive\" --service hiveserver2 > /dev/null ", hive_path);

    snprintf(stop_cmd, sizeof(stop_cmd),
             "pkill -f '\"%s/bin/hive\" --service hiveserver2'", hive_path);

    switch (a) {
    case START: {
        ret = executeSystemCommand(start_cmd);
        if (ret == -1) {
            PERROR(global_client_socket, "Failed to execute start command");
            exit(EXIT_FAILURE);
        }

        PRINTF(global_client_socket, "Hive service started successfully\n");
        break;
    }

    case STOP: {
        ret = executeSystemCommand(stop_cmd);
        if (ret == -1) {
            PERROR(global_client_socket, "Failed to execute stop command");
            exit(EXIT_FAILURE);
        }

        PRINTF(global_client_socket, "Hive service stopped successfully\n");
        break;
    }

    case RESTART: {
        // Stop phase
        ret = executeSystemCommand(stop_cmd);
        if (ret == -1) {
            PERROR(global_client_socket, "Failed to stop during restart");
            exit(EXIT_FAILURE);
        }

        // Start phase
        ret = executeSystemCommand(start_cmd);
        if (ret == -1) {
            PERROR(global_client_socket, "Failed to start during restart");
            exit(EXIT_FAILURE);
        }

        PRINTF(global_client_socket, "Hive service restarted successfully\n");
        break;
    }

    default:
        FPRINTF(global_client_socket,  "Invalid action specified\n");
        exit(EXIT_FAILURE);
    }
}
void Zeppelin_action(Action a) {
    // Determine Zeppelin installation path
    const char *zeppelin_home = getenv("ZEPPELIN_HOME");
    char detected_path[PATH_MAX] = {0};

    // Fallback path detection if environment variable not set
    if (!zeppelin_home) {
        // Check Debian-based systems
        if (access("/etc/debian_version", F_OK) == 0) {
            strncpy(detected_path, "/usr/local/zeppelin", PATH_MAX);
        }
        // Check Red Hat-based systems
        else if (access("/etc/redhat-release", F_OK) == 0 ||
                 access("/etc/system-release", F_OK) == 0) {
            strncpy(detected_path, "/opt/zeppelin", PATH_MAX);
        }
        else {
            FPRINTF(global_client_socket,  "ZEPPELIN_HOME not set and OS detection failed\n");
            exit(EXIT_FAILURE);
        }
        zeppelin_home = detected_path;
    }

    // Verify daemon script existence
    char daemon_script[PATH_MAX];
    snprintf(daemon_script, sizeof(daemon_script),
             "%s/bin/zeppelin-daemon.sh", zeppelin_home);

    if (access(daemon_script, X_OK) != 0) {
        FPRINTF(global_client_socket,  "Zeppelin daemon script not found or not executable at: %s\n",
                daemon_script);
        exit(EXIT_FAILURE);
    }

    // Construct base command
    char command[PATH_MAX + 20];
    int ret;

    switch(a) {
    case START:
        snprintf(command, sizeof(command), "sudo %s start", daemon_script);
        //  PRINTF(global_client_socket, "Starting Zeppelin...\n");
        ret = executeSystemCommand(command);
        if (WEXITSTATUS(ret) != 0) {
            FPRINTF(global_client_socket,  "Start failed with exit code: %d\n", WEXITSTATUS(ret));
            exit(EXIT_FAILURE);
        }
        PRINTF(global_client_socket, "Zeppelin Started completed successfully\n");
        break;

    case STOP:
        snprintf(command, sizeof(command), "sudo %s stop", daemon_script);
        // PRINTF(global_client_socket, "Stopping Zeppelin...\n");
        ret = executeSystemCommand(command);
        if (WEXITSTATUS(ret) != 0) {
            FPRINTF(global_client_socket,  "Stop failed with exit code: %d\n", WEXITSTATUS(ret));
            exit(EXIT_FAILURE);
        }
        PRINTF(global_client_socket, "Zeppelin Stopped completed successfully\n");
        break;

    case RESTART:
        // Execute stop followed by start
        snprintf(command, sizeof(command), "sudo %s stop", daemon_script);
        //  PRINTF(global_client_socket, "Initiating restart...\n");
        ret = executeSystemCommand(command);
        if (WEXITSTATUS(ret) != 0) {
            FPRINTF(global_client_socket,  "Restart aborted - stop phase failed: %d\n",
                    WEXITSTATUS(ret));
            exit(EXIT_FAILURE);
        }

        snprintf(command, sizeof(command), "sudo %s start", daemon_script);
        ret = executeSystemCommand(command);
        if (WEXITSTATUS(ret) != 0) {
            FPRINTF(global_client_socket,  "Restart failed - start phase: %d\n",
                    WEXITSTATUS(ret));
            exit(EXIT_FAILURE);
        }
        PRINTF(global_client_socket, "Zeppelin Restarted successfully\n");
        break;

    default:
        FPRINTF(global_client_socket,  "Invalid action command\n");
        exit(EXIT_FAILURE);
    }
}


// Helper function to execute Livy commands
static int execute_command(const char* script_path, const char* arg) {
    char command[PATH_MAX + 128]; // Increased buffer size
    size_t len = snprintf(command, sizeof(command), "sudo %s %s", script_path, arg);

    if (len >= sizeof(command)) {
        FPRINTF(global_client_socket,  "Command truncated: 'sudo %s %s'\n", script_path, arg);
        return -1;
    }

    int status = executeSystemCommand(command);
    if (status == -1) {
        PERROR(global_client_socket, "system() failed");
        return -1;
    }

    if (WIFEXITED(status)) {
        int exit_status = WEXITSTATUS(status);
        if (exit_status != 0) {
            FPRINTF(global_client_socket,  "Command failed with exit code %d: %s\n", exit_status, command);
            return -1;
        }
    }
    return 0;
}

void livy_action(Action a) {
    const char *livy_home = NULL;
    // Detect OS distribution
    if (access("/etc/debian_version", F_OK) == 0) {
        livy_home = "/usr/local/livy";
    } else if (access("/etc/redhat-release", F_OK) == 0 ||
               access("/etc/system-release", F_OK) == 0) {
        livy_home = "/opt/livy";
    } else {
        FPRINTF(global_client_socket,  "Error: Unsupported Linux distribution\n");
        return;
    }
    // Construct and validate server script path
    char script_path[PATH_MAX];
    size_t path_len = snprintf(script_path, sizeof(script_path), "%s/bin/livy-server", livy_home);

    if (path_len >= sizeof(script_path)) {
        FPRINTF(global_client_socket,  "Path construction failed: Maximum length exceeded\n");
        return;
    }

    if (access(script_path, X_OK) != 0) {
        FPRINTF(global_client_socket,  "Livy server script not found or not executable: %s\n", script_path);
        return;
    }

    // Execute the requested action
    switch(a) {
    case START: {
        int rc = execute_command(script_path, "start");
        if (rc == 0) {
            PRINTF(global_client_socket, "Successfully started Livy service\n");
        } else {
            FPRINTF(global_client_socket,  "Start failed with exit code: %d\n", rc);
        }
        break;
    }
    case STOP: {
        int rc = execute_command(script_path, "stop");
        if (rc == 0) {
            PRINTF(global_client_socket, "Successfully stopped Livy service\n");
        } else {
            FPRINTF(global_client_socket,  "Stop failed with exit code: %d\n", rc);
        }
        break;
    }
    case RESTART: {
        int stop_rc = execute_command(script_path, "stop");
        if (stop_rc != 0) {
            FPRINTF(global_client_socket,  "Restart aborted - stop failed with code: %d\n", stop_rc);
            return;
        }
        //   PRINTF(global_client_socket, "Service stopped. Attempting restart...\n");
        int start_rc = execute_command(script_path, "start");
        if (start_rc == 0) {
            PRINTF(global_client_socket, "Successfully restarted Livy service\n");
        } else {
            FPRINTF(global_client_socket,  "Restart failed - start failed with code: %d\n", start_rc);
        }
        break;
    }
    default:
        FPRINTF(global_client_socket,  "Invalid action requested\n");
        break;
    }
}


const char *find_pig_home() {
    const char *locations[] = {
        getenv("PIG_HOME"),  // Check PIG_HOME first
        "/opt/pig",          // Red Hat fallback
        "/usr/local/pig"     // Debian fallback
    };

    char pig_path[PATH_MAX];
    struct stat st;

    for (int i = 0; i < 3; i++) {
        const char *location = locations[i];
        if (!location) continue;

        snprintf(pig_path, sizeof(pig_path), "%s/bin/pig", location);
        if (stat(pig_path, &st)) continue;
        if (S_ISREG(st.st_mode)) return location;
    }

    return NULL;
}


void pig_action(Action a) {
    const char *pig_home = find_pig_home();
    if (!pig_home) {
        FPRINTF(global_client_socket,  "Error: Pig installation not found. Checked:\n"
                "1. PIG_HOME environment variable\n"
                "2. /opt/pig (Red Hat)\n"
                "3. /usr/local/pig (Debian)\n");
        exit(EXIT_FAILURE);
    }

    char command[512];
    int ret, len;

    switch (a) {
    case START: {
        len = snprintf(command, sizeof(command),
                       "%s/bin/pig -x local > /dev/null 2>&1 &", pig_home);
        if (len < 0 || len >= (int)sizeof(command)) {
            FPRINTF(global_client_socket,  "Command construction error\n");
            exit(EXIT_FAILURE);
        }

        ret = executeSystemCommand(command);
        if (ret == -1) {
            FPRINTF(global_client_socket,  "Failed to start Pig. Error: %d\n"
                    "Verify Pig is installed at: %s\n", ret, pig_home);
            exit(EXIT_FAILURE);
        }
        break;
    }

    case STOP: {
        len = snprintf(command, sizeof(command),
                       "pkill -f '^%s/bin/pig -x local'", pig_home);
        if (len < 0 || len >= (int)sizeof(command)) {
            FPRINTF(global_client_socket,  "Command construction error\n");
            exit(EXIT_FAILURE);
        }

        ret = executeSystemCommand(command);
        if (ret == -1) {
            if (ret == 1) {
                PRINTF(global_client_socket, "No running Pig processes found\n");
            } else {
                FPRINTF(global_client_socket,  "Failed to stop Pig. Error: %d\n", ret);
                exit(EXIT_FAILURE);
            }
        }
        break;
    }

    case RESTART:
        // PRINTF(global_client_socket, "Restarting Pig...\n");
        pig_action(STOP);
        sleep(2);  // Allow processes to terminate
        pig_action(START);
        break;

    default:
        FPRINTF(global_client_socket,  "Invalid action. Use START, STOP, or RESTART\n");
        exit(EXIT_FAILURE);
        PRINTF(global_client_socket, "Pig Service action performed successfully\n");

    }
}

void HBase_action(Action a) {
    char hbase_home[256];
    int os_found = 0;

    // Determine installation directory
    if (access("/etc/debian_version", F_OK) == 0) {
        strncpy(hbase_home, "/usr/local/hbase", sizeof(hbase_home));
        os_found = 1;
    } else if (access("/etc/redhat-release", F_OK) == 0) {
        strncpy(hbase_home, "/opt/hbase", sizeof(hbase_home));
        os_found = 1;
    }

    if (!os_found) {
        FPRINTF(global_client_socket,  "Error: Unsupported OS\n");
        exit(EXIT_FAILURE);
    }

    // Verify HBase installation directory
    if (access(hbase_home, F_OK) != 0) {
        FPRINTF(global_client_socket,  "Error: HBase not found at %s\n", hbase_home);
        exit(EXIT_FAILURE);
    }

    // Verify critical configuration directory
    char conf_dir[512];
    snprintf(conf_dir, sizeof(conf_dir), "%s/conf", hbase_home);
    if (access(conf_dir, F_OK) != 0) {
        FPRINTF(global_client_socket,  "Missing configuration directory: %s\n", conf_dir);
        exit(EXIT_FAILURE);
    }

    // Verify regionservers file exists
    char regionservers_path[512];
    snprintf(regionservers_path, sizeof(regionservers_path), "%s/conf/regionservers", hbase_home);
    if (access(regionservers_path, F_OK) != 0) {
        FPRINTF(global_client_socket,  "Missing regionservers file at %s\n", regionservers_path);
        exit(EXIT_FAILURE);
    }

    // Construct script paths
    char start_cmd[512], stop_cmd[512];
    snprintf(start_cmd, sizeof(start_cmd), "%s/bin/start-hbase.sh", hbase_home);
    snprintf(stop_cmd, sizeof(stop_cmd), "%s/bin/stop-hbase.sh", hbase_home);

    switch(a) {
    case START:
        if (!executeSystemCommand(start_cmd)) {
            FPRINTF(global_client_socket,  "Failed to start HBase\n");
            exit(EXIT_FAILURE);
        }
        FPRINTF(global_client_socket,  "Hbase start successfully \n");
        break;
    case STOP:
        if (!executeSystemCommand(stop_cmd)) {
            FPRINTF(global_client_socket,  "Failed to stop HBase\n");
            exit(EXIT_FAILURE);
        }
        FPRINTF(global_client_socket,  "Hbase stop successfully \n");
        break;
    case RESTART:
        if (!executeSystemCommand(stop_cmd) || !executeSystemCommand(start_cmd)) {
            FPRINTF(global_client_socket,  "Restart failed\n");
            exit(EXIT_FAILURE);
        }
        FPRINTF(global_client_socket,  "Hbase restart successfully \n");
        break;
    default:
        FPRINTF(global_client_socket,  "Invalid action\n");
        exit(EXIT_FAILURE);
    }

    // Verify service state
    char verify_cmd[512];
    snprintf(verify_cmd, sizeof(verify_cmd),
             "sh -c 'jps | grep HMaster >/dev/null && jps | grep HRegionServer >/dev/null'");

    if (a == START || a == RESTART) {
        if (!executeSystemCommand(verify_cmd)) {
            FPRINTF(global_client_socket,  "Service verification failed after %s\n",
                    (a == RESTART) ? "restart" : "start");
            exit(EXIT_FAILURE);
        }
    } else if (a == STOP) {
        if (executeSystemCommand(verify_cmd)) {
            FPRINTF(global_client_socket,  "HBase processes still running after stop\n");
            exit(EXIT_FAILURE);
        }
    }
}

#define true 1
#define false 0


const char *get_tez_home() {
    struct stat st;
    const char *tez_home = getenv("TEZ_HOME");
    const char *paths[] = {"/opt/tez", "/usr/local/tez"};

    // Check TEZ_HOME first if set
    if (tez_home != NULL) {
        if (stat(tez_home, &st) == 0 && S_ISDIR(st.st_mode)) {
            return tez_home;
        }
        FPRINTF(global_client_socket,  "TEZ_HOME set to invalid directory: %s\n", tez_home);
    }

    // Check standard installation paths
    for (size_t i = 0; i < sizeof(paths)/sizeof(paths[0]); i++) {
        if (stat(paths[i], &st) == 0 && S_ISDIR(st.st_mode)) {
            return paths[i];
        }
    }

    return NULL;
}

bool verify_tez_installation(const char *tez_home) {
    char script_path[PATH_MAX];
    struct stat st;

    if (snprintf(script_path, sizeof(script_path), "%s/bin/tez-daemon.sh", tez_home) >= (int)sizeof(script_path)) {
        FPRINTF(global_client_socket,  "Path to tez-daemon.sh exceeds maximum length\n");
        return false;
    }

    if (stat(script_path, &st) != 0 || !S_ISREG(st.st_mode)) {
        FPRINTF(global_client_socket,  "tez-daemon.sh not found at: %s\n", script_path);
        return false;
    }

    if (access(script_path, X_OK) != 0) {
        FPRINTF(global_client_socket,  "tez-daemon.sh is not executable at: %s\n", script_path);
        return false;
    }

    return true;
}

bool execute_tez_command(const char *operation) {
    const char *tez_home = get_tez_home();
    if (!tez_home) {
        FPRINTF(global_client_socket,  "Tez installation not found. Checked:\n"
                "- TEZ_HOME environment variable\n"
                "- /opt/tez (Red Hat)\n"
                "- /usr/local/tez (Debian)\n");
        return false;
    }

    if (!verify_tez_installation(tez_home)) {
        return false;
    }

    // Build command safely with dynamic allocation
    char *command = NULL;
    int required_len = snprintf(NULL, 0, "%s/bin/tez-daemon.sh %s historyserver", tez_home, operation);
    if (required_len < 0) {
        FPRINTF(global_client_socket,  "Error formatting command string\n");
        return false;
    }

    command = malloc(required_len + 1);
    if (!command) {
        FPRINTF(global_client_socket,  "Memory allocation failed for command\n");
        return false;
    }

    snprintf(command, required_len + 1, "%s/bin/tez-daemon.sh %s historyserver", tez_home, operation);

    int status = executeSystemCommand(command);
    free(command);

    if (WIFEXITED(status)) {
        int exit_status = WEXITSTATUS(status);
        if (exit_status != 0) {
            FPRINTF(global_client_socket,  "Command failed with exit code: %d\n", exit_status);
            return false;
        }
        return true;
    }

    FPRINTF(global_client_socket,  "Command terminated abnormally\n");
    return false;
}

void tez_action(Action a) {
    bool success = false;

    switch(a) {
    case START: {
        //  PRINTF(global_client_socket, "Initializing Tez service startup...\n");
        success = execute_tez_command("start");
        break;
    }
    case STOP: {
        //PRINTF(global_client_socket, "Initiating Tez service shutdown...\n");
        success = execute_tez_command("stop");
        break;
    }
    case RESTART: {
        // PRINTF(global_client_socket, "Beginning Tez service restart...\n");
        bool stop_success = execute_tez_command("stop");
        if (!stop_success) {
            FPRINTF(global_client_socket,  "Warning: Tez service stop encountered issues\n");
        }

        // Add brief delay to allow service shutdown
        sleep(2);

        bool start_success = execute_tez_command("start");
        success = start_success;
        break;
    }
    default: {
        FPRINTF(global_client_socket,  "Invalid action requested: %d\n", a);
        return;
    }
    }

    if (success) {
        PRINTF(global_client_socket, "Operation completed successfully\n");
    } else {
        FPRINTF(global_client_socket,  "Operation failed to complete\n");
    }
}

void kafka_action(Action action) {
    char kafka_home[PATH_MAX] = {0};
    char command[2048] = {0};
    int result;
    struct stat st;

    // Determine Kafka installation directory
    char* env_kafka = getenv("KAFKA_HOME");
    if (env_kafka) {
        strncpy(kafka_home, env_kafka, sizeof(kafka_home)-1);
        kafka_home[sizeof(kafka_home)-1] = '\0';
    } else {
        if (access("/etc/debian_version", F_OK) == 0) {
            strncpy(kafka_home, "/usr/local/kafka", sizeof(kafka_home)-1);
        } else if (access("/etc/redhat-release", F_OK) == 0) {
            strncpy(kafka_home, "/opt/kafka", sizeof(kafka_home)-1);
        } else {
            FPRINTF(global_client_socket,  "Unable to determine Kafka home\n");
            exit(EXIT_FAILURE);
        }
        kafka_home[sizeof(kafka_home)-1] = '\0';
    }

    // Validate Kafka installation
    if (stat(kafka_home, &st) == -1 || !S_ISDIR(st.st_mode)) {
        FPRINTF(global_client_socket,  "Kafka not installed at %s\n", kafka_home);
        exit(EXIT_FAILURE);
    }

    // Construct script paths
    char kafka_start[PATH_MAX], kafka_stop[PATH_MAX], kafka_config[PATH_MAX];
    char zk_start[PATH_MAX], zk_stop[PATH_MAX], zk_config[PATH_MAX];

    snprintf(kafka_start, sizeof(kafka_start), "%s/bin/kafka-server-start.sh", kafka_home);
    snprintf(kafka_stop, sizeof(kafka_stop), "%s/bin/kafka-server-stop.sh", kafka_home);
    snprintf(kafka_config, sizeof(kafka_config), "%s/config/server.properties", kafka_home);
    snprintf(zk_start, sizeof(zk_start), "%s/bin/zookeeper-server-start.sh", kafka_home);
    snprintf(zk_stop, sizeof(zk_stop), "%s/bin/zookeeper-server-stop.sh", kafka_home);
    snprintf(zk_config, sizeof(zk_config), "%s/config/zookeeper.properties", kafka_home);

    // Verify required files
    int missing = 0;
    if (access(zk_start, X_OK) != 0) { FPRINTF(global_client_socket,  "Missing Zookeeper start script\n"); missing = 1; }
    if (access(zk_stop, X_OK) != 0) { FPRINTF(global_client_socket,  "Missing Zookeeper stop script\n"); missing = 1; }
    if (access(kafka_start, X_OK) != 0) { FPRINTF(global_client_socket,  "Missing Kafka start script\n"); missing = 1; }
    if (access(kafka_stop, X_OK) != 0) { FPRINTF(global_client_socket,  "Missing Kafka stop script\n"); missing = 1; }
    if (missing) exit(EXIT_FAILURE);

    switch(action) {
    case START: {
        size_t ret = snprintf(command, sizeof(command),
                              "%s %s >/dev/null 2>&1 & echo $! > %s/zookeeper.pid; "
                              "%s %s >/dev/null 2>&1 & echo $! > %s/kafka.pid",
                              zk_start, zk_config, kafka_home,
                              kafka_start, kafka_config, kafka_home
                             );
        if (ret >= sizeof(command)) {
            FPRINTF(global_client_socket,  "Command buffer overflow\n");
            exit(EXIT_FAILURE);
        }
        break;
    }

    case STOP: {
        size_t ret = snprintf(command, sizeof(command),
                              "%s; %s; "
                              "rm -f %s/zookeeper.pid %s/kafka.pid 2>/dev/null",
                              kafka_stop, zk_stop, kafka_home, kafka_home
                             );
        if (ret >= sizeof(command)) {
            FPRINTF(global_client_socket,  "Command buffer overflow\n");
            exit(EXIT_FAILURE);
        }
        break;
    }

    case RESTART:
        kafka_action(STOP);
        sleep(3);
        kafka_action(START);
        return;

    default:
        FPRINTF(global_client_socket,  "Invalid action\n");
        exit(EXIT_FAILURE);
    }

    // Execute command
    if ((result = executeSystemCommand(command)) == -1) {
        FPRINTF(global_client_socket,  "Action failed with code %d\n", result);
        exit(EXIT_FAILURE);
    }

    // Verification command buffers
    char verify_cmd[512];

    // Verify execution
    if (action == START) {
        sleep(2);
        // Check Zookeeper
        snprintf(verify_cmd, sizeof(verify_cmd),
                 "pgrep -F '%s/zookeeper.pid' >/dev/null", kafka_home);
        int zk_up = executeSystemCommand(verify_cmd);
        // Check Kafka
        snprintf(verify_cmd, sizeof(verify_cmd),
                 "pgrep -F '%s/kafka.pid' >/dev/null", kafka_home);
        int kafka_up = executeSystemCommand(verify_cmd);

        if (zk_up == -1 || kafka_up == -1) {
            FPRINTF(global_client_socket,  "Startup verification failed\n");
            exit(EXIT_FAILURE);
        }
    } else if (action == STOP) {
        // Check Zookeeper
        snprintf(verify_cmd, sizeof(verify_cmd),
                 "pgrep -F '%s/zookeeper.pid' >/dev/null", kafka_home);
        int zk_down = executeSystemCommand(verify_cmd);
        // Check Kafka
        snprintf(verify_cmd, sizeof(verify_cmd),
                 "pgrep -F '%s/kafka.pid' >/dev/null", kafka_home);
        int kafka_down = executeSystemCommand(verify_cmd);

        if (zk_down == 0 || kafka_down == 0) {
            FPRINTF(global_client_socket,  "Shutdown verification failed\n");
            exit(EXIT_FAILURE);
        }
    }
    PRINTF(global_client_socket, "Service operations completed successfully.\n");
}
void Solr_action(Action a) {
    const char *install_dir = NULL;
    char solr_script[512];
    int status;

    // Determine installation directory
    if (access("/etc/debian_version", F_OK) == 0) {
        install_dir = "/usr/local/solr";
    } else if (access("/etc/redhat-release", F_OK) == 0) {
        install_dir = "/opt/solr";
    } else {
        FPRINTF(global_client_socket,  "Error: Unsupported Linux distribution\n");
        return;
    }

    // Verify Solr installation
    snprintf(solr_script, sizeof(solr_script), "%s/bin/solr", install_dir);
    if (access(solr_script, X_OK) != 0) {
        FPRINTF(global_client_socket,  "Error: Solr not found at %s\n", solr_script);
        FPRINTF(global_client_socket,  "Run install_Solr first or check permissions\n");
        return;
    }

    // Execute requested action
    switch(a) {
    case START: {
        //PRINTF(global_client_socket, "Starting Solr service...\n");
        pid_t pid = fork();
        if (pid == 0) {
            execl(solr_script, solr_script, "start", (char *)NULL);
            _exit(EXIT_FAILURE);
        } else if (pid > 0) {
            waitpid(pid, &status, 0);
            if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
                PRINTF(global_client_socket, "Solr started successfully\n");
            } else {
                FPRINTF(global_client_socket,  "Failed to start Solr (exit code: %d)\n", WEXITSTATUS(status));
            }
        }
        break;
    }

    case STOP: {
        // PRINTF(global_client_socket, "Stopping Solr service...\n");
        pid_t pid = fork();
        if (pid == 0) {
            execl(solr_script, solr_script, "stop", (char *)NULL);
            _exit(EXIT_FAILURE);
        } else if (pid > 0) {
            waitpid(pid, &status, 0);
            if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
                PRINTF(global_client_socket, "Solr stopped successfully\n");
            } else {
                FPRINTF(global_client_socket,  "Failed to stop Solr (exit code: %d)\n", WEXITSTATUS(status));
            }
        }
        break;
    }

    case RESTART: {
        //   PRINTF(global_client_socket, "Restarting Solr service...\n");
        pid_t pid = fork();
        if (pid == 0) {
            execl(solr_script, solr_script, "restart", (char *)NULL);
            _exit(EXIT_FAILURE);
        } else if (pid > 0) {
            waitpid(pid, &status, 0);
            if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
                PRINTF(global_client_socket, "Solr restarted successfully\n");
            } else {
                FPRINTF(global_client_socket,  "Failed to restart Solr (exit code: %d)\n", WEXITSTATUS(status));
                // Fallback to stop/start sequence
                //PRINTF(global_client_socket, "Attempting stop/start sequence...\n");
                Solr_action(STOP);
                Solr_action(START);
            }
        }
        break;
    }

    default:
        FPRINTF(global_client_socket,  "Error: Invalid action specified\n");
        break;
    }
}

#define MAX_CMD_LEN 512

static int execute_script_action(const char *script_path, const char *action) {
    char command[MAX_CMD_LEN];
    int ret = snprintf(command, sizeof(command), "%s %s", script_path, action);

    if (ret < 0 || ret >= (int)sizeof(command)) {
        FPRINTF(global_client_socket,  "Command buffer overflow for action: %s\n", action);
        return -1;
    }

    int exit_code = executeSystemCommand(command);
    if (exit_code != 0) {
        FPRINTF(global_client_socket,  "Action '%s' failed with exit code: %d\n", action, exit_code);
    }
    return exit_code;
}

void phoenix_action(Action a) {
    const char *search_paths[] = {
        getenv("PHOENIX_HOME"),  // Primary location
        "/opt/phoenix",          // Red Hat fallback
        "/usr/local/phoenix"     // Debian fallback
    };

    char script_path[MAX_PATH_LEN];
    int found = 0;

    // Search for valid Phoenix installation
    for (size_t i = 0; i < sizeof(search_paths)/sizeof(search_paths[0]); i++) {
        const char *base_path = search_paths[i];
        if (!base_path) continue;

        int path_len = snprintf(script_path, sizeof(script_path),
                                "%s/bin/phoenix-service.sh", base_path);

        if (path_len < 0 || path_len >= (int)sizeof(script_path)) {
            continue;  // Invalid path or buffer overflow
        }

        if (access(script_path, X_OK) == 0) {
            found = 1;
            break;
        }
    }

    if (!found) {
        FPRINTF(global_client_socket,  "Phoenix installation not found. Checked:\n"
                "1. PHOENIX_HOME environment variable\n"
                "2. /opt/phoenix\n"
                "3. /usr/local/phoenix\n");
        exit(EXIT_FAILURE);
    }

    switch (a) {
    case START: {
        if (execute_script_action(script_path, "start") != 0) {
            exit(EXIT_FAILURE);
        }
        PRINTF(global_client_socket, "Phoenix service started successfully.\n");
        break;
    }

    case STOP: {
        if (execute_script_action(script_path, "stop") != 0) {
            exit(EXIT_FAILURE);
        }
        PRINTF(global_client_socket, "Phoenix service stopped successfully.\n");
        break;
    }

    case RESTART: {
        // Try to stop, but continue even if it fails
        int stop_status = execute_script_action(script_path, "stop");
        if (stop_status == 0) {
            //  PRINTF(global_client_socket, "Phoenix service stopped successfully.\n");
        } else {
            // PRINTF(global_client_socket, "Attempting restart despite stop failure (exit code: %d)\n", stop_status);
        }

        if (execute_script_action(script_path, "start") != 0) {
            exit(EXIT_FAILURE);
        }
        PRINTF(global_client_socket, "Phoenix service started successfully.\n");
        break;
    }

    default: {
        FPRINTF(global_client_socket,  "Invalid action. Valid options: START, STOP, RESTART\n");
        exit(EXIT_FAILURE);
    }
    }
}

void ranger_action(Action a) {
    const char *ranger_home = NULL;
    const char *env_home = getenv("RANGER_HOME");
    const char *candidates[] = {env_home, "/opt/ranger", "/usr/local/ranger", NULL};

    // Search for valid Ranger installation by checking embeddedwebserver/scripts directory
    for (int i = 0; candidates[i] != NULL; i++) {
        const char *candidate = candidates[i];
        if (candidate == NULL) continue;

        char scripts_dir[1024];
        int len = snprintf(scripts_dir, sizeof(scripts_dir), "%s/embeddedwebserver/scripts", candidate);
        if (len < 0 || len >= (int)sizeof(scripts_dir)) continue;

        struct stat st;
        if (stat(scripts_dir, &st) == 0 && S_ISDIR(st.st_mode)) {
            ranger_home = candidate;
            break;
        }
    }

    if (ranger_home == NULL) {
        FPRINTF(global_client_socket,  "Ranger installation not found. Checked locations:\n");
        FPRINTF(global_client_socket,  "1. RANGER_HOME: %s\n", env_home ? env_home : "(not set)");
        FPRINTF(global_client_socket,  "2. /opt/ranger\n3. /usr/local/ranger\n");
        exit(EXIT_FAILURE);
    }

    int execute_script(const char *script_name) {
        char script_path[1024];
        int len = snprintf(script_path, sizeof(script_path), "%s/embeddedwebserver/scripts/%s", ranger_home, script_name);
        if (len < 0 || len >= (int)sizeof(script_path)) {
            FPRINTF(global_client_socket,  "Path construction error\n");
            return -1;
        }

        if (access(script_path, X_OK) != 0) {
            FPRINTF(global_client_socket,  "Script %s not found or not executable\n", script_name);
            return -1;
        }

        char command[2048];
        len = snprintf(command, sizeof(command), "%s", script_path);
        if (len < 0 || len >= (int)sizeof(command)) {
            FPRINTF(global_client_socket,  "Command too long\n");
            return -1;
        }

        int status = executeSystemCommand(command);
        if (status == -1) {
            FPRINTF(global_client_socket,  "Command execution failed: %s\n", strerror(errno));
            return -1;
        }
        return status;
    }

    void handle_command_result(int status, const char *action, const char *context) {
        if (status == -1) exit(EXIT_FAILURE);

        if (WIFEXITED(status)) {
            int exit_code = WEXITSTATUS(status);
            if (exit_code != 0) {
                FPRINTF(global_client_socket,  "Failed to %s Ranger service%s. Exit code: %d\n",
                        action, context, exit_code);
                exit(EXIT_FAILURE);
            }
        } else {
            FPRINTF(global_client_socket,  "Service %s terminated abnormally%s (Signal: %d)\n",
                    action, context, WTERMSIG(status));
            exit(EXIT_FAILURE);
        }
    }

    switch (a) {
    case START: {
        int status = execute_script("ranger-admin-services.sh");
        handle_command_result(status, "start", "");
        break;
    }
    case STOP: {
        int status = execute_script("ranger-admin-services.sh");
        handle_command_result(status, "stop", "");
        break;
    }
    case RESTART: {
        int stop_status = execute_script("ranger-admin-services.sh");
        handle_command_result(stop_status, "stop", " during restart");

        int start_status = execute_script("ranger-admin-services.sh");
        handle_command_result(start_status, "start", " during restart");
        break;
    }
    default:
        FPRINTF(global_client_socket,  "Invalid action specified\n");
        exit(EXIT_FAILURE);
    }

    PRINTF(global_client_socket, "Ranger service action completed successfully.\n");
}

char *get_atlas_home() {
    // Check ATLAS_HOME environment variable first
    const char *env_path = getenv("ATLAS_HOME");
    if (env_path) {
        char start_script[PATH_MAX], stop_script[PATH_MAX];
        snprintf(start_script, sizeof(start_script), "%s/bin/atlas_start.py", env_path);
        snprintf(stop_script, sizeof(stop_script), "%s/bin/atlas_stop.py", env_path);
        if (access(start_script, X_OK) == 0 && access(stop_script, X_OK) == 0) {
            return strdup(env_path);
        }
    }

    // Check standard base directories and patterns
    const char *base_dirs[] = {"/opt", "/usr/local", NULL};
    const char *dir_patterns[] = {"atlas", "atlas-*", NULL}; // Check both unversioned and versioned

    glob_t globbuf;
    for (int i = 0; base_dirs[i] != NULL; i++) {
        const char *base = base_dirs[i];
        for (int j = 0; dir_patterns[j] != NULL; j++) {
            char pattern[PATH_MAX];
            snprintf(pattern, sizeof(pattern), "%s/%s", base, dir_patterns[j]);
            if (glob(pattern, GLOB_NOSORT, NULL, &globbuf) != 0) {
                continue; // No matches, skip
            }

            for (size_t k = 0; k < globbuf.gl_pathc; k++) {
                const char *candidate = globbuf.gl_pathv[k];
                // Verify it's a directory
                struct stat statbuf;
                if (stat(candidate, &statbuf) != 0 || !S_ISDIR(statbuf.st_mode)) {
                    continue;
                }

                char start_script[PATH_MAX], stop_script[PATH_MAX];
                snprintf(start_script, sizeof(start_script), "%s/bin/atlas_start.py", candidate);
                snprintf(stop_script, sizeof(stop_script), "%s/bin/atlas_stop.py", candidate);

                if (access(start_script, X_OK) == 0 && access(stop_script, X_OK) == 0) {
                    char *result = strdup(candidate);
                    globfree(&globbuf);
                    return result;
                }
            }
            globfree(&globbuf);
        }
    }

    return NULL; // No valid installation found
}

int execute_script(const char *script_path) {
    pid_t pid = fork();
    if (pid == -1) {
        PERROR(global_client_socket, "fork");
        return -1;
    } else if (pid == 0) {
        execlp(script_path, script_path, (char *)NULL);
        PERROR(global_client_socket, "execlp");
        exit(EXIT_FAILURE);
    }

    int status;
    waitpid(pid, &status, 0);
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

int atlas_action(Action a) {
    //if (geteuid() != 0) {
    //  FPRINTF(global_client_socket,  "Error: Requires root privileges. Use sudo.\n");
    //return -1;
    //}

    char *atlas_home = get_atlas_home();
    if (!atlas_home) {
        FPRINTF(global_client_socket,  "Error: Atlas installation not found.\n");
        return -1;
    }

    char script_path[PATH_MAX];
    int result = -1;

    switch(a) {
    case START:
        snprintf(script_path, sizeof(script_path), "%s/bin/atlas_start.py", atlas_home);
        // PRINTF(global_client_socket, "Starting Apache Atlas...\n");
        result = execute_script(script_path);
        break;

    case STOP:
        snprintf(script_path, sizeof(script_path), "%s/bin/atlas_stop.py", atlas_home);
        // PRINTF(global_client_socket, "Stopping Apache Atlas...\n");
        result = execute_script(script_path);
        break;

    case RESTART:
        // PRINTF(global_client_socket, "Restarting Apache Atlas...\n");
        if ((result = atlas_action(STOP)) == 0) {
            result = atlas_action(START);
        }
        break;

    default:
        FPRINTF(global_client_socket,  "Error: Invalid action specified.\n");
    }

    free(atlas_home);

    if (result != 0) {
        FPRINTF(global_client_socket,  "Error: Action failed. Check installation and permissions.\n");
    }
    return result;
}

// Helper to iterate through services (C11 compatible)
//static const char* services[] = {"nimbus", "supervisor", "ui"};
//#define for_each_service(i) for (size_t i = 0; i < sizeof(services)/sizeof(services[0]); i++)

static int stop_services(const char *storm_cmd, const char **services, size_t num_services) {
    int all_stopped = 1;
    for (size_t i = 0; i < num_services; i++) {
        char cmd[PATH_MAX * 2];
        int ret = snprintf(cmd, sizeof(cmd), "pkill -f '%s %s'", storm_cmd, services[i]);
        if (ret >= (int)sizeof(cmd)) {
            FPRINTF(global_client_socket,  "Command buffer overflow for service %s\n", services[i]);
            all_stopped = 0;
            continue;
        }

        int status = executeSystemCommand(cmd);
        if (WIFEXITED(status)) {
            int exit_code = WEXITSTATUS(status);
            if (exit_code == 0) {
                PRINTF(global_client_socket, "Successfully stopped %s\n", services[i]);
            } else if (exit_code == 1) {
                PRINTF(global_client_socket, "%s was not running\n", services[i]);
            } else {
                FPRINTF(global_client_socket,  "Error stopping %s (code %d)\n", services[i], exit_code);
                all_stopped = 0;
            }
        } else {
            FPRINTF(global_client_socket,  "Process termination error for %s\n", services[i]);
            all_stopped = 0;
        }
    }
    return all_stopped;
}

static int start_services(const char *storm_cmd, const char **services, size_t num_services) {
    int all_started = 1;
    for (size_t i = 0; i < num_services; i++) {
        char cmd[PATH_MAX * 2];
        // Include stderr redirection to capture all output
        int ret = snprintf(cmd, sizeof(cmd), "%s %s 2>&1", storm_cmd, services[i]);
        if (ret >= (int)sizeof(cmd)) {
            FPRINTF(global_client_socket, "Command buffer overflow for service %s\n", services[i]);
            all_started = 0;
            continue;
        }

        FILE *fp = popen(cmd, "r");
        if (!fp) {
            FPRINTF(global_client_socket, "Failed to run command for service %s\n", services[i]);
            all_started = 0;
            continue;
        }

        char buffer[256];
        int found_python_error = 0;

        // Read and process command output line by line
        while (fgets(buffer, sizeof(buffer), fp) != NULL) {
            // Display command output to user
            PRINTF(global_client_socket, "%s", buffer);

            // Check for Python version requirement message
            if (strstr(buffer, "Need python version > 2.6") != NULL) {
                found_python_error = 1;
            }
        }

        int status = pclose(fp);
        if (WIFEXITED(status)) {
            int exit_code = WEXITSTATUS(status);
            if (exit_code == 0) {
                if (found_python_error) {
                    FPRINTF(global_client_socket, "Failed to start %s: Python version > 2.6 is required.\n", services[i]);
                    all_started = 0;
                } else {
                    PRINTF(global_client_socket, "Successfully started %s\n", services[i]);
                }
            } else {
                FPRINTF(global_client_socket, "Failed to start %s (exit code %d)\n", services[i], exit_code);
                all_started = 0;
            }
        } else {
            FPRINTF(global_client_socket, "Process termination error for %s\n", services[i]);
            all_started = 0;
        }
    }
    return all_started;
}
void storm_action(Action a) {
    const char *services[] = {"nimbus", "supervisor", "ui"};
    size_t num_services = sizeof(services) / sizeof(services[0]);
    char *storm_home = NULL;
    char storm_cmd[PATH_MAX];

    // 1. Check STORM_HOME environment variable
    char *env_storm_home = getenv("STORM_HOME");
    if (env_storm_home) {
        snprintf(storm_cmd, sizeof(storm_cmd), "%s/bin/storm", env_storm_home);
        if (access(storm_cmd, X_OK) == 0) {
            storm_home = env_storm_home;
        } else {
            FPRINTF(global_client_socket,  "STORM_HOME set to invalid path: %s\n", env_storm_home);
        }
    }

    // 2. OS-detection based fallback (aligned with install_Storm logic)
    if (!storm_home) {
        const char *debian_path = "/usr/local/storm";
        const char *redhat_path = "/opt/storm";
        int is_debian = (access("/etc/debian_version", F_OK) == 0);
        int is_redhat = (access("/etc/redhat-release", F_OK) == 0);

        const char *paths[2];
        size_t num_paths = 0;

        if (is_debian) {
            paths[num_paths++] = debian_path;
            paths[num_paths++] = redhat_path;
        } else if (is_redhat) {
            paths[num_paths++] = redhat_path;
            paths[num_paths++] = debian_path;
        } else {
            // Fallback for unknown distributions
            paths[num_paths++] = redhat_path;
            paths[num_paths++] = debian_path;
        }

        for (size_t i = 0; i < num_paths; i++) {
            snprintf(storm_cmd, sizeof(storm_cmd), "%s/bin/storm", paths[i]);
            if (access(storm_cmd, X_OK) == 0) {
                storm_home = (char*)paths[i];
                break;
            }
        }
    }

    // 3. Final validation
    if (!storm_home) {
        FPRINTF(global_client_socket,
                "Storm installation not found. Checked:\n"
                "- STORM_HOME environment variable\n"
                "- OS-specific default locations\n");
        return;
    }

    // Reconstruct storm_cmd for consistency
    snprintf(storm_cmd, sizeof(storm_cmd), "%s/bin/storm", storm_home);
    if (access(storm_cmd, X_OK) != 0) {
        FPRINTF(global_client_socket,  "Storm executable not accessible: %s\n", storm_cmd);
        return;
    }

    // Rest of the original storm_action implementation remains the same
    switch(a) {
    case START: {
        int success = start_services(storm_cmd, services, num_services);
        PRINTF(global_client_socket, success ? "All services started successfully\n"
               : "Some services failed to start\n");
        break;
    }

    case STOP: {
        int success = stop_services(storm_cmd, services, num_services);
        PRINTF(global_client_socket, success ? "All services stopped successfully\n"
               : "Some services failed to stop\n");
        break;
    }

    case RESTART: {
        int stop_success = stop_services(storm_cmd, services, num_services);
        PRINTF(global_client_socket, stop_success ? "All services stopped\n" : "Stop phase completed with errors\n");

        sleep(2);

        int start_success = start_services(storm_cmd, services, num_services);
        PRINTF(global_client_socket, start_success ? "All services restarted successfully\n"
               : "Restart completed with errors\n");
        break;
    }

    default:
        FPRINTF(global_client_socket,  "Invalid action specified\n");
    }
}

void flink_action(Action action) {
    const char* flink_home = getenv("FLINK_HOME");
    struct stat dir_info;
    int ret;

    // Determine FLINK_HOME if not set in environment
    if (!flink_home) {
        if (stat("/etc/debian_version", &dir_info) == 0) {
            flink_home = "/usr/local/flink";
        } else if (stat("/etc/redhat-release", &dir_info) == 0) {
            flink_home = "/opt/flink";
        } else {
            FPRINTF(global_client_socket,  "FLINK_HOME not set and OS detection failed\n");
            exit(EXIT_FAILURE);
        }
    }

    // Verify Flink installation directory
    if (stat(flink_home, &dir_info) != 0 || !S_ISDIR(dir_info.st_mode)) {
        FPRINTF(global_client_socket,  "Flink directory not found: %s\n", flink_home);
        exit(EXIT_FAILURE);
    }

    // Validate cluster scripts
    char start_script[PATH_MAX];
    char stop_script[PATH_MAX];
    snprintf(start_script, sizeof(start_script), "%s/bin/start-cluster.sh", flink_home);
    snprintf(stop_script, sizeof(stop_script), "%s/bin/stop-cluster.sh", flink_home);

    if (access(start_script, X_OK) != 0 || access(stop_script, X_OK) != 0) {
        FPRINTF(global_client_socket,  "Missing or inaccessible Flink cluster scripts\n");
        exit(EXIT_FAILURE);
    }

    // Execute requested action
    switch(action) {
    case START:
        ret = executeSystemCommand(start_script);
        if (WEXITSTATUS(ret) == -1) {
            FPRINTF(global_client_socket,  "Start failed with exit code %d\n", WEXITSTATUS(ret));
            exit(EXIT_FAILURE);
        }
        break;

    case STOP: {
        ret = executeSystemCommand(stop_script);
        if (WEXITSTATUS(ret) == -1) {
            FPRINTF(global_client_socket,  "Stop failed with exit code %d\n", WEXITSTATUS(ret));
            exit(EXIT_FAILURE);
        }
        break;
    }

    case RESTART: {
        //PRINTF(global_client_socket, "Initiating Flink restart...\n");

        // Stop phase
        ret = executeSystemCommand(stop_script);
        if (WEXITSTATUS(ret) == -1) {
            FPRINTF(global_client_socket,  "Restart aborted - stop failed with code %d\n", WEXITSTATUS(ret));
            exit(EXIT_FAILURE);
        }

        // Start phase
        ret = executeSystemCommand(start_script);
        if (WEXITSTATUS(ret) == -1) {
            FPRINTF(global_client_socket,  "Restart incomplete - start failed with code %d\n", WEXITSTATUS(ret));
            exit(EXIT_FAILURE);
        }
        break;
    }

    default:
        FPRINTF(global_client_socket,  "Invalid action specified\n");
        exit(EXIT_FAILURE);
    }

    PRINTF(global_client_socket, "Operation completed successfully\n");
}


void zookeeper_action(Action a) {
    char *zk_home = getenv("ZOOKEEPER_HOME");
    int isDebian, isRedHat;
    char zk_script[PATH_MAX];
    char command[1024];
    int ret;

    // Determine ZOOKEEPER_HOME if not set
    if (zk_home == NULL) {
        isDebian = (access("/etc/debian_version", F_OK) == 0);
        isRedHat = (access("/etc/redhat-release", F_OK) == 0) || (access("/etc/redhat_version", F_OK) == 0);

        if (isDebian) {
            zk_home = "/usr/local/zookeeper";
        } else if (isRedHat) {
            zk_home = "/opt/zookeeper";
        } else {
            FPRINTF(global_client_socket,  "Error: Unable to detect OS and ZOOKEEPER_HOME is not set.\n");
            return;
        }

        // Verify the determined Zookeeper home directory exists
        if (access(zk_home, F_OK) != 0) {
            FPRINTF(global_client_socket,  "Error: Zookeeper directory not found at %s\n", zk_home);
            return;
        }
    }

    // Build the path to zkServer.sh
    long unsigned int ret2 = snprintf(zk_script, sizeof(zk_script), "%s/bin/zkServer.sh", zk_home);
    if (ret2 >= sizeof(zk_script)) {
        FPRINTF(global_client_socket,  "Error: command buffer overflow.\n");
    }

    // Check if the script exists and is executable
    if (access(zk_script, X_OK) != 0) {
        FPRINTF(global_client_socket,  "Error: zkServer.sh not found or not executable at %s\n", zk_script);
        return;
    }

    // Execute the appropriate action
    switch(a) {
    case START: {
        long unsigned int ret3 = snprintf(command, sizeof(command), "\"%s\" start", zk_script);
        if (ret3 >= sizeof(command)) {
            FPRINTF(global_client_socket,  "Error: command buffer overflow.\n");
        }
        ret = executeSystemCommand(command);
        if (ret == -1) {
            FPRINTF(global_client_socket,  "Failed to start Zookeeper. Exit code: %d\n", ret);
        }
        PRINTF(global_client_socket, "Zookeeper started successfully\n");
        break;
    }
    case STOP: {
        long unsigned int ret4 = snprintf(command, sizeof(command), "\"%s\" stop", zk_script);
        if (ret4 >= sizeof(command)) {
            FPRINTF(global_client_socket,  "Error: command buffer overflow.\n");
        }
        ret = executeSystemCommand(command);
        if (ret == -1) {
            FPRINTF(global_client_socket,  "Failed to stop Zookeeper. Exit code: %d\n", ret);
        }
        PRINTF(global_client_socket, "Zookeeper stopped  successfully\n");
        break;
    }
    case RESTART: {
        // Stop the service
        long unsigned int ret5 = snprintf(command, sizeof(command), "\"%s\" stop", zk_script);
        if (ret5 >= sizeof(command)) {
            FPRINTF(global_client_socket,  "Error: command buffer overflow.\n");
        }
        ret = executeSystemCommand(command);
        if (ret == -1) {
            FPRINTF(global_client_socket,  "Failed to stop Zookeeper during restart. Exit code: %d\n", ret);
        }
        // Start the service
        long unsigned int ret9 = snprintf(command, sizeof(command), "\"%s\" start", zk_script);
        if (ret9 >= sizeof(command)) {
            FPRINTF(global_client_socket,  "Error: command buffer overflow.\n");
        }
        ret = executeSystemCommand(command);
        if (ret == -1) {
            FPRINTF(global_client_socket,  "Failed to start Zookeeper during restart. Exit code: %d\n", ret);
        }
        PRINTF(global_client_socket, "Zookeeper restarted successfully\n");
        break;
    }
    default:
        FPRINTF(global_client_socket,  "Error: Invalid action specified.\n");
        break;
    }
}

