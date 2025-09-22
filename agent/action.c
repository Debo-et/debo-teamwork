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
#include <fcntl.h>

#include "utiles.h"

// Structure to capture command execution results
typedef struct {
    int exit_code;
    char stdout_output[4096];
    char stderr_output[4096];
} CommandResult;

// Execute command and capture output
CommandResult executeCommand(const char* command) {
    CommandResult result = {0};
    int stdout_pipe[2], stderr_pipe[2];
    pid_t pid;
    
    // Create pipes for stdout and stderr
    if (pipe(stdout_pipe) < 0 || pipe(stderr_pipe) < 0) {
        result.exit_code = -1;
        return result;
    }
    
    pid = fork();
    if (pid < 0) {
        result.exit_code = -1;
        return result;
    }
    
    if (pid == 0) {
        // Child process
        close(stdout_pipe[0]);
        close(stderr_pipe[0]);
        
        // Redirect stdout and stderr to pipes
        dup2(stdout_pipe[1], STDOUT_FILENO);
        dup2(stderr_pipe[1], STDERR_FILENO);
        close(stdout_pipe[1]);
        close(stderr_pipe[1]);
        
        // Execute the command
        execl("/bin/sh", "sh", "-c", command, NULL);
        exit(127); // execl failed
    } else {
        // Parent process
        close(stdout_pipe[1]);
        close(stderr_pipe[1]);
        
        char buffer[256];
        ssize_t count;
        
        // Read stdout
        while ((count = read(stdout_pipe[0], buffer, sizeof(buffer)-1)) > 0) {
            buffer[count] = '\0';
            strncat(result.stdout_output, buffer, sizeof(result.stdout_output)-strlen(result.stdout_output)-1);
        }
        
        // Read stderr
        while ((count = read(stderr_pipe[0], buffer, sizeof(buffer)-1)) > 0) {
            buffer[count] = '\0';
            strncat(result.stderr_output, buffer, sizeof(result.stderr_output)-strlen(result.stderr_output)-1);
        }
        
        close(stdout_pipe[0]);
        close(stderr_pipe[0]);
        
        // Wait for process to complete and get exit status
        int status;
        waitpid(pid, &status, 0);
        
        if (WIFEXITED(status)) {
            result.exit_code = WEXITSTATUS(status);
        } else {
            result.exit_code = -1;
        }
    }
    
    return result;
}

// Check if output indicates success
int isSuccessOutput(const char* output, const char* service, Action action) {
    const char* success_patterns[] = {
        "started", "success", "running", "completed", "ready"
    };
    
    const char* failure_patterns[] = {
        "error", "fail", "not found", "cannot", "unable", "refused"
    };
    
    // Check for failure patterns first
    for (size_t i = 0; i < sizeof(failure_patterns)/sizeof(failure_patterns[0]); i++) {
        if (strstr(output, failure_patterns[i]) != NULL) {
            return 0;
        }
    }
    
    // Check for success patterns
    for (size_t i = 0; i < sizeof(success_patterns)/sizeof(success_patterns[0]); i++) {
        if (strstr(output, success_patterns[i]) != NULL) {
            return 1;
        }
    }
    
    // Default to checking exit code only
    return -1; // Unknown - rely on exit code
}

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
    int len;
    CommandResult result;

    switch (a) {
    case START:
        len = snprintf(cmd, sizeof(cmd), "\"%s/sbin/start-all.sh\"", hadoop_home);
        if (len < 0 || (size_t)len >= sizeof(cmd)) {
            FPRINTF(global_client_socket,  "Command buffer overflow detected\n");
            return;
        }
        
        result = executeCommand(cmd);
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Hadoop", START) == 0) {
            FPRINTF(global_client_socket,  "Start failed (Code: %d). Output:\n%s\nError:\n%s\n", 
                   result.exit_code, result.stdout_output, result.stderr_output);
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
        
        result = executeCommand(cmd);
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Hadoop", STOP) == 0) {
            FPRINTF(global_client_socket,  "Stop failed (Code: %d). Output:\n%s\nError:\n%s\n", 
                   result.exit_code, result.stdout_output, result.stderr_output);
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
        
        result = executeCommand(cmd);
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Hadoop", STOP) == 0) {
            FPRINTF(global_client_socket,  "Restart aborted - stop phase failed (Code: %d)\nOutput:\n%s\nError:\n%s\n", 
                   result.exit_code, result.stdout_output, result.stderr_output);
            return;
        }

        // Add delay for service shutdown (adjust as needed)
        sleep(5);

        // Start phase
        len = snprintf(cmd, sizeof(cmd), "\"%s/sbin/start-all.sh\"", hadoop_home);
        if (len < 0 || (size_t)len >= sizeof(cmd)) {
            FPRINTF(global_client_socket,  "Command buffer overflow detected\n");
            return;
        }
        
        result = executeCommand(cmd);
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Hadoop", START) == 0) {
            FPRINTF(global_client_socket,  "Restart incomplete - start phase failed (Code: %d)\nOutput:\n%s\nError:\n%s\n"
                    "System may be in inconsistent state!\n", 
                   result.exit_code, result.stdout_output, result.stderr_output);
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
    CommandResult result;

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

    result = executeCommand(command);
    if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Presto", action) == 0) {
        FPRINTF(global_client_socket, "Error: Failed to %s Presto (%d)\nOutput:\n%s\nError:\n%s\n", 
               action_name, result.exit_code, result.stdout_output, result.stderr_output);
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
    CommandResult result;

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
    int execute_service_command(const char* command, Action action) {
        result = executeCommand(command);
        
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Spark", action) == 0) {
            FPRINTF(global_client_socket, "Command execution failed. Output:\n%s\nError:\n%s\n", 
                   result.stdout_output, result.stderr_output);
            return -1;
        }
        return 0;
    }

    // Handle different actions
    switch(a) {
    case START:
        if (execute_service_command(start_cmd, START) == -1) {
            FPRINTF(global_client_socket,  "Spark service startup aborted\n");
            return;
        }
        PRINTF(global_client_socket, "Spark service started successfully\n");
        break;

    case STOP:
        if (execute_service_command(stop_cmd, STOP) == -1) {
            FPRINTF(global_client_socket,  "Spark service shutdown aborted\n");
            return;
        }
        PRINTF(global_client_socket, "Spark service stopped successfully\n");
        break;

    case RESTART:
        // Stop phase
        if (execute_service_command(stop_cmd, STOP) == -1) {
            FPRINTF(global_client_socket,  "Restart aborted due to stop failure\n");
            return;
        }

        // Add delay for service shutdown
        sleep(3);

        // Start phase
        if (execute_service_command(start_cmd, START) == -1) {
            FPRINTF(global_client_socket,  "Spark service restart aborted\n");
            return;
        }
        PRINTF(global_client_socket, "Spark service restarted successfully\n");
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
    CommandResult result;

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

    // Construct commands with proper shell escaping
    snprintf(start_cmd, sizeof(start_cmd),
             "\"%s/bin/hive\" --service hiveserver2", hive_path);

    snprintf(stop_cmd, sizeof(stop_cmd),
             "pkill -f '\"%s/bin/hive\" --service hiveserver2'", hive_path);

    switch (a) {
    case START: {
        result = executeCommand(start_cmd);
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Hive", START) == 0) {
            FPRINTF(global_client_socket, "Failed to start Hive. Output:\n%s\nError:\n%s\n", 
                   result.stdout_output, result.stderr_output);
            exit(EXIT_FAILURE);
        }

        PRINTF(global_client_socket, "Hive service started successfully\n");
        break;
    }

    case STOP: {
        result = executeCommand(stop_cmd);
        if (result.exit_code != 0) {
            FPRINTF(global_client_socket, "Failed to stop Hive. Output:\n%s\nError:\n%s\n", 
                   result.stdout_output, result.stderr_output);
            exit(EXIT_FAILURE);
        }

        PRINTF(global_client_socket, "Hive service stopped successfully\n");
        break;
    }

    case RESTART: {
        // Stop phase
        result = executeCommand(stop_cmd);
        if (result.exit_code != 0) {

            FPRINTF(global_client_socket, "Failed to stop Hive during restart. Output:\n%s\nError:\n%s\n", 
                   result.stdout_output, result.stderr_output);
            exit(EXIT_FAILURE);
        }

        // Start phase
        result = executeCommand(start_cmd);
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Hive", START) == 0) {
            FPRINTF(global_client_socket, "Failed to start Hive during restart. Output:\n%s\nError:\n%s\n", 
                   result.stdout_output, result.stderr_output);
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
    CommandResult result;

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

    switch(a) {
    case START:
        snprintf(command, sizeof(command), "%s start", daemon_script);
        result = executeCommand(command);
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Zeppelin", START) == 0) {
            FPRINTF(global_client_socket,  "Start failed. Output:\n%s\nError:\n%s\n", 
                   result.stdout_output, result.stderr_output);
            exit(EXIT_FAILURE);
        }
        PRINTF(global_client_socket, "Zeppelin started successfully\n");
        break;

    case STOP:
        snprintf(command, sizeof(command), "%s stop", daemon_script);
        result = executeCommand(command);
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Zeppelin", STOP) == 0) {
            FPRINTF(global_client_socket,  "Stop failed. Output:\n%s\nError:\n%s\n", 
                   result.stdout_output, result.stderr_output);
            exit(EXIT_FAILURE);
        }
        PRINTF(global_client_socket, "Zeppelin stopped successfully\n");
        break;

    case RESTART:
        // Execute stop followed by start
        snprintf(command, sizeof(command), "%s stop", daemon_script);
        result = executeCommand(command);
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Zeppelin", STOP) == 0) {
            FPRINTF(global_client_socket,  "Restart aborted - stop phase failed. Output:\n%s\nError:\n%s\n",
                    result.stdout_output, result.stderr_output);
            exit(EXIT_FAILURE);
        }

        snprintf(command, sizeof(command), "%s start", daemon_script);
        result = executeCommand(command);
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Zeppelin", START) == 0) {
            FPRINTF(global_client_socket,  "Restart failed - start phase. Output:\n%s\nError:\n%s\n",
                    result.stdout_output, result.stderr_output);
            exit(EXIT_FAILURE);
        }
        PRINTF(global_client_socket, "Zeppelin restarted successfully\n");
        break;

    default:
        FPRINTF(global_client_socket,  "Invalid action command\n");
        exit(EXIT_FAILURE);
    }
}

void livy_action(Action a) {
    const char *livy_home = NULL;
    CommandResult result;
    
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
        char command[PATH_MAX + 10];
        snprintf(command, sizeof(command), "%s start", script_path);
        result = executeCommand(command);
        
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Livy", START) == 0) {
            FPRINTF(global_client_socket,  "Start failed. Output:\n%s\nError:\n%s\n", 
                   result.stdout_output, result.stderr_output);
        } else {
            PRINTF(global_client_socket, "Successfully started Livy service\n");
        }
        break;
    }
    case STOP: {
        char command[PATH_MAX + 10];
        snprintf(command, sizeof(command), "%s stop", script_path);
        result = executeCommand(command);
        
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Livy", STOP) == 0) {
            FPRINTF(global_client_socket,  "Stop failed. Output:\n%s\nError:\n%s\n", 
                   result.stdout_output, result.stderr_output);
        } else {
            PRINTF(global_client_socket, "Successfully stopped Livy service\n");
        }
        break;
    }
    case RESTART: {
        char stop_command[PATH_MAX + 10];
        snprintf(stop_command, sizeof(stop_command), "%s stop", script_path);
        result = executeCommand(stop_command);
        
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Livy", STOP) == 0) {
            FPRINTF(global_client_socket,  "Restart aborted - stop failed. Output:\n%s\nError:\n%s\n", 
                   result.stdout_output, result.stderr_output);
            return;
        }
        
        char start_command[PATH_MAX + 10];
        snprintf(start_command, sizeof(start_command), "%s start", script_path);
        result = executeCommand(start_command);
        
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Livy", START) == 0) {
            FPRINTF(global_client_socket,  "Restart failed - start failed. Output:\n%s\nError:\n%s\n", 
                   result.stdout_output, result.stderr_output);
        } else {
            PRINTF(global_client_socket, "Successfully restarted Livy service\n");
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
    CommandResult result;
    
    if (!pig_home) {
        FPRINTF(global_client_socket,  "Error: Pig installation not found. Checked:\n"
                "1. PIG_HOME environment variable\n"
                "2. /opt/pig (Red Hat)\n"
                "3. /usr/local/pig (Debian)\n");
        exit(EXIT_FAILURE);
    }

    char command[512];
    int len;

    switch (a) {
    case START: {
        len = snprintf(command, sizeof(command),
                       "%s/bin/pig -x local", pig_home);
        if (len < 0 || len >= (int)sizeof(command)) {
            FPRINTF(global_client_socket,  "Command construction error\n");
            exit(EXIT_FAILURE);
        }

        result = executeCommand(command);
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Pig", START) == 0) {
            FPRINTF(global_client_socket,  "Failed to start Pig. Error: %d\nOutput:\n%s\nError:\n%s\n",
                   result.exit_code, result.stdout_output, result.stderr_output);
            exit(EXIT_FAILURE);
        }
        PRINTF(global_client_socket, "Pig started successfully\n");
        break;
    }

    case STOP: {
        len = snprintf(command, sizeof(command),
                       "pkill -f '^%s/bin/pig -x local'", pig_home);
        if (len < 0 || len >= (int)sizeof(command)) {
            FPRINTF(global_client_socket,  "Command construction error\n");
            exit(EXIT_FAILURE);
        }

        result = executeCommand(command);
        if (result.exit_code != 0) {
            if (result.exit_code == 1) {
                PRINTF(global_client_socket, "No running Pig processes found\n");
            } else {
                FPRINTF(global_client_socket,  "Failed to stop Pig. Error: %d\nOutput:\n%s\nError:\n%s\n", 
                       result.exit_code, result.stdout_output, result.stderr_output);
                exit(EXIT_FAILURE);
            }
        } else {
            PRINTF(global_client_socket, "Pig stopped successfully\n");
        }
        break;
    }

    case RESTART:
        pig_action(STOP);
        sleep(2);  // Allow processes to terminate
        pig_action(START);
        PRINTF(global_client_socket, "Pig restarted successfully\n");
        break;

    default:
        FPRINTF(global_client_socket,  "Invalid action. Use START, STOP, or RESTART\n");
        exit(EXIT_FAILURE);
    }
}

void HBase_action(Action a) {
    char hbase_home[256];
    int os_found = 0;
    CommandResult result;

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
        result = executeCommand(start_cmd);
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "HBase", START) == 0) {
            FPRINTF(global_client_socket,  "Failed to start HBase. Output:\n%s\nError:\n%s\n", 
                   result.stdout_output, result.stderr_output);
            exit(EXIT_FAILURE);
        }
        PRINTF(global_client_socket, "HBase started successfully\n");
        break;
        
    case STOP:
        result = executeCommand(stop_cmd);
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "HBase", STOP) == 0) {
            FPRINTF(global_client_socket,  "Failed to stop HBase. Output:\n%s\nError:\n%s\n", 
                   result.stdout_output, result.stderr_output);
            exit(EXIT_FAILURE);
        }
        PRINTF(global_client_socket, "HBase stopped successfully\n");
        break;
        
    case RESTART:
        result = executeCommand(stop_cmd);
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "HBase", STOP) == 0) {
            FPRINTF(global_client_socket,  "Restart failed - stop phase. Output:\n%s\nError:\n%s\n", 
                   result.stdout_output, result.stderr_output);
            exit(EXIT_FAILURE);
        }
        
        result = executeCommand(start_cmd);
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "HBase", START) == 0) {
            FPRINTF(global_client_socket,  "Restart failed - start phase. Output:\n%s\nError:\n%s\n", 
                   result.stdout_output, result.stderr_output);
            exit(EXIT_FAILURE);
        }
        PRINTF(global_client_socket, "HBase restarted successfully\n");
        break;
        
    default:
        FPRINTF(global_client_socket,  "Invalid action\n");
        exit(EXIT_FAILURE);
    }

    // Verify service state
    char verify_cmd[512];
    snprintf(verify_cmd, sizeof(verify_cmd),
             "jps | grep HMaster >/dev/null && jps | grep HRegionServer >/dev/null");

    if (a == START || a == RESTART) {
        result = executeCommand(verify_cmd);
        if (result.exit_code != 0) {
            FPRINTF(global_client_socket,  "Service verification failed after %s\n",
                    (a == RESTART) ? "restart" : "start");
            exit(EXIT_FAILURE);
        }
    } else if (a == STOP) {
        result = executeCommand(verify_cmd);
        if (result.exit_code == 0) {
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

    if (tez_home != NULL) {
        if (stat(tez_home, &st) == 0 && S_ISDIR(st.st_mode)) {
            return tez_home;
        }
        FPRINTF(global_client_socket, "TEZ_HOME set to invalid directory: %s\n", tez_home);
    }

    for (size_t i = 0; i < sizeof(paths)/sizeof(paths[0]); i++) {
        if (stat(paths[i], &st) == 0 && S_ISDIR(st.st_mode)) {
            return paths[i];
        }
    }

    return NULL;
}

bool is_tez_installed() {
    const char *tez_home = get_tez_home();
    if (tez_home == NULL) {
        FPRINTF(global_client_socket,
                "Tez is NOT installed. Checked:\n"
                "- TEZ_HOME environment variable\n"
                "- /opt/tez (Red Hat)\n"
                "- /usr/local/tez (Debian)\n");
        return false;
    }

    PRINTF(global_client_socket, "Tez is installed at: %s\n", tez_home);
    return true;
}

void tez_action(Action a) {
    if (!is_tez_installed()) {
        return;
    }

    switch (a) {
    case START:
        PRINTF(global_client_socket, "Tez does not have a daemon to start. No action taken.\n");
        break;
    case STOP:
        PRINTF(global_client_socket, "Tez does not have a daemon to stop. No action taken.\n");
        break;
    case RESTART:
        PRINTF(global_client_socket, "Tez does not have a daemon to restart. No action taken.\n");
        break;
    default:
        FPRINTF(global_client_socket, "Invalid action requested: %d\n", a);
        break;
    }
}

int kafka_action(Action action) {
    char kafka_home[PATH_MAX] = {0};
    char command[2048] = {0};
    struct stat st;
    const char *env_kafka = getenv("KAFKA_HOME");
    CommandResult result;

    // Determine Kafka installation directory
    if (env_kafka) {
        strncpy(kafka_home, env_kafka, sizeof(kafka_home) - 1);
    } else if (access("/etc/debian_version", F_OK) == 0) {
        strncpy(kafka_home, "/usr/local/kafka", sizeof(kafka_home) - 1);
    } else if (access("/etc/redhat-release", F_OK) == 0) {
        strncpy(kafka_home, "/opt/kafka", sizeof(kafka_home) - 1);
    } else {
        FPRINTF(global_client_socket, "Unable to determine Kafka home\n");
        return -1;
    }
    kafka_home[sizeof(kafka_home) - 1] = '\0';

    // Validate Kafka installation
    if (stat(kafka_home, &st) < 0 || !S_ISDIR(st.st_mode)) {
        FPRINTF(global_client_socket, "Kafka not installed at %s: %s\n", kafka_home, strerror(errno));
        return -1;
    }

    // Build paths
    char kafka_start[PATH_MAX], kafka_stop[PATH_MAX], kafka_conf[PATH_MAX];
    char zk_start[PATH_MAX], zk_stop[PATH_MAX], zk_conf[PATH_MAX];
    snprintf(kafka_start, sizeof(kafka_start), "%s/bin/kafka-server-start.sh", kafka_home);
    snprintf(kafka_stop,  sizeof(kafka_stop),  "%s/bin/kafka-server-stop.sh",  kafka_home);
    snprintf(kafka_conf,  sizeof(kafka_conf),  "%s/config/server.properties", kafka_home);
    snprintf(zk_start,    sizeof(zk_start),    "%s/bin/zookeeper-server-start.sh", kafka_home);
    snprintf(zk_stop,     sizeof(zk_stop),     "%s/bin/zookeeper-server-stop.sh",  kafka_home);
    snprintf(zk_conf,     sizeof(zk_conf),     "%s/config/zookeeper.properties", kafka_home);

    // Check executables
    int missing = 0;
    if (access(zk_start, X_OK) != 0) { FPRINTF(global_client_socket, "Missing Zookeeper start script\n"); missing = 1; }
    if (access(zk_stop,  X_OK) != 0) { FPRINTF(global_client_socket, "Missing Zookeeper stop script\n");  missing = 1; }
    if (access(kafka_start, X_OK) != 0) { FPRINTF(global_client_socket, "Missing Kafka start script\n");   missing = 1; }
    if (access(kafka_stop,  X_OK) != 0) { FPRINTF(global_client_socket, "Missing Kafka stop script\n");    missing = 1; }
    if (missing) return -1;

    int ret;
    switch (action) {
    case START:
        ret = snprintf(command, sizeof(command),
                       "%s %s && %s %s",
                       zk_start, zk_conf,
                       kafka_start, kafka_conf);
        break;

    case STOP:
        ret = snprintf(command, sizeof(command),
                       "%s && %s",
                       kafka_stop, zk_stop);
        break;

    case RESTART:
        if (kafka_action(STOP) < 0) return -1;
        sleep(3);
        return kafka_action(START);

    default:
        FPRINTF(global_client_socket, "Invalid action %d\n", action);
        return -1;
    }
    
    if (ret < 0 || ret >= (int)sizeof(command)) {
        FPRINTF(global_client_socket, "Command construction overflow\n");
        return -1;
    }

    // Execute action
    result = executeCommand(command);
    if (result.exit_code < 0) {
        FPRINTF(global_client_socket, "Command execution failed. Output:\n%s\nError:\n%s\n", 
               result.stdout_output, result.stderr_output);
        return result.exit_code;
    }

    // Verify
    char verify_cmd[512];
    if (action == START) {
        sleep(2);
        snprintf(verify_cmd, sizeof(verify_cmd), "jps | grep -E '(QuorumPeerMain|Kafka)'");
        result = executeCommand(verify_cmd);
        if (result.exit_code != 0) {
            FPRINTF(global_client_socket, "Kafka/Zookeeper failed to start. Output:\n%s\nError:\n%s\n", 
                   result.stdout_output, result.stderr_output);
            return -1;
        }
    } else if (action == STOP) {
        snprintf(verify_cmd, sizeof(verify_cmd), "jps | grep -E '(QuorumPeerMain|Kafka)'");
        result = executeCommand(verify_cmd);
        if (result.exit_code == 0) {
            FPRINTF(global_client_socket, "Kafka/Zookeeper failed to stop. Output:\n%s\n", 
                   result.stdout_output);
            return -1;
        }
    }

    PRINTF(global_client_socket, "Service operations completed successfully.\n");
    return 0;
}

void Solr_action(Action a) {
    const char *install_dir = NULL;
    char solr_script[512];
    CommandResult result;

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
        char command[PATH_MAX + 10];
        snprintf(command, sizeof(command), "%s start", solr_script);
        result = executeCommand(command);
        
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Solr", START) == 0) {
            FPRINTF(global_client_socket,  "Failed to start Solr. Output:\n%s\nError:\n%s\n", 
                   result.stdout_output, result.stderr_output);
        } else {
            PRINTF(global_client_socket, "Solr started successfully\n");
        }
        break;
    }

    case STOP: {
        char command[PATH_MAX + 10];
        snprintf(command, sizeof(command), "%s stop", solr_script);
        result = executeCommand(command);
        
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Solr", STOP) == 0) {
            FPRINTF(global_client_socket,  "Failed to stop Solr. Output:\n%s\nError:\n%s\n", 
                   result.stdout_output, result.stderr_output);
        } else {
            PRINTF(global_client_socket, "Solr stopped successfully\n");
        }
        break;
    }

    case RESTART: {
        char command[PATH_MAX + 10];
        snprintf(command, sizeof(command), "%s restart", solr_script);
        result = executeCommand(command);
        
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Solr", RESTART) == 0) {
            FPRINTF(global_client_socket,  "Failed to restart Solr. Output:\n%s\nError:\n%s\n", 
                   result.stdout_output, result.stderr_output);
            // Fallback to stop/start sequence
            Solr_action(STOP);
            Solr_action(START);
        } else {
            PRINTF(global_client_socket, "Solr restarted successfully\n");
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
    CommandResult result;

    if (ret < 0 || ret >= (int)sizeof(command)) {
        FPRINTF(global_client_socket,  "Command buffer overflow for action: %s\n", action);
        return -1;
    }

    result = executeCommand(command);
    if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Phoenix", 
            (strcmp(action, "start") == 0) ? START : 
            (strcmp(action, "stop") == 0) ? STOP : RESTART) == 0) {
        FPRINTF(global_client_socket,  "Action '%s' failed. Output:\n%s\nError:\n%s\n", 
               action, result.stdout_output, result.stderr_output);
        return -1;
    }
    
    return 0;
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
            // Stop was successful
        } else {
            FPRINTF(global_client_socket, "Stop phase failed (exit code: %d), attempting start anyway\n", stop_status);
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
    CommandResult result;

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

        result = executeCommand(command);
        if (result.exit_code == -1) {
            FPRINTF(global_client_socket,  "Command execution failed: %s\n", strerror(errno));
            return -1;
        }
        return result.exit_code;
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

int execute_atlas_script(const char *script_path) {
    CommandResult result = executeCommand(script_path);
    if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Atlas", 
            (strstr(script_path, "start") != NULL) ? START : STOP) == 0) {
        FPRINTF(global_client_socket, "Script execution failed. Output:\n%s\nError:\n%s\n", 
               result.stdout_output, result.stderr_output);
        return -1;
    }
    return 0;
}

int atlas_action(Action a) {
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
        result = execute_atlas_script(script_path);
        if (result == 0) {
            PRINTF(global_client_socket, "Apache Atlas started successfully\n");
        }
        break;

    case STOP:
        snprintf(script_path, sizeof(script_path), "%s/bin/atlas_stop.py", atlas_home);
        result = execute_atlas_script(script_path);
        if (result == 0) {
            PRINTF(global_client_socket, "Apache Atlas stopped successfully\n");
        }
        break;

    case RESTART:
        if ((result = atlas_action(STOP)) == 0) {
            result = atlas_action(START);
            if (result == 0) {
                PRINTF(global_client_socket, "Apache Atlas restarted successfully\n");
            }
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

static int stop_services(const char *storm_cmd, const char **services, size_t num_services) {
    int all_stopped = 1;
    CommandResult result;
    
    for (size_t i = 0; i < num_services; i++) {
        char cmd[PATH_MAX * 2];
        int ret = snprintf(cmd, sizeof(cmd), "pkill -f '%s %s'", storm_cmd, services[i]);
        if (ret >= (int)sizeof(cmd)) {
            FPRINTF(global_client_socket,  "Command buffer overflow for service %s\n", services[i]);
            all_stopped = 0;
            continue;
        }

        result = executeCommand(cmd);
        if (result.exit_code == 0) {
            PRINTF(global_client_socket, "Successfully stopped %s\n", services[i]);
        } else if (result.exit_code == 1) {
            PRINTF(global_client_socket, "%s was not running\n", services[i]);
        } else {
            FPRINTF(global_client_socket,  "Error stopping %s. Output:\n%s\nError:\n%s\n", 
                   services[i], result.stdout_output, result.stderr_output);
            all_stopped = 0;
        }
    }
    return all_stopped;
}

static int start_services(const char *storm_cmd, const char **services, size_t num_services) {
    int all_started = 1;
    CommandResult result;
    
    for (size_t i = 0; i < num_services; i++) {
        char cmd[PATH_MAX * 2];
        int ret = snprintf(cmd, sizeof(cmd), "%s %s", storm_cmd, services[i]);
        if (ret >= (int)sizeof(cmd)) {
            FPRINTF(global_client_socket, "Command buffer overflow for service %s\n", services[i]);
            all_started = 0;
            continue;
        }

        result = executeCommand(cmd);
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Storm", START) == 0) {
            FPRINTF(global_client_socket, "Failed to start %s. Output:\n%s\nError:\n%s\n", 
                   services[i], result.stdout_output, result.stderr_output);
            all_started = 0;
        } else {
            PRINTF(global_client_socket, "Successfully started %s\n", services[i]);
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
    CommandResult result;

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
        result = executeCommand(start_script);
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Flink", START) == 0) {
            FPRINTF(global_client_socket,  "Start failed. Output:\n%s\nError:\n%s\n", 
                   result.stdout_output, result.stderr_output);
            exit(EXIT_FAILURE);
        }
        PRINTF(global_client_socket, "Flink started successfully\n");
        break;

    case STOP: {
        result = executeCommand(stop_script);
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Flink", STOP) == 0) {
            FPRINTF(global_client_socket,  "Stop failed. Output:\n%s\nError:\n%s\n", 
                   result.stdout_output, result.stderr_output);
            exit(EXIT_FAILURE);
        }
        PRINTF(global_client_socket, "Flink stopped successfully\n");
        break;
    }

    case RESTART: {
        // Stop phase
        result = executeCommand(stop_script);
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Flink", STOP) == 0) {
            FPRINTF(global_client_socket,  "Restart aborted - stop failed. Output:\n%s\nError:\n%s\n", 
                   result.stdout_output, result.stderr_output);
            exit(EXIT_FAILURE);
        }

        // Start phase
        result = executeCommand(start_script);
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Flink", START) == 0) {
            FPRINTF(global_client_socket,  "Restart incomplete - start failed. Output:\n%s\nError:\n%s\n", 
                   result.stdout_output, result.stderr_output);
            exit(EXIT_FAILURE);
        }
        PRINTF(global_client_socket, "Flink restarted successfully\n");
        break;
    }

    default:
        FPRINTF(global_client_socket,  "Invalid action specified\n");
        exit(EXIT_FAILURE);
    }
}

void zookeeper_action(Action a) {
    char *zk_home = getenv("ZOOKEEPER_HOME");
    int isDebian, isRedHat;
    char zk_script[PATH_MAX];
    char command[1024];
    CommandResult result;

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
        result = executeCommand(command);
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Zookeeper", START) == 0) {
            FPRINTF(global_client_socket,  "Failed to start Zookeeper. Output:\n%s\nError:\n%s\n", 
                   result.stdout_output, result.stderr_output);
        } else {
            PRINTF(global_client_socket, "Zookeeper started successfully\n");
        }
        break;
    }
    case STOP: {
        long unsigned int ret4 = snprintf(command, sizeof(command), "\"%s\" stop", zk_script);
        if (ret4 >= sizeof(command)) {
            FPRINTF(global_client_socket,  "Error: command buffer overflow.\n");
        }
        result = executeCommand(command);
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Zookeeper", STOP) == 0) {
            FPRINTF(global_client_socket,  "Failed to stop Zookeeper. Output:\n%s\nError:\n%s\n", 
                   result.stdout_output, result.stderr_output);
        } else {
            PRINTF(global_client_socket, "Zookeeper stopped successfully\n");
        }
        break;
    }
    case RESTART: {
        // Stop the service
        long unsigned int ret5 = snprintf(command, sizeof(command), "\"%s\" stop", zk_script);
        if (ret5 >= sizeof(command)) {
            FPRINTF(global_client_socket,  "Error: command buffer overflow.\n");
        }
        result = executeCommand(command);
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Zookeeper", STOP) == 0) {
            FPRINTF(global_client_socket,  "Failed to stop Zookeeper during restart. Output:\n%s\nError:\n%s\n", 
                   result.stdout_output, result.stderr_output);
        }

        // Start the service
        long unsigned int ret9 = snprintf(command, sizeof(command), "\"%s\" start", zk_script);
        if (ret9 >= sizeof(command)) {
            FPRINTF(global_client_socket,  "Error: command buffer overflow.\n");
        }
        result = executeCommand(command);
        if (result.exit_code != 0 || isSuccessOutput(result.stdout_output, "Zookeeper", START) == 0) {
            FPRINTF(global_client_socket,  "Failed to start Zookeeper during restart. Output:\n%s\nError:\n%s\n", 
                   result.stdout_output, result.stderr_output);
        } else {
            PRINTF(global_client_socket, "Zookeeper restarted successfully\n");
        }
        break;
    }
    default:
        FPRINTF(global_client_socket,  "Error: Invalid action specified.\n");
        break;
    }
}
