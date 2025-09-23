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

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "utiles.h"

// Helper function to execute command and capture output
int executeCommandWithOutput(const char *command, char *output, size_t output_size) {
    FILE *fp = popen(command, "r");
    if (fp == NULL) {
        return -1;
    }
    
    size_t bytes_read = 0;
    char buffer[256];
    while (fgets(buffer, sizeof(buffer), fp) != NULL && bytes_read < output_size - 1) {
        size_t len = strlen(buffer);
        strncpy(output + bytes_read, buffer, len);
        bytes_read += len;
    }
    output[bytes_read] = '\0';
    
    return pclose(fp);
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
        fprintf(stderr,  "Hadoop installation not found. Searched:\n"
                "- HADOOP_HOME environment variable\n"
                "- /opt/hadoop (Red Hat)\n"
                "- /usr/local/hadoop (Debian)\n");
        return;
    }

    char cmd[4096];
    int ret, len;
    char output[1024];

    switch (a) {
    case START:
        len = snprintf(cmd, sizeof(cmd), "\"%s/sbin/start-all.sh\"", hadoop_home);
        if (len < 0 || (size_t)len >= sizeof(cmd)) {
            fprintf(stderr,  "Command buffer overflow detected\n");
            return;
        }
        ret = executeCommandWithOutput(cmd, output, sizeof(output));
        if (ret != 0 || strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
            fprintf(stderr,  "Start failed (Code: %d). Output: %s\n", ret, output);
        } else {
            printf( "All Hadoop services started successfully\n");
        }
        break;

    case STOP:
        len = snprintf(cmd, sizeof(cmd), "\"%s/sbin/stop-all.sh\"", hadoop_home);
        if (len < 0 || (size_t)len >= sizeof(cmd)) {
            fprintf(stderr,  "Command buffer overflow detected\n");
            return;
        }
        ret = executeCommandWithOutput(cmd, output, sizeof(output));
        if (ret != 0 || strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
            fprintf(stderr,  "Stop failed (Code: %d). Output: %s\n", ret, output);
        } else {
            printf( "All Hadoop services stopped successfully\n");
        }
        break;

    case RESTART: {
        // Stop phase
        len = snprintf(cmd, sizeof(cmd), "\"%s/sbin/stop-all.sh\"", hadoop_home);
        if (len < 0 || (size_t)len >= sizeof(cmd)) {
            fprintf(stderr,  "Command buffer overflow detected\n");
            return;
        }
        ret = executeCommandWithOutput(cmd, output, sizeof(output));
        if (ret != 0 || strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
            fprintf(stderr,  "Restart aborted - stop phase failed (Code: %d). Output: %s\n", ret, output);
            return;
        }

        // Add delay for service shutdown (adjust as needed)
        sleep(5);

        // Start phase
        len = snprintf(cmd, sizeof(cmd), "\"%s/sbin/start-all.sh\"", hadoop_home);
        if (len < 0 || (size_t)len >= sizeof(cmd)) {
            fprintf(stderr,  "Command buffer overflow detected\n");
            return;
        }
        ret = executeCommandWithOutput(cmd, output, sizeof(output));
        if (ret != 0 || strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
            fprintf(stderr,  "Restart incomplete - start phase failed (Code: %d). Output: %s\n"
                    "System may be in inconsistent state!\n", ret, output);
        } else {
            printf( "All services restarted successfully\n");
        }
        break;
    }

    default:
        fprintf(stderr,  "Invalid action. Valid options:\n"
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
    char output[1024];

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
        fprintf(stderr, "Error: Presto installation not found\n");
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
        fprintf(stderr, "Error: Invalid action\n");
        free(launcher_path);
        exit(EXIT_FAILURE);
    }

    if ((ret = executeCommandWithOutput(command, output, sizeof(output)))) {
        fprintf(stderr, "Error: Failed to %s Presto (%d). Output: %s\n", action_name, ret, output);
        free(launcher_path);
        exit(EXIT_FAILURE);
    }

    printf("Presto service %s completed successfully\n", action_name);
    free(launcher_path);
}

void spark_action(Action a) {
    const char* install_path = NULL;
    char output[1024];
    int status;

    // Check SPARK_HOME environment variable first
    const char* spark_home = getenv("SPARK_HOME");
    if (spark_home != NULL) {
        char spark_bin[PATH_MAX];
        snprintf(spark_bin, sizeof(spark_bin), "%s/sbin/start-all.sh", spark_home);
        if (access(spark_bin, X_OK) == 0) {
            install_path = spark_home;
        } else {
            fprintf(stderr, "Warning: SPARK_HOME set but start script not found at %s\n", spark_bin);
        }
    }

    // If SPARK_HOME not set or invalid, fall back to OS detection
    if (install_path == NULL) {
        if (access("/etc/debian_version", F_OK) == 0) {
            install_path = "/usr/local/spark";
        } else if (access("/etc/redhat-release", F_OK) == 0 ||
                   access("/etc/system-release", F_OK) == 0) {
            install_path = "/opt/spark";
        } else {
            fprintf(stderr, "Error: Unsupported Linux distribution and SPARK_HOME not set\n");
            return;
        }
    }

    // Verify installation
    char start_script[PATH_MAX];
    snprintf(start_script, sizeof(start_script), "%s/sbin/start-all.sh", install_path);
    if (access(start_script, X_OK) != 0) {
        fprintf(stderr, "Error: Spark installation not found at %s\n", install_path);
        fprintf(stderr, "Checked locations:\n");
        if (spark_home) fprintf(stderr, "- SPARK_HOME: %s\n", spark_home);
        fprintf(stderr, "- /usr/local/spark (Debian/Ubuntu)\n");
        fprintf(stderr, "- /opt/spark (Red Hat/CentOS)\n");
        return;
    }

    // Construct base commands with environment configuration
    char start_cmd[512];
    char stop_cmd[512];
    size_t cmd_len;

    cmd_len = snprintf(start_cmd, sizeof(start_cmd),
                       "export SPARK_HOME=%s && %s/sbin/start-all.sh",
                       install_path, install_path);
    if (cmd_len >= sizeof(start_cmd)) {
        fprintf(stderr, "Error: Start command buffer overflow\n");
        return;
    }

    cmd_len = snprintf(stop_cmd, sizeof(stop_cmd),
                       "export SPARK_HOME=%s && %s/sbin/stop-all.sh",
                       install_path, install_path);
    if (cmd_len >= sizeof(stop_cmd)) {
        fprintf(stderr, "Error: Stop command buffer overflow\n");
        return;
    }

    // Execute command with error handling
    int execute_service_command(const char* command, char* output_buf) {
        int result = executeCommandWithOutput(command, output_buf, 1024);
        if (result != 0 || strstr(output_buf, "error") != NULL || strstr(output_buf, "ERROR") != NULL) {
            fprintf(stderr, "Command execution failed. Output: %s\n", output_buf);
            return -1;
        }
        return 0;
    }

    // Handle different actions
    switch(a) {
    case START:
        if (execute_service_command(start_cmd, output) == -1) {
            fprintf(stderr, "Spark service startup aborted\n");
            return;
        }
        printf("Spark service started successfully\n");
        break;

    case STOP:
        if (execute_service_command(stop_cmd, output) == -1) {
            fprintf(stderr, "Spark service shutdown aborted\n");
            return;
        }
        printf("Spark service stopped successfully\n");
        break;

    case RESTART:
        // Stop phase
        if (execute_service_command(stop_cmd, output) == -1) {
            fprintf(stderr, "Restart aborted due to stop failure\n");
            return;
        }

        // Add delay for service shutdown
        sleep(3);

        // Start phase
        if (execute_service_command(start_cmd, output) == -1) {
            fprintf(stderr, "Spark service restart aborted\n");
            return;
        }
        printf("Spark service restarted successfully\n");
        break;

    default:
        fprintf(stderr, "Error: Invalid action specified\n");
        break;
    }
}

static bool is_hive_running() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        return false;
    }

    // Set socket to non-blocking
    int flags = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(10000);
    inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);

    // Start non-blocking connection
    connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr));

    fd_set fdset;
    FD_ZERO(&fdset);
    FD_SET(sock, &fdset);
    struct timeval tv = {.tv_sec = 1, .tv_usec = 0}; // 1s timeout

    // Wait for connection or timeout
    if (select(sock + 1, NULL, &fdset, NULL, &tv) == 1) {
        int so_error;
        socklen_t len = sizeof(so_error);
        getsockopt(sock, SOL_SOCKET, SO_ERROR, &so_error, &len);
        close(sock);
        return (so_error == 0);
    }

    close(sock);
    return false;
}

// Checks if Hive started successfully by testing the port
static bool check_hive_started() {
    const int max_attempts = 10;
    const int wait_seconds = 2;

    for (int i = 0; i < max_attempts; i++) {
        if (is_hive_running()) {
            return true;
        }
        sleep(wait_seconds);
    }
    return false;
}

void hive_action(Action a) {
    const char *hive_path = NULL;
    const char *hive_home = getenv("HIVE_HOME");
    const char *default_paths[] = { "/opt/hive", "/usr/local/hive" };
    char output[1024];

    // Try HIVE_HOME first
    if (hive_home != NULL) {
        if (access(hive_home, F_OK) == 0) {
            hive_path = hive_home;
        } else {
            fprintf(stderr,  "Warning: HIVE_HOME directory '%s' not found. Checking defaults...\n", hive_home);
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
        fprintf(stderr,  "Error: Hive installation not found. Checked:\n"
                "• HIVE_HOME environment variable\n"
                "• /opt/hive (Red Hat)\n"
                "• /usr/local/hive (Debian)\n");
        exit(EXIT_FAILURE);
    }

    // Validate Hive binary exists
    char hive_bin[PATH_MAX];
    snprintf(hive_bin, sizeof(hive_bin), "%s/bin/hive", hive_path);
    if (access(hive_bin, X_OK) != 0) {
        fprintf(stderr,  "Error: Hive executable not found or not executable: %s\n", hive_bin);
        exit(EXIT_FAILURE);
    }

    char start_cmd[PATH_MAX * 2];
    char stop_cmd[PATH_MAX * 2];
    int ret;

    // Construct commands
    snprintf(start_cmd, sizeof(start_cmd),
             "\"%s/bin/hive\" --service hiveserver2", hive_path);

    snprintf(stop_cmd, sizeof(stop_cmd),
             "pkill -f '\"%s/bin/hive\" --service hiveserver2'", hive_path);

    switch (a) {
    case START: {
        ret = executeCommandWithOutput(start_cmd, output, sizeof(output));
        if (ret != 0 || strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
            fprintf(stderr, "Failed to start Hive. Exit code: %d, Output: %s\n", ret, output);
            exit(EXIT_FAILURE);
        }

        // Verify service actually started
        if (check_hive_started()) {
            printf("Hive service started successfully\n");
        } else {
            fprintf(stderr, "Error: Hive service failed to start. Port 10000 not listening after 20 seconds.\n");
            exit(EXIT_FAILURE);
        }
        break;
    }

    case STOP: {
        ret = executeCommandWithOutput(stop_cmd, output, sizeof(output));
        if (ret != 0) {
            fprintf(stderr, "Failed to stop Hive. Exit code: %d, Output: %s\n", ret, output);
            exit(EXIT_FAILURE);
        }

        printf("Hive service stopped successfully\n");
        break;
    }

    case RESTART: {
        // Stop phase
        ret = executeCommandWithOutput(stop_cmd, output, sizeof(output));
        if (ret != 0) {
            fprintf(stderr, "Failed to stop Hive during restart. Exit code: %d, Output: %s\n", ret, output);
            exit(EXIT_FAILURE);
        }

        // Start phase
        ret = executeCommandWithOutput(start_cmd, output, sizeof(output));
        if (ret != 0 || strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
            fprintf(stderr, "Failed to start Hive during restart. Exit code: %d, Output: %s\n", ret, output);
            exit(EXIT_FAILURE);
        }

        // Verify service restarted
        if (check_hive_started()) {
            printf("Hive service restarted successfully\n");
        } else {
            fprintf(stderr, "Error: Hive service failed to restart. Port 10000 not listening after 20 seconds.\n");
            exit(EXIT_FAILURE);
        }
        break;
    }

    default:
        fprintf(stderr,  "Invalid action specified\n");
        exit(EXIT_FAILURE);
    }
}

void Zeppelin_action(Action a) {
    // Determine Zeppelin installation path
    const char *zeppelin_home = getenv("ZEPPELIN_HOME");
    char detected_path[PATH_MAX] = {0};
    char output[1024];

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
            fprintf(stderr,  "ZEPPELIN_HOME not set and OS detection failed\n");
            exit(EXIT_FAILURE);
        }
        zeppelin_home = detected_path;
    }

    // Verify daemon script existence
    char daemon_script[PATH_MAX];
    snprintf(daemon_script, sizeof(daemon_script),
             "%s/bin/zeppelin-daemon.sh", zeppelin_home);

    if (access(daemon_script, X_OK) != 0) {
        fprintf(stderr,  "Zeppelin daemon script not found or not executable at: %s\n",
                daemon_script);
        exit(EXIT_FAILURE);
    }

    // Construct base command
    char command[PATH_MAX + 20];
    int ret;

    switch(a) {
    case START:
        snprintf(command, sizeof(command), "%s start", daemon_script);
        ret = executeCommandWithOutput(command, output, sizeof(output));
        if (ret != 0 || strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
            fprintf(stderr,  "Start failed. Exit code: %d, Output: %s\n", ret, output);
            exit(EXIT_FAILURE);
        }
        printf( "Zeppelin started successfully\n");
        break;

    case STOP:
        snprintf(command, sizeof(command), "%s stop", daemon_script);
        ret = executeCommandWithOutput(command, output, sizeof(output));
        if (ret != 0 || strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
            fprintf(stderr,  "Stop failed. Exit code: %d, Output: %s\n", ret, output);
            exit(EXIT_FAILURE);
        }
        printf( "Zeppelin stopped successfully\n");
        break;

    case RESTART:
        // Execute stop followed by start
        snprintf(command, sizeof(command), "%s stop", daemon_script);
        ret = executeCommandWithOutput(command, output, sizeof(output));
        if (ret != 0 || strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
            fprintf(stderr,  "Restart aborted - stop phase failed. Exit code: %d, Output: %s\n", ret, output);
            exit(EXIT_FAILURE);
        }

        snprintf(command, sizeof(command), "%s start", daemon_script);
        ret = executeCommandWithOutput(command, output, sizeof(output));
        if (ret != 0 || strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
            fprintf(stderr,  "Restart failed - start phase. Exit code: %d, Output: %s\n", ret, output);
            exit(EXIT_FAILURE);
        }
        printf( "Zeppelin restarted successfully\n");
        break;

    default:
        fprintf(stderr,  "Invalid action command\n");
        exit(EXIT_FAILURE);
    }
}

// Helper function to execute Livy commands
static int execute_command(const char* script_path, const char* arg, char* output, size_t output_size) {
    char command[PATH_MAX + 128];
    size_t len = snprintf(command, sizeof(command), "%s %s", script_path, arg);

    if (len >= sizeof(command)) {
        fprintf(stderr,  "Command truncated: '%s %s'\n", script_path, arg);
        return -1;
    }

    int status = executeCommandWithOutput(command, output, output_size);
    if (status == -1) {
        perror( "system() failed");
        return -1;
    }

    if (WIFEXITED(status)) {
        int exit_status = WEXITSTATUS(status);
        if (exit_status != 0) {
            fprintf(stderr,  "Command failed with exit code %d: %s\n", exit_status, command);
            return -1;
        }
    }
    return 0;
}

void livy_action(Action a) {
    const char *livy_home = NULL;
    char output[1024];

    // Check LIVY_HOME environment variable first
    const char* env_livy_home = getenv("LIVY_HOME");
    if (env_livy_home != NULL) {
        char livy_script[PATH_MAX];
        snprintf(livy_script, sizeof(livy_script), "%s/bin/livy-server", env_livy_home);
        if (access(livy_script, X_OK) == 0) {
            livy_home = env_livy_home;
        } else {
            fprintf(stderr, "Warning: LIVY_HOME set but livy-server script not found at %s\n", livy_script);
        }
    }

    // If LIVY_HOME not set or invalid, fall back to OS detection
    if (livy_home == NULL) {
        if (access("/etc/debian_version", F_OK) == 0) {
            livy_home = "/usr/local/livy";
        } else if (access("/etc/redhat-release", F_OK) == 0 ||
                   access("/etc/system-release", F_OK) == 0) {
            livy_home = "/opt/livy";
        } else {
            fprintf(stderr, "Error: Unsupported Linux distribution and LIVY_HOME not set\n");
            return;
        }
    }

    // Construct and validate server script path
    char script_path[PATH_MAX];
    size_t path_len = snprintf(script_path, sizeof(script_path), "%s/bin/livy-server", livy_home);

    if (path_len >= sizeof(script_path)) {
        fprintf(stderr, "Path construction failed: Maximum length exceeded\n");
        return;
    }

    if (access(script_path, X_OK) != 0) {
        fprintf(stderr, "Livy server script not found or not executable: %s\n", script_path);
        fprintf(stderr, "Checked locations:\n");
        if (env_livy_home) fprintf(stderr, "- LIVY_HOME: %s\n", env_livy_home);
        fprintf(stderr, "- /usr/local/livy (Debian/Ubuntu)\n");
        fprintf(stderr, "- /opt/livy (Red Hat/CentOS)\n");
        return;
    }

    // Execute the requested action
    switch(a) {
    case START: {
        int rc = execute_command(script_path, "start", output, sizeof(output));
        if (rc == 0 && strstr(output, "error") == NULL && strstr(output, "ERROR") == NULL) {
            printf("Successfully started Livy service\n");
        } else {
            fprintf(stderr, "Start failed. Output: %s\n", output);
        }
        break;
    }
    case STOP: {
        int rc = execute_command(script_path, "stop", output, sizeof(output));
        if (rc == 0 && strstr(output, "error") == NULL && strstr(output, "ERROR") == NULL) {
            printf("Successfully stopped Livy service\n");
        } else {
            fprintf(stderr, "Stop failed. Output: %s\n", output);
        }
        break;
    }
    case RESTART: {
        int stop_rc = execute_command(script_path, "stop", output, sizeof(output));
        if (stop_rc != 0 || strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
            fprintf(stderr, "Restart aborted - stop failed. Output: %s\n", output);
            return;
        }
        
        int start_rc = execute_command(script_path, "start", output, sizeof(output));
        if (start_rc == 0 && strstr(output, "error") == NULL && strstr(output, "ERROR") == NULL) {
            printf("Successfully restarted Livy service\n");
        } else {
            fprintf(stderr, "Restart failed - start failed. Output: %s\n", output);
        }
        break;
    }
    default:
        fprintf(stderr, "Invalid action requested\n");
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
        fprintf(stderr,  "Error: Pig installation not found. Checked:\n"
                "1. PIG_HOME environment variable\n"
                "2. /opt/pig (Red Hat)\n"
                "3. /usr/local/pig (Debian)\n");
        exit(EXIT_FAILURE);
    }

    char command[512];
    int ret, len;
    char output[1024];

    switch (a) {
    case START: {
        len = snprintf(command, sizeof(command),
                       "%s/bin/pig -x local", pig_home);
        if (len < 0 || len >= (int)sizeof(command)) {
            fprintf(stderr,  "Command construction error\n");
            exit(EXIT_FAILURE);
        }

        ret = executeCommandWithOutput(command, output, sizeof(output));
        if (ret != 0 || strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
            fprintf(stderr,  "Failed to start Pig. Error: %d, Output: %s\n", ret, output);
            exit(EXIT_FAILURE);
        }
        printf("Pig started successfully\n");
        break;
    }

    case STOP: {
        len = snprintf(command, sizeof(command),
                       "pkill -f '^%s/bin/pig -x local'", pig_home);
        if (len < 0 || len >= (int)sizeof(command)) {
            fprintf(stderr,  "Command construction error\n");
            exit(EXIT_FAILURE);
        }

        ret = executeCommandWithOutput(command, output, sizeof(output));
        if (ret != 0) {
            if (ret == 1) {
                printf( "No running Pig processes found\n");
            } else {
                fprintf(stderr,  "Failed to stop Pig. Error: %d, Output: %s\n", ret, output);
                exit(EXIT_FAILURE);
            }
        } else {
            printf("Pig stopped successfully\n");
        }
        break;
    }

    case RESTART:
        pig_action(STOP);
        sleep(2);  // Allow processes to terminate
        pig_action(START);
        printf("Pig restarted successfully\n");
        break;

    default:
        fprintf(stderr,  "Invalid action. Use START, STOP, or RESTART\n");
        exit(EXIT_FAILURE);
    }
}

void HBase_action(Action a) {
    char hbase_home[PATH_MAX] = {0};
    int os_found = 0;
    char output[1024];

    // Check HBASE_HOME environment variable first
    const char* env_hbase_home = getenv("HBASE_HOME");
    if (env_hbase_home != NULL) {
        char hbase_bin[PATH_MAX];
        snprintf(hbase_bin, sizeof(hbase_bin), "%s/bin/start-hbase.sh", env_hbase_home);
        if (access(hbase_bin, X_OK) == 0) {
            strncpy(hbase_home, env_hbase_home, sizeof(hbase_home)-1);
            os_found = 1;
        } else {
            fprintf(stderr, "Warning: HBASE_HOME set but start script not found at %s\n", hbase_bin);
        }
    }

    // If HBASE_HOME not set or invalid, fall back to OS detection
    if (!os_found) {
        if (access("/etc/debian_version", F_OK) == 0) {
            strncpy(hbase_home, "/usr/local/hbase", sizeof(hbase_home)-1);
            os_found = 1;
        } else if (access("/etc/redhat-release", F_OK) == 0) {
            strncpy(hbase_home, "/opt/hbase", sizeof(hbase_home)-1);
            os_found = 1;
        }
    }

    if (!os_found) {
        fprintf(stderr, "Error: Unsupported OS and HBASE_HOME not set\n");
        exit(EXIT_FAILURE);
    }

    // Verify HBase installation directory
    if (access(hbase_home, F_OK) != 0) {
        fprintf(stderr, "Error: HBase not found at %s\n", hbase_home);
        fprintf(stderr, "Checked locations:\n");
        if (env_hbase_home) fprintf(stderr, "- HBASE_HOME: %s\n", env_hbase_home);
        fprintf(stderr, "- /usr/local/hbase (Debian/Ubuntu)\n");
        fprintf(stderr, "- /opt/hbase (Red Hat/CentOS)\n");
        exit(EXIT_FAILURE);
    }

    // Verify critical configuration directory
    char conf_dir[512];
    snprintf(conf_dir, sizeof(conf_dir), "%s/conf", hbase_home);
    if (access(conf_dir, F_OK) != 0) {
        fprintf(stderr, "Missing configuration directory: %s\n", conf_dir);
        exit(EXIT_FAILURE);
    }

    // Verify regionservers file exists
    char regionservers_path[512];
    snprintf(regionservers_path, sizeof(regionservers_path), "%s/conf/regionservers", hbase_home);
    if (access(regionservers_path, F_OK) != 0) {
        fprintf(stderr, "Missing regionservers file at %s\n", regionservers_path);
        exit(EXIT_FAILURE);
    }

    // Construct script paths
    char start_cmd[512], stop_cmd[512];
    snprintf(start_cmd, sizeof(start_cmd), "%s/bin/start-hbase.sh", hbase_home);
    snprintf(stop_cmd, sizeof(stop_cmd), "%s/bin/stop-hbase.sh", hbase_home);

    switch(a) {
    case START:
        if (executeCommandWithOutput(start_cmd, output, sizeof(output)) != 0 || 
            strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
            fprintf(stderr, "Failed to start HBase. Output: %s\n", output);
            exit(EXIT_FAILURE);
        }
        printf("HBase started successfully\n");
        break;
    case STOP:
        if (executeCommandWithOutput(stop_cmd, output, sizeof(output)) != 0 || 
            strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
            fprintf(stderr, "Failed to stop HBase. Output: %s\n", output);
            exit(EXIT_FAILURE);
        }
        printf("HBase stopped successfully\n");
        break;
    case RESTART:
        if (executeCommandWithOutput(stop_cmd, output, sizeof(output)) != 0 || 
            strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
            fprintf(stderr, "Restart failed at stop phase. Output: %s\n", output);
            exit(EXIT_FAILURE);
        }
        
        if (executeCommandWithOutput(start_cmd, output, sizeof(output)) != 0 || 
            strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
            fprintf(stderr, "Restart failed at start phase. Output: %s\n", output);
            exit(EXIT_FAILURE);
        }
        printf("HBase restarted successfully\n");
        break;
    default:
        fprintf(stderr, "Invalid action\n");
        exit(EXIT_FAILURE);
    }

    // Verify service state
    char verify_cmd[512];
    snprintf(verify_cmd, sizeof(verify_cmd),
             "jps | grep HMaster >/dev/null && jps | grep HRegionServer >/dev/null");

    if (a == START || a == RESTART) {
        if (executeSystemCommand(verify_cmd) != 0) {
            fprintf(stderr, "Service verification failed after %s\n",
                    (a == RESTART) ? "restart" : "start");
            exit(EXIT_FAILURE);
        }
    } else if (a == STOP) {
        if (executeSystemCommand(verify_cmd) == 0) {
            fprintf(stderr, "HBase processes still running after stop\n");
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
        fprintf(stderr,  "TEZ_HOME set to invalid directory: %s\n", tez_home);
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
        fprintf(stderr,  "Path to tez-daemon.sh exceeds maximum length\n");
        return false;
    }

    if (stat(script_path, &st) != 0 || !S_ISREG(st.st_mode)) {
        fprintf(stderr,  "tez-daemon.sh not found at: %s\n", script_path);
        return false;
    }

    if (access(script_path, X_OK) != 0) {
        fprintf(stderr,  "tez-daemon.sh is not executable at: %s\n", script_path);
        return false;
    }

    return true;
}

bool execute_tez_command(const char *operation, char *output, size_t output_size) {
    const char *tez_home = get_tez_home();
    if (!tez_home) {
        fprintf(stderr,  "Tez installation not found. Checked:\n"
                "- TEZ_HOME environment variable\n"
                "- /opt/tez (Red Hat)\n"
                "- /usr/local/tez (Debian)\n");
        return false;
    }

    if (!verify_tez_installation(tez_home)) {
        return false;
    }

    // Build command
    char command[PATH_MAX * 2];
    snprintf(command, sizeof(command), "%s/bin/tez-daemon.sh %s historyserver", tez_home, operation);

    int status = executeCommandWithOutput(command, output, output_size);
    if (WIFEXITED(status)) {
        int exit_status = WEXITSTATUS(status);
        if (exit_status != 0) {
            fprintf(stderr,  "Command failed with exit code: %d, Output: %s\n", exit_status, output);
            return false;
        }
        return true;
    }

    fprintf(stderr,  "Command terminated abnormally\n");
    return false;
}

void tez_action(Action a) {
    bool success = false;
    char output[1024];

    switch(a) {
    case START: {
        success = execute_tez_command("start", output, sizeof(output));
        if (success) {
            printf("Tez service started successfully\n");
        } else {
            fprintf(stderr, "Failed to start Tez service. Output: %s\n", output);
        }
        break;
    }
    case STOP: {
        success = execute_tez_command("stop", output, sizeof(output));
        if (success) {
            printf("Tez service stopped successfully\n");
        } else {
            fprintf(stderr, "Failed to stop Tez service. Output: %s\n", output);
        }
        break;
    }
    case RESTART: {
        bool stop_success = execute_tez_command("stop", output, sizeof(output));
        if (!stop_success) {
            fprintf(stderr,  "Warning: Tez service stop encountered issues. Output: %s\n", output);
        }

        // Add brief delay to allow service shutdown
        sleep(2);

        bool start_success = execute_tez_command("start", output, sizeof(output));
        success = start_success;
        if (success) {
            printf("Tez service restarted successfully\n");
        } else {
            fprintf(stderr, "Failed to restart Tez service. Output: %s\n", output);
        }
        break;
    }
    default: {
        fprintf(stderr,  "Invalid action requested: %d\n", a);
        return;
    }
    }
}

void kafka_action(Action action) {
    char kafka_home[PATH_MAX] = {0};
    char command[2048] = {0};
    int result;
    struct stat st;
    char output[1024];

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
            fprintf(stderr,  "Unable to determine Kafka home\n");
            exit(EXIT_FAILURE);
        }
        kafka_home[sizeof(kafka_home)-1] = '\0';
    }

    // Validate Kafka installation
    if (stat(kafka_home, &st) == -1 || !S_ISDIR(st.st_mode)) {
        fprintf(stderr,  "Kafka not installed at %s\n", kafka_home);
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
    if (access(zk_start, X_OK) != 0) { fprintf(stderr,  "Missing Zookeeper start script\n"); missing = 1; }
    if (access(zk_stop, X_OK) != 0) { fprintf(stderr,  "Missing Zookeeper stop script\n"); missing = 1; }
    if (access(kafka_start, X_OK) != 0) { fprintf(stderr,  "Missing Kafka start script\n"); missing = 1; }
    if (access(kafka_stop, X_OK) != 0) { fprintf(stderr,  "Missing Kafka stop script\n"); missing = 1; }
    if (missing) exit(EXIT_FAILURE);

    switch(action) {
    case START: {
        size_t ret = snprintf(command, sizeof(command),
                              "%s %s; "
                              "%s %s",
                              zk_start, zk_config,
                              kafka_start, kafka_config
                             );
        if (ret >= sizeof(command)) {
            fprintf(stderr,  "Command buffer overflow\n");
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
            fprintf(stderr,  "Command buffer overflow\n");
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
        fprintf(stderr,  "Invalid action\n");
        exit(EXIT_FAILURE);
    }

    // Execute command
    if ((result = executeCommandWithOutput(command, output, sizeof(output)))) {
        fprintf(stderr,  "Action failed with code %d. Output: %s\n", result, output);
        exit(EXIT_FAILURE);
    }

    // Verification command buffers
    char verify_cmd[512];

    // Verify execution
    if (action == START) {
        sleep(2);
        // Check Zookeeper
        snprintf(verify_cmd, sizeof(verify_cmd),
                 "pgrep -f 'zookeeper' >/dev/null");
        int zk_up = executeSystemCommand(verify_cmd);
        // Check Kafka
        snprintf(verify_cmd, sizeof(verify_cmd),
                 "pgrep -f 'kafka' >/dev/null");
        int kafka_up = executeSystemCommand(verify_cmd);

        if (zk_up == -1 || kafka_up == -1) {
            fprintf(stderr,  "Startup verification failed\n");
            exit(EXIT_FAILURE);
        }
    } else if (action == STOP) {
        // Check Zookeeper
        snprintf(verify_cmd, sizeof(verify_cmd),
                 "pgrep -f 'zookeeper' >/dev/null");
        int zk_down = executeSystemCommand(verify_cmd);
        // Check Kafka
        snprintf(verify_cmd, sizeof(verify_cmd),
                 "pgrep -f 'kafka' >/dev/null");
        int kafka_down = executeSystemCommand(verify_cmd);

        if (zk_down == 0 || kafka_down == 0) {
            fprintf(stderr,  "Shutdown verification failed\n");
            exit(EXIT_FAILURE);
        }
    }
    printf( "Kafka service operation completed successfully.\n");
}

void Solr_action(Action a) {
    const char *install_dir = NULL;
    char solr_script[512];
    int status;
    char output[1024];

    // Check SOLR_HOME environment variable first
    const char* solr_home = getenv("SOLR_HOME");
    if (solr_home != NULL) {
        char solr_bin[PATH_MAX];
        snprintf(solr_bin, sizeof(solr_bin), "%s/bin/solr", solr_home);
        if (access(solr_bin, X_OK) == 0) {
            install_dir = solr_home;
        } else {
            fprintf(stderr, "Warning: SOLR_HOME set but solr binary not found at %s\n", solr_bin);
        }
    }

    // If SOLR_HOME not set or invalid, fall back to OS detection
    if (install_dir == NULL) {
        if (access("/etc/debian_version", F_OK) == 0) {
            install_dir = "/usr/local/solr";
        } else if (access("/etc/redhat-release", F_OK) == 0) {
            install_dir = "/opt/solr";
        } else {
            fprintf(stderr, "Error: Unsupported Linux distribution and SOLR_HOME not set\n");
            return;
        }
    }

    // Verify Solr installation
    snprintf(solr_script, sizeof(solr_script), "%s/bin/solr", install_dir);
    if (access(solr_script, X_OK) != 0) {
        fprintf(stderr, "Error: Solr not found at %s\n", solr_script);
        fprintf(stderr, "Checked locations:\n");
        if (solr_home) fprintf(stderr, "- SOLR_HOME: %s\n", solr_home);
        fprintf(stderr, "- /usr/local/solr (Debian/Ubuntu)\n");
        fprintf(stderr, "- /opt/solr (Red Hat/CentOS)\n");
        fprintf(stderr, "Run install_Solr first or check permissions\n");
        return;
    }

    // Execute requested action
    switch(a) {
    case START: {
        char command[512];
        snprintf(command, sizeof(command), "%s start", solr_script);
        status = executeCommandWithOutput(command, output, sizeof(output));
        if (status != 0 || strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
            fprintf(stderr, "Failed to start Solr. Exit code: %d, Output: %s\n", status, output);
        } else {
            printf("Solr started successfully\n");
        }
        break;
    }

    case STOP: {
        char command[512];
        snprintf(command, sizeof(command), "%s stop", solr_script);
        status = executeCommandWithOutput(command, output, sizeof(output));
        if (status != 0 || strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
            fprintf(stderr, "Failed to stop Solr. Exit code: %d, Output: %s\n", status, output);
        } else {
            printf("Solr stopped successfully\n");
        }
        break;
    }

    case RESTART: {
        char command[512];
        snprintf(command, sizeof(command), "%s restart", solr_script);
        status = executeCommandWithOutput(command, output, sizeof(output));
        if (status != 0 || strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
            fprintf(stderr, "Failed to restart Solr. Exit code: %d, Output: %s\n", status, output);
            // Fallback to stop/start sequence
            Solr_action(STOP);
            Solr_action(START);
        } else {
            printf("Solr restarted successfully\n");
        }
        break;
    }

    default:
        fprintf(stderr, "Error: Invalid action specified\n");
        break;
    }
}

#define MAX_CMD_LEN 512

static int execute_script_action(const char *script_path, const char *action, char *output, size_t output_size) {
    char command[MAX_CMD_LEN];
    int ret = snprintf(command, sizeof(command), "%s %s", script_path, action);

    if (ret < 0 || ret >= (int)sizeof(command)) {
        fprintf(stderr,  "Command buffer overflow for action: %s\n", action);
        return -1;
    }

    int exit_code = executeCommandWithOutput(command, output, output_size);
    if (exit_code != 0 || strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
        fprintf(stderr,  "Action '%s' failed with exit code: %d, Output: %s\n", action, exit_code, output);
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
    char output[1024];

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
        fprintf(stderr,  "Phoenix installation not found. Checked:\n"
                "1. PHOENIX_HOME environment variable\n"
                "2. /opt/phoenix\n"
                "3. /usr/local/phoenix\n");
        exit(EXIT_FAILURE);
    }

    switch (a) {
    case START: {
        if (execute_script_action(script_path, "start", output, sizeof(output)) != 0) {
            exit(EXIT_FAILURE);
        }
        printf( "Phoenix service started successfully.\n");
        break;
    }

    case STOP: {
        if (execute_script_action(script_path, "stop", output, sizeof(output)) != 0) {
            exit(EXIT_FAILURE);
        }
        printf( "Phoenix service stopped successfully.\n");
        break;
    }

    case RESTART: {
        // Try to stop, but continue even if it fails
        int stop_status = execute_script_action(script_path, "stop", output, sizeof(output));
        if (stop_status == 0) {
            // Service stopped successfully
        } else {
            fprintf(stderr, "Stop phase completed with issues. Output: %s\n", output);
        }

        if (execute_script_action(script_path, "start", output, sizeof(output)) != 0) {
            exit(EXIT_FAILURE);
        }
        printf( "Phoenix service restarted successfully.\n");
        break;
    }

    default: {
        fprintf(stderr,  "Invalid action. Valid options: START, STOP, RESTART\n");
        exit(EXIT_FAILURE);
    }
    }
}

void ranger_action(Action a) {
    const char *ranger_home = NULL;
    const char *env_home = getenv("RANGER_HOME");
    const char *candidates[] = {env_home, "/opt/ranger", "/usr/local/ranger", NULL};
    char output[1024];

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
        fprintf(stderr,  "Ranger installation not found. Checked locations:\n");
        fprintf(stderr,  "1. RANGER_HOME: %s\n", env_home ? env_home : "(not set)");
        fprintf(stderr,  "2. /opt/ranger\n3. /usr/local/ranger\n");
        exit(EXIT_FAILURE);
    }

    int execute_script(const char *script_name, char *output_buf) {
        char script_path[1024];
        int len = snprintf(script_path, sizeof(script_path), "%s/embeddedwebserver/scripts/%s", ranger_home, script_name);
        if (len < 0 || len >= (int)sizeof(script_path)) {
            fprintf(stderr,  "Path construction error\n");
            return -1;
        }

        if (access(script_path, X_OK) != 0) {
            fprintf(stderr,  "Script %s not found or not executable\n", script_name);
            return -1;
        }

        int status = executeCommandWithOutput(script_path, output_buf, 1024);
        if (status == -1) {
            fprintf(stderr,  "Command execution failed: %s\n", strerror(errno));
            return -1;
        }
        return status;
    }

    void handle_command_result(int status, const char *action, const char *context, const char *output) {
        if (status == -1) exit(EXIT_FAILURE);

        if (WIFEXITED(status)) {
            int exit_code = WEXITSTATUS(status);
            if (exit_code != 0 || strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
                fprintf(stderr,  "Failed to %s Ranger service%s. Exit code: %d, Output: %s\n",
                        action, context, exit_code, output);
                exit(EXIT_FAILURE);
            }
        } else {
            fprintf(stderr,  "Service %s terminated abnormally%s (Signal: %d)\n",
                    action, context, WTERMSIG(status));
            exit(EXIT_FAILURE);
        }
    }

    switch (a) {
    case START: {
        int status = execute_script("ranger-admin-services.sh", output);
        handle_command_result(status, "start", "", output);
        printf("Ranger service started successfully\n");
        break;
    }
    case STOP: {
        int status = execute_script("ranger-admin-services.sh", output);
        handle_command_result(status, "stop", "", output);
        printf("Ranger service stopped successfully\n");
        break;
    }
    case RESTART: {
        int stop_status = execute_script("ranger-admin-services.sh", output);
        handle_command_result(stop_status, "stop", " during restart", output);

        int start_status = execute_script("ranger-admin-services.sh", output);
        handle_command_result(start_status, "start", " during restart", output);
        printf("Ranger service restarted successfully\n");
        break;
    }
    default:
        fprintf(stderr,  "Invalid action specified\n");
        exit(EXIT_FAILURE);
    }
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

int execute_atlas_script(const char *script_path, char *output, size_t output_size) {
    return executeCommandWithOutput(script_path, output, output_size);
}

int atlas_action(Action a) {
    char *atlas_home = get_atlas_home();
    if (!atlas_home) {
        fprintf(stderr,  "Error: Atlas installation not found.\n");
        return -1;
    }

    char script_path[PATH_MAX];
    int result = -1;
    char output[1024];

    switch(a) {
    case START:
        snprintf(script_path, sizeof(script_path), "%s/bin/atlas_start.py", atlas_home);
        result = execute_atlas_script(script_path, output, sizeof(output));
        if (result != 0 || strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
            fprintf(stderr,  "Failed to start Atlas. Exit code: %d, Output: %s\n", result, output);
        } else {
            printf("Apache Atlas started successfully\n");
        }
        break;

    case STOP:
        snprintf(script_path, sizeof(script_path), "%s/bin/atlas_stop.py", atlas_home);
        result = execute_atlas_script(script_path, output, sizeof(output));
        if (result != 0 || strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
            fprintf(stderr,  "Failed to stop Atlas. Exit code: %d, Output: %s\n", result, output);
        } else {
            printf("Apache Atlas stopped successfully\n");
        }
        break;

    case RESTART:
        if ((result = atlas_action(STOP)) == 0) {
            result = atlas_action(START);
            if (result == 0) {
                printf("Apache Atlas restarted successfully\n");
            }
        }
        break;

    default:
        fprintf(stderr,  "Error: Invalid action specified.\n");
    }

    free(atlas_home);

    if (result != 0) {
        fprintf(stderr,  "Error: Action failed. Check installation and permissions.\n");
    }
    return result;
}

static int stop_services(const char *storm_cmd, const char **services, size_t num_services, char *output, size_t output_size) {
    int all_stopped = 1;
    for (size_t i = 0; i < num_services; i++) {
        char cmd[PATH_MAX * 2];
        int ret = snprintf(cmd, sizeof(cmd), "pkill -f '%s %s'", storm_cmd, services[i]);
        if (ret >= (int)sizeof(cmd)) {
            fprintf(stderr,  "Command buffer overflow for service %s\n", services[i]);
            all_stopped = 0;
            continue;
        }

        int status = executeCommandWithOutput(cmd, output, output_size);
        if (WIFEXITED(status)) {
            int exit_code = WEXITSTATUS(status);
            if (exit_code == 0) {
                printf( "Successfully stopped %s\n", services[i]);
            } else if (exit_code == 1) {
                printf( "%s was not running\n", services[i]);
            } else {
                fprintf(stderr,  "Error stopping %s (code %d). Output: %s\n", services[i], exit_code, output);
                all_stopped = 0;
            }
        } else {
            fprintf(stderr,  "Process termination error for %s\n", services[i]);
            all_stopped = 0;
        }
    }
    return all_stopped;
}

static int start_services(const char *storm_cmd, const char **services, size_t num_services, char *output, size_t output_size) {
    int all_started = 1;
    for (size_t i = 0; i < num_services; i++) {
        char cmd[PATH_MAX * 2];
        int ret = snprintf(cmd, sizeof(cmd), "%s %s", storm_cmd, services[i]);
        if (ret >= (int)sizeof(cmd)) {
            fprintf(stderr,  "Command buffer overflow for service %s\n", services[i]);
            all_started = 0;
            continue;
        }

        int status = executeCommandWithOutput(cmd, output, output_size);
        if (WIFEXITED(status)) {
            int exit_code = WEXITSTATUS(status);
            if (exit_code == 0 && strstr(output, "error") == NULL && strstr(output, "ERROR") == NULL) {
                printf("Successfully started %s\n", services[i]);
            } else {
                fprintf(stderr,  "Failed to start %s (exit code %d). Output: %s\n", services[i], exit_code, output);
                all_started = 0;
            }
        } else {
            fprintf(stderr,  "Process termination error for %s\n", services[i]);
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
    char output[1024];

    // 1. Check STORM_HOME environment variable
    char *env_storm_home = getenv("STORM_HOME");
    if (env_storm_home) {
        snprintf(storm_cmd, sizeof(storm_cmd), "%s/bin/storm", env_storm_home);
        if (access(storm_cmd, X_OK) == 0) {
            storm_home = env_storm_home;
        } else {
            fprintf(stderr,  "STORM_HOME set to invalid path: %s\n", env_storm_home);
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
        fprintf(stderr,
                "Storm installation not found. Checked:\n"
                "- STORM_HOME environment variable\n"
                "- OS-specific default locations\n");
        return;
    }

    // Reconstruct storm_cmd for consistency
    snprintf(storm_cmd, sizeof(storm_cmd), "%s/bin/storm", storm_home);
    if (access(storm_cmd, X_OK) != 0) {
        fprintf(stderr,  "Storm executable not accessible: %s\n", storm_cmd);
        return;
    }

    switch(a) {
    case START: {
        int success = start_services(storm_cmd, services, num_services, output, sizeof(output));
        printf( success ? "All services started successfully\n"
                : "Some services failed to start\n");
        break;
    }

    case STOP: {
        int success = stop_services(storm_cmd, services, num_services, output, sizeof(output));
        printf( success ? "All services stopped successfully\n"
                : "Some services failed to stop\n");
        break;
    }

    case RESTART: {
        int stop_success = stop_services(storm_cmd, services, num_services, output, sizeof(output));
        printf( stop_success ? "All services stopped\n" : "Stop phase completed with errors\n");

        sleep(2);

        int start_success = start_services(storm_cmd, services, num_services, output, sizeof(output));
        printf( start_success ? "All services restarted successfully\n"
                : "Restart completed with errors\n");
        break;
    }

    default:
        fprintf(stderr,  "Invalid action specified\n");
    }
}

void flink_action(Action action) {
    const char* flink_home = getenv("FLINK_HOME");
    struct stat dir_info;
    int ret;
    char output[1024];

    // Determine FLINK_HOME if not set in environment
    if (!flink_home) {
        if (stat("/etc/debian_version", &dir_info) == 0) {
            flink_home = "/usr/local/flink";
        } else if (stat("/etc/redhat-release", &dir_info) == 0) {
            flink_home = "/opt/flink";
        } else {
            fprintf(stderr,  "FLINK_HOME not set and OS detection failed\n");
            exit(EXIT_FAILURE);
        }
    }

    // Verify Flink installation directory
    if (stat(flink_home, &dir_info) != 0 || !S_ISDIR(dir_info.st_mode)) {
        fprintf(stderr,  "Flink directory not found: %s\n", flink_home);
        exit(EXIT_FAILURE);
    }

    // Validate cluster scripts
    char start_script[PATH_MAX];
    char stop_script[PATH_MAX];
    snprintf(start_script, sizeof(start_script), "%s/bin/start-cluster.sh", flink_home);
    snprintf(stop_script, sizeof(stop_script), "%s/bin/stop-cluster.sh", flink_home);

    if (access(start_script, X_OK) != 0 || access(stop_script, X_OK) != 0) {
        fprintf(stderr,  "Missing or inaccessible Flink cluster scripts\n");
        exit(EXIT_FAILURE);
    }

    // Execute requested action
    switch(action) {
    case START:
        ret = executeCommandWithOutput(start_script, output, sizeof(output));
        if (ret != 0 || strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
            fprintf(stderr,  "Start failed. Exit code: %d, Output: %s\n", ret, output);
            exit(EXIT_FAILURE);
        }
        printf("Flink started successfully\n");
        break;

    case STOP: {
        ret = executeCommandWithOutput(stop_script, output, sizeof(output));
        if (ret != 0 || strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
            fprintf(stderr,  "Stop failed. Exit code: %d, Output: %s\n", ret, output);
            exit(EXIT_FAILURE);
        }
        printf("Flink stopped successfully\n");
        break;
    }

    case RESTART: {
        // Stop phase
        ret = executeCommandWithOutput(stop_script, output, sizeof(output));
        if (ret != 0 || strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
            fprintf(stderr,  "Restart aborted - stop failed. Exit code: %d, Output: %s\n", ret, output);
            exit(EXIT_FAILURE);
        }

        // Start phase
        ret = executeCommandWithOutput(start_script, output, sizeof(output));
        if (ret != 0 || strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
            fprintf(stderr,  "Restart incomplete - start failed. Exit code: %d, Output: %s\n", ret, output);
            exit(EXIT_FAILURE);
        }
        printf("Flink restarted successfully\n");
        break;
    }

    default:
        fprintf(stderr,  "Invalid action specified\n");
        exit(EXIT_FAILURE);
    }
}

void zookeeper_action(Action a) {
    char *zk_home = getenv("ZOOKEEPER_HOME");
    int isDebian, isRedHat;
    char zk_script[PATH_MAX];
    char command[1024];
    int ret;
    char output[1024];

    // Determine ZOOKEEPER_HOME if not set
    if (zk_home == NULL) {
        isDebian = (access("/etc/debian_version", F_OK) == 0);
        isRedHat = (access("/etc/redhat-release", F_OK) == 0) || (access("/etc/redhat_version", F_OK) == 0);

        if (isDebian) {
            zk_home = "/usr/local/zookeeper";
        } else if (isRedHat) {
            zk_home = "/opt/zookeeper";
        } else {
            fprintf(stderr,  "Error: Unable to detect OS and ZOOKEEPER_HOME is not set.\n");
            return;
        }

        // Verify the determined Zookeeper home directory exists
        if (access(zk_home, F_OK) != 0) {
            fprintf(stderr,  "Error: Zookeeper directory not found at %s\n", zk_home);
            return;
        }
    }

    // Build the path to zkServer.sh
    long unsigned int ret2 = snprintf(zk_script, sizeof(zk_script), "%s/bin/zkServer.sh", zk_home);
    if (ret2 >= sizeof(zk_script)) {
        fprintf(stderr,  "Error: command buffer overflow.\n");
    }

    // Check if the script exists and is executable
    if (access(zk_script, X_OK) != 0) {
        fprintf(stderr,  "Error: zkServer.sh not found or not executable at %s\n", zk_script);
        return;
    }

    // Execute the appropriate action
    switch(a) {
    case START: {
        long unsigned int ret3 = snprintf(command, sizeof(command), "\"%s\" start", zk_script);
        if (ret3 >= sizeof(command)) {
            fprintf(stderr,  "Error: command buffer overflow.\n");
        }
        ret = executeCommandWithOutput(command, output, sizeof(output));
        if (ret != 0 || strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
            fprintf(stderr,  "Failed to start Zookeeper. Exit code: %d, Output: %s\n", ret, output);
        } else {
            printf( "Zookeeper started successfully\n");
        }
        break;
    }
    case STOP: {
        long unsigned int ret4 = snprintf(command, sizeof(command), "\"%s\" stop", zk_script);
        if (ret4 >= sizeof(command)) {
            fprintf(stderr,  "Error: command buffer overflow.\n");
        }
        ret = executeCommandWithOutput(command, output, sizeof(output));
        if (ret != 0 || strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
            fprintf(stderr,  "Failed to stop Zookeeper. Exit code: %d, Output: %s\n", ret, output);
        } else {
            printf( "Zookeeper stopped successfully\n");
        }
        break;
    }
    case RESTART: {
        // Stop the service
        long unsigned int ret5 = snprintf(command, sizeof(command), "\"%s\" stop", zk_script);
        if (ret5 >= sizeof(command)) {
            fprintf(stderr,  "Error: command buffer overflow.\n");
        }
        ret = executeCommandWithOutput(command, output, sizeof(output));
        if (ret != 0 || strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
            fprintf(stderr,  "Failed to stop Zookeeper during restart. Exit code: %d, Output: %s\n", ret, output);
        }

        // Start the service
        long unsigned int ret9 = snprintf(command, sizeof(command), "\"%s\" start", zk_script);
        if (ret9 >= sizeof(command)) {
            fprintf(stderr,  "Error: command buffer overflow.\n");
        }
        ret = executeCommandWithOutput(command, output, sizeof(output));
        if (ret != 0 || strstr(output, "error") != NULL || strstr(output, "ERROR") != NULL) {
            fprintf(stderr,  "Failed to start Zookeeper during restart. Exit code: %d, Output: %s\n", ret, output);
        } else {
            printf( "Zookeeper restarted successfully\n");
        }
        break;
    }
    default:
        fprintf(stderr,  "Error: Invalid action specified.\n");
        break;
    }
}
