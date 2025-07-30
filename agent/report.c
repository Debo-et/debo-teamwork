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
#include <sys/stat.h>
#include <unistd.h>
#include <stdarg.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdbool.h>
#include <limits.h>
#include "utiles.h"



char *report_hdfs() {
    const char *hadoop_home = getenv("HADOOP_HOME");
    const char *paths[] = {"/opt/hadoop", "/usr/local/hadoop"};
    const char *hadoop_dir = NULL;
    struct stat sb;

    // Check HADOOP_HOME environment variable
    if (hadoop_home != NULL && stat(hadoop_home, &sb) == 0 && S_ISDIR(sb.st_mode)) {
        hadoop_dir = hadoop_home;
    } else {
        // Check standard installation paths
        for (int i = 0; i < 2; i++) {
            if (stat(paths[i], &sb) == 0 && S_ISDIR(sb.st_mode)) {
                hadoop_dir = paths[i];
                break;
            }
        }
    }

    if (hadoop_dir == NULL) {
        return strdup("Hadoop installation directory not found");
    }

    // Build hdfs command path
    char command[1024];
    snprintf(command, sizeof(command), "%s/bin/hdfs dfsadmin -report 2>&1", hadoop_dir);

    // Execute command
    FILE *fp = popen(command, "r");
    if (fp == NULL) {
        return strdup("Error executing command");
    }

    // Read command output
    char buffer[4096];
    size_t output_size = 4096;
    char *output = malloc(output_size);
    if (!output) {
        pclose(fp);
        return NULL;
    }
    output[0] = '\0';

    while (fgets(buffer, sizeof(buffer), fp) != NULL) {
        if (strlen(output) + strlen(buffer) >= output_size - 1) {
            // Truncate output if buffer is full
            break;
        }
        strcat(output, buffer);
    }

    // Check command exit status
    int status = pclose(fp);
    if (WIFEXITED(status) && WEXITSTATUS(status) != 0) {
        free(output);
        return strdup("Hadoop is not started");
    }

    return output;
}

static int dir_exists(const char *path) {
    struct stat sb;
    return (stat(path, &sb) == 0 && S_ISDIR(sb.st_mode));
}

char *report_hbase() {
    const char *hbase_home = getenv("HBASE_HOME");
    const char *hbase_dir = NULL;

    // Check HBASE_HOME environment variable
    if (hbase_home != NULL && dir_exists(hbase_home)) {
        hbase_dir = hbase_home;
    } else {
        // Check common installation paths
        if (dir_exists("/opt/hbase")) {
            hbase_dir = "/opt/hbase";
        } else if (dir_exists("/usr/local/hbase")) {
            hbase_dir = "/usr/local/hbase";
        } else {
            return strdup("HBase installation directory not found");
        }
    }

    // Construct command using pipe instead of options
    char command[1024];
    snprintf(command, sizeof(command), 
             "echo \"status\" | %s/bin/hbase shell 2>&1", hbase_dir);

    FILE *fp = popen(command, "r");
    if (!fp) {
        return strdup("Error executing hbase command");
    }

    char buffer[4096];
    size_t output_size = 0;
    char *output = malloc(1);
    if (!output) {
        pclose(fp);
        return strdup("Memory allocation error");
    }
    output[0] = '\0';

    while (fgets(buffer, sizeof(buffer), fp) != NULL) {
        size_t len = strlen(buffer);
        char *new_output = realloc(output, output_size + len + 1);
        if (!new_output) {
            free(output);
            pclose(fp);
            return strdup("Memory allocation error");
        }
        output = new_output;
        strcpy(output + output_size, buffer);
        output_size += len;
    }

    int status = pclose(fp);
    if (WIFEXITED(status)) {
        return output;  // Return output regardless of exit status
    } else {
        free(output);
        return strdup("Error checking HBase status");
    }
}

char *report_hive() {
    const char *hive_home = getenv("HIVE_HOME");
    const char *locations[] = {hive_home, "/opt/hive", "/usr/local/hive"};
    const char *hive_dir = NULL;
    struct stat dir_stat;

    // Check installation directory existence
    for (int i = 0; i < 3; i++) {
        const char *dir = locations[i];
        if (!dir) continue;
        if (stat(dir, &dir_stat) == 0 && S_ISDIR(dir_stat.st_mode)) {
            hive_dir = dir;
            break;
        }
    }

    if (!hive_dir) {
        return strdup("Hive installation directory not found.");
    }

    // Check if Hive processes are running
    FILE *pgrep = popen("pgrep -f 'HiveMetaStore\\|HiveServer2'", "r");
    if (!pgrep) return strdup("Error checking Hive status.");

    char buf[128];
    int running = fgets(buf, sizeof(buf), pgrep) != NULL;
    pclose(pgrep);

    if (!running) {
        return strdup("Hive is not started.");
    }

    // Build path to Hive executable
    char hive_path[1024];
    snprintf(hive_path, sizeof(hive_path), "%s/bin/hive", hive_dir);

    if (access(hive_path, X_OK) != 0) {
        return strdup("Hive executable not found or not executable.");
    }

    // Execute version command
    char cmd[2048];
    snprintf(cmd, sizeof(cmd), "%s --version 2>&1", hive_path);
    FILE *ver = popen(cmd, "r");
    if (!ver) return strdup("Failed to check Hive version.");

    char output[4096] = {0};
    while (fgets(buf, sizeof(buf), ver)) {
        strncat(output, buf, sizeof(output) - strlen(output) - 1);
    }
    pclose(ver);

    return strdup(output);
}


char *report_kafka() {
    const char *kafka_home = getenv("KAFKA_HOME");
    const char *paths[] = {kafka_home, "/opt/kafka", "/usr/local/kafka"};
    const char *install_dir = NULL;
    struct stat st;

    // Determine Kafka installation directory
    for (int i = 0; i < 3; i++) {
        const char *path = paths[i];
        if (path != NULL && stat(path, &st) == 0 && S_ISDIR(st.st_mode)) {
            install_dir = path;
            break;
        }
    }

    if (install_dir == NULL) {
        return strdup("Kafka installation directory not found.");
    }

    // Check if Kafka is running
    int status = system("pgrep -f kafka.Kafka > /dev/null 2>&1");
    if (!(WIFEXITED(status) && WEXITSTATUS(status) == 0)) {
        return strdup("Kafka is not started.");
    }

    // Construct and execute JMX command
    char command[4096];
    size_t cmd_len = snprintf(command, sizeof(command),
                              "%s/bin/kafka-run-class.sh kafka.tools.JmxTool "
                              "--object-name \"kafka.server:type=KafkaServer,name=BrokerState\" "
                              "--attributes Value --one-time true 2>&1",
                              install_dir);

    if (cmd_len >= sizeof(command)) {
        return strdup("Command too long.");
    }

    FILE *fp = popen(command, "r");
    if (!fp) {
        return strdup("Failed to execute command.");
    }

    // Capture command output
    char buffer[1024];
    char *output = NULL;
    size_t total_len = 0;

    while (fgets(buffer, sizeof(buffer), fp)) {
        size_t len = strlen(buffer);
        char *new_output = realloc(output, total_len + len + 1);
        if (!new_output) {
            free(output);
            pclose(fp);
            return strdup("Memory allocation failed.");
        }
        output = new_output;
        memcpy(output + total_len, buffer, len);
        total_len += len;
        output[total_len] = '\0';
    }

    pclose(fp);

    return output ? output : strdup("No output from command.");
}

char *report_livy() {
    const char *livy_home_env = getenv("LIVY_HOME");
    const char *paths[] = {livy_home_env, "/opt/livy", "/usr/local/livy"};
    const char *found_path = NULL;
    struct stat statbuf;

    // Check installation directory
    for (int i = 0; i < 3; i++) {
        if (paths[i] && stat(paths[i], &statbuf) == 0 && S_ISDIR(statbuf.st_mode)) {
            found_path = paths[i];
            break;
        }
    }

    if (!found_path) {
        return strdup("Apache Livy installation directory could not be located.");
    }

    // Check if Livy is running (port 8998)
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        char *msg;
        if (asprintf(&msg, "Apache Livy is installed at %s but is not started.", found_path) == -1)
            FPRINTF(global_client_socket, "Error: Failed to allocate memory for the message.\n");

        return msg;
    }

    struct sockaddr_in server = {
        .sin_family = AF_INET,
        .sin_port = htons(8998),
        .sin_addr.s_addr = inet_addr("127.0.0.1")
    };

    if (connect(sock, (struct sockaddr*)&server, sizeof(server))) {
        close(sock);
        char *msg;
        if (asprintf(&msg, "Apache Livy is installed at %s but is not started.", found_path) == -1)
            FPRINTF(global_client_socket, "Error: Failed to allocate memory for the message.\n");

        return msg;
    }
    close(sock);

    // Execute curl command
    FILE *curl = popen("curl -s -X GET http://localhost:8998/sessions", "r");
    if (!curl) {
        char *msg;
        if (asprintf(&msg, "Apache Livy is running at %s, but failed to execute curl command.", found_path) == -1)
            FPRINTF(global_client_socket, "Error: Failed to allocate memory for the message.\n");
        return msg;
    }

    // Read curl output
    char buffer[128];
    size_t output_size = 0;
    char *output = malloc(1);
    output[0] = '\0';

    while (fgets(buffer, sizeof(buffer), curl)) {
        output_size += strlen(buffer);
        output = realloc(output, output_size + 1);
        strcat(output, buffer);
    }

    pclose(curl);
    return output;
}

char *report_phoenix() {
    const char *install_dir = NULL;
    char *phoenix_home = getenv("PHOENIX_HOME");

    // Check installation directory priority
    if (phoenix_home != NULL && dir_exists(phoenix_home)) {
        install_dir = phoenix_home;
    } else if (dir_exists("/opt/phoenix")) {
        install_dir = "/opt/phoenix";
    } else if (dir_exists("/usr/local/phoenix")) {
        install_dir = "/usr/local/phoenix";
    }

    if (install_dir == NULL) {
        return strdup("Phoenix installation directory not found.");
    }

    // Check Phoenix status
    FILE *fp = popen("phoenix-queryserver status", "r");
    if (!fp) {
        return strdup("Phoenix is not started.");
    }

    char buffer[128];
    char *output = NULL;
    size_t output_size = 0;

    // Read command output
    while (fgets(buffer, sizeof(buffer), fp) != NULL) {
        size_t len = strlen(buffer);
        char *temp = realloc(output, output_size + len + 1);
        if (!temp) {
            free(output);
            pclose(fp);
            return strdup("Error checking Phoenix status.");
        }
        output = temp;
        strcpy(output + output_size, buffer);
        output_size += len;
    }

    int status = pclose(fp);

    // Handle potential empty output
    if (output == NULL) {
        output = malloc(1);
        if (output) {
            *output = '\0';
        } else {
            return strdup("Error checking Phoenix status.");
        }
    }

    // Check command exit status
    if (WIFEXITED(status)) {
        int exit_code = WEXITSTATUS(status);
        if (exit_code != 0) {
            free(output);
            return strdup("Phoenix is not started.");
        }
    } else {
        free(output);
        return strdup("Phoenix is not started.");
    }

    return output;
}


char *report_flink() {
    const char *install_dir = NULL;
    const char *flink_home = getenv("flink_HOME");

    // Check flink_HOME first
    if (flink_home != NULL && dir_exists(flink_home)) {
        install_dir = flink_home;
    } else {
        // Check Red Hat path
        if (dir_exists("/opt/flink")) {
            install_dir = "/opt/flink";
        } else {
            // Check Debian path
            if (dir_exists("/usr/local/flink")) {
                install_dir = "/usr/local/flink";
            } else {
                return strdup("Flink installation directory not found.");
            }
        }
    }

    // Build the command
    char command[1024];
    snprintf(command, sizeof(command), "%s/bin/flink list 2>&1", install_dir);

    // Execute the command
    FILE *fp = popen(command, "r");
    if (!fp) {
        return strdup("Error executing Flink command.");
    }

    // Read the command output
    char buffer[128];
    size_t output_size = 0;
    char *output = NULL;
    while (fgets(buffer, sizeof(buffer), fp) != NULL) {
        size_t len = strlen(buffer);
        char *new_output = realloc(output, output_size + len + 1);
        if (!new_output) {
            free(output);
            pclose(fp);
            return strdup("Memory allocation error.");
        }
        output = new_output;
        strcpy(output + output_size, buffer);
        output_size += len;
    }

    // Check the exit code
    int status = pclose(fp);
    if (WIFEXITED(status)) {
        int exit_code = WEXITSTATUS(status);
        if (exit_code != 0) {
            free(output);
            return strdup("Flink is not started.");
        }
    } else {
        free(output);
        return strdup("Flink is not started.");
    }

    // Handle empty output
    if (output == NULL) {
        return strdup("");
    }

    return output;
}

char *report_storm() {
    const char *storm_home_env = getenv("STORM_HOME");
    const char *possible_paths[] = {
        storm_home_env,
        "/opt/storm",
        "/usr/local/storm",
        NULL  // Sentinel to mark end of array
    };
    struct stat stat_buf;
    const char *storm_home = NULL;

    // Check each possible path in order
    for (int i = 0; possible_paths[i] != NULL; i++) {
        // Skip if STORM_HOME is not set (i=0 and possible_paths[0] is NULL)
        if (possible_paths[i] == NULL) continue;

        if (stat(possible_paths[i], &stat_buf) == 0 && S_ISDIR(stat_buf.st_mode)) {
            storm_home = possible_paths[i];
            break;
        }
    }

    if (storm_home == NULL) {
        return strdup("Storm installation directory not found.");
    }

    // Construct the storm list command with full path
    char command[4096];
    snprintf(command, sizeof(command), "%s/bin/storm list 2>&1", storm_home);

    FILE *fp = popen(command, "r");
    if (fp == NULL) {
        return strdup("Storm is not started.");
    }

    // Read command output into dynamically allocated buffer
    char buffer[128];
    char *output = NULL;
    size_t output_size = 0;

    while (fgets(buffer, sizeof(buffer), fp) != NULL) {
        size_t len = strlen(buffer);
        char *temp = realloc(output, output_size + len + 1);
        if (temp == NULL) {
            pclose(fp);
            free(output);
            return strdup("Storm is not started.");
        }
        output = temp;
        strcpy(output + output_size, buffer);
        output_size += len;
    }

    // Check command exit status
    int status = pclose(fp);
    if (WIFEXITED(status)) {
        int exit_code = WEXITSTATUS(status);
        if (exit_code == 0) {
            // Command succeeded, return the output (handle empty case)
            if (output == NULL) {
                return strdup("");
            } else {
                return output;
            }
        }
    }

    // Command failed or did not exit normally
    free(output);
    return strdup("Storm is not started.");
}


static int check_pig_running() {
    FILE *fp = popen("pgrep pig", "r");
    if (!fp) return 0;
    char buffer[128];
    int found = (fgets(buffer, sizeof(buffer), fp) != NULL);
    pclose(fp);
    return found;
}

char *report_pig() {
    const char *pig_home = getenv("PIG_HOME");
    const char *paths[] = {pig_home, "/opt/pig", "/usr/local/pig"};
    const char *install_dir = NULL;

    for (int i = 0; i < 3; ++i) {
        const char *path = paths[i];
        if (path && dir_exists(path)) {
            install_dir = path;
            break;
        }
    }

    if (!install_dir) {
        return strdup("Pig installation directory not found.");
    }

    if (!check_pig_running()) {
        char *msg = malloc(strlen(install_dir) + 100);
        if (!msg) return strdup("Memory allocation error.");
        sprintf(msg, "Pig is installed at %s but not started.", install_dir);
        return msg;
    }

    char command[1024];
    snprintf(command, sizeof(command), "%s/bin/pig -version", install_dir);
    FILE *fp = popen(command, "r");
    if (!fp) {
        return strdup("Failed to execute pig -version.");
    }

    char buffer[128];
    size_t output_size = 0;
    char *output = NULL;

    while (fgets(buffer, sizeof(buffer), fp)) {
        size_t buffer_len = strlen(buffer);
        char *new_output = realloc(output, output_size + buffer_len + 1);
        if (!new_output) {
            free(output);
            pclose(fp);
            return strdup("Memory allocation error.");
        }
        output = new_output;
        strcpy(output + output_size, buffer);
        output_size += buffer_len;
    }

    pclose(fp);

    if (!output) {
        return strdup("Failed to retrieve pig version.");
    }

    return output;
}


char* report_presto() {
    const char* presto_home = getenv("presto_HOME");
    struct stat st;
    char* install_dir = NULL;

    // Locate Presto installation directory
    if (presto_home != NULL && stat(presto_home, &st) == 0 && S_ISDIR(st.st_mode)) {
        install_dir = strdup(presto_home);
    } else if (stat("/opt/presto", &st) == 0 && S_ISDIR(st.st_mode)) {
        install_dir = strdup("/opt/presto");
    } else if (stat("/usr/local/presto", &st) == 0 && S_ISDIR(st.st_mode)) {
        install_dir = strdup("/usr/local/presto");
    } else {
        return strdup("Presto installation directory not found");
    }

    // Verify CLI executable exists
    char cli_path[PATH_MAX];
    snprintf(cli_path, sizeof(cli_path), "%s/bin/presto", install_dir);
    if (access(cli_path, X_OK) != 0) {
        free(install_dir);
        return strdup("Presto CLI not found in installation directory");
    }
    free(install_dir);

    // Check if Presto is running (port 8080)
    int is_running = 0;
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock >= 0) {
        struct sockaddr_in addr = {
            .sin_family = AF_INET,
            .sin_port = htons(8080),
            .sin_addr.s_addr = inet_addr("127.0.0.1")
        };
        if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) == 0) {
            is_running = 1;
        }
        close(sock);
    }

    if (!is_running) {
        return strdup("Presto is not started");
    }

    // Execute Presto command
    char cmd[PATH_MAX + 50];
    snprintf(cmd, sizeof(cmd), "\"%s\" --execute \"SHOW NODES;\"", cli_path);
    FILE* cmd_fp = popen(cmd, "r");
    if (!cmd_fp) {
        return strdup("Failed to execute Presto command");
    }

    // Capture command output
    char buffer[1024];
    char* result = NULL;
    size_t result_size = 0;
    while (fgets(buffer, sizeof(buffer), cmd_fp) != NULL) {
        size_t len = strlen(buffer);
        char* new_result = realloc(result, result_size + len + 1);
        if (!new_result) {
            free(result);
            pclose(cmd_fp);
            return strdup("Memory allocation error");
        }
        result = new_result;
        strcpy(result + result_size, buffer);
        result_size += len;
    }
    pclose(cmd_fp);

    return result ? result : strdup("No output from Presto");
}


char *report_atlas() {
    const char *env_path = getenv("atlas_HOME");
    const char *paths[] = {"/opt/atlas", "/usr/local/atlas"};
    const char *found_path = NULL;
    struct stat st;

    // Check atlas_HOME environment variable
    if (env_path != NULL) {
        if (stat(env_path, &st) == 0 && S_ISDIR(st.st_mode)) {
            found_path = env_path;
        }
    }

    // Check standard installation paths if not found in atlas_HOME
    if (found_path == NULL) {
        for (int i = 0; i < 2; i++) {
            if (stat(paths[i], &st) == 0 && S_ISDIR(st.st_mode)) {
                found_path = paths[i];
                break;
            }
        }
    }

    // Return if installation directory not found
    if (found_path == NULL) {
        char *result = malloc(strlen("Atlas installation directory not found.") + 1);
        if (result != NULL) {
            strcpy(result, "Atlas installation directory not found.");
        }
        return result;
    }

    // Check if Atlas service is running by connecting to port 21000
    bool is_running = false;
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock >= 0) {
        struct sockaddr_in server;
        memset(&server, 0, sizeof(server));
        server.sin_family = AF_INET;
        server.sin_port = htons(21000);
        if (inet_pton(AF_INET, "127.0.0.1", &server.sin_addr) > 0) {
            if (connect(sock, (struct sockaddr *)&server, sizeof(server)) == 0) {
                is_running = true;
            }
        }
        close(sock);
    }

    if (!is_running) {
        char *result = malloc(strlen("Atlas is not started.") + 1);
        if (result != NULL) {
            strcpy(result, "Atlas is not started.");
        }
        return result;
    }

    // Execute curl command to retrieve entity bulk data
    FILE *curl_output = popen("curl -s -u admin:admin -X GET http://localhost:21000/api/atlas/v2/entity/bulk", "r");
    if (curl_output == NULL) {
        char *result = malloc(strlen("Error executing curl command.") + 1);
        if (result != NULL) {
            strcpy(result, "Error executing curl command.");
        }
        return result;
    }

    // Read the curl command output
    char buffer[128];
    size_t output_size = 0;
    char *output = NULL;
    size_t bytes_read;

    while ((bytes_read = fread(buffer, 1, sizeof(buffer), curl_output)) > 0) {
        char *new_output = realloc(output, output_size + bytes_read + 1);
        if (new_output == NULL) {
            free(output);
            pclose(curl_output);
            return NULL;
        }
        output = new_output;
        memcpy(output + output_size, buffer, bytes_read);
        output_size += bytes_read;
    }

    pclose(curl_output);

    if (output == NULL) {
        output = malloc(1);
        if (output == NULL) {
            return NULL;
        }
        output[0] = '\0';
    } else {
        output = realloc(output, output_size + 1);
        if (output != NULL) {
            output[output_size] = '\0';
        }
    }

    return output;
}


int check_process_running() {
    int status = system("pgrep -f ranger-admin > /dev/null 2>&1");
    if (status == -1) return 0;
    return WEXITSTATUS(status) == 0;
}

char *report_ranger() {
    const char *ranger_home = getenv("RANGER_HOME");
    char *install_dir = NULL;

    // Check RANGER_HOME
    if (ranger_home && dir_exists(ranger_home)) {
        install_dir = strdup(ranger_home);
    } else {
        // Check standard paths
        const char *paths[] = {"/opt/ranger", "/usr/local/ranger"};
        for (size_t i = 0; i < sizeof(paths)/sizeof(paths[0]); i++) {
            if (dir_exists(paths[i])) {
                install_dir = strdup(paths[i]);
                break;
            }
        }
    }

    if (!install_dir) return strdup("Apache Ranger installation directory not found.");

    // Check if service is running
    int is_running = check_process_running();
    if (!is_running) {
        char *result = malloc(strlen(install_dir) + 50);
        sprintf(result, "Apache Ranger is installed at %s but is not started.", install_dir);
        free(install_dir);
        return result;
    }

    // Execute status command
    char command[PATH_MAX];
    snprintf(command, sizeof(command), "sudo %s/bin/ranger-admin status", install_dir);
    FILE *fp = popen(command, "r");
    free(install_dir);

    if (!fp) return strdup("Error executing status command.");

    char buffer[128];
    char *output = NULL;
    size_t output_size = 0;

    while (fgets(buffer, sizeof(buffer), fp)) {
        size_t len = strlen(buffer);
        char *temp = realloc(output, output_size + len + 1);
        if (!temp) {
            free(output);
            pclose(fp);
            return strdup("Memory allocation error.");
        }
        output = temp;
        memcpy(output + output_size, buffer, len);
        output_size += len;
        output[output_size] = '\0';
    }

    if (pclose(fp) != 0) {
        free(output);
        return strdup("Error getting service status.");
    }

    return output ? output : strdup("No output from status command.");
}



char *report_solr() {
    const char *solr_home = getenv("SOLR_HOME");
    const char *paths[] = {solr_home, "/opt/solr", "/usr/local/solr"};
    const int num_paths = 3;
    struct stat st;
    int found = 0;

    // Check each possible installation path
    for (int i = 0; i < num_paths; ++i) {
        const char *path = paths[i];
        if (path == NULL) continue; // Skip if SOLR_HOME is not set
        if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) {
            found = 1;
            break;
        }
    }

    if (!found) {
        return strdup("Solr installation not found.");
    }

    // Check if Solr is running by accessing the admin interface
    int check = system("curl --silent --fail -o /dev/null http://localhost:8983/solr/ >/dev/null 2>&1");
    if (check != 0) {
        return strdup("Solr is not started.");
    }

    // Execute curl to get cluster status
    FILE *fp = popen("curl -s 'http://localhost:8983/solr/admin/collections?action=CLUSTERSTATUS'", "r");
    if (!fp) {
        return strdup("Error executing curl command.");
    }

    char buffer[4096];
    char *output = NULL;
    size_t total_size = 0;
    size_t buffer_len;

    while (fgets(buffer, sizeof(buffer), fp) != NULL) {
        buffer_len = strlen(buffer);
        char *temp = realloc(output, total_size + buffer_len + 1);
        if (!temp) {
            free(output);
            pclose(fp);
            return strdup("Memory allocation failed.");
        }
        output = temp;
        memcpy(output + total_size, buffer, buffer_len);
        total_size += buffer_len;
        output[total_size] = '\0';
    }

    int status = pclose(fp);
    if (status != 0) {
        free(output);
        return strdup("Failed to retrieve cluster status.");
    }

    if (output == NULL) {
        return strdup("No output from cluster status.");
    }

    return output;
}


char *report_spark() {
    const char *dir = NULL;
    char *spark_home = getenv("spark_HOME");
    char spark_shell_path[4096]; // Use a reasonable buffer size

    // Determine the Spark installation directory
    if (spark_home != NULL && access(spark_home, F_OK) == 0) {
        dir = spark_home;
    } else if (access("/opt/spark", F_OK) == 0) {
        dir = "/opt/spark";
    } else if (access("/usr/local/spark", F_OK) == 0) {
        dir = "/usr/local/spark";
    } else {
        return strdup("Spark installation directory not found.");
    }

    // Check if spark-shell exists and is executable
    snprintf(spark_shell_path, sizeof(spark_shell_path), "%s/bin/spark-shell", dir);
    if (access(spark_shell_path, X_OK) != 0) {
        return strdup("Spark installation directory found but spark-shell is missing or not executable.");
    }

    // Check if Spark processes are running
    int is_running = 0;
    int ret = system("pgrep -f spark > /dev/null 2>&1");
    if (ret != -1 && WIFEXITED(ret)) {
        is_running = (WEXITSTATUS(ret) == 0);
    }

    if (!is_running) {
        return strdup("Spark is not started.");
    }

    // Execute spark-shell command and capture output
    char command[8192]; // Sufficient size for command
    snprintf(command, sizeof(command), "%s --master yarn", spark_shell_path);
    FILE *fp = popen(command, "r");
    if (!fp) {
        return strdup("Error executing spark-shell.");
    }

    char buffer[1024];
    char *output = NULL;
    size_t output_size = 0;
    size_t buffer_len;

    while (fgets(buffer, sizeof(buffer), fp) != NULL) {
        buffer_len = strlen(buffer);
        char *new_output = realloc(output, output_size + buffer_len + 1);
        if (!new_output) {
            free(output);
            pclose(fp);
            return strdup("Memory allocation error.");
        }
        output = new_output;
        memcpy(output + output_size, buffer, buffer_len + 1);
        output_size += buffer_len;
    }

    // Check if pclose encountered an error
    int exit_status = pclose(fp);
    if (exit_status == -1) {
        free(output);
        return strdup("Error closing command stream.");
    }

    if (output == NULL) {
        return strdup("No output from spark-shell.");
    }

    return output;
}



char *report_tez() {
    const char *tez_home = getenv("TEZ_HOME");
    const char *paths[] = {tez_home, "/opt/tez", "/usr/local/tez"};
    struct stat st;
    int found = 0;

    // Check TEZ installation directory
    for (int i = 0; i < 3; i++) {
        const char *path = paths[i];
        if (i == 0 && path == NULL) continue; // Skip TEZ_HOME if not set
        if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) {
            found = 1;
            break;
        }
    }

    if (!found) {
        return strdup("TEZ installation directory not found");
    }

    // Check if TEZ is running
    FILE *fp = popen("yarn application -list -appTypes TEZ 2>&1", "r");
    if (!fp) {
        return strdup("Error executing yarn command");
    }

    char buffer[128];
    size_t output_size = 0;
    char *output = NULL;

    while (fgets(buffer, sizeof(buffer), fp) != NULL) {
        size_t len = strlen(buffer);
        char *temp = realloc(output, output_size + len + 1);
        if (!temp) {
            free(output);
            pclose(fp);
            return strdup("Memory allocation error");
        }
        output = temp;
        memcpy(output + output_size, buffer, len);
        output_size += len;
        output[output_size] = '\0';
    }

    int status = pclose(fp);

    // Handle command execution errors
    if (status != 0) {
        free(output);
        return strdup("TEZ is not started");
    }

    // Handle empty output
    if (output == NULL || output[0] == '\0') {
        free(output);
        return strdup("TEZ is not started");
    }

    // Count newlines to determine if there are application lines
    int lines = 0;
    for (char *p = output; *p; p++) {
        if (*p == '\n') {
            lines++;
        }
    }

    // TEZ is considered running if there are more than one line (header + data)
    if (lines > 1) {
        return output;
    } else {
        free(output);
        return strdup("TEZ is not started");
    }
}

char *report_zeppelin() {
    struct stat st;
    const char *install_dir = NULL;
    char *zeppelin_home = getenv("ZEPPELIN_HOME");

    // Check ZEPPELIN_HOME environment variable (fixed case sensitivity)
    if (zeppelin_home != NULL) {
        if (stat(zeppelin_home, &st) == 0 && S_ISDIR(st.st_mode)) {
            install_dir = zeppelin_home;
        }
    }

    // Check Red Hat-based path if not found
    if (install_dir == NULL) {
        if (stat("/opt/zeppelin", &st) == 0 && S_ISDIR(st.st_mode)) {
            install_dir = "/opt/zeppelin";
        }
    }

    // Check Debian-based path if still not found
    if (install_dir == NULL) {
        if (stat("/usr/local/zeppelin", &st) == 0 && S_ISDIR(st.st_mode)) {
            install_dir = "/usr/local/zeppelin";
        }
    }

    // Return if installation directory not found
    if (install_dir == NULL) {
        return strdup("Zeppelin installation directory not found.");
    }

    // Construct the status command
    char command[1024];
    snprintf(command, sizeof(command), "%s/bin/zeppelin-daemon.sh status 2>&1", install_dir);

    // Execute the command and capture output
    FILE *fp = popen(command, "r");
    if (!fp) {
        return strdup("Error checking Zeppelin status.");
    }

    // Read the command output
    char output[4096] = {0};
    char buffer[128];
    while (fgets(buffer, sizeof(buffer), fp) != NULL) {
        strcat(output, buffer);
    }

    // Close pipe and return output regardless of exit status
    pclose(fp);
    return strdup(output);
}


char *report_zookeeper() {
    // Corrected environment variable name (ZOOKEEPER_HOME instead of ZOOKEPER_HOME)
    const char *env_home = getenv("ZOOKEEPER_HOME");
    // Fixed directory spellings (zookeeper instead of zookeper)
    const char *paths[] = {env_home, "/opt/zookeeper", "/usr/local/zookeeper"};
    struct stat st;
    const char *install_dir = NULL;

    // Check for installation directory
    for (int i = 0; i < 3; i++) {
        const char *path = paths[i];
        if (path == NULL) continue;
        if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) {
            install_dir = path;
            break;
        }
    }

    if (install_dir == NULL) {
        // Corrected spelling in error message
        return strdup("ZooKeeper installation not found.");
    }

    // Check if ZooKeeper is running (class name already correct)
    FILE *fp = popen("jps -l 2>/dev/null", "r");
    if (fp == NULL) {
        return strdup("Error checking ZooKeeper status.");
    }

    int is_running = 0;
    char line[256];
    while (fgets(line, sizeof(line), fp) != NULL) {
        if (strstr(line, "org.apache.zookeeper.server.quorum.QuorumPeerMain") != NULL) {
            is_running = 1;
            break;
        }
    }
    pclose(fp);

    if (!is_running) {
        // Corrected spelling in error message
        return strdup("ZooKeeper is not started.");
    }

    // Execute zkCli.sh command (uses corrected install_dir path)
    char command[1024];
    snprintf(command, sizeof(command), "echo 'ls /' | %s/bin/zkCli.sh -server localhost:2181 2>&1", install_dir);

    fp = popen(command, "r");
    if (fp == NULL) {
        return strdup("Error executing zkCli.sh.");
    }

    // Capture command output
    char *output = NULL;
    size_t output_size = 0;
    char buffer[1024];
    while (fgets(buffer, sizeof(buffer), fp) != NULL) {
        size_t len = strlen(buffer);
        char *new_output = realloc(output, output_size + len + 1);
        if (!new_output) {
            free(output);
            pclose(fp);
            return strdup("Memory allocation error.");
        }
        output = new_output;
        memcpy(output + output_size, buffer, len + 1);
        output_size += len;
    }

    int exit_status = pclose(fp);
    if (exit_status != 0) {
        free(output);
        return strdup("Failed to execute zkCli.sh command.");
    }

    if (output == NULL) {
        return strdup("No output from zkCli.sh.");
    }

    return output;
}
