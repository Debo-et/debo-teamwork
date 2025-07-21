#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <limits.h>
#include <stdbool.h>
#include <dirent.h>
#include <glob.h>
#include <pwd.h>

#include "utiles.h"
#include "action.h"

#define MAX_LINE 1024
#define MAX_ATTEMPTS 3
#define MAX_PATHS 10
#define CONFIG_FILES_MAX 5

void uninstall_hadoop() {
    char *hadoop_home = getenv("HADOOP_HOME");
    char install_path[PATH_MAX] = {0};
    char config_files[][PATH_MAX] = {".bashrc"};  // Only target .bashrc
    int found_installation = 0;

    // Step 1: Identify installation location
    if (hadoop_home && access(hadoop_home, F_OK) == 0) {
        strncpy(install_path, hadoop_home, PATH_MAX - 1);
        found_installation = 1;
    } else {
        const char *potential_paths[] = {
            "/usr/local/hadoop",
            "/opt/hadoop",
            "/usr/share/hadoop",
            getenv("HOME") ? strcat(strcpy(install_path, getenv("HOME")), "/hadoop") : NULL
        };

        for (size_t i = 0; i < sizeof(potential_paths)/sizeof(potential_paths[0]); i++) {
            if (potential_paths[i] && access(potential_paths[i], F_OK) == 0) {
                strncpy(install_path, potential_paths[i], PATH_MAX - 1);
                found_installation = 1;
                break;
            }
        }
    }

    if (!found_installation) {
        FPRINTF(global_client_socket, "Error: Hadoop installation not found\n");
        return;
    }

    // Step 2: Remove installation directory
    char rm_cmd[PATH_MAX + 50];
    long unsigned int ret = snprintf(rm_cmd, sizeof(rm_cmd), "sudo rm -rf \"%s\"", install_path);
    if (ret >= sizeof(rm_cmd)) {
        FPRINTF(global_client_socket, "Error: command buffer overflow\n");
        return;
    }
    if (!executeSystemCommand(rm_cmd)) {
        FPRINTF(global_client_socket, "Warning: Failed to remove installation directory\n");
    }

    // Step 3: Clean environment variables from .bashrc only
    for (size_t i = 0; i < sizeof(config_files)/sizeof(config_files[0]); i++) {
        char path[PATH_MAX];
        long unsigned int ret2 = snprintf(path, sizeof(path), "%s/%s", getenv("HOME") ?: "", config_files[i]);
        if (ret2 >= sizeof(path)) {
            FPRINTF(global_client_socket, "Error: path buffer overflow\n");
            return;
        }

        FILE *src = fopen(path, "r");
        if (!src) continue;

        char temp_path[] = "/tmp/hadoop_uninstall_XXXXXX";
        int fd = mkstemp(temp_path);
        FILE *dst = fdopen(fd, "w");

        char line[MAX_LINE];
        int modified = 0;

        while (fgets(line, MAX_LINE, src)) {
            if (strstr(line, "HADOOP_HOME") ||
                strstr(line, "HADOOP_CONF_DIR") ||
                strstr(line, "HADOOP_HOME/bin")) {
                modified = 1;
                continue;
            }
            fputs(line, dst);
        }

        fclose(src);
        fclose(dst);

        if (modified) {
            // Create backup
            char backup_cmd[PATH_MAX + 50];
            long unsigned int ret3 = snprintf(backup_cmd, sizeof(backup_cmd), "cp %s %s.bak", path, path);
            if (ret3 >= sizeof(backup_cmd)) {
                FPRINTF(global_client_socket, "Error: backup command buffer overflow\n");
                remove(temp_path);
                return;
            }
            executeSystemCommand(backup_cmd);

            // Apply changes
            char apply_cmd[PATH_MAX + 50];
            long unsigned int ret4 = snprintf(apply_cmd, sizeof(apply_cmd), "mv %s %s", temp_path, path);
            if (ret4 >= sizeof(apply_cmd)) {
                FPRINTF(global_client_socket, "Error: apply command buffer overflow\n");
                remove(temp_path);
                return;
            }
            if (!executeSystemCommand(apply_cmd)) {
                FPRINTF(global_client_socket, "Environment variable not Successfully updated %s\n", config_files[i]);
            }
        } else {
            remove(temp_path);
        }
    }

    PRINTF(global_client_socket, "Hadoop uninstallation completed: Installation directory removed and environment variables cleaned from .bashrc\n");
}

void uninstall_Presto() {
    char command[1024];
    int ret;
    const char* install_dir = NULL;
    struct stat st = {0};

    // 1. Stop Presto service if running
    //PRINTF(global_client_socket,"Stopping Presto service...\n");
    int result = executeSystemCommand("sudo pkill -f presto-server || true");
    if (result != 0) {
        PRINTF(global_client_socket,"Command failed with return code %d\n", result);
    }

    // 2. Determine installation location
    if (access("/etc/debian_version", F_OK) == 0) {
        install_dir = "/usr/local/presto";
    } else if (access("/etc/redhat-release", F_OK) == 0) {
        install_dir = "/opt/presto";
    } else {
        FPRINTF(global_client_socket, "Warning: Unknown distribution - trying default locations\n");
        install_dir = "/usr/local/presto"; // Fallback
    }

    // 3. Remove installation directory
    //PRINTF(global_client_socket,"Removing installation directory...\n");
    snprintf(command, sizeof(command), "sudo rm -rf %s", install_dir);
    if ((ret = !executeSystemCommand(command))) {
        FPRINTF(global_client_socket, "Warning: Failed to remove %s (error %d)\n", install_dir, ret);
    }

    // 4. Remove data directories
    const char *data_dirs[] = {
        "/var/lib/presto",
        "/var/log/presto",
        "/etc/presto",
        "/tmp/presto"
    };

    for (size_t i = 0; i < sizeof(data_dirs)/sizeof(data_dirs[0]); i++) {
        if (stat(data_dirs[i], &st) == 0) {
            //PRINTF(global_client_socket,"Removing data directory: %s\n", data_dirs[i]);
            snprintf(command, sizeof(command), "sudo rm -rf %s", data_dirs[i]);
            if (!executeSystemCommand(command)) {
                FPRINTF(global_client_socket, "Warning: Failed to remove %s\n", data_dirs[i]);
            }
        }
    }

    // 5. Clean environment variables
    //PRINTF(global_client_socket,"Cleaning environment configuration...\n");
    char* home = getenv("HOME");
    if (home) {
        char bashrc[256];
        snprintf(bashrc, sizeof(bashrc), "%s/.bashrc", home);

        // Remove PRESTO_HOME entries
        snprintf(command, sizeof(command),
                 "sudo sed -i.bak '/export PRESTO_HOME=/d' %s", bashrc);
        int result = executeSystemCommand(command);
        if (result != 0) {
            PRINTF(global_client_socket,"Command failed with return code %d\n", result);
        }

        // Remove PATH modifications
        snprintf(command, sizeof(command),
                 "sudo sed -i.bak '/PRESTO_HOME\\/bin/d' %s", bashrc);
        int resultcmd = executeSystemCommand(command);
        if (resultcmd != 0) {
            PRINTF(global_client_socket,"Command failed with return code %d\n", resultcmd);
        }
    }

    // 6. Remove systemd service if exists
    if (stat("/etc/systemd/system/presto.service", &st) == 0) {
        //PRINTF(global_client_socket,"Removing systemd service...\n");
        int resultStart = executeSystemCommand("sudo systemctl stop presto.service");
        if (resultStart != 0) {
            PRINTF(global_client_socket,"Command failed with return code %d\n", resultStart);
        }
        int resultRm = executeSystemCommand("sudo rm -f /etc/systemd/system/presto.service");
        if (resultRm != 0) {
            PRINTF(global_client_socket,"Command failed with return code %d\n", resultRm);
        }
        int resultRelod = executeSystemCommand("sudo systemctl daemon-reload");
        if (resultRelod != 0) {
            PRINTF(global_client_socket,"Command failed with return code %d\n", resultRelod);
        }
    }

    // 7. Clean package manager artifacts
    if (access("/etc/debian_version", F_OK) == 0) {
        int resultPurg = executeSystemCommand("sudo apt-get purge presto -y 2>/dev/null || true");
        if (resultPurg != 0) {
            PRINTF(global_client_socket,"Command failed with return code %d\n", resultPurg);
        }
    } else if (access("/etc/redhat-release", F_OK) == 0) {
        int resultRele = executeSystemCommand("sudo yum remove presto -y 2>/dev/null || true");
        if (resultRele != 0) {
            PRINTF(global_client_socket,"Command failed with return code %d\n", resultRele);
        }
    }

    // 8. Remove temporary files
    int resultRm = executeSystemCommand("sudo rm -rf /tmp/presto*");
    if (resultRm != 0) {
        PRINTF(global_client_socket,"Command failed with return code %d\n", resultRm);
    }

    // 9. Remove cron entries
    int resultCron = executeSystemCommand("sudo crontab -l | grep -v presto | sudo crontab -");
    if (resultCron != 0) {
        PRINTF(global_client_socket,"Command failed with return code %d\n", resultCron);
    }

    PRINTF(global_client_socket,"\nPresto uninstallation complete.\n");
    //PRINTF(global_client_socket,"Note: Some user-specific files may need manual removal.\n");
}


void uninstall_spark() {
    char install_path[512] = {0};
    char spark_home[512] = {0};

    // Check SPARK_HOME environment variable first
    const char* env_home = getenv("SPARK_HOME");
    if (env_home && access(env_home, F_OK) == 0) {
        strncpy(spark_home, env_home, sizeof(spark_home)-1);
    }

    // Detect OS-based installation path
    if (access("/etc/debian_version", F_OK) == 0) {
        strncpy(install_path, "/usr/local/spark", sizeof(install_path)-1);
    } else if (access("/etc/redhat-release", F_OK) == 0 ||
               access("/etc/system-release", F_OK) == 0) {
        strncpy(install_path, "/opt/spark", sizeof(install_path)-1);
    }

    // 3. Remove installation directories
    //PRINTF(global_client_socket,"[3/5] Removing installation files...\n");
    char rm_cmd[1024];
    int paths_removed = 0;

    // Remove SPARK_HOME location if different from default
    if (spark_home[0] != '\0' && strcmp(spark_home, install_path) != 0) {
        snprintf(rm_cmd, sizeof(rm_cmd), "sudo rm -rf \"%s\"", spark_home);
        if (executeSystemCommand(rm_cmd)) paths_removed++;
    }

    // Remove default installation path
    if (install_path[0] != '\0') {
        snprintf(rm_cmd, sizeof(rm_cmd), "sudo rm -rf \"%s\"", install_path);
        if (executeSystemCommand(rm_cmd)) paths_removed++;
    }

    // 4. Clean environment configurations
    //PRINTF(global_client_socket,"[4/5] Cleaning environment settings...\n");
    const char* shell_files[] = {
        ".bashrc", ".bash_profile", ".profile", ".zshrc", ".cshrc", NULL
    };

    const char* home = getenv("HOME");
    if (home) {
        for (int i = 0; shell_files[i]; i++) {
            char shell_path[512];
            long unsigned int written_shell= snprintf(shell_path, sizeof(shell_path), "%s/%s", home, shell_files[i]);
            if (written_shell >= sizeof(shell_path)) {
                // Handle truncation
                FPRINTF(global_client_socket, "Warning: Truncated output.\n");
            }

            // Create temp file for filtering
            char temp_path[512];
            long unsigned int written =  snprintf(temp_path, sizeof(temp_path), "%s.tmp", shell_path);
            if (written >= sizeof(temp_path)) {
                // Handle truncation
                FPRINTF(global_client_socket, "Warning: Truncated output.\n");
            }

            FILE *orig = fopen(shell_path, "r");
            if (orig) {
                FILE *temp = fopen(temp_path, "w");
                if (temp) {
                    char line[1024];
                    int modified = 0;

                    while (fgets(line, sizeof(line), orig)) {
                        if (strstr(line, "SPARK_HOME") ||
                            strstr(line, "spark-/sbin") ||
                            strstr(line, "spark/bin")) {
                            modified = 1;
                            continue;
                        }
                        fputs(line, temp);
                    }

                    fclose(orig);
                    fclose(temp);

                    if (modified) {
                        char backup_cmd[512];
                        long unsigned int written =  snprintf(backup_cmd, sizeof(backup_cmd), "cp %s %s.bak", shell_path, shell_path);
                        if (written >= sizeof(backup_cmd)) {
                            // Handle truncation
                            FPRINTF(global_client_socket, "Warning: Truncated output.\n");
                        }
                        int result = executeSystemCommand(backup_cmd);
                        if (result == -1) {
                            PRINTF(global_client_socket,"Command failed with return code %d\n", result);
                        }

                        rename(temp_path, shell_path);
                        //PRINTF(global_client_socket,"Cleaned Spark references from %s\n", shell_files[i]);
                    } else {
                        remove(temp_path);
                    }
                }
            }
        }
    }

    // 5. Remove downloaded archives
    // PRINTF(global_client_socket,"[5/5] Cleaning residual files...\n");
    int resultRm =  executeSystemCommand("rm -f spark-*-bin-hadoop3.tgz");
    if (resultRm == -1) {
        PRINTF(global_client_socket,"Command failed with return code %d\n", resultRm);
    }
    int resultRmrf =  executeSystemCommand("rm -rf /tmp/spark-*");
    if (resultRmrf == -1) {
        PRINTF(global_client_socket,"Command failed with return code %d\n", resultRmrf);
    }

    // Verification
    PRINTF(global_client_socket,"\nUninstallation complete. Verification:\n");
    int remains = 0;

    if (spark_home[0] != '\0' && access(spark_home, F_OK) == 0) remains++;
    if (install_path[0] != '\0' && access(install_path, F_OK) == 0) remains++;

    if (remains) {
        FPRINTF(global_client_socket, "Warning: Some Spark components could not be removed\n");
        FPRINTF(global_client_socket, "You may need to manually remove:\n");
        if (spark_home[0] != '\0') FPRINTF(global_client_socket, "- %s\n", spark_home);
        if (install_path[0] != '\0') FPRINTF(global_client_socket, "- %s\n", install_path);
    } else {
        PRINTF(global_client_socket,"Verification successful: All Spark components removed\n");
    }

    //  PRINTF(global_client_socket,"Please restart your shell session to apply environment changes\n");
}


void uninstall_hive() {
    char *home_dir = getenv("HOME");
    const char *bashrc_paths[] = {".bashrc", ".bash_profile", ".profile"};
    int found_hive_vars = 0;

    // Phase 1: Locate Hive installation directory
    char *hive_install_dir = NULL;
    const char *possible_paths[] = {getenv("HIVE_HOME"), "/opt/hive", "/usr/local/hive"};
    struct stat st;

    for (int i = 0; i < 3; i++) {
        const char *path = possible_paths[i];
        if (!path) continue;

        if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) {
            hive_install_dir = strdup(path);
            break;
        }
    }

    if (!hive_install_dir) {
        FPRINTF(global_client_socket, "Error: Hive installation directory not found\n");
        exit(EXIT_FAILURE);
    }

    // Phase 2: Remove installation directory
    char rm_cmd[512];
    snprintf(rm_cmd, sizeof(rm_cmd), "sudo rm -rf \"%s\"", hive_install_dir);
    int ret = executeSystemCommand(rm_cmd);
    if (ret == -1) {
        FPRINTF(global_client_socket, "Error: Failed to remove installation directory '%s' (code %d)\n",
                hive_install_dir, ret);
        free(hive_install_dir);
        exit(EXIT_FAILURE);
    }
    free(hive_install_dir);

    if (!home_dir) {
        FPRINTF(global_client_socket, "Error: HOME environment variable not set\n");
        exit(EXIT_FAILURE);
    }

    // Phase 3: Clean environment variables
    for (int i = 0; i < 3; i++) {
        char path[256];
        int written = snprintf(path, sizeof(path), "%s/%s", home_dir, bashrc_paths[i]);
        if (written >= (int)sizeof(path)) {
            FPRINTF(global_client_socket, "Warning: Path truncated for %s, skipping\n", bashrc_paths[i]);
            continue;
        }

        FILE *src = fopen(path, "r");
        if (!src) continue;

        char temp_path[] = "/tmp/bashrc_XXXXXX";
        int fd = mkstemp(temp_path);
        if (fd == -1) {
            fclose(src);
            continue;
        }
        FILE *dst = fdopen(fd, "w");

        char line[1024];
        int modified = 0;
        while (fgets(line, sizeof(line), src)) {
            if (strstr(line, "HIVE_HOME") || (strstr(line, "PATH") && strstr(line, "hive"))) {
                modified = 1;
                continue;
            }
            fputs(line, dst);
        }

        fclose(src);
        fclose(dst);

        if (modified) {
            char backup_cmd[512];
            written = snprintf(backup_cmd, sizeof(backup_cmd), "cp \"%s\" \"%s.bak\"", path, path);
            if (written >= (int)sizeof(backup_cmd)) {
                FPRINTF(global_client_socket, "Backup command truncated, skipping backup for %s\n", path);
                remove(temp_path);
                continue;
            }

            int result = executeSystemCommand(backup_cmd);
            if (result == -1) {
                FPRINTF(global_client_socket, "Error: Failed to backup %s (code %d)\n", path, result);
                remove(temp_path);
                continue;
            }

            if (rename(temp_path, path) != 0) {
                remove(temp_path);
                FPRINTF(global_client_socket, "Warning: Failed to update %s\n", path);
            } else {
                found_hive_vars = 1;
            }
        } else {
            remove(temp_path);
        }
    }

    // Phase 4: Remove residual files
    const char *base_residual_paths[] = {
        "/tmp/hive*", "/var/log/hive", "/var/lib/hive", "~/.hive_history"
    };
    char user_hive_history[256];
    snprintf(user_hive_history, sizeof(user_hive_history), "%s/.hive_history", home_dir);

    for (int i = 0; i < 4; i++) {
        const char *path = base_residual_paths[i];
        if (i == 3) path = user_hive_history;

        char cleanup_cmd[256];
        size_t ret3 = snprintf(cleanup_cmd, sizeof(cleanup_cmd), "sudo rm -rf %s", path);
        if (ret3 >= sizeof(cleanup_cmd)) {
            FPRINTF(global_client_socket, "Warning: size error\n");
        }
        int result = executeSystemCommand(cleanup_cmd);
        if (result == -1) {
            FPRINTF(global_client_socket, "Warning: Failed to clean %s (code %d)\n", path, result);
        }
    }

    // Phase 5: User feedback
    PRINTF(global_client_socket,"\nUninstallation complete. Manual cleanup suggestions:\n");
    // PRINTF(global_client_socket,"1. Remove Hadoop/Hive dependencies if unused\n");
    // PRINTF(global_client_socket,"2. Remove remaining Hive data in HDFS/Warehouse\n");
    // PRINTF(global_client_socket,"3. Logout and login to refresh environment\n");

    if (found_hive_vars) {
        //PRINTF(global_client_socket,"\nNote: Environment variables removed - restart shells to apply changes\n");
    }
}

void uninstall_Zeppelin() {
    // 1. Determine installation location
    const char *zeppelin_home = getenv("ZEPPELIN_HOME");

    // Safely build HOME/zeppelin path
    const char *home = getenv("HOME");
    char *home_zeppelin = NULL;
    if (home) {
        size_t len = strlen(home) + strlen("/zeppelin") + 1;
        home_zeppelin = malloc(len);
        if (home_zeppelin) {
            snprintf(home_zeppelin, len, "%s/zeppelin", home);
        } else {
            FPRINTF(global_client_socket,"Memory allocation failed\n");
        }
    }

    char *search_paths[MAX_PATHS] = {
        "/usr/share/zeppelin",
        "/opt/zeppelin",
        "/usr/local/zeppelin",
        "/opt/apache/zeppelin",
        home_zeppelin  // Now properly allocated
    };

    char detected_path[PATH_MAX] = {0};
    struct stat path_stat;

    // Verify ZEPPELIN_HOME validity
    if (zeppelin_home && stat(zeppelin_home, &path_stat) == 0 && S_ISDIR(path_stat.st_mode)) {
        strncpy(detected_path, zeppelin_home, PATH_MAX);
        detected_path[PATH_MAX - 1] = '\0';
    } else {
        // Search through common installation paths
        for (int i = 0; i < MAX_PATHS; i++) {
            if (search_paths[i] && stat(search_paths[i], &path_stat) == 0 &&
                S_ISDIR(path_stat.st_mode)) {
                strncpy(detected_path, search_paths[i], PATH_MAX);
                detected_path[PATH_MAX - 1] = '\0';
                break;
            }
        }
    }

    free(home_zeppelin);  // Free allocated memory

    if (strlen(detected_path) == 0) {
        fprintf(stderr, "No Zeppelin installation found\n");
        exit(EXIT_FAILURE);
    }
    // 2. Service cleanup
    // 3. File system cleanup
    char cleanup_cmd[PATH_MAX + 50];
    snprintf(cleanup_cmd, sizeof(cleanup_cmd), "sudo rm -rf %s", detected_path);
    int rm_status = executeSystemCommand(cleanup_cmd);
    if (WEXITSTATUS(rm_status) == -1) {
        FPRINTF(global_client_socket, "Failed to remove installation directory\n");
    }

    // 4. Environment cleanup
    const char *config_files[CONFIG_FILES_MAX] = {
        ".bashrc", ".profile", ".zshrc"
    };

    for (int i = 0; i < CONFIG_FILES_MAX; i++) {
        char config_path[PATH_MAX];
        snprintf(config_path, sizeof(config_path), "%s/%s",
                 getenv("HOME") ? getenv("HOME") : "", config_files[i]);

        if (access(config_path, F_OK) == 0) {
            char sed_cmd[PATH_MAX + 100];
            snprintf(sed_cmd, sizeof(sed_cmd),
                     "sed -i '/ZEPPELIN_HOME/d' %s", config_path);
            int result = executeSystemCommand(sed_cmd);
            if (result == -1) {
                PRINTF(global_client_socket,"Command failed with return code %d\n", result);
            }
        }
    }

    // 5. Service file cleanup
    const char *service_paths[] = {
        "/etc/systemd/system/zeppelin.service",
        "/etc/init.d/zeppelin",
        "/usr/lib/systemd/system/zeppelin.service"
    };

    for (size_t i = 0; i < sizeof(service_paths)/sizeof(service_paths[0]); i++) {
        if (access(service_paths[i], F_OK) == 0) {
            char rm_service[PATH_MAX + 50];
            snprintf(rm_service, sizeof(rm_service), "sudo rm -f %s", service_paths[i]);
            int result = executeSystemCommand(rm_service);
            if (result == -1) {
                PRINTF(global_client_socket,"Command failed with return code %d\n", result);
            }
        }
    }

    // 6. System updates
    int resultReload = executeSystemCommand("sudo systemctl daemon-reload 2>/dev/null");
    if (resultReload == -1) {
        PRINTF(global_client_socket,"Command failed with return code %d\n", resultReload);
    }
    int resultupdate = executeSystemCommand("sudo updatedb 2>/dev/null");
    if (resultupdate == -1) {
        PRINTF(global_client_socket,"Command failed with return code %d\n", resultupdate);
    }

    // 7. Log and temp file cleanup
    const char *log_paths[] = {
        "/var/log/zeppelin",
        "/tmp/zeppelin",
        "/var/tmp/zeppelin"
    };

    for (size_t i = 0; i < sizeof(log_paths)/sizeof(log_paths[0]); i++) {
        if (access(log_paths[i], F_OK) == 0) {
            char rm_logs[PATH_MAX + 50];
            snprintf(rm_logs, sizeof(rm_logs), "sudo rm -rf %s", log_paths[i]);
            int result = executeSystemCommand(rm_logs);
            if (result == -1) {
                PRINTF(global_client_socket,"Command failed with return code %d\n", result);
            }
        }
    }

    PRINTF(global_client_socket,"Zeppelin uninstallation completed\n");
    //PRINTF(global_client_socket,"Note: Manual verification recommended\n");
}

void uninstall_Livy() {
    char* livy_home = NULL;
    char potential_paths[][PATH_MAX] = {
        "/opt/livy",
        "/usr/local/livy",
        "/usr/lib/livy",
        "/opt/apache/livy"
    };

    // 1. Determine installation location
    livy_home = getenv("LIVY_HOME");
    if (!livy_home || access(livy_home, F_OK) != 0) {
        // Check common installation locations
        for (size_t i = 0; i < sizeof(potential_paths)/sizeof(potential_paths[0]); i++) {
            if (access(potential_paths[i], F_OK) == 0) {
                livy_home = potential_paths[i];
                break;
            }
        }
    }

    if (!livy_home) {
        FPRINTF(global_client_socket, "Livy installation not found\n");
        return;
    }

    // 2. Service termination
    char stop_script[PATH_MAX];
    long unsigned int len = snprintf(stop_script, sizeof(stop_script), "%s/bin/livy-server", livy_home);
    if (len >= sizeof(stop_script)) {
        FPRINTF(global_client_socket, "snprintf output was truncated or failed.\n");
    }

    if (access(stop_script, X_OK) == 0) {
        char stop_cmd[PATH_MAX + 50];
        long unsigned int len = snprintf(stop_cmd, sizeof(stop_cmd), "sudo %s stop", stop_script);
        if (len >= sizeof(stop_cmd)) {
            FPRINTF(global_client_socket, "snprintf output was truncated or failed.\n");
        }
        if (executeSystemCommand(stop_cmd)) {
            FPRINTF(global_client_socket, "Warning: Failed to stop Livy service\n");
        }
    }

    // 3. Remove installation files
    char rm_cmd[PATH_MAX + 50];
    long unsigned int len2 = snprintf(rm_cmd, sizeof(rm_cmd), "sudo rm -rf %s", livy_home);
    if (len2 >= sizeof(rm_cmd)) {
        FPRINTF(global_client_socket, "snprintf output was truncated or failed.\n");
    }
    if (executeSystemCommand(rm_cmd)) {
        FPRINTF(global_client_socket, "Failed to remove installation directory\n");
    }

    // 4. Remove service files
    const char* service_files[] = {
        "/etc/systemd/system/livy.service",
        "/etc/init.d/livy",
        "/usr/lib/systemd/system/livy.service"
    };

    for (size_t i = 0; i < sizeof(service_files)/sizeof(service_files[0]); i++) {
        if (access(service_files[i], F_OK) == 0) {
            char rm_service[PATH_MAX + 50];
            long unsigned int len = snprintf(rm_service, sizeof(rm_service), "sudo rm -f %s", service_files[i]);
            if (len >= sizeof(rm_service)) {
                FPRINTF(global_client_socket, "snprintf output was truncated or failed.\n");
            }
            int result =executeSystemCommand(rm_service);
            if (result != 0) {
                PRINTF(global_client_socket,"Command failed with return code %d\n", result);
            }
        }
    }

    // 5. Clean environment configuration
    const char* env_files[] = {
        "/etc/profile.d/livy.sh",
        "/etc/environment",
        "/etc/bash.bashrc",
        "/etc/zsh/zshrc"
    };

    for (size_t i = 0; i < sizeof(env_files)/sizeof(env_files[0]); i++) {
        if (access(env_files[i], F_OK) == 0) {
            // Use sed to remove LIVY_HOME from files
            char clean_cmd[PATH_MAX + 100];
            long unsigned int len =  snprintf(clean_cmd, sizeof(clean_cmd),
                                              "sudo sed -i '/LIVY_HOME/d' %s", env_files[i]);
            if (len >= sizeof(clean_cmd)) {
                FPRINTF(global_client_socket, "snprintf output was truncated or failed.\n");
            }
            int result = executeSystemCommand(clean_cmd);
            if (result != 0) {
                PRINTF(global_client_socket,"Command failed with return code %d\n", result);
            }
        }
    }

    // 6. Remove symbolic links
    const char* potential_links[] = {
        "/usr/bin/livy-server",
        "/usr/local/bin/livy",
        "/opt/bin/livy"
    };

    for (size_t i = 0; i < sizeof(potential_links)/sizeof(potential_links[0]); i++) {
        if (access(potential_links[i], F_OK) == 0) {
            char rm_link[PATH_MAX + 50];
            snprintf(rm_link, sizeof(rm_link), "sudo rm -f %s", potential_links[i]);
            int result =  executeSystemCommand(rm_link);
            if (result != 0) {
                PRINTF(global_client_socket,"Command failed with return code %d\n", result);
            }
        }
    }

    // 7. Update current environment
    unsetenv("LIVY_HOME");
    PRINTF(global_client_socket,"Livy uninstallation completed\n");

    // 8. Optional: Package cleanup
    struct stat st;
    if (stat("/etc/debian_version", &st) == 0) {
        int result = executeSystemCommand("sudo apt-get purge -y livy* >/dev/null 2>&1");
        if (result != 0) {
            PRINTF(global_client_socket,"Command failed with return code %d\n", result);
        }
    } else if (stat("/etc/redhat-release", &st) == 0) {
        int result = executeSystemCommand("sudo yum remove -y livy* >/dev/null 2>&1");
        if (result != 0) {
            PRINTF(global_client_socket,"Command failed with return code %d\n", result);
        }
    }
}


void uninstall_pig() {
    char command[512];
    int ret;
    struct stat st;
    char* home;
    char bashrc_path[PATH_MAX];

    sleep(2);  // Allow time for process termination

    // 2. Identify installation location
    char install_dir[PATH_MAX] = {0};
    char* pig_home = getenv("PIG_HOME");

    if (pig_home) {
        strncpy(install_dir, pig_home, PATH_MAX-1);
    } else {
        const char* default_paths[] = {"/usr/local/pig", "/opt/pig"};
        for (int i = 0; i < 2; i++) {
            if (stat(default_paths[i], &st) == 0 && S_ISDIR(st.st_mode)) {
                strncpy(install_dir, default_paths[i], PATH_MAX-1);
                break;
            }
        }
    }

    // Validate installation directory
    if (install_dir[0] && stat(install_dir, &st) == 0 && S_ISDIR(st.st_mode)) {
        // 3. Remove installation files
        snprintf(command, sizeof(command), "rm -rf \"%s\"", install_dir);
        if ((ret = !executeSystemCommand(command))) {
            FPRINTF(global_client_socket, "Failed to remove installation directory (Code %d)\n", WEXITSTATUS(ret));
            exit(EXIT_FAILURE);
        }
        //  PRINTF(global_client_socket,"Removed installation directory: %s\n", install_dir);
    } else {
        FPRINTF(global_client_socket, "No valid Pig installation found\n");
    }

    // 4. Clean residual files
    //  PRINTF(global_client_socket,"Cleaning residual files...\n");
    int result = executeSystemCommand("rm -f pig-*.tar.gz");
    if (result == -1) {
        PRINTF(global_client_socket,"Command failed with return code %d\n", result);
    }
    int resultRM = executeSystemCommand("rm -rf pig-*");  // Remove any extracted directories in CWD
    if (resultRM == -1) {
        PRINTF(global_client_socket,"Command failed with return code %d\n", resultRM);
    }
    // 5. Environment cleanup
    if ((home = getenv("HOME"))) {
        snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home);

        // Remove PIG_HOME and PATH modifications using sed
        long unsigned int len = snprintf(command, sizeof(command),
                                         "sed -i '/PIG_HOME=/d; /export PATH.*PIG_HOME/d' %s",
                                         bashrc_path);
        if (len >= sizeof(command)) {
            FPRINTF(global_client_socket, "snprintf output was truncated or failed.\n");
        }
        if ((ret = !executeSystemCommand(command))) {
            FPRINTF(global_client_socket, "Failed to clean environment variables (Code %d)\n", WEXITSTATUS(ret));
        } //else {
          // PRINTF(global_client_socket,"Removed environment configurations\n");
          //}
    }
    PRINTF(global_client_socket,"Uninstallation complete. Restart your shell to finalize changes.\n");
}


#define MAX_PATH 1024
#define CONFIG_MARKER "# HBase Configuration"

// Helper function to clean shell configuration files
void clean_shell_config(const char *file_path) {
    FILE *fp = fopen(file_path, "r");
    if (!fp) return;

    char temp_path[MAX_PATH];
    snprintf(temp_path, sizeof(temp_path), "%s.tmp", file_path);
    FILE *temp = fopen(temp_path, "w");
    if (!temp) {
        fclose(fp);
        return;
    }

    char line[1024];
    int skip_mode = 0;
    while (fgets(line, sizeof(line), fp)) {
        if (strstr(line, CONFIG_MARKER)) {
            skip_mode = 1;
            continue;
        }
        if (skip_mode) {
            if (strstr(line, "export HBASE_HOME") || strstr(line, "export PATH")) {
                continue;
            }
            skip_mode = 0;
        }
        fputs(line, temp);
    }

    fclose(fp);
    fclose(temp);
    rename(temp_path, file_path);
}

void uninstall_HBase() {
    char *hbase_home = getenv("HBASE_HOME");
    char install_path[PATH_MAX] = {0};
    char config_files[][PATH_MAX] = {".bashrc"};
    int found_installation = 0;

    // Step 1: Identify installation location
    if (hbase_home && access(hbase_home, F_OK) == 0) {
        strncpy(install_path, hbase_home, PATH_MAX - 1);
        found_installation = 1;
    } else {
        const char *potential_paths[] = {
            "/usr/local/hbase",
            "/opt/hbase",
            "/usr/share/hbase",
            getenv("HOME") ? strcat(strcpy(install_path, getenv("HOME")), "/hbase") : NULL
        };

        for (size_t i = 0; i < sizeof(potential_paths)/sizeof(potential_paths[0]); i++) {
            if (potential_paths[i] && access(potential_paths[i], F_OK) == 0) {
                strncpy(install_path, potential_paths[i], PATH_MAX - 1);
                found_installation = 1;
                break;
            }
        }
    }

    if (!found_installation) {
        FPRINTF(global_client_socket, "Error: HBase installation not found\n");
        return;
    }

    // Step 2: Remove installation directory
    char rm_cmd[PATH_MAX + 50];
    long unsigned int ret = snprintf(rm_cmd, sizeof(rm_cmd), "sudo rm -rf \"%s\"", install_path);
    if (ret >= sizeof(rm_cmd)) {
        FPRINTF(global_client_socket, "Error: command buffer overflow\n");
        return;
    }
    if (!executeSystemCommand(rm_cmd)) {
        FPRINTF(global_client_socket, "Warning: Failed to remove HBase installation directory\n");
    }

    // Step 3: Clean environment variables from .bashrc
    for (size_t i = 0; i < sizeof(config_files)/sizeof(config_files[0]); i++) {
        char path[PATH_MAX];
        long unsigned int ret2 = snprintf(path, sizeof(path), "%s/%s", getenv("HOME") ?: "", config_files[i]);
        if (ret2 >= sizeof(path)) {
            FPRINTF(global_client_socket, "Error: path buffer overflow\n");
            return;
        }

        FILE *src = fopen(path, "r");
        if (!src) continue;

        char temp_path[] = "/tmp/hbase_uninstall_XXXXXX";
        int fd = mkstemp(temp_path);
        FILE *dst = fdopen(fd, "w");

        char line[MAX_LINE];
        int modified = 0;

        while (fgets(line, MAX_LINE, src)) {
            if (strstr(line, "HBASE_HOME") ||
                strstr(line, "HBASE_CONF_DIR") ||
                strstr(line, "HBASE_HOME/bin")) {
                modified = 1;
                continue;
            }
            fputs(line, dst);
        }

        fclose(src);
        fclose(dst);

        if (modified) {
            // Create backup
            char backup_cmd[PATH_MAX + 50];
            long unsigned int ret3 = snprintf(backup_cmd, sizeof(backup_cmd), "cp %s %s.bak", path, path);
            if (ret3 >= sizeof(backup_cmd)) {
                FPRINTF(global_client_socket, "Error: backup command buffer overflow\n");
                remove(temp_path);
                return;
            }
            executeSystemCommand(backup_cmd);

            // Apply changes
            char apply_cmd[PATH_MAX + 50];
            long unsigned int ret4 = snprintf(apply_cmd, sizeof(apply_cmd), "mv %s %s", temp_path, path);
            if (ret4 >= sizeof(apply_cmd)) {
                FPRINTF(global_client_socket, "Error: apply command buffer overflow\n");
                remove(temp_path);
                return;
            }
            if (!executeSystemCommand(apply_cmd)) {
                FPRINTF(global_client_socket, "can't Successfully updated %s\n", config_files[i]);
            }
        } else {
            remove(temp_path);
        }
    }

    PRINTF(global_client_socket, "HBase uninstallation completed: Removed installation directory and cleaned environment variables\n");
}

void uninstall_Tez() {
    char* tez_home = getenv("TEZ_HOME");
    char* config_paths[] = {
        "/usr/local/tez", "/opt/tez", "/usr/share/tez",
        "/opt/apache/tez", "/var/lib/tez"
    };
    char* config_files[] = {
        ".bashrc", ".bash_profile", ".profile",
        ".zshrc", ".cshrc"
    };

    // Step 1: Stop running services
    //PRINTF(global_client_socket,"Stopping Tez services...\n");
    int result = executeSystemCommand("tez-daemon.sh stop historyserver 2>/dev/null");
    if (result != 0) {
        PRINTF(global_client_socket,"Command failed with return code %d\n", result);
    }

    // Step 2: Identify installation locations
    char* install_paths[MAX_PATHS] = {0};
    int path_count = 0;

    // Check environment variable first
    if (tez_home && access(tez_home, F_OK) == 0) {
        install_paths[path_count++] = tez_home;
    }

    // Check common installation locations
    for (size_t i = 0; i < sizeof(config_paths)/sizeof(char*); i++) {
        if (access(config_paths[i], F_OK) == 0) {
            install_paths[path_count++] = config_paths[i];
            if (path_count >= MAX_PATHS) break;
        }
    }

    // Step 3: Remove installation directories
    //PRINTF(global_client_socket,"Removing installation files...\n");
    for (int i = 0; i < path_count; i++) {
        char cmd[1024];
        snprintf(cmd, sizeof(cmd), "sudo rm -rf \"%s\"", install_paths[i]);
        if (!executeSystemCommand(cmd)) {
            FPRINTF(global_client_socket, "Failed to remove: %s\n", install_paths[i]);
        }
    }

    // Step 4: Clean environment variables
    //PRINTF(global_client_socket,"Cleaning environment configuration...\n");
    char* home = getenv("HOME");
    if (home) {
        for (int i = 0; i < CONFIG_FILES_MAX; i++) {
            char rc_file[1024];
            snprintf(rc_file, sizeof(rc_file), "%s/%s", home, config_files[i]);

            FILE *fp = fopen(rc_file, "r");
            if (!fp) continue;

            // Create temp file for clean configuration
            char temp_file[1024];
            snprintf(temp_file, sizeof(temp_file), "%s/.tmp_tez_clean", home);
            FILE *tmp = fopen(temp_file, "w");
            if (!tmp) {
                fclose(fp);
                continue;
            }

            char line[1024];
            bool modified = false;
            while (fgets(line, sizeof(line), fp)) {
                if (strstr(line, "TEZ_HOME") ||
                    strstr(line, "tez-daemon") ||
                    strstr(line, "apache-tez")) {
                    modified = true;
                    continue;
                }
                fputs(line, tmp);
            }

            fclose(fp);
            fclose(tmp);

            // Replace original file if changes were made
            if (modified) {
                char backup[1024];
                long unsigned int len = snprintf(backup, sizeof(backup), "cp %s %s.bak", rc_file, rc_file);
                if (len >= sizeof(backup)) {
                    FPRINTF(global_client_socket, "snprintf output was truncated or failed.\n");
                }
                int result = executeSystemCommand(backup);
                if (result != 0) {
                    PRINTF(global_client_socket,"Command failed with return code %d\n", result);
                }
                char move_cmd[1024];
                long unsigned int len2 =  snprintf(move_cmd, sizeof(move_cmd), "mv %s %s", temp_file, rc_file);
                if (len2 >= sizeof(move_cmd)) {
                    FPRINTF(global_client_socket, "snprintf output was truncated or failed.\n");
                }
                int resultRemove = executeSystemCommand(move_cmd);
                if (resultRemove != 0) {
                    PRINTF(global_client_socket,"Command failed with return code %d\n", resultRemove);
                }
                //PRINTF(global_client_socket,"Cleaned: %s\n", rc_file);
            } else {
                remove(temp_file);
            }
        }
    }

    // Step 5: Remove system-wide configurations
    char* system_configs[] = {
        "/etc/profile.d/tez.sh",
        "/etc/environment",
        "/etc/default/tez"
    };

    for (size_t i = 0; i < sizeof(system_configs)/sizeof(char*); i++) {
        if (access(system_configs[i], F_OK) == 0) {
            char cmd[1024];
            snprintf(cmd, sizeof(cmd), "sudo rm -f %s", system_configs[i]);
            int result = executeSystemCommand(cmd);
            if (result != 0) {
                PRINTF(global_client_socket,"Command failed with return code %d\n", result);
            }
        }
    }

    // Step 6: Clean package manager artifacts
    const char* pm_cmds[] = {
        "dpkg -l | grep tez | awk '{print $2}' | xargs sudo apt-get purge -y",
        "rpm -qa | grep tez | xargs sudo yum remove -y",
        "brew uninstall tez 2>/dev/null"
    };

    for (size_t i = 0; i < sizeof(pm_cmds)/sizeof(char*); i++) {
        int result = executeSystemCommand(pm_cmds[i]);
        if (result != 0) {
            PRINTF(global_client_socket,"Command failed with return code %d\n", result);
        }
    }

    // Step 7: Remove cache and temp files
    int resultRm = executeSystemCommand("sudo rm -rf /var/log/tez /var/cache/tez /tmp/tez*");
    if (resultRm != 0) {
        PRINTF(global_client_socket,"Command failed with return code %d\n", resultRm);
    }

    PRINTF(global_client_socket,"Tez uninstallation complete.\n");
    //PRINTF(global_client_socket,"Please start a new shell session to apply environment changes.\n");
}

void uninstall_kafka() {
    char *kafka_home = getenv("KAFKA_HOME");
    char install_path[PATH_MAX] = {0};
    char config_files[][PATH_MAX] = {".bashrc"};
    int found_installation = 0;

    // Step 1: Identify installation location
    if (kafka_home && access(kafka_home, F_OK) == 0) {
        strncpy(install_path, kafka_home, PATH_MAX - 1);
        found_installation = 1;
    } else {
        const char *potential_paths[] = {
            "/usr/local/kafka",
            "/opt/kafka",
            "/usr/share/kafka",
            getenv("HOME") ? strcat(strcpy(install_path, getenv("HOME")), "/kafka") : NULL
        };

        for (size_t i = 0; i < sizeof(potential_paths)/sizeof(potential_paths[0]); i++) {
            if (potential_paths[i] && access(potential_paths[i], F_OK) == 0) {
                strncpy(install_path, potential_paths[i], PATH_MAX - 1);
                found_installation = 1;
                break;
            }
        }
    }

    if (!found_installation) {
        FPRINTF(global_client_socket, "Error: Kafka installation not found\n");
        return;
    }

    // Step 2: Remove installation directory
    char rm_cmd[PATH_MAX + 50];
    long unsigned int ret = snprintf(rm_cmd, sizeof(rm_cmd), "sudo rm -rf \"%s\"", install_path);
    if (ret >= sizeof(rm_cmd)) {
        FPRINTF(global_client_socket, "Error: command buffer overflow\n");
        return;
    }
    if (!executeSystemCommand(rm_cmd)) {
        FPRINTF(global_client_socket, "Warning: Failed to remove Kafka installation directory\n");
    }

    // Step 3: Clean environment variables from .bashrc
    for (size_t i = 0; i < sizeof(config_files)/sizeof(config_files[0]); i++) {
        char path[PATH_MAX];
        long unsigned int ret2 = snprintf(path, sizeof(path), "%s/%s", getenv("HOME") ?: "", config_files[i]);
        if (ret2 >= sizeof(path)) {
            FPRINTF(global_client_socket, "Error: path buffer overflow\n");
            return;
        }

        FILE *src = fopen(path, "r");
        if (!src) continue;

        char temp_path[] = "/tmp/kafka_uninstall_XXXXXX";
        int fd = mkstemp(temp_path);
        FILE *dst = fdopen(fd, "w");

        char line[MAX_LINE];
        int modified = 0;

        while (fgets(line, MAX_LINE, src)) {
            if (strstr(line, "KAFKA_HOME") ||
                strstr(line, "KAFKA_CONF_DIR") ||
                strstr(line, "KAFKA_OPTS") ||
                strstr(line, "KAFKA_HOME/bin")) {
                modified = 1;
                continue;
            }
            fputs(line, dst);
        }

        fclose(src);
        fclose(dst);

        if (modified) {
            // Create backup
            char backup_cmd[PATH_MAX + 50];
            long unsigned int ret3 = snprintf(backup_cmd, sizeof(backup_cmd), "cp %s %s.bak", path, path);
            if (ret3 >= sizeof(backup_cmd)) {
                FPRINTF(global_client_socket, "Error: backup command buffer overflow\n");
                remove(temp_path);
                return;
            }
            executeSystemCommand(backup_cmd);

            // Apply changes
            char apply_cmd[PATH_MAX + 50];
            long unsigned int ret4 = snprintf(apply_cmd, sizeof(apply_cmd), "mv %s %s", temp_path, path);
            if (ret4 >= sizeof(apply_cmd)) {
                FPRINTF(global_client_socket, "Error: apply command buffer overflow\n");
                remove(temp_path);
                return;
            }
            if (!executeSystemCommand(apply_cmd)) {
                FPRINTF(global_client_socket, "Environment variable not Successfully updated %s\n", config_files[i]);
            }
        } else {
            remove(temp_path);
        }
    }

    PRINTF(global_client_socket, "Kafka uninstallation completed: Removed installation directory and cleaned environment variables\n");
}
void uninstall_Solr() {
    char command[512];
    const char *install_dir = NULL;
    const char *data_dir = "/var/solr";
    const char *log_dir = "/var/log/solr";
    char bashrc_path[256];
    FILE *bashrc;
    char temp_file[] = "/tmp/bashrc_temp_XXXXXX";
    int found_solr_vars = 0;

    // 1. Determine installation directory
    if (access("/etc/debian_version", F_OK) == 0) {
        install_dir = "/usr/local/solr";
    } else if (access("/etc/redhat-release", F_OK) == 0) {
        install_dir = "/opt/solr";
    } else {
        FPRINTF(global_client_socket, "Error: Unsupported Linux distribution\n");
        return;
    }

    // 2. Stop Solr service
    //PRINTF(global_client_socket,"Stopping Solr service...\n");
    // Solr_action(STOP);

    // 3. Remove installation directory
    if (access(install_dir, F_OK) == 0) {
        //PRINTF(global_client_socket,"Removing installation directory: %s\n", install_dir);
        snprintf(command, sizeof(command), "sudo rm -rf %s", install_dir);
        if (!executeSystemCommand(command)) {
            FPRINTF(global_client_socket, "Failed to remove installation directory\n");
        }
    }

    // 4. Remove data directory
    if (access(data_dir, F_OK) == 0) {
        //PRINTF(global_client_socket,"Removing data directory: %s\n", data_dir);
        snprintf(command, sizeof(command), "sudo rm -rf %s", data_dir);
        if (!executeSystemCommand(command)) {
            FPRINTF(global_client_socket, "Failed to remove data directory\n");
        }
    }

    // 5. Remove log directory
    if (access(log_dir, F_OK) == 0) {
        //PRINTF(global_client_socket,"Removing log directory: %s\n", log_dir);
        snprintf(command, sizeof(command), "sudo rm -rf %s", log_dir);
        if (!executeSystemCommand(command)) {
            FPRINTF(global_client_socket, "Failed to remove log directory\n");
        }
    }

    // 6. Clean environment variables from .bashrc
    const char *home = getenv("HOME");
    if (home) {
        snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home);

        // Create temporary file
        int fd = mkstemp(temp_file);
        if (fd == -1) {
            FPRINTF(global_client_socket, "Error creating temporary file\n");
            return;
        }
        FILE *temp = fdopen(fd, "w");

        bashrc = fopen(bashrc_path, "r");
        if (bashrc && temp) {
            char line[512];
            while (fgets(line, sizeof(line), bashrc)) {
                if (strstr(line, "SOLR_HOME") || strstr(line, "solr/bin")) {
                    found_solr_vars = 1;
                    continue;
                }
                fputs(line, temp);
            }
            fclose(bashrc);
            fclose(temp);

            if (found_solr_vars) {
                // Replace original .bashrc
                snprintf(command, sizeof(command), "mv %s %s", temp_file, bashrc_path);
                int result = executeSystemCommand(command);
                if (result == -1) {
                    PRINTF(global_client_socket,"Command failed with return code %d\n", result);
                }
                //PRINTF(global_client_socket,"Removed Solr environment variables from .bashrc\n");
            } else {
                remove(temp_file);
            }
        }
    }

    // 7. Final verification
    PRINTF(global_client_socket,"\nUninstallation completed \n");
    int residual = 0;

    if (access(install_dir, F_OK) == 0) {
        FPRINTF(global_client_socket, "  - Installation directory still exists: %s\n", install_dir);
        residual = 1;
    }

    if (access(data_dir, F_OK) == 0) {
        FPRINTF(global_client_socket, "  - Data directory still exists: %s\n", data_dir);
        residual = 1;
    }

    if (access(log_dir, F_OK) == 0) {
        FPRINTF(global_client_socket, "  - Log directory still exists: %s\n", log_dir);
        residual = 1;
    }

    if (!residual) {
        //PRINTF(global_client_socket,"No residual components found\n");
    }

    PRINTF(global_client_socket,"Solr uninstallation process completed\n");
}


void uninstall_phoenix() {
    char *phoenix_home = getenv("PHOENIX_HOME");
    char install_dir[512] = {0};
    struct stat st;

    // Determine installation directory
    if (phoenix_home != NULL) {
        strncpy(install_dir, phoenix_home, sizeof(install_dir) - 1);
    } else {
        // Detect OS to infer default installation path
        if (stat("/etc/debian_version", &st) == 0) {
            strncpy(install_dir, "/usr/local/phoenix", sizeof(install_dir) - 1);
        } else if (stat("/etc/redhat-release", &st) == 0) {
            strncpy(install_dir, "/opt/phoenix", sizeof(install_dir) - 1);
        } else {
            FPRINTF(global_client_socket, "Error: PHOENIX_HOME not set and OS detection failed.\n");
            exit(EXIT_FAILURE);
        }
    }

    // Verify installation directory exists
    if (stat(install_dir, &st) != 0 || !S_ISDIR(st.st_mode)) {
        FPRINTF(global_client_socket, "Error: Phoenix installation directory not found: %s\n", install_dir);
        exit(EXIT_FAILURE);
    }

    // Remove installation directory (requires elevated privileges)
    char command[1024];
    snprintf(command, sizeof(command), "sudo rm -rf \"%s\"", install_dir);
    int ret = executeSystemCommand(command);
    if (ret != 0) {
        FPRINTF(global_client_socket, "Error: Failed to remove directory (code %d). Check permissions.\n", ret);
        exit(EXIT_FAILURE);
    }

    // Clean environment variables from shell profiles
    char *shell_files[] = {".bashrc", ".bash_profile", ".profile", ".zshrc"};
    char *home = getenv("HOME");
    if (!home) {
        FPRINTF(global_client_socket, "Error: Cannot determine user home directory.\n");
        exit(EXIT_FAILURE);
    }

    for (size_t i = 0; i < sizeof(shell_files)/sizeof(shell_files[0]); i++) {
        char profile_path[512];
        snprintf(profile_path, sizeof(profile_path), "%s/%s", home, shell_files[i]);

        // Check if profile file exists
        if (access(profile_path, F_OK) != 0) continue;

        // Create temp file
        char temp_path[] = "/tmp/phoenix_uninstall_XXXXXX";
        int fd = mkstemp(temp_path);
        if (fd == -1) {
            PERROR(global_client_socket,"Error creating temp file");
            continue; // Skip this file
        }
        FILE *temp = fdopen(fd, "w");
        if (!temp) {
            PERROR(global_client_socket,"Error opening temp file");
            close(fd);
            continue;
        }

        // Process original file
        FILE *profile = fopen(profile_path, "r");
        if (!profile) {
            PERROR(global_client_socket,"Error opening profile file");
            fclose(temp);
            continue;
        }

        char line[1024];
        int modified = 0;
        while (fgets(line, sizeof(line), profile)) {
            // Remove lines setting PHOENIX_HOME or referencing the install path
            if (strstr(line, "export PHOENIX_HOME=") || strstr(line, install_dir)) {
                modified = 1;
                continue;
            }
            fputs(line, temp);
        }

        fclose(profile);
        fclose(temp);

        // Replace original file if changes were made
        if (modified) {
            snprintf(command, sizeof(command), "mv \"%s\" \"%s\"", temp_path, profile_path);
            ret = executeSystemCommand(command);
            if (ret != 0) {
                FPRINTF(global_client_socket, "Error updating %s\n", shell_files[i]);
                unlink(temp_path);
            } else {
                //PRINTF(global_client_socket,"Removed Phoenix references from %s\n", profile_path);
            }
        } else {
            unlink(temp_path);
        }
    }

    // Remove system-wide environment settings (if any)
    char etc_profile[] = "/etc/profile.d/phoenix.sh";
    if (access(etc_profile, F_OK) == 0) {
        snprintf(command, sizeof(command), "sudo rm -f %s", etc_profile);
        ret = executeSystemCommand(command);
        if (ret == 0) {
            //PRINTF(global_client_socket,"Removed system-wide Phoenix profile\n");
        }
    }

    PRINTF(global_client_socket,"\nPhoenix uninstallation complete.\n");
    // PRINTF(global_client_socket,"Note: You may need to restart your shell or log out/in to fully unset environment variables.\n");
}

const char* find_ranger_install_dir() {
    const char *candidates[] = {
        getenv("RANGER_HOME"),  // Environment variable
        "/opt/ranger",          // Red Hat standard path
        "/usr/local/ranger",    // Debian standard path
        NULL
    };

    for (int i = 0; candidates[i] != NULL; i++) {
        if (candidates[i] == NULL) continue;

        struct stat st;
        if (stat(candidates[i], &st) == 0 && S_ISDIR(st.st_mode)) {
            return candidates[i];
        }
    }
    return NULL;
}

void uninstall_ranger() {
    char command[2048];
    const char *install_dir = find_ranger_install_dir();

    if (!install_dir) {
        FPRINTF(global_client_socket, "Error: Ranger installation directory not found\n");
        return;
    }

    // 1. Stop Ranger services
    //PRINTF(global_client_socket,"Stopping Ranger services...\n");
    int rc = executeSystemCommand("sudo systemctl stop ranger >/dev/null 2>&1");
    if (rc == -1) {
        rc = executeSystemCommand("which ranger-admin >/dev/null 2>&1 && sudo ranger-admin stop");
        if (rc == -1) {
            FPRINTF(global_client_socket, "Warning: Failed to stop services (code %d)\n", rc);
        }
    }

    // 2. Remove installation directory
    //PRINTF(global_client_socket,"Removing installation directory: %s\n", install_dir);
    snprintf(command, sizeof(command), "sudo rm -rf \"%s\"", install_dir);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket, "Error: Failed to remove installation directory\n");
    }

    // 3. Clean environment variables
    struct passwd *pw = getpwuid(getuid());
    if (pw) {
        char bashrc_path[1024], tmp_path[1024];
        snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", pw->pw_dir);
        snprintf(tmp_path, sizeof(tmp_path), "%s/.bashrc.tmp", pw->pw_dir);

        FILE *src = fopen(bashrc_path, "r");
        FILE *dst = fopen(tmp_path, "w");
        if (src && dst) {
            char line[1024];
            int in_block = 0;
            while (fgets(line, sizeof(line), src)) {
                if (strstr(line, "# Ranger Environment")) in_block = 1;
                if (in_block) {
                    if (strstr(line, "RANGER_HOME") || strstr(line, "PATH") || line[0] == '\n') continue;
                    in_block = 0;
                }
                fputs(line, dst);
            }
            fclose(src);
            fclose(dst);
            rename(tmp_path, bashrc_path);
        }
    }

    // 4. Remove system artifacts
    const char *artifacts[] = {
        "/etc/systemd/system/ranger.service",
        "/usr/lib/systemd/system/ranger.service",
        "/var/log/ranger",
        NULL
    };

    for (int i = 0; artifacts[i]; i++) {
        snprintf(command, sizeof(command), "sudo rm -rf %s", artifacts[i]);
        executeSystemCommand(command);
    }

    // 5. Package manager cleanup
    rc = executeSystemCommand("command -v apt-get >/dev/null");
    if (rc == 0) {
        executeSystemCommand("sudo apt-get purge -y ranger* >/dev/null");
    } else {
        executeSystemCommand("sudo yum remove -y ranger* >/dev/null");
    }

    PRINTF(global_client_socket,"\nUninstallation complete.\n");
    //PRINTF(global_client_socket,"Note: Start a new shell session to apply environment changes\n");
}

void uninstall_livy() {
    char command[1024];
    int status;
    // FILE *fp;
    char *home_dir = getenv("HOME");
    char path[PATH_MAX];

    // 1. Stop Livy service if running
    //PRINTF(global_client_socket,"Attempting to stop Livy service...\n");
    livy_action(STOP);
    sleep(2); // Allow time for shutdown

    // 2. Find installation locations
    char *livy_home = getenv("LIVY_HOME");
    char *possible_paths[] = {
        "/opt/livy",
        "/usr/local/livy",
        "/usr/lib/livy",
        "/opt/apache/livy",
        livy_home,
        NULL
    };

    // 3. Remove installation directories
    //PRINTF(global_client_socket,"\nRemoving installation directories:\n");
    for(int i = 0; possible_paths[i]; i++) {
        if(!possible_paths[i]) continue;

        long unsigned int len =  snprintf(path, sizeof(path), "%s", possible_paths[i]);
        if (len >= sizeof(path)) {
            FPRINTF(global_client_socket, "snprintf output was truncated or failed.\n");
        }
        struct stat st;
        if(stat(path, &st) == 0 && S_ISDIR(st.st_mode)) {
            //PRINTF(global_client_socket,"Removing %s...\n", path);
            long unsigned int len =  snprintf(command, sizeof(command), "sudo rm -rf \"%s\"", path);
            if (len >= sizeof(path)) {
                FPRINTF(global_client_socket, "snprintf output was truncated or failed.\n");
            }
            status = executeSystemCommand(command);
            if(status != 0) {
                FPRINTF(global_client_socket, "Failed to remove %s (error: %d)\n", path, status);
            }
        }
    }

    // 4. Clean environment variables
    //PRINTF(global_client_socket,"\nCleaning environment configuration:\n");
    char *shell_files[] = {".bashrc", ".bash_profile", ".zshrc", ".profile", NULL};

    for(int i = 0; shell_files[i]; i++) {
        char shell_path[PATH_MAX];
        long unsigned int len = snprintf(shell_path, sizeof(shell_path), "%s/%s", home_dir, shell_files[i]);
        if (len >= sizeof(path)) {
            FPRINTF(global_client_socket, "snprintf output was truncated or failed.\n");
        }
        if(access(shell_path, F_OK) == 0) {
            //PRINTF(global_client_socket,"Cleaning %s...\n", shell_path);
            // Use temporary file for safe editing
            long unsigned int len = snprintf(command, sizeof(command),
                                             "cp \"%s\" \"%s.bak\" && "
                                             "grep -v 'LIVY_HOME\\|apache-livy' \"%s\" > \"%s.tmp\" && "
                                             "mv \"%s.tmp\" \"%s\"",
                                             shell_path, shell_path, shell_path, shell_path, shell_path, shell_path);
            if (len >= sizeof(command)) {
                FPRINTF(global_client_socket, "snprintf output was truncated or failed.\n");
            }

            status = executeSystemCommand(command);
            if(status != 0) {
                FPRINTF(global_client_socket, "Failed to clean %s (error: %d)\n", shell_files[i], status);
            }
        }
    }

    // 5. Remove systemd service if exists
    //PRINTF(global_client_socket,"\nChecking for systemd services:\n");
    char *service_files[] = {
        "/etc/systemd/system/livy.service",
        "/usr/lib/systemd/system/livy.service",
        NULL
    };

    for(int i = 0; service_files[i]; i++) {
        if(access(service_files[i], F_OK) == 0) {
            //PRINTF(global_client_socket,"Removing systemd service: %s\n", service_files[i]);
            snprintf(command, sizeof(command), "sudo rm -f \"%s\"", service_files[i]);
            int result = executeSystemCommand(command);
            if (result != 0) {
                PRINTF(global_client_socket,"Command failed with return code %d\n", result);
            }
        }
    }

    // 6. Kill any remaining processes
    //PRINTF(global_client_socket,"\nChecking for remaining processes:\n");
    int resultGrep=  executeSystemCommand("pgrep -af livy-server");
    if (resultGrep != 0) {
        PRINTF(global_client_socket,"Command failed with return code %d\n", resultGrep);
    }
    int resultKill = executeSystemCommand("sudo pkill -9 -f livy-server");
    if (resultKill != 0) {
        PRINTF(global_client_socket,"Command failed with return code %d\n", resultKill);
    }

    // 7. Remove cache and temp files
    //PRINTF(global_client_socket,"\nCleaning cache and temporary files:\n");
    char *cache_paths[] = {
        "/var/log/livy",
        "/tmp/livy*",
        "/var/cache/livy",
        NULL
    };

    for(int i = 0; cache_paths[i]; i++) {
        //PRINTF(global_client_socket,"Removing %s...\n", cache_paths[i]);
        snprintf(command, sizeof(command), "sudo rm -rf %s", cache_paths[i]);
        int result = executeSystemCommand(command);
        if (result != 0) {
            PRINTF(global_client_socket,"Command failed with return code %d\n", result);
        }
    }

    // 8. Refresh shell environment
    //PRINTF(global_client_socket,"\nRefreshing shell environment...\n");
    unsetenv("LIVY_HOME");
    int resultHash = executeSystemCommand("hash -r");
    if (resultHash != 0) {
        PRINTF(global_client_socket,"Command failed with return code %d\n", resultKill);
    }
    PRINTF(global_client_socket,"\nUninstallation complete.\n");
    //PRINTF(global_client_socket,"- Manual verification of remaining files is recommended\n");
    //PRINTF(global_client_socket,"- Java runtime and other dependencies were not removed\n");
    //PRINTF(global_client_socket,"- Check package manager for residual dependencies\n");
}


#define MAX_PATH 1024

// Helper function to execute commands with error checking
int execute_cleanup_command(const char *command) {
    int status = executeSystemCommand(command);
    if (status == -1 || WEXITSTATUS(status) != 0) {
        FPRINTF(global_client_socket, "Failed to execute: %s\n", command);
        return -1;
    }
    return 0;
}

// Recursive directory removal
int remove_directory(const char *path) {
    DIR *d = opendir(path);
    if (!d) {
        PERROR(global_client_socket,"opendir");
        return -1;
    }

    struct dirent *entry;
    while ((entry = readdir(d)) != NULL) {
        if (strcmp(entry->d_name, ".") && strcmp(entry->d_name, "..")) {
            char subpath[MAX_PATH];
            snprintf(subpath, MAX_PATH, "%s/%s", path, entry->d_name);

            struct stat st;
            if (lstat(subpath, &st) == -1) {
                PERROR(global_client_socket,"lstat");
                continue;
            }

            if (S_ISDIR(st.st_mode)) {
                remove_directory(subpath);
            } else {
                if (unlink(subpath) == -1) {
                    PERROR(global_client_socket,"unlink");
                }
            }
        }
    }
    closedir(d);
    return rmdir(path);
}

void uninstall_Atlas() {
    char command[MAX_PATH];
    char atlas_home[MAX_PATH] = {0};
    char *env_home = getenv("ATLAS_HOME");
    int total_errors = 0;

    // 1. Stop running services
    //PRINTF(global_client_socket,"Stopping Atlas services...\n");
    int result = executeSystemCommand("atlas_action STOP 2>/dev/null");
    if (result == -1) {
        PRINTF(global_client_socket,"Command failed with return code %d\n", result);
    }

    // 2. Remove installation directory
    if (env_home) {
        strncpy(atlas_home, env_home, MAX_PATH-1);
    } else {
        // Check common installation locations
        const char *locations[] = {
            "/usr/local/atlas-*",
            "/opt/atlas-*",
            getenv("HOME") ? strcat(getenv("HOME"), "/atlas-*") : NULL
        };

        for (size_t i = 0; i < sizeof(locations)/sizeof(locations[0]); i++) {
            if (locations[i]) {
                DIR *dir = opendir(locations[i]);
                if (dir) {
                    closedir(dir);
                    strncpy(atlas_home, locations[i], MAX_PATH-1);
                    break;
                }
            }
        }
    }

    if (atlas_home[0]) {
        //PRINTF(global_client_socket,"Removing installation at: %s\n", atlas_home);
        snprintf(command, MAX_PATH, "sudo rm -rf \"%s\"", atlas_home);
        total_errors += execute_cleanup_command(command);
    }

    // 3. Remove downloaded archives
    //PRINTF(global_client_socket,"Cleaning up downloaded packages...\n");
    int resultFind = executeSystemCommand("find . -maxdepth 1 -name 'apache-atlas-*-bin.tar.gz' -exec rm -f {} \\;");
    if (resultFind == -1) {
        PRINTF(global_client_socket,"Command failed with return code %d\n", resultFind);
    }

    // 4. Remove environment configuration
    //PRINTF(global_client_socket,"Removing environment settings...\n");
    const char *shell_files[] = {".bashrc", ".bash_profile", ".zshrc", ".profile"};
    for (size_t i = 0; i < sizeof(shell_files)/sizeof(shell_files[0]); i++) {
        char path[MAX_PATH];
        long unsigned int len = snprintf(path, MAX_PATH, "%s/%s", getenv("HOME"), shell_files[i]);
        if (len >= sizeof(path)) {
            FPRINTF(global_client_socket, "snprintf output was truncated or failed.\n");
        }
        FILE *fp = fopen(path, "r+");
        if (fp) {
            char temp_path[MAX_PATH];
            long unsigned int len = snprintf(temp_path, MAX_PATH, "%s.temp", path);
            if (len >= sizeof(temp_path)) {
                FPRINTF(global_client_socket, "snprintf output was truncated or failed.\n");
            }
            FILE *temp = fopen(temp_path, "w");

            char line[1024];
            int found = 0;
            while (fgets(line, sizeof(line), fp)) {
                if (strstr(line, "ATLAS_HOME") || strstr(line, "atlas/bin")) {
                    found = 1;
                    continue;
                }
                fputs(line, temp);
            }

            fclose(fp);
            fclose(temp);

            if (found) {
                remove(path);
                rename(temp_path, path);
            } else {
                remove(temp_path);
            }
        }
    }

    // 5. Remove system service files
    const char *service_files[] = {
        "/etc/systemd/system/atlas.service",
        "/etc/init.d/atlas",
        "/usr/lib/systemd/system/atlas.service"
    };

    for (size_t i = 0; i < sizeof(service_files)/sizeof(service_files[0]); i++) {
        if (access(service_files[i], F_OK) == 0) {
            snprintf(command, MAX_PATH, "sudo rm -f %s", service_files[i]);
            total_errors += execute_cleanup_command(command);
        }
    }

    // 6. Remove data and log directories
    const char *data_dirs[] = {
        "/var/lib/atlas",
        "/var/log/atlas",
        "/tmp/hbase-atlas",
        "/tmp/atlas"
    };

    for (size_t i = 0; i < sizeof(data_dirs)/sizeof(data_dirs[0]); i++) {
        if (access(data_dirs[i], F_OK) == 0) {
            // PRINTF(global_client_socket,"Removing data directory: %s\n", data_dirs[i]);
            snprintf(command, MAX_PATH, "sudo rm -rf %s", data_dirs[i]);
            total_errors += execute_cleanup_command(command);
        }
    }

    // 7. Package manager cleanup
#if defined(__linux__)
    if (access("/etc/redhat-release", F_OK) == 0) {
        int result =executeSystemCommand("sudo rpm -qa | grep atlas | xargs -r sudo rpm -e");
        if (result == -1) {
            PRINTF(global_client_socket,"Command failed with return code %d\n", result);
        }
    } else if (access("/etc/debian_version", F_OK) == 0) {
        int result = executeSystemCommand("sudo dpkg -l | grep atlas | awk '{print $2}' | xargs -r sudo dpkg -r");
        if (result == -1) {
            PRINTF(global_client_socket,"Command failed with return code %d\n", result);
        }
    }
#endif

    // 8. Final verification
    if (total_errors == 0) {
        PRINTF(global_client_socket,"\nAtlas uninstallation completed successfully.\n");
        //PRINTF(global_client_socket,"Please restart your shell session to update environment changes.\n");
    } //else {
      //PRINTF(global_client_socket,"\nUninstallation completed with %d non-fatal errors.\n", total_errors);
      // PRINTF(global_client_socket,"Some residual files may require manual removal.\n");
      //}
}

int validate_storm_home(const char *path) {
    char bin_path[PATH_MAX];
    struct stat st;

    snprintf(bin_path, sizeof(bin_path), "%s/bin/storm", path);
    if (stat(bin_path, &st) == 0 && S_ISREG(st.st_mode)) {
        return 1;
    }
    return 0;
}

void remove_environment_vars() {
    const char *config_files[CONFIG_FILES_MAX] = {
        ".bashrc", ".profile", ".zshrc"
    };

    char *home = getenv("HOME");
    if (!home) return;

    for (int i = 0; i < CONFIG_FILES_MAX; i++) {
        char src_path[PATH_MAX], tmp_path[PATH_MAX];
        snprintf(src_path, sizeof(src_path), "%s/%s", home, config_files[i]);
        snprintf(tmp_path, sizeof(tmp_path), "%s/%s.tmp", home, config_files[i]);

        FILE *src = fopen(src_path, "r");
        if (!src) continue;

        FILE *dst = fopen(tmp_path, "w");
        if (!dst) {
            fclose(src);
            continue;
        }

        char line[1024];
        int modified = 0;
        while (fgets(line, sizeof(line), src)) {
            if (strstr(line, "STORM_HOME") ||
                (strstr(line, "PATH") && strstr(line, "storm/bin"))) {
                modified = 1;
                continue;
            }
            fputs(line, dst);
        }

        fclose(src);
        fclose(dst);

        if (modified) {
            rename(tmp_path, src_path);
            chmod(src_path, 0644);
        } else {
            remove(tmp_path);
        }
    }
}

void remove_directory_contents(const char *path) {
    DIR *d = opendir(path);
    if (!d) return;

    struct dirent *dir;
    while ((dir = readdir(d)) != NULL) {
        if (!strcmp(dir->d_name, ".") || !strcmp(dir->d_name, "..")) continue;

        char full_path[PATH_MAX];
        snprintf(full_path, sizeof(full_path), "%s/%s", path, dir->d_name);

        struct stat st;
        if (stat(full_path, &st) == 0) {
            if (S_ISDIR(st.st_mode)) {
                remove_directory_contents(full_path);
                rmdir(full_path);
            } else {
                remove(full_path);
            }
        }
    }
    closedir(d);
}

void uninstall_Storm() {
    char *storm_home = getenv("STORM_HOME");
    const char *search_paths[MAX_PATHS] = {
        "/usr/local/storm",
        "/opt/storm",
        "/usr/share/storm",
        "/var/lib/storm",
        storm_home
    };

    // Phase 1: Locate installation
    char install_path[PATH_MAX] = {0};
    for (int i = 0; i < MAX_PATHS; i++) {
        if (!search_paths[i]) continue;

        struct stat st;
        if (stat(search_paths[i], &st) == 0 && S_ISDIR(st.st_mode)) {
            if (validate_storm_home(search_paths[i])) {
                strncpy(install_path, search_paths[i], sizeof(install_path)-1);
                break;
            }
        }
    }

    if (!install_path[0]) {
        FPRINTF(global_client_socket, "No valid Storm installation found\n");
        return;
    }

    // Phase 2: Remove installation
    // PRINTF(global_client_socket,"Removing installation at: %s\n", install_path);
    remove_directory_contents(install_path);
    if (rmdir(install_path) != 0) {
        FPRINTF(global_client_socket, "Failed to remove directory. Check permissions.\n");
    }

    // Phase 3: Environment cleanup
    remove_environment_vars();

    // Phase 4: Remove residual files
    const char *residual_paths[] = {
        "/var/log/storm",
        "/var/run/storm",
        "/tmp/storm"
    };

    for (size_t i = 0; i < sizeof(residual_paths)/sizeof(residual_paths[0]); i++) {
        struct stat st;
        if (stat(residual_paths[i], &st) == 0) {
            //PRINTF(global_client_socket,"Found residual directory: %s\n", residual_paths[i]);
            //PRINTF(global_client_socket,"Remove manually with: sudo rm -rf %s\n", residual_paths[i]);
        }
    }

    // Phase 5: Dependency check
    //PRINTF(global_client_socket,"\nPotential dependencies to review:\n");
    //PRINTF(global_client_socket,"- Java Runtime Environment\n");
    //PRINTF(global_client_socket,"- ZooKeeper\n");
    // PRINTF(global_client_socket,"- Python (for storm CLI)\n");
    //   PRINTF(global_client_socket,"- Any installed Storm topologies\n");

    PRINTF(global_client_socket,"\nUninstallation complete. Some elements may require manual cleanup.\n");
}

void uninstall_flink() {
    char flink_dir[PATH_MAX] = {0};
    char cmd[PATH_MAX * 2];
    struct stat st;
    int ret;
    // FILE *fp;

    // 1. Determine installation location
    char* flink_home = getenv("FLINK_HOME");
    if (flink_home) {
        strncpy(flink_dir, flink_home, sizeof(flink_dir)-1);
    } else {
        // Check standard installation locations
        const char* possible_paths[] = {
            "/usr/local/flink",
            "/opt/flink",
            getenv("HOME") ? strcat(strcpy(flink_dir, getenv("HOME")), "/flink") : NULL
        };

        for (size_t i = 0; i < sizeof(possible_paths)/sizeof(possible_paths[0]); i++) {
            if (possible_paths[i] && stat(possible_paths[i], &st) == 0 && S_ISDIR(st.st_mode)) {
                strncpy(flink_dir, possible_paths[i], sizeof(flink_dir)-1);
                break;
            }
        }
    }

    // 2. Validate Flink installation
    if (stat(flink_dir, &st) != 0 || !S_ISDIR(st.st_mode)) {
        FPRINTF(global_client_socket, "Flink installation not found\n");
        exit(EXIT_FAILURE);
    }

    // 3. Stop running services
    char stop_script[PATH_MAX];
    snprintf(stop_script, sizeof(stop_script), "%s/bin/stop-cluster.sh", flink_dir);
    if (access(stop_script, X_OK) == 0) {
        //PRINTF(global_client_socket,"Stopping Flink services...\n");
        snprintf(cmd, sizeof(cmd), "%s", stop_script);
        ret = executeSystemCommand(cmd);
        if (WEXITSTATUS(ret) != 0) {
            FPRINTF(global_client_socket, "Warning: Failed to stop services (exit code %d)\n", WEXITSTATUS(ret));
        }
    }

    // 4. Remove installation directory
    //PRINTF(global_client_socket,"Removing installation directory...\n");
    snprintf(cmd, sizeof(cmd), "sudo rm -rf \"%s\"", flink_dir);
    ret = executeSystemCommand(cmd);
    if (ret == -1) {
        FPRINTF(global_client_socket, "Failed to remove installation directory\n");
        exit(EXIT_FAILURE);
    }

    // 5. Clean residual files
    //PRINTF(global_client_socket,"Cleaning residual files...\n");
    const char* patterns[] = {"flink-*.tgz", "flink-*"};
    for (size_t i = 0; i < sizeof(patterns)/sizeof(patterns[0]); i++) {
        snprintf(cmd, sizeof(cmd), "find %s -maxdepth 3 -name '%s' -exec rm -rf {} + 2>/dev/null",
                 getenv("HOME") ? getenv("HOME") : ".", patterns[i]);
        int result = executeSystemCommand(cmd);
        if (result == -1) {
            PRINTF(global_client_socket,"Command failed with return code %d\n", result);
        }
    }

    // 6. Remove environment configuration
    //PRINTF(global_client_socket,"Cleaning environment variables...\n");
    const char* shell_files[] = {".bashrc", ".bash_profile", ".zshrc"};
    for (size_t i = 0; i < sizeof(shell_files)/sizeof(shell_files[0]); i++) {
        char path[PATH_MAX];
        long unsigned int len = snprintf(path, sizeof(path), "%s/%s", getenv("HOME"), shell_files[i]);
        if (len >= sizeof(path)) {
            FPRINTF(global_client_socket, "snprintf output was truncated or failed.\n");
        }
        if (stat(path, &st) == 0) {
            // Create temporary file
            char temp_path[PATH_MAX];
            long unsigned int len = snprintf(temp_path, sizeof(temp_path), "%s.tmp", path);
            if (len >= sizeof(path)) {
                FPRINTF(global_client_socket, "snprintf output was truncated or failed.\n");
            }
            FILE *src = fopen(path, "r");
            FILE *dst = fopen(temp_path, "w");

            if (src && dst) {
                char line[1024];
                int in_flink_block = 0;

                while (fgets(line, sizeof(line), src)) {
                    if (strstr(line, "# Apache Flink configuration")) {
                        in_flink_block = 1;
                        continue;
                    }
                    if (in_flink_block) {
                        if (strstr(line, "FLINK_HOME") || strstr(line, "flink/bin")) {
                            continue;
                        }
                        if (strchr(line, '\n') && in_flink_block) {
                            in_flink_block = 0;
                        }
                    } else {
                        fputs(line, dst);
                    }
                }

                fclose(src);
                fclose(dst);

                // Replace original file
                rename(temp_path, path);
            }
        }
    }

    // 7. Optional: Package manager cleanup
    //PRINTF(global_client_socket,"Checking package manager artifacts...\n");
    const char* pkg_commands[] = {
        "dpkg -l | grep -i flink && echo 'Warning: Potential debian packages found'",
        "rpm -qa | grep -i flink && echo 'Warning: Potential RPM packages found'"
    };
    for (size_t i = 0; i < sizeof(pkg_commands)/sizeof(pkg_commands[0]); i++) {
        int result = executeSystemCommand(pkg_commands[i]);
        if (result == -1) {
            PRINTF(global_client_socket,"Command failed with return code %d\n", result);
        }
    }

    PRINTF(global_client_socket,"Uninstallation complete\n");
}

void uninstall_zookeeper() {
    char *zk_home = getenv("ZOOKEEPER_HOME");
    //int isDebian;
    char command[1024];
    int ret, attempt;
    char *install_paths[] = {"/usr/local/zookeeper", "/opt/zookeeper", NULL};
    char *data_dir = "/var/lib/zookeeper";
    char bashrc_path[PATH_MAX];
    FILE *bashrc;
    char temp_file[] = "/tmp/bashrc_tempXXXXXX";
    char line[1024];
    int modified = 0;
    char *home;

    // Phase 1: Service termination
    //  PRINTF(global_client_socket,"[1/5] Stopping ZooKeeper service...\n");
    if (zk_home) {
        snprintf(command, sizeof(command), "\"%s/bin/zkServer.sh\" stop 2>/dev/null", zk_home);
    } else {
        strcpy(command, "pkill -f 'java.*zookeeper' 2>/dev/null");
    }
    for (attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
        ret = executeSystemCommand(command);
        if (ret == 0) break;
        sleep(1);
    }

    // Phase 2: Installation directory removal
    //   PRINTF(global_client_socket,"[2/5] Removing installation directories...\n");
    if (zk_home) {
        snprintf(command, sizeof(command), "sudo rm -rf \"%s\"", zk_home);
        int result = executeSystemCommand(command);
        if (result == -1) {
            PRINTF(global_client_socket,"Command failed with return code %d\n", result);
        }
    } else {
        for (char **path = install_paths; *path; path++) {
            if (access(*path, F_OK) == 0) {
                snprintf(command, sizeof(command), "sudo rm -rf \"%s\"", *path);
                int result = executeSystemCommand(command);
                if (result == -1) {
                    PRINTF(global_client_socket,"Command failed with return code %d\n", result);
                }
            }
        }
    }

    // Phase 3: Data directory cleanup
    //PRINTF(global_client_socket,"[3/5] Removing data directories...\n");
    snprintf(command, sizeof(command), "sudo rm -rf %s", data_dir);
    int result = executeSystemCommand(command);
    if (result == -1) {
        PRINTF(global_client_socket,"Command failed with return code %d\n", result);
    }
    // Phase 4: Environment cleanup
    //PRINTF(global_client_socket,"[4/5] Cleaning environment configurations...\n");
    home = getenv("HOME");
    if (home) {
        snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home);

        // Create temporary file
        int temp_fd = mkstemp(temp_file);
        if (temp_fd == -1) return;
        FILE *temp = fdopen(temp_fd, "w");

        // Filter .bashrc
        bashrc = fopen(bashrc_path, "r");
        if (bashrc && temp) {
            while (fgets(line, sizeof(line), bashrc)) {
                if (strstr(line, "ZOOKEEPER_HOME") ||
                    strstr(line, "zookeeper/bin")) {
                    modified = 1;
                    continue;
                }
                fputs(line, temp);
            }
            fclose(bashrc);
            fclose(temp);

            // Replace original if modified
            if (modified) {
                long unsigned int len =   snprintf(command, sizeof(command), "cp %s %s && rm %s",
                                                   temp_file, bashrc_path, temp_file);
                if (len >= sizeof(command)) {
                    FPRINTF(global_client_socket, "snprintf output was truncated or failed.\n");
                }
                int result = executeSystemCommand(command);
                if (result == -1) {
                    PRINTF(global_client_socket,"Command failed with return code %d\n", result);
                }
            } else {
                unlink(temp_file);
            }
        }
    }

    // Phase 5: Dependency cleanup
    //PRINTF(global_client_socket,"[5/5] Checking for Java dependencies...\n");
    int resuultGrep = executeSystemCommand("dpkg -l | grep -q 'openjdk' && echo 'Consider removing Java: sudo apt remove openjdk-*'");
    if (resuultGrep == -1) {
        PRINTF(global_client_socket,"Command failed with return code %d\n", resuultGrep);
    }
    int resultQa = executeSystemCommand("rpm -qa | grep -q 'java' && echo 'Consider removing Java: sudo yum remove java-*'");
    if (resultQa == -1) {
        PRINTF(global_client_socket,"Command failed with return code %d\n", resultQa);
    }

    PRINTF(global_client_socket,"Uninstallation complete. Manual verification recommended.\n");
}


