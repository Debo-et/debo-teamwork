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


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <ctype.h>
#include <pwd.h>
#include "utiles.h"
#include <limits.h>
#include <glob.h>

#include "configuration.h"
#include "utiles.h"

int sourceBashrc() {
    const struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        return -1;
    }

    char bashrc_path[PATH_MAX];
    if (snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", pwd->pw_dir) >= (int)sizeof(bashrc_path)) {
        return -1; // Path truncated
    }

    char command[PATH_MAX + 200];
    if (snprintf(command, sizeof(command),
                 "bash -c \"(source \\\"%s\\\" >/dev/null 2>&1); status=\\$?; env; echo SOURCE_EXIT_STATUS=\\$status\"",
                 bashrc_path) >= (int)sizeof(command)) {
        return -1; // Command truncated
    }

    FILE *fp = popen(command, "r");
    if (!fp) {
        return -1;
    }

    char line[4096];
    int source_exit_status = -1;

    while (fgets(line, sizeof(line), fp)) {
        size_t len = strlen(line);
        if (len > 0 && line[len - 1] == '\n') {
            line[len - 1] = '\0';
        }

        if (strncmp(line, "SOURCE_EXIT_STATUS=", 19) == 0) {
            char *status_str = line + 19;
            source_exit_status = atoi(status_str);
        } else {
            char *eq = strchr(line, '=');
            if (eq) {
                *eq = '\0';
                char *value = eq + 1;
                if (setenv(line, value, 1) != 0) {
                    pclose(fp);
                    return -1;
                }
            }
        }
    }

    int pclose_ret = pclose(fp);
    if (pclose_ret == -1) {
        return -1;
    } else if (WIFEXITED(pclose_ret)) {
        int bash_exit = WEXITSTATUS(pclose_ret);
        if (bash_exit != 0) {
            return -1;
        }
    } else {
        return -1;
    }

    if (source_exit_status == -1) {
        return -1;
    }

    return source_exit_status;
}
void install_hadoop(const char *version, char *location) {
    // Determine package manager and base OS
    int debian = (executeSystemCommand("command -v apt >/dev/null 2>&1") != 0);
    int redhat = (executeSystemCommand("command -v yum >/dev/null 2>&1") != 0) ||
        (executeSystemCommand("command -v dnf >/dev/null 2>&1") != 0);

    if (!debian && !redhat) {
        FPRINTF(global_client_socket,  "Error: Unsupported OS distribution (no apt/yum/dnf found)\n");
        return;
    }

    // Install wget if missing
    if (!executeSystemCommand("command -v wget >/dev/null 2>&1")) {
        char cmd[256];
        if (debian) {
            snprintf(cmd, sizeof(cmd), "apt update && apt install -y wget");
        } else {
            const char *pm = (executeSystemCommand("command -v dnf >/dev/null 2>&1") == 0) ? "dnf" : "yum";
            snprintf(cmd, sizeof(cmd), "%s install -y wget", pm);
        }
        if (!executeSystemCommand(cmd)) {
            FPRINTF(global_client_socket,  "Error: Failed to install wget. Check network or permissions.\n");
            return;
        }
    }

    // Construct download URL
    char url[512];
    const char *hadoop_version = version ? version : "3.3.6";
    snprintf(url, sizeof(url), "https://dlcdn.apache.org/hadoop/common/hadoop-%s/hadoop-%s.tar.gz",
             hadoop_version, hadoop_version);

    // Validate URL buffer safety
    if (strlen(url) >= sizeof(url)) {
        FPRINTF(global_client_socket,  "Error: Hadoop version string too long\n");
        return;
    }

    // Download archive
    char wget_cmd[512];
    size_t ret = snprintf(wget_cmd, sizeof(wget_cmd), "wget -q %s", url); // -q for quiet mode
    if (ret >= sizeof(wget_cmd)) {
        FPRINTF(global_client_socket,  "Error: size error  sizeof(wget_cmd)\n");
        return;
    }
    if (!executeSystemCommand(wget_cmd)) {
        FPRINTF(global_client_socket,  "Error: Failed to download Hadoop %s (invalid version?)\n", hadoop_version);
        return;
    }

    // Extract archive
    char tar_cmd[512];
    snprintf(tar_cmd, sizeof(tar_cmd), "tar -xvzf hadoop-%s.tar.gz", hadoop_version);
    if (!executeSystemCommand(tar_cmd)) {
        FPRINTF(global_client_socket,  "Error: Extraction failed. Corrupted download?\n");
        return;
    }
    char *install_dir;
    // Determine installation path
    if (!location)
        install_dir = debian ? "/usr/local/hadoop" : "/opt/hadoop";
    else
        install_dir = location;

    // Create installation directory
    char mkdir_cmd[512];
    snprintf(mkdir_cmd, sizeof(mkdir_cmd), "mkdir -p %s", install_dir);
    if (!executeSystemCommand(mkdir_cmd)) {
        FPRINTF(global_client_socket,  "Error: Failed to create %s. Check permissions.\n", install_dir);
        return;
    }

    // Move extracted files
    char mv_cmd[512];
    snprintf(mv_cmd, sizeof(mv_cmd),
             "mv hadoop-%s/* %s && rm -rf hadoop-%s && rm -f hadoop-%s.tar.gz",
             hadoop_version, install_dir, hadoop_version, hadoop_version);
    if (!executeSystemCommand(mv_cmd)) {
        FPRINTF(global_client_socket,  "Error: Failed to move files to %s\n", install_dir);
        return;
    }

    // Adjust ownership of install_dir to current user
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        FPRINTF(global_client_socket,  "Error: Could not determine current user\n");
        return;
    }
    char chown_cmd[512];
    snprintf(chown_cmd, sizeof(chown_cmd), "chown -R %s:%s %s", pwd->pw_name, pwd->pw_name, install_dir);
    if (!executeSystemCommand(chown_cmd)) {
        FPRINTF(global_client_socket,  "Error: Failed to set ownership of %s\n", install_dir);
        return;
    }

    // Update .bashrc (with duplication check)
    char *home = getenv("HOME");
    if (!home) {
        FPRINTF(global_client_socket,  "Error: $HOME not set\n");
        return;
    }
    char bashrc_path[512];
    snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home);

    // Check if Hadoop config already exists
    int hadoop_config_exists = 0;
    FILE *bashrc_check = fopen(bashrc_path, "r");
    if (bashrc_check) {
        char line[256];
        while (fgets(line, sizeof(line), bashrc_check)) {
            if (strstr(line, "HADOOP_HOME")) {
                hadoop_config_exists = 1;
                break;
            }
        }
        fclose(bashrc_check);
    }

    if (!hadoop_config_exists) {
        FILE *bashrc = fopen(bashrc_path, "a");
        if (bashrc) {
            fprintf(bashrc, "\n# Hadoop Environment Variables\nexport HADOOP_HOME=%s\n", install_dir);
            fprintf(bashrc, "export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin\n");
            fclose(bashrc);
        } else {
            FPRINTF(global_client_socket,  "Warning: Could not update .bashrc. Manual configuration needed.\n");
        }
    }

    // Source .bashrc
    if (sourceBashrc() != 0) {
        FPRINTF(global_client_socket,  "Warning: Failed to source .bashrc. Run 'source ~/.bashrc' manually.\n");
    }

    // Configure JAVA_HOME
    char javahome[512] = {0};
    FILE *fp = popen("update-alternatives --query javac | grep 'Value: ' | cut -d' ' -f2 | xargs dirname | xargs dirname 2>/dev/null", "r");
    if (fp) {
        if (fgets(javahome, sizeof(javahome), fp)) {
            size_t len = strlen(javahome);
            if (len > 0 && javahome[len-1] == '\n') javahome[len-1] = '\0';
        }
        pclose(fp);
    }

    if (strlen(javahome) == 0) {
        char *env_java = getenv("JAVA_HOME");
        if (env_java) strncpy(javahome, env_java, sizeof(javahome)-1);
    }

    if (strlen(javahome) == 0) {
        FPRINTF(global_client_socket,  "Error: JAVA_HOME not found. Install Java JDK.\n");
        return;
    }

    // Update hadoop-env.sh
    char hadoop_env_path[512];
    snprintf(hadoop_env_path, sizeof(hadoop_env_path), "%s/etc/hadoop/hadoop-env.sh", install_dir);
    FILE *hadoop_env = fopen(hadoop_env_path, "a");
    if (hadoop_env) {
        fprintf(hadoop_env, "\nexport JAVA_HOME=%s\n", javahome);
        fclose(hadoop_env);
    } else {
        FPRINTF(global_client_socket,  "Error: Could not configure Hadoop environment. Check permissions.\n");
        return;
    }

    // Create HDFS directories
    char nameNode[512], dataNode[512];
    snprintf(nameNode, sizeof(nameNode), "%s/hdfs/namenode", home);
    snprintf(dataNode, sizeof(dataNode), "%s/hdfs/datanode", home);
    char mkdir_hdfs_cmd[512];
    size_t ret2 =  snprintf(mkdir_hdfs_cmd, sizeof(mkdir_hdfs_cmd), "mkdir -p %s %s", nameNode, dataNode);
    if (ret2 >= sizeof(mkdir_hdfs_cmd)) {
        FPRINTF(global_client_socket,  "Error: size error  \n");
    }
    if (!executeSystemCommand(mkdir_hdfs_cmd)) {
        FPRINTF(global_client_socket,  "Error: Failed to create HDFS directories\n");
        return;
    }

    // Format NameNode
    char format_cmd[512];
    snprintf(format_cmd, sizeof(format_cmd), "%s/bin/hdfs namenode -format -force", install_dir);
    if (!executeSystemCommand(format_cmd)) {
        FPRINTF(global_client_socket,  "Error: NameNode format failed. Check HDFS config.\n");
        return;
    }

    char candidate_path[PATH_MAX];
    snprintf(candidate_path, sizeof(candidate_path), "%s/etc/hadoop/", install_dir);

    if (create_xml_file(candidate_path, "ranger-hdfs-audit.xml") !=0)
        FPRINTF(global_client_socket,  "Failed to create XML file\n");


    if (create_xml_file(candidate_path, "ranger-policymgr-ssl.xml") !=0)
        FPRINTF(global_client_socket,  "Failed to create XML file\n");

    if (create_xml_file(candidate_path, "ranger-hdfs-policymgr-ssl.xml") !=0)
        FPRINTF(global_client_socket,  "Failed to create XML file\n");

    if (create_xml_file(candidate_path, "ssl-server.xml") !=0)
        FPRINTF(global_client_socket,  "Failed to create XML file\n");


    if (create_xml_file(candidate_path, "ranger-hdfs-security.xml") !=0)
        FPRINTF(global_client_socket,  "Failed to create XML file\n");


    if (create_xml_file(candidate_path, "ssl-client.xml") !=0)
        FPRINTF(global_client_socket,  "Failed to create XML file\n");


    if (create_xml_file(candidate_path, "ranger-yarn-audit.xml") !=0)
        FPRINTF(global_client_socket,  "Failed to create XML file\n");

    if (create_xml_file(candidate_path, "ranger-yarn-policymgr-ssl.xml") !=0)
        FPRINTF(global_client_socket,  "Failed to create XML file\n");

    if (create_xml_file(candidate_path, "ranger-yarn-security.xml") !=0)
        FPRINTF(global_client_socket,  "Failed to create XML file\n");

    if (create_xml_file(candidate_path, "resource-types.xml") !=0)
        FPRINTF(global_client_socket,  "Failed to create XML file\n");

    if (create_properties_file(candidate_path, "ranger-hdfs-plugin.properties") !=0)
        FPRINTF(global_client_socket,   "Failed to create XML file\n");

    if (create_properties_file(candidate_path, "ranger-yarn-plugin.properties") !=0)
        FPRINTF(global_client_socket,   "Failed to create XML file\n");

    //			 NameNode Configuration
    modify_hdfs_config("dfs.namenode.name.dir", nameNode, "hdfs-site.xml");
    //  handle_result(status, "dfs.namenode.name.dir", nameNode, "hdfs-site.xml");

    modify_hdfs_config("dfs.datanode.data.dir", dataNode, "hdfs-site.xml");
    // handle_result(status, "dfs.datanode.data.dir", dataNode, "hdfs-site.xml");


    PRINTF(global_client_socket, "Hadoop installed successfully to %s\n", install_dir);
}

void install_Atlas(char *version, char *location) {
    char url[512];
    char filename[256];
    char command[1024];
    FILE *fp;

    const char *actual_version = version ? version : "1.2.0";

    // Download source distribution
    snprintf(url, sizeof(url), "https://downloads.apache.org/atlas/%s/apache-atlas-%s-sources.tar.gz",
             actual_version, actual_version);
    snprintf(filename, sizeof(filename), "apache-atlas-%s-sources.tar.gz", actual_version);

    // Download using wget
    snprintf(command, sizeof(command), "wget -q %s -O %s", url, filename);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket, "Download failed. Check version availability.\n");
        return;
    }

    // Extract source archive
    snprintf(command, sizeof(command), "tar -xvzf %s", filename);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket, "Source extraction failed.\n");
        return;
    }

    // Remove source archive
    snprintf(command, sizeof(command), "rm -f %s", filename);
    executeSystemCommand(command);  // Optional error handling

    // Build with Maven
    snprintf(command, sizeof(command),
             "cd apache-atlas-sources-%s && "
             "mvn clean -DskipTests package -Pdist,embedded-hbase-solr",
             actual_version);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket, "Maven build failed.\n");
        return;
    }

    // Extract binary distribution
    char binary_tarball[512];
    snprintf(binary_tarball, sizeof(binary_tarball),
             "apache-atlas-sources-%s/distro/target/apache-atlas-%s-bin.tar.gz",
             actual_version, actual_version);
    snprintf(command, sizeof(command), "tar -xvzf %s", binary_tarball);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket, "Binary extraction failed.\n");
        return;
    }

    // Cleanup source directory
    snprintf(command, sizeof(command), "rm -rf apache-atlas-sources-%s", actual_version);
    executeSystemCommand(command);  // Optional error handling

    // Determine installation path
    char *install_path;
    if (!location) {
        install_path = (access("/etc/debian_version", F_OK) == 0) ? "/usr/local" : "/opt";
    } else {
        install_path = location;
    }

    // Move binary directory
    char extracted_dir[256];
    snprintf(extracted_dir, sizeof(extracted_dir), "apache-atlas-%s", actual_version);
    snprintf(command, sizeof(command), "mv %s %s/atlas-%s",
             extracted_dir, install_path, actual_version);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket, "Moving installation failed.\n");
        return;
    }

    // Set ownership
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        FPRINTF(global_client_socket, "Could not determine current user\n");
        return;
    }
    snprintf(command, sizeof(command), "chown -R %s:%s %s/atlas-%s",
             pwd->pw_name, pwd->pw_name, install_path, actual_version);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket, "Ownership change failed\n");
        return;
    }

    // Configure Hadoop integration
    char *hadoop_home = getenv("HADOOP_HOME");
    if (!hadoop_home) {
        FPRINTF(global_client_socket, "HADOOP_HOME not set!\n");
        return;
    }

    char atlas_conf_dir[512];
    snprintf(atlas_conf_dir, sizeof(atlas_conf_dir),
             "%s/atlas-%s/conf", install_path, actual_version);

    // Link core-site.xml
    snprintf(command, sizeof(command), "ln -sf %s/etc/hadoop/core-site.xml %s/",
             hadoop_home, atlas_conf_dir);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket, "core-site.xml linking failed\n");
    }

    // Link hdfs-site.xml
    snprintf(command, sizeof(command), "ln -sf %s/etc/hadoop/hdfs-site.xml %s/",
             hadoop_home, atlas_conf_dir);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket, "hdfs-site.xml linking failed\n");
    }

    // Update environment variables
    const char *home = getenv("HOME");
    if (home) {
        char bashrc[512];
        snprintf(bashrc, sizeof(bashrc), "%s/.bashrc", home);
        fp = fopen(bashrc, "a");
        if (fp) {
            fprintf(fp, "\n# Apache Atlas settings\n");
            fprintf(fp, "export ATLAS_HOME=%s/atlas-%s\n", install_path, actual_version);
            fprintf(fp, "export PATH=\"$PATH:$ATLAS_HOME/bin\"\n");
            fclose(fp);
        }
    }

    // Source updated environment
    if (sourceBashrc() != 0) {
        FPRINTF(global_client_socket, "Sourcing .bashrc failed\n");
    }

    // Initialize HBase schema
    snprintf(command, sizeof(command),
             "%s/atlas-%s/bin/atlas_start.py --setup",
             install_path, actual_version);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket, "HBase schema initialization failed\n");
    }

    PRINTF(global_client_socket, "Apache Atlas %s installed successfully at %s/atlas-%s\n",
           actual_version, install_path, actual_version);
}

void install_Storm(char *version, char *location) {

    // Create download URL
    char url[256];
    if (version)
        snprintf(url, sizeof(url),
                 "https://downloads.apache.org/storm/apache-storm-%s/apache-storm-%s.tar.gz",
                 version, version);
    else
        snprintf(url, sizeof(url),
                 "https://downloads.apache.org/storm/apache-storm-1.2.4/apache-storm-1.2.4.tar.gz");

    // Download the archive
    // PRINTF(global_client_socket, "Downloading Storm ...\n");
    char wget_cmd[512];
    snprintf(wget_cmd, sizeof(wget_cmd), "wget -q %s", url);
    if (!executeSystemCommand(wget_cmd)) {
        FPRINTF(global_client_socket,  "Download failed. Check version availability.\n");
        return;
    }

    // Extract the archive
    // PRINTF(global_client_socket, "Extracting archive...\n");
    char tar_cmd[512];
    if (version)
        snprintf(tar_cmd, sizeof(tar_cmd),
                 "tar -xzf apache-storm-%s.tar.gz", version);
    else
        snprintf(tar_cmd, sizeof(tar_cmd),
                 "tar -xzf apache-storm-1.2.4.tar.gz");

    if (!executeSystemCommand(tar_cmd)) {
        FPRINTF(global_client_socket,  "Extraction failed\n");
        return;
    }

    // Remove downloaded archive after extraction
    //PRINTF(global_client_socket, "Removing archive...\n");
    char rm_cmd[512];
    if (version)
        snprintf(rm_cmd, sizeof(rm_cmd), "rm -f apache-storm-%s.tar.gz", version);
    else
        snprintf(rm_cmd, sizeof(rm_cmd), "rm -f apache-storm-1.2.4.tar.gz");

    if (!executeSystemCommand(rm_cmd)) {
        FPRINTF(global_client_socket,  "Failed to remove archive.\n");
    }

    // Determine OS type and installation path
    char *install_dir = NULL;
    if (!location)
    {
        if (!access("/etc/debian_version", F_OK)) {
            install_dir = "/usr/local/storm";
        } else if (!access("/etc/redhat-release", F_OK)) {
            install_dir = "/opt/storm";
        } else {
            FPRINTF(global_client_socket,  "Unsupported Linux distribution\n");
            return;
        }
    }
    else
        install_dir = location;
    // Move extracted directory (with sudo if needed)
    // PRINTF(global_client_socket, "Installing to %s...\n", install_dir);
    char mv_cmd[512];
    int is_root = (geteuid() == 0);
    if (version)
        snprintf(mv_cmd, sizeof(mv_cmd), "%smv apache-storm-%s %s",
                 is_root ? "" : " ",
                 version,
                 install_dir);
    else
        snprintf(mv_cmd, sizeof(mv_cmd), "%smv apache-storm-1.2.4 %s",
                 is_root ? "" : " ",
                 install_dir);

    if (!executeSystemCommand(mv_cmd)) {
        FPRINTF(global_client_socket,  "Installation directory error. Check permissions.\n");
        return;
    }

    // Adjust ownership of install_dir to current user
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        FPRINTF(global_client_socket,  "Error: Could not determine current user\n");
        return;
    }
    char chown_cmd[512];
    snprintf(chown_cmd, sizeof(chown_cmd)," chown -R %s:%s %s", pwd->pw_name, pwd->pw_name, install_dir);
    if (!executeSystemCommand(chown_cmd)) {
        FPRINTF(global_client_socket,  "Error: Failed to set ownership of %s\n", install_dir);
        return;
    }

    // Configure environment variables
    //PRINTF(global_client_socket, "Configuring environment...\n");
    char *home = getenv("HOME");
    if (!home) {
        FPRINTF(global_client_socket,  "Could not determine user home directory\n");
        return;
    }

    char bashrc_path[512];
    snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home);

    FILE *bashrc = fopen(bashrc_path, "a");
    if (!bashrc) {
        PERROR(global_client_socket,"Error updating bashrc");
        return;
    }

    fprintf(bashrc, "\n# Apache Storm configuration\n");
    fprintf(bashrc, "export STORM_HOME=%s\n", install_dir);
    fprintf(bashrc, "export PATH=$PATH:$STORM_HOME/bin\n");
    fclose(bashrc);

    // Source the updated bashrc
    if (sourceBashrc() != 0) {
        FPRINTF(global_client_socket,  "Failed to source .bashrc\n");
        return;
    }

    // Construct path to storm.yaml
    char storm_yaml_path[512];
    snprintf(storm_yaml_path, sizeof(storm_yaml_path), "%s/conf/storm.yaml", install_dir);

    // Configure Storm settings with sudo if needed
    char sudo_cmd[256];
    snprintf(sudo_cmd, sizeof(sudo_cmd), "%s", is_root ? "" : " ");

    // Create Storm data directory with proper permissions
    // Create Storm data directory with proper permissions (FIXED)
    //printf("Creating Storm data directory...\n");
    char mkdir_cmd[512];
    snprintf(mkdir_cmd, sizeof(mkdir_cmd),
             "%smkdir -p /var/lib/storm-data && %schown -R %s:%s /var/lib/storm-data >/dev/null 2>&1",  // FIXED
             is_root ? "" : " ",
             is_root ? "" : " ",
             pwd->pw_name, pwd->pw_name);  // Use current user's credentials
    if (!executeSystemCommand(mkdir_cmd)) {
        FPRINTF(global_client_socket,  "Failed to create Storm data directory. Check permissions.\n");
        return;
    }

    // Final instructions
    PRINTF(global_client_socket, "\nInstallation complete!\n");
    // PRINTF(global_client_socket, "Ensure user 'storm' exists and has proper permissions on data directory.\n");
}


void install_Ranger(char* version, char *location) {
    char url[512];
    char filename[256];
    char extracted_dir[256];
    char command[4096];  // Increased buffer size for complex commands
    char* install_dir = NULL;
    const char* home = getenv("HOME");
    if (!home) {
        FPRINTF(global_client_socket, "HOME environment variable not set.\n");
        exit(EXIT_FAILURE);
    }

    // Generate URL and filenames for source distribution
    const char* ranger_version = version ? version : "2.6.0";
    snprintf(url, sizeof(url), "https://dlcdn.apache.org/ranger/%s/apache-ranger-%s.tar.gz",
             ranger_version, ranger_version);
    snprintf(filename, sizeof(filename), "apache-ranger-%s.tar.gz", ranger_version);
    snprintf(extracted_dir, sizeof(extracted_dir), "apache-ranger-%s", ranger_version);

    // Download with resume capability
    snprintf(command, sizeof(command), "wget -c -O %s %s", filename, url);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket, "Failed to download Ranger source.\n");
        exit(EXIT_FAILURE);
    }

    // Verify archive integrity before extraction
    snprintf(command, sizeof(command), "gzip -t %s", filename);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket, "Downloaded archive is corrupt.\n");
        exit(EXIT_FAILURE);
    }

    // Clean extraction directory if exists
    snprintf(command, sizeof(command), "rm -rf %s", extracted_dir);
    executeSystemCommand(command);

    // Extract with verbose output
    snprintf(command, sizeof(command), "tar -xvzf %s", filename);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket, "Extraction failed.\n");
        exit(EXIT_FAILURE);
    }

    // Fix 1: Clean potentially corrupted JARs
    snprintf(command, sizeof(command), "rm -rf %s/.m2/repository/javax/activation", home);
    executeSystemCommand(command);

    // Fix 2: Upgrade assembly plugin in ALL relevant POMs
    const char* pom_files[] = {
        "pom.xml",
        "ranger-distro/pom.xml",
        "ranger-plugins/pom.xml",
        "security-admin/pom.xml"
    };

    for (int i = 0; i < sizeof(pom_files)/sizeof(pom_files[0]); i++) {
        char pom_path[512];
        snprintf(pom_path, sizeof(pom_path), "%s/%s", extracted_dir, pom_files[i]);

        // Check if POM exists before modifying
        snprintf(command, sizeof(command), "test -f %s", pom_path);
        if (executeSystemCommand(command)) {
            snprintf(command, sizeof(command),
                     "sed -i.bak "
                     "'s/<maven-assembly-plugin.version>\\([0-9]\\+\\.[0-9]\\+\\.[0-9]\\+\\)<\\/maven-assembly-plugin.version>/"
                     "<maven-assembly-plugin.version>3.6.0<\\/maven-assembly-plugin.version>/g' "
                     "%s",
                     pom_path
                    );
            if (!executeSystemCommand(command)) {
                FPRINTF(global_client_socket, "Warning: Failed to update %s\n", pom_files[i]);
            }
        }
    }

    // Fix 3: Enhanced build with robust retry
    snprintf(command, sizeof(command),
             "cd %s && "
             "export MAVEN_OPTS=\"-Xmx6144m -XX:MaxPermSize=2048m\" && "
             "mvn -B clean compile package install assembly:assembly "
             "-DskipTests -Denunciate.skip=true "
             "-Dmaven-assembly-plugin.version=3.6.0 || "
             "{ "
             "  echo 'Build failed, retrying...' && "
             "  rm -rf ranger-distro/target && "
             "  mvn -B -rf :ranger-distro assembly:assembly "
             "    -DskipTests -Dmaven-assembly-plugin.version=3.6.0; "
             "}",
             extracted_dir
            );
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket, "Critical: Build failed after retries\n");
        exit(EXIT_FAILURE);
    }

    // Locate the built binary archive
    char built_filename[512];
    snprintf(built_filename, sizeof(built_filename),
             "%s/target/apache-ranger-%s-bin.tar.gz",
             extracted_dir, ranger_version);

    // Check if the built archive exists
    if (access(built_filename, F_OK) != 0) {
        FPRINTF(global_client_socket, "Built Ranger archive not found at: %s\n", built_filename);
        exit(EXIT_FAILURE);
    }

    // Extract the built binary archive
    snprintf(command, sizeof(command), "tar -xvzf %s", built_filename);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket, "Failed to extract built Ranger archive.\n");
        exit(EXIT_FAILURE);
    }

    // Determine extracted directory name
    char built_extracted_dir[256];
    snprintf(built_extracted_dir, sizeof(built_extracted_dir), "apache-ranger-%s", ranger_version);

    // Cleanup source artifacts
    snprintf(command, sizeof(command), "rm -f %s", filename);
    executeSystemCommand(command);  // Non-critical

    snprintf(command, sizeof(command), "rm -rf %s", extracted_dir);
    executeSystemCommand(command);  // Non-critical

    // Determine installation directory
    if (!location) {
        if (access("/etc/debian_version", F_OK) == 0) {
            install_dir = "/usr/local/ranger";
        } else if (access("/etc/redhat-release", F_OK) == 0 || access("/etc/system-release", F_OK) == 0) {
            install_dir = "/opt/ranger";
        } else {
            FPRINTF(global_client_socket, "Unsupported Linux distribution.\n");
            exit(EXIT_FAILURE);
        }
    } else {
        install_dir = location;
    }

    // Create installation directory if missing
    snprintf(command, sizeof(command), "mkdir -p %s", install_dir);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket, "Failed to create directory: %s\n", install_dir);
        exit(EXIT_FAILURE);
    }

    // Move built distribution to install location
    snprintf(command, sizeof(command), "mv %s %s", built_extracted_dir, install_dir);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket, "Failed to move to %s. Check permissions.\n", install_dir);
        exit(EXIT_FAILURE);
    }

    // Set ownership
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        FPRINTF(global_client_socket, "Could not determine current user\n");
        return;
    }
    snprintf(command, sizeof(command), "chown -R %s:%s %s",
             pwd->pw_name, pwd->pw_name, install_dir);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket, "Failed to set ownership of %s\n", install_dir);
        return;
    }

    // Update environment
    char bashrc_path[256];
    snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home);
    FILE* bashrc = fopen(bashrc_path, "a");
    if (!bashrc) {
        PERROR(global_client_socket, "Failed to open .bashrc");
        exit(EXIT_FAILURE);
    }

    fprintf(bashrc, "\n# Apache Ranger environment\n");
    fprintf(bashrc, "export RANGER_HOME=%s\n", install_dir);
    fprintf(bashrc, "export PATH=\"$PATH:$RANGER_HOME/bin\"\n");
    fclose(bashrc);

    // Finalize installation
    PRINTF(global_client_socket, "Ranger %s installed successfully at %s\n", ranger_version, install_dir);

    if (sourceBashrc() != 0) {
        FPRINTF(global_client_socket, "Warning: Sourcing .bashrc failed\n");
    }
}



void install_phoenix(char *version, char *location) {
    // Set default version if not specified
    if (!version) {
        version = "5.2.0";
        printf("Using default Phoenix version: %s\n", version);
    }

    // Determine HBASE_HOME
    char *hbase_home = getenv("HBASE_HOME");
    struct stat buffer;
    if (hbase_home == NULL) {
        const char *paths[] = {"/usr/local/hbase", "/opt/hbase", "/usr/lib/hbase"};
        size_t num_paths = sizeof(paths) / sizeof(paths[0]);

        for (size_t i = 0; i < num_paths; i++) {
            if (stat(paths[i], &buffer) == 0 && S_ISDIR(buffer.st_mode)) {
                hbase_home = (char *)paths[i];
                break;
            }
        }

        if (!hbase_home) {
            FPRINTF(global_client_socket, "HBase installation not found. Set HBASE_HOME or install HBase.\n");
            exit(1);
        }
    }

    // Verify HBase bin directory exists
    char hbase_bin[PATH_MAX];
    snprintf(hbase_bin, sizeof(hbase_bin), "%s/bin", hbase_home);
    if (stat(hbase_bin, &buffer) != 0 || !S_ISDIR(buffer.st_mode)) {
        FPRINTF(global_client_socket, "HBase bin directory not found: %s\n", hbase_bin);
        exit(1);
    }

    // Get HBase version
    char command[1024];
    snprintf(command, sizeof(command), "%s/bin/hbase version 2>&1", hbase_home);
    FILE *fp = popen(command, "r");
    if (!fp) {
        FPRINTF(global_client_socket, "Failed to get HBase version");
        exit(1);
    }

    char hbase_version[256] = "";
    char line[256];
    while (fgets(line, sizeof(line), fp) != NULL) {
        char *hb_pos = strstr(line, "HBase ");
        if (hb_pos) {
            char *ver_start = hb_pos + 6;
            char *ver_end = strchr(ver_start, '-');
            if (!ver_end) ver_end = strchr(ver_start, ' ');
            if (ver_end) *ver_end = '\0';
            strncpy(hbase_version, ver_start, sizeof(hbase_version)-1);
            hbase_version[sizeof(hbase_version)-1] = '\0';
            break;
        }
    }
    pclose(fp);

    if (!hbase_version[0]) {
        FPRINTF(global_client_socket, "Failed to detect HBase version\n");
        exit(1);
    }

    // Determine HBase compatibility string
    char hbase_compat[8];
    if (strncmp(hbase_version, "2.", 2) == 0) {
        // Handle new URL format for HBase 2.4+
        char *dot = strchr(hbase_version, '.');
        if (dot && isdigit(dot[1])) {
            int minor_version = atoi(dot + 1);
            if (minor_version >= 4) {
                snprintf(hbase_compat, sizeof(hbase_compat), "2.%d", minor_version);
            } else {
                strncpy(hbase_compat, "2x", sizeof(hbase_compat));
            }
        } else {
            strncpy(hbase_compat, "2x", sizeof(hbase_compat));
        }
    } else if (strncmp(hbase_version, "1.", 2) == 0) {
        strncpy(hbase_compat, "1x", sizeof(hbase_compat));
    } else {
        FPRINTF(global_client_socket,  "Unsupported HBase version: %s\n", hbase_version);
        exit(1);
    }

    // Generate URL for Phoenix binary
    char url[512], filename[256], dirname[256];
    snprintf(url, sizeof(url),
             "https://dlcdn.apache.org/phoenix/phoenix-%s/phoenix-hbase-%s-%s-bin.tar.gz",
             version,
             hbase_compat,
             version);

    snprintf(filename, sizeof(filename), "phoenix-%s-bin.tar.gz", version);
    snprintf(dirname, sizeof(dirname), "phoenix-hbase-%s-%s-bin",
             hbase_compat,
             version);

    // Download the binary archive
    printf("Downloading Phoenix %s...\n", version);
    snprintf(command, sizeof(command), "wget -q --show-progress '%s' -O %s", url, filename);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "Failed to download Phoenix. Check version or network.\n");
        unlink(filename);
        exit(1);
    }

    // Extract the archive
    printf("Extracting archive...\n");
    snprintf(command, sizeof(command), "tar -xzf %s", filename);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "Extraction failed. File may be corrupt.\n");
        unlink(filename);
        exit(1);
    }

    // Remove the downloaded archive
    unlink(filename);

    // Verify extraction
    if (stat(dirname, &buffer) != 0 || !S_ISDIR(buffer.st_mode)) {
        FPRINTF(global_client_socket,  "Extracted directory not found: %s\n", dirname);
        exit(1);
    }

    // Determine installation directory
    char install_dir[PATH_MAX];
    if (location) {
        strncpy(install_dir, location, sizeof(install_dir)-1);
        install_dir[sizeof(install_dir)-1] = '\0';
    } else {
        const char *default_dirs[] = {"/usr/local/phoenix", "/opt/phoenix"};
        size_t num_dirs = sizeof(default_dirs) / sizeof(default_dirs[0]);

        for (size_t i = 0; i < num_dirs; i++) {
            if (stat(default_dirs[i], &buffer) == 0) {
                strncpy(install_dir, default_dirs[i], sizeof(install_dir)-1);
                break;
            }
            if (i == num_dirs-1) {
                strncpy(install_dir, default_dirs[0], sizeof(install_dir)-1);
            }
        }
        install_dir[sizeof(install_dir)-1] = '\0';
    }

    // Create installation directory if needed
    if (stat(install_dir, &buffer) != 0) {
        printf("Creating installation directory: %s\n", install_dir);
        snprintf(command, sizeof(command), "mkdir -p %s", install_dir);
        if (!executeSystemCommand(command)) {
            FPRINTF(global_client_socket,  "Failed to create directory: %s\n", install_dir);
            exit(1);
        }
    }

    // Move extracted directory to install path
    printf("Installing to: %s\n", install_dir);
    snprintf(command, sizeof(command), "mv %s/* %s", dirname, install_dir);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "Failed to move Phoenix to %s. Check permissions.\n", install_dir);
        exit(1);
    }

    // Remove temporary directory
    rmdir(dirname);

    // Copy Phoenix server JAR to HBase's lib directory
    char pattern[PATH_MAX];
    snprintf(pattern, sizeof(pattern), "%s/phoenix-*-hbase-*-server.jar", install_dir);

    glob_t glob_result;
    if (glob(pattern, 0, NULL, &glob_result) == 0) {
        if (glob_result.gl_pathc > 0) {
            char *source_jar = glob_result.gl_pathv[0];
            char *jar_name = strrchr(source_jar, '/');
            if (jar_name) jar_name++;
            else jar_name = source_jar;

            char dest_jar[PATH_MAX];
            snprintf(dest_jar, sizeof(dest_jar), "%s/lib/%s", hbase_home, jar_name);

            printf("Copying server JAR to HBase: %s\n", dest_jar);
            snprintf(command, sizeof(command), "cp '%s' '%s'", source_jar, dest_jar);
            if (!executeSystemCommand(command)) {
                FPRINTF(global_client_socket,  "Failed to copy server JAR to HBase.\n");
                globfree(&glob_result);
                exit(1);
            }
        }
        globfree(&glob_result);
    } else {
        FPRINTF(global_client_socket,  "Phoenix server JAR not found in %s\n", install_dir);
        exit(1);
    }

    // Configure Phoenix in hbase-env.sh
    char hbase_env_path[PATH_MAX];
    snprintf(hbase_env_path, sizeof(hbase_env_path), "%s/conf/hbase-env.sh", hbase_home);

    char classpath_entry[1024];
    snprintf(classpath_entry, sizeof(classpath_entry), "export HBASE_CLASSPATH=\"$HBASE_CLASSPATH:%s\"", install_dir);

    // Check if entry already exists
    bool entry_exists = false;
    FILE *hbase_env = fopen(hbase_env_path, "r");
    if (hbase_env) {
        char line[1024];
        while (fgets(line, sizeof(line), hbase_env)) {
            if (strstr(line, classpath_entry)) {
                entry_exists = true;
                break;
            }
        }
        fclose(hbase_env);
    }

    // Append if not exists
    if (!entry_exists) {
        printf("Updating hbase-env.sh...\n");
        hbase_env = fopen(hbase_env_path, "a");
        if (hbase_env) {
            fprintf(hbase_env, "\n# Added by Phoenix installer\n%s\n", classpath_entry);
            fclose(hbase_env);
        } else {
            FPRINTF(global_client_socket, "Failed to update hbase-env.sh");
            exit(1);
        }
    }

    // Update .bashrc with PHOENIX_HOME
    char *home = getenv("HOME");
    if (!home) {
        FPRINTF(global_client_socket,  "Home directory not found.\n");
        exit(1);
    }

    char bashrc_path[PATH_MAX];
    snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home);

    char bashrc_entry[256];
    snprintf(bashrc_entry, sizeof(bashrc_entry), "export PHOENIX_HOME=%s", install_dir);

    // Check if entry already exists
    entry_exists = false;
    FILE *bashrc = fopen(bashrc_path, "r");
    if (bashrc) {
        char line[1024];
        while (fgets(line, sizeof(line), bashrc)) {
            if (strstr(line, bashrc_entry)) {
                entry_exists = true;
                break;
            }
        }
        fclose(bashrc);
    }

    // Append if not exists
    if (!entry_exists) {
        printf("Updating .bashrc...\n");
        bashrc = fopen(bashrc_path, "a");
        if (bashrc) {
            fprintf(bashrc, "\n# Added by Phoenix installer\n%s\n", bashrc_entry);
            fclose(bashrc);
        } else {
            FPRINTF(global_client_socket, "Failed to update .bashrc");
            exit(1);
        }
    }

    // Set ownership
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        FPRINTF(global_client_socket, "Error: Could not determine current user\n");
        exit(1);
    }

    snprintf(command, sizeof(command), "chown -R %s:%s %s", pwd->pw_name, pwd->pw_name, install_dir);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket, "Error: Failed to set ownership of %s\n", install_dir);
        exit(1);
    }

    PRINTF(global_client_socket, "\nSuccessfully installed Apache Phoenix %s\n", version);
}

void install_Solr(char* version, char *location) {
    char command[512];
    FILE *bashrc;
    char* install_dir = NULL;

    struct stat buffer;
    if (stat("/etc/debian_version", &buffer) == 0) {
        install_dir = "/usr/local/solr";
    } else if (stat("/etc/redhat-release", &buffer) == 0) {
        install_dir = "/opt/solr";
    } else {
        FPRINTF(global_client_socket,  "Unsupported OS. Exiting.\n");
        exit(1);
    }

    // 1. Download Solr archive
    if (version)
    {
        snprintf(command, sizeof(command),
                 "wget https://downloads.apache.org/solr/solr/9.9.0/solr-%s.tgz", version);
    }
    else
        snprintf(command, sizeof(command),
                 "wget https://downloads.apache.org/solr/solr/9.9.0/solr-9.9.0.tgz");

    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "downloading faild\n");
        return;
    }
    // 2. Extract the archive
    if (version)
        snprintf(command, sizeof(command),
                 "tar xzf solr-%s.tgz", version);
    else
        snprintf(command, sizeof(command),
                 "tar xzf solr-9.9.0.tgz");

    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "download failed.\n");
        return;
    }

    // Remove downloaded archive after extraction
    //  PRINTF(global_client_socket, "Removing archive...\n");
    if (version)
        snprintf(command, sizeof(command), "rm -f solr-%s.tgz", version);
    else
        snprintf(command, sizeof(command), "rm -f solr-9.9.0.tgz");

    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "Failed to remove archive.\n");
    }

    // 3. Determine OS and set installation path
    if (!location)
    {
        if(access("/etc/debian_version", F_OK) == 0) {
            install_dir = "/usr/local/solr";
        } else if(access("/etc/redhat-release", F_OK) == 0) {
            install_dir = "/opt/solr";
        }
    }
    else
        install_dir = location;
    // 4. Move files to installation directory
    if (version)
        snprintf(command, sizeof(command),
                 "mv solr-%s %s && chown -R $(whoami): %s",
                 version, install_dir, install_dir);
    else
        snprintf(command, sizeof(command),
                 "mv solr-9.9.0 %s &&  chown -R $(whoami): %s", install_dir, install_dir);

    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "move failed.\n");
        return;
    }

    // Adjust ownership of install_dir to current user
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        FPRINTF(global_client_socket,  "Error: Could not determine current user\n");
        return;
    }
    char chown_cmd[512];
    snprintf(chown_cmd, sizeof(chown_cmd), "chown -R %s:%s %s", pwd->pw_name, pwd->pw_name, install_dir);
    if (!executeSystemCommand(chown_cmd)) {
        FPRINTF(global_client_socket,  "Error: Failed to set ownership of %s\n", install_dir);
        return;
    }

    // 5. Set environment variables
    const char *home = getenv("HOME");
    if(home) {
        char path[256];
        snprintf(path, sizeof(path), "%s/.bashrc", home);

        bashrc = fopen(path, "a");
        if(bashrc) {
            fprintf(bashrc, "\n# Apache Solr Environment Variables\n");
            fprintf(bashrc, "export SOLR_HOME=%s\n", install_dir);
            fprintf(bashrc, "export PATH=$PATH:%s/bin\n", install_dir);
            fclose(bashrc);

            // Source the updated bashrc
            if (sourceBashrc() != 0) {
                FPRINTF(global_client_socket,  "bashing failed.\n");
                return;
            }

        }
    }

    PRINTF(global_client_socket, "Solr installed successfully at %s\n", install_dir);
}

void install_kafka(char* version, char *location) {
    char url[512];
    char command[1024];
    char filename[256];
    char dir_name[256];
    char* install_dir = NULL;


    // Construct download URL
    if (version)
        snprintf(url, sizeof(url),
                 "https://downloads.apache.org/kafka/%s/kafka_2.13-%s.tgz",
                 version, version);
    else
        snprintf(url, sizeof(url),
                 "https://downloads.apache.org/kafka/3.7.2/kafka_2.13-3.7.2.tgz");

    // Download Kafka
    snprintf(command, sizeof(command), "wget -q %s", url);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "Failed to download Kafka\n");
        exit(EXIT_FAILURE);
    }

    // Extract archive
    if (version)
        snprintf(filename, sizeof(filename), "kafka_2.13-%s.tgz", version);
    else
        snprintf(filename, sizeof(filename), "kafka_2.13-3.7.2.tgz");

    snprintf(command, sizeof(command), "tar -xvzf %s", filename);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "Failed to extract archive\n");
        exit(EXIT_FAILURE);
    }

    // Remove downloaded archive after extraction
    //PRINTF(global_client_socket, "Removing archive...\n");
    snprintf(command, sizeof(command), "rm -f %s", filename);

    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "Failed to remove archive.\n");
    }

    // Determine OS type
    int is_debian = access("/etc/debian_version", F_OK) == 0;
    int is_redhat = access("/etc/redhat-release", F_OK) == 0;

    if (!is_debian && !is_redhat) {
        FPRINTF(global_client_socket,  "Unsupported Linux distribution\n");
        exit(EXIT_FAILURE);
    }
    if (!location)
        install_dir = is_debian ? "/usr/local/kafka" :"/opt/kafka" ;
    else
        install_dir = location;
    // Move Kafka directory
    if (version)
        snprintf(dir_name, sizeof(dir_name), "kafka_2.13-%s", version);
    else
        snprintf(dir_name, sizeof(dir_name), "kafka_2.13-3.7.2");

    snprintf(command, sizeof(command), "mv %s %s"
             , dir_name, install_dir);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "Failed to move Kafka directory\n");
        exit(EXIT_FAILURE);
    }

    // Adjust ownership of install_dir to current user
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        FPRINTF(global_client_socket,  "Error: Could not determine current user\n");
        return;
    }
    char chown_cmd[512];
    snprintf(chown_cmd, sizeof(chown_cmd), "chown -R %s:%s %s", pwd->pw_name, pwd->pw_name, install_dir);
    if (!executeSystemCommand(chown_cmd)) {
        FPRINTF(global_client_socket,  "Error: Failed to set ownership of %s\n", install_dir);
        return;
    }


    // Configure environment variables
    char* home = getenv("HOME");
    if (!home) {
        FPRINTF(global_client_socket,  "Could not determine home directory\n");
        exit(EXIT_FAILURE);
    }

    char bashrc_path[512];
    snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home);

    FILE* bashrc = fopen(bashrc_path, "a");
    if (!bashrc) {
        PERROR(global_client_socket,"Failed to update .bashrc");
        exit(EXIT_FAILURE);
    }

    fprintf(bashrc, "\n# Apache Kafka configuration\n");
    fprintf(bashrc, "export KAFKA_HOME=%s\n", install_dir);
    fprintf(bashrc, "export PATH=\"$KAFKA_HOME/bin:$PATH\"\n");
    fclose(bashrc);


    char candidate_path[PATH_MAX];
    snprintf(candidate_path, sizeof(candidate_path), "%s/config", install_dir);

    if (create_xml_file(candidate_path, "ranger-kafka-audit.xml") !=0)
        FPRINTF(global_client_socket,  "Failed to create XML file\n");


    if (create_xml_file(candidate_path, "ranger-kafka-policymgr-ssl.xml") !=0)
        FPRINTF(global_client_socket,  "Failed to create XML file\n");


    if (create_xml_file(candidate_path, "ranger-kafka-security.xml") !=0)
        FPRINTF(global_client_socket,  "Failed to create XML file\n");


    if (create_properties_file(candidate_path, "ranger-kafka-plugin.properties") !=0)
        FPRINTF(global_client_socket,   "Failed to create XML file\n");


    PRINTF(global_client_socket, "\nKafka installed successfully to %s\n", install_dir);

    if (sourceBashrc() != 0) {
        FPRINTF(global_client_socket,  "bashing failed.\n");
        return;
    }

}


void install_pig(char* version, char *location) {
    char url[256];
    if (version)
        snprintf(url, sizeof(url), "https://downloads.apache.org/pig/pig-%s/pig-%s.tar.gz", version, version);
    else
        snprintf(url, sizeof(url), "https://downloads.apache.org/pig/pig-0.17.0/pig-0.17.0.tar.gz");


    char command[512];
    snprintf(command, sizeof(command), "wget %s", url);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "download failed\n");
        exit(EXIT_FAILURE);
    }

    if (!executeSystemCommand("tar -xvzf pig-*.tar.gz")) {
        FPRINTF(global_client_socket,  "taring failed\n");
        exit(EXIT_FAILURE);
    }

    // Remove downloaded archive after extraction
    //PRINTF(global_client_socket, "Removing archive...\n");
    if (version)
        snprintf(command, sizeof(command), "rm -f pig-%s.tar.gz", version);
    else
        snprintf(command, sizeof(command), "rm -f pig-0.17.0.tar.gz");

    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "Failed to remove archive.\n");
    }

    struct stat buffer;
    int isDebian = (stat("/etc/debian_version", &buffer) == 0);
    //  int isRedHat = (stat("/etc/redhat-release", &buffer) == 0);

    char *install_dir = NULL;
    if (!location)
        install_dir = isDebian ? "/usr/local/pig" :"/opt/pig" ;
    else
        install_dir = location;

    if (version)
        snprintf(command, sizeof(command), "mv pig-%s %s", version, install_dir);
    else
        snprintf(command, sizeof(command), "mv pig-0.17.0 %s", install_dir);

    //if (ret1 >= sizeof(command))  {// Truncation occurred
    //  FPRINTF(global_client_socket,  "Error: Command truncated.\n");
    //}
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "moving failed\n");
        exit(EXIT_FAILURE);
    }

    // Adjust ownership of install_dir to current user
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        FPRINTF(global_client_socket,  "Error: Could not determine current user\n");
        return;
    }
    char chown_cmd[512];
    snprintf(chown_cmd, sizeof(chown_cmd), "chown -R %s:%s %s", pwd->pw_name, pwd->pw_name, install_dir);
    if (!executeSystemCommand(chown_cmd)) {
        FPRINTF(global_client_socket,  "Error: Failed to set ownership of %s\n", install_dir);
        return;
    }

    char bashrc_path[256];
    char* home = getenv("HOME");
    if (home == NULL) {
        FPRINTF(global_client_socket,  "Error: HOME environment variable not set.\n");
        return;
    }
    snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home);

    long unsigned int ret2 =  snprintf(command, sizeof(command), "echo 'export PIG_HOME=\"%s\"' >> %s", install_dir, bashrc_path);
    if (ret2 >= sizeof(command)) { // Truncation occurred
        FPRINTF(global_client_socket,  "Error: Command truncated.\n");
    }
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "echo failed\n");
        exit(EXIT_FAILURE);
    }

    snprintf(command, sizeof(command), "echo 'export PATH=$PATH:$PIG_HOME/bin' >> %s", bashrc_path);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "echo failed\n");
        exit(EXIT_FAILURE);
    }
    snprintf(command, sizeof(command), "echo 'export PIG_CLASSPATH=$HADOOP_HOME/etc/hadoop' >> ");
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "echo failed\n");
        exit(EXIT_FAILURE);
    }
    if (sourceBashrc() != 0) {
        FPRINTF(global_client_socket,  "bashing failed.\n");
        return;
    }

    FPRINTF(global_client_socket,  "Pig Installed successfully\n");

}
// Helper function to execute a command and capture output
char* executeCommandAndGetOutput(const char* command) {
    char buffer[128];
    char* result = malloc(256);
    if (!result) {
        PERROR(global_client_socket,"malloc failed");
        return NULL;
    }
    result[0] = '\0';

    FILE* fp = popen(command, "r");
    if (!fp) {
        PERROR(global_client_socket,"popen failed");
        free(result);
        return NULL;
    }

    while (fgets(buffer, sizeof(buffer), fp) != NULL) {
        strcat(result, buffer);
    }

    pclose(fp);

    // Trim trailing newline
    size_t len = strlen(result);
    if (len > 0 && result[len-1] == '\n') {
        result[len-1] = '\0';
    }

    return result;
}

void install_HBase(char *version, char *location) {
    char dir_name[256];
    char command[1024];
    char url[256];
    const char* default_version = "2.5.12";

    // Create download URL
    snprintf(url, sizeof(url), "https://downloads.apache.org/hbase/%s/hbase-%s-bin.tar.gz",
             version ? version : default_version,
             version ? version : default_version);

    // Download the archive
    char wget_cmd[512];
    snprintf(wget_cmd, sizeof(wget_cmd), "wget %s", url);
    if (!executeSystemCommand(wget_cmd)) {
        FPRINTF(global_client_socket,  "Download failed for version: %s\n", version ? version : default_version);
        exit(EXIT_FAILURE);
    }

    // Extract archive
    char tar_cmd[512];
    snprintf(tar_cmd, sizeof(tar_cmd), "tar -xvzf hbase-%s-bin.tar.gz",
             version ? version : default_version);
    if (!executeSystemCommand(tar_cmd)) {
        FPRINTF(global_client_socket,  "Extraction failed\n");
        exit(EXIT_FAILURE);
    }

    // Remove downloaded archive
    char rm_cmd[512];
    snprintf(rm_cmd, sizeof(rm_cmd), "rm -f hbase-%s-bin.tar.gz",
             version ? version : default_version);
    if (!executeSystemCommand(rm_cmd)) {
        FPRINTF(global_client_socket,  "Failed to remove archive\n");
        exit(EXIT_FAILURE);
    }

    // Determine OS family
    char *target_dir = NULL;
    if (!location) {
        if (access("/etc/debian_version", F_OK) == 0) {
            target_dir = "/usr/local";
        } else if (access("/etc/redhat-release", F_OK) == 0) {
            target_dir = "/opt";
        } else {
            FPRINTF(global_client_socket,  "Unsupported OS\n");
            exit(EXIT_FAILURE);
        }
    } else {
        target_dir = location;
    }

    // Corrected directory name construction
    snprintf(dir_name, sizeof(dir_name), "hbase-%s",  // Removed -bin suffix
             version ? version : default_version);

    // Rename extracted directory to 'hbase'
    char rename_cmd[512];
    snprintf(rename_cmd, sizeof(rename_cmd), "mv %s hbase", dir_name);
    if (!executeSystemCommand(rename_cmd)) {
        FPRINTF(global_client_socket,  "Failed to rename HBase directory from %s to hbase\n", dir_name);
        exit(EXIT_FAILURE);
    }

    // Move HBase directory to target_dir
    snprintf(command, sizeof(command), "mv hbase %s", target_dir);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "Failed to move HBase to %s\n", target_dir);
        exit(EXIT_FAILURE);
    }

    // Adjust ownership
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        FPRINTF(global_client_socket,  "Error: Could not determine current user\n");
        exit(EXIT_FAILURE);
    }
    char chown_cmd[512];
    snprintf(chown_cmd, sizeof(chown_cmd), "chown -R %s:%s %s/hbase",
             pwd->pw_name, pwd->pw_name, target_dir);
    if (!executeSystemCommand(chown_cmd)) {
        FPRINTF(global_client_socket,  "Error: Failed to set ownership of %s/hbase\n", target_dir);
        exit(EXIT_FAILURE);
    }

    // Update environment variables
    char *home = getenv("HOME");
    if (!home) {
        FPRINTF(global_client_socket,  "Home directory not found\n");
        exit(EXIT_FAILURE);
    }

    char bashrc_path[256];
    snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home);
    FILE *bashrc = fopen(bashrc_path, "a");
    if (!bashrc) {
        PERROR(global_client_socket,"Failed to open .bashrc");
        exit(EXIT_FAILURE);
    }
    fprintf(bashrc, "\n# HBase Configuration\nexport HBASE_HOME=%s/hbase\n", target_dir);
    fprintf(bashrc, "export PATH=$HBASE_HOME/bin:$PATH\n");
    fclose(bashrc);

    // Set JAVA_HOME in hbase-env.sh
    char* java_home = executeCommandAndGetOutput("bash -c 'source ~/.bashrc && echo $JAVA_HOME'");
    if (!java_home || strlen(java_home) == 0) {
        FPRINTF(global_client_socket,  "JAVA_HOME not set in .bashrc\n");
        exit(EXIT_FAILURE);
    }

    char hbase_env_path[512];
    snprintf(hbase_env_path, sizeof(hbase_env_path), "%s/hbase/conf/hbase-env.sh", target_dir);

    // Update JAVA_HOME configuration
    char sed_cmd[1024];
    snprintf(sed_cmd, sizeof(sed_cmd),
             "sed -i.bak '/^#[[:space:]]*export JAVA_HOME=/c\\export JAVA_HOME=\"%s\"' %s",
             java_home, hbase_env_path);
    if (!executeSystemCommand(sed_cmd)) {
        FPRINTF(global_client_socket,  "Failed to set JAVA_HOME in hbase-env.sh\n");
        free(java_home);
        exit(EXIT_FAILURE);
    }
    free(java_home);

    FPRINTF(global_client_socket, "HBase successfully installed to %s/hbase\n", target_dir);
}

void install_Tez(char* version, char *location) {
    char url[512];
    char command[1024];
    char* install_dir = NULL;
    const char* home_dir;
    struct passwd *pw;
    char bashrc_path[512];
    FILE* bashrc;

    // Generate download URL
    if(version)
        snprintf(url, sizeof(url),
                 "https://downloads.apache.org/tez/%s/apache-tez-%s-bin.tar.gz",
                 version, version);
    else
        snprintf(url, sizeof(url),
                 "https://downloads.apache.org/tez/0.10.1/apache-tez-0.10.1-bin.tar.gz");

    // Download Tez archive
    snprintf(command, sizeof(command), "wget -q %s", url);

    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "downloading faild\n");
        return;
    }
    // Extract downloaded archive
    if (version)
        snprintf(command, sizeof(command),
                 "tar -xvzf apache-tez-%s-bin.tar.gz", version);
    else
        snprintf(command, sizeof(command),
                 "tar -xvzf apache-tez-0.10.1-bin.tar.gz");


    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "taring  faild\n");
        return;
    }
    // Remove downloaded archive after extraction
    //PRINTF(global_client_socket, "Removing archive...\n");
    if (version)
        snprintf(command, sizeof(command), "rm -f apache-tez-%s-bin.tar.gz", version);
    else
        snprintf(command, sizeof(command), "rm -f apache-tez-0.10.1-bin.tar.gz");

    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "Failed to remove archive.\n");
    }

    // Determine installation directory based on OS
    if (!location)
    {
        if (access("/etc/debian_version", F_OK) == 0) {
            install_dir = "/usr/local/tez";
        } else if (access("/etc/redhat-release", F_OK) == 0) {
            install_dir = "/opt/tez";
        } else {
            install_dir = "/usr/local/tez";
        }
    }
    else
        install_dir = location;
    // Move extracted directory to installation location
    if (version)
        snprintf(command, sizeof(command),
                 "mv -f apache-tez-%s-bin %s", version, install_dir);
    else
        snprintf(command, sizeof(command),
                 "mv -f apache-tez-0.10.1-bin %s", install_dir);

    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "moving   faild\n");
        return;
    }

    // Adjust ownership of install_dir to current user
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        FPRINTF(global_client_socket,  "Error: Could not determine current user\n");
        return;
    }
    char chown_cmd[512];
    snprintf(chown_cmd, sizeof(chown_cmd), "chown -R %s:%s %s", pwd->pw_name, pwd->pw_name, install_dir);
    if (!executeSystemCommand(chown_cmd)) {
        FPRINTF(global_client_socket,  "Error: Failed to set ownership of %s\n", install_dir);
        return;
    }
    // Set environment variables in .bashrc
    if ((home_dir = getenv("HOME")) == NULL) {
        pw = getpwuid(getuid());
        home_dir = pw->pw_dir;
    }

    snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home_dir);
    bashrc = fopen(bashrc_path, "a");

    if (bashrc) {
        fprintf(bashrc, "\n# Apache Tez configuration\nexport TEZ_HOME=%s\n", install_dir);
        fprintf(bashrc, "export PATH=$TEZ_HOME/bin:$PATH\n");
        fclose(bashrc);
    }

    // Source the updated configuration
    if (sourceBashrc() != 0) {
        FPRINTF(global_client_socket,  "bashing failed.\n");
        return;
    }

}

void install_flink(char* version, char *location) {
    char url[256];
    char filename[256];
    char cmd[512];
    struct stat buffer;
    int ret;

    // Construct download URL and filename
    if (version)
    {
        snprintf(url, sizeof(url), "https://dlcdn.apache.org/flink/flink-%s-bin-scala_2.12.tgz", version);
        snprintf(filename, sizeof(filename), "flink-%s-bin-scala_2.12.tgz", version);
    }
    else
    {
        snprintf(url, sizeof(url), "https://downloads.apache.org/flink/flink-2.0.0/flink-2.0.0-bin-scala_2.12.tgz");
        snprintf(filename, sizeof(filename), "flink-2.0.0-bin-scala_2.12.tgz");
    }
    // Download the archive
    //PRINTF(global_client_socket, "Downloading Flink ...\n");
    snprintf(cmd, sizeof(cmd), "wget -q %s", url);
    ret = executeSystemCommand(cmd);
    if (ret == -1) {
        FPRINTF(global_client_socket,  "Failed to download Flink archive\n");
        exit(EXIT_FAILURE);
    }

    // Extract the archive
    //PRINTF(global_client_socket, "Extracting archive...\n");
    snprintf(cmd, sizeof(cmd), "tar -xvzf %s >/dev/null", filename);
    ret = executeSystemCommand(cmd);
    if (ret == -1) {
        FPRINTF(global_client_socket,  "Failed to extract archive\n");
        exit(EXIT_FAILURE);
    }

    // Remove downloaded archive after extraction
    //PRINTF(global_client_socket, "Removing archive...\n");
    if (version)
        snprintf(cmd, sizeof(cmd), "rm -f %s", filename);

    if (!executeSystemCommand(cmd)) {
        FPRINTF(global_client_socket,  "Failed to remove archive.\n");
    }

    // Determine OS type
    char* install_dir;
    if (!location)
    {
        if (stat("/etc/debian_version", &buffer) == 0) {
            install_dir = "/usr/local/flink";
        } else if (stat("/etc/redhat-release", &buffer) == 0) {
            install_dir = "/opt/flink";
        } else {
            FPRINTF(global_client_socket,  "Unsupported Linux distribution\n");
            exit(EXIT_FAILURE);
        }
    }
    else
        install_dir = location;
    // Move Flink directory
    PRINTF(global_client_socket, "Installing to %s...\n", install_dir);
    char src_dir[256];
    if (version)
        snprintf(src_dir, sizeof(src_dir), "flink-%s", version);
    else
        snprintf(src_dir, sizeof(src_dir), "flink-2.0.0");

    snprintf(cmd, sizeof(cmd), "mv %s %s", src_dir, install_dir);
    ret = executeSystemCommand(cmd);
    if (ret == -1) {
        FPRINTF(global_client_socket,  "Failed to move installation directory\n");
        exit(EXIT_FAILURE);
    }

    // Adjust ownership of install_dir to current user
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        FPRINTF(global_client_socket,  "Error: Could not determine current user\n");
        return;
    }
    char chown_cmd[512];
    snprintf(chown_cmd, sizeof(chown_cmd), "chown -R %s:%s %s", pwd->pw_name, pwd->pw_name, install_dir);
    if (!executeSystemCommand(chown_cmd)) {
        FPRINTF(global_client_socket,  "Error: Failed to set ownership of %s\n", install_dir);
        return;
    }

    // Update environment variables
    // PRINTF(global_client_socket, "Configuring environment...\n");
    const char* home = getenv("HOME");
    if (!home) {
        FPRINTF(global_client_socket,  "HOME environment variable not set\n");
        exit(EXIT_FAILURE);
    }

    char bashrc_path[256];
    snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home);
    FILE* bashrc = fopen(bashrc_path, "a");
    if (!bashrc) {
        PERROR(global_client_socket,"Failed to open .bashrc");
        exit(EXIT_FAILURE);
    }

    fprintf(bashrc, "\n# Apache Flink configuration\nexport FLINK_HOME=%s\nexport PATH=\"$PATH:$FLINK_HOME/bin\"\n", install_dir);
    fclose(bashrc);

    PRINTF(global_client_socket, "Installation completed successfully.\n");

    if (sourceBashrc() != 0) {
        FPRINTF(global_client_socket,  "bashing failed.\n");
        return;
    }
}


void install_zookeeper(char *version, char *location) {
    char command[1024];
    char url[256];
    char src_dir[256];
    char *install_dir = NULL;
    int isDebian, isRedHat;
    char *user;
    char *home;
    char bashrc_path[256];

    // Generate download URL
    if (version)
        snprintf(url, sizeof(url), "https://downloads.apache.org/zookeeper/zookeeper-%s/apache-zookeeper-%s-bin.tar.gz", version, version);
    else
        snprintf(url, sizeof(url), "https://downloads.apache.org/zookeeper/zookeeper-3.8.4/apache-zookeeper-3.8.4-bin.tar.gz");

    // Download using wget
    snprintf(command, sizeof(command), "wget -q %s", url);

    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "downloading  faild\n");
        return;
    }
    // Extract the archive
    if (version)
        snprintf(command, sizeof(command), "tar -xvzf apache-zookeeper-%s-bin.tar.gz > /dev/null", version);
    else
        snprintf(command, sizeof(command), "tar -xvzf apache-zookeeper-3.8.4-bin.tar.gz");

    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "taring  faild\n");
        return;
    }

    // Remove downloaded archive after extraction
    // PRINTF(global_client_socket, "Removing archive...\n");
    if (version)
        snprintf(command, sizeof(command), "rm -f apache-zookeeper-%s-bin.tar.gz", version);
    else
        snprintf(command, sizeof(command), "rm -f apache-zookeeper-3.8.4-bin.tar.gz");
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "Failed to remove archive.\n");
    }

    // Determine OS type
    isDebian = (access("/etc/debian_version", F_OK) == 0);
    isRedHat = (access("/etc/redhat-release", F_OK) == 0) || (access("/etc/redhat_version", F_OK) == 0);
    if (!location)
    {
        if (isDebian) {
            install_dir = "/usr/local";
        } else if (isRedHat) {
            install_dir = "/opt";
        } else {
            FPRINTF(global_client_socket,  "Unsupported OS. Defaulting to /usr/local.\n");
            install_dir = "/usr/local";
        }
    }
    else
        install_dir = location;
    // Move and rename the extracted directory
    if (version)
        snprintf(src_dir, sizeof(src_dir), "apache-zookeeper-%s-bin", version);
    else
        snprintf(src_dir, sizeof(src_dir), "apache-zookeeper-3.8.4-bin");

    snprintf(command, sizeof(command), "mv -f %s %s/zookeeper 2> /dev/null", src_dir, install_dir);

    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "moving  faild\n");
        return;
    }

    // Adjust ownership of install_dir to current user
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        FPRINTF(global_client_socket,  "Error: Could not determine current user\n");
        return;
    }
    char chown_cmd[512];
    snprintf(chown_cmd, sizeof(chown_cmd), "chown -R %s:%s %s", pwd->pw_name, pwd->pw_name, install_dir);
    if (!executeSystemCommand(chown_cmd)) {
        FPRINTF(global_client_socket,  "Error: Failed to set ownership of %s\n", install_dir);
        return;
    }

    // Copy configuration file
    snprintf(command, sizeof(command), "cp %s/zookeeper/conf/zoo_sample.cfg %s/zookeeper/conf/zoo.cfg", install_dir, install_dir);

    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "copying  faild\n");
        return;
    }

    if (!executeSystemCommand("mkdir -p /var/lib/zookeeper > /dev/null 2>&1")) {
        FPRINTF(global_client_socket,  "make directory failed\n");
        return;
    }


    user = getenv("USER");
    if (user) {
        snprintf(command, sizeof(command), "chown -R %s:%s /var/lib/zookeeper > /dev/null 2>&1", user, user);

        if (!executeSystemCommand(command)) {
            FPRINTF(global_client_socket,  "copying  faild\n");
            return;
        }
    } else {
        FPRINTF(global_client_socket,  "Failed to get current user.\n");
    }

    // Update .bashrc with environment variables
    home = getenv("HOME");
    if (home) {
        snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home);

        // Append ZOOKEEPER_HOME and PATH
        snprintf(command, sizeof(command), "echo 'export ZOOKEEPER_HOME=%s/zookeeper' >> %s", install_dir, bashrc_path);
        if (!executeSystemCommand(command)) {
            FPRINTF(global_client_socket,  "eko  faild\n");
            return;
        }
        snprintf(command, sizeof(command), "echo 'export PATH=\"$PATH:$ZOOKEEPER_HOME/bin\"' >> %s", bashrc_path);
        if (!executeSystemCommand(command)) {
            FPRINTF(global_client_socket,  "eko  faild\n");
            return;
        }
    }
    // Source the .bashrc

    if (sourceBashrc() != 0) {
        FPRINTF(global_client_socket,  "bashing failed.\n");
        return;
    }
    char candidate_path[PATH_MAX];
    snprintf(candidate_path, sizeof(candidate_path), "%s/zookeeper/conf", install_dir);

    if (create_properties_file(candidate_path, "log4j.properties") !=0)
        FPRINTF(global_client_socket,  "Failed to create properties file\n");

    if (create_properties_file(candidate_path, "zookeeper-env.properties") !=0)
        FPRINTF(global_client_socket,  "Failed to create properties file\n");

    PRINTF(global_client_socket,  "Zookeeper installed successfully\n");

}



void install_Presto(char* version, char *location) {
    char url[512];
    char command[1024];
    char* install_dir = NULL;
    const char* actual_version = version ? version : "0.282";

    // Construct download URL
    snprintf(url, sizeof(url),
             "https://repo1.maven.org/maven2/com/facebook/presto/presto-server/%s/presto-server-%s.tar.gz",
             actual_version, actual_version);

    // Download the archive
    snprintf(command, sizeof(command), "wget -q %s >/dev/null 2>&1", url);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "Failed to download Presto version %s\n", actual_version);
        exit(EXIT_FAILURE);
    }

    // Determine OS family if location not specified
    if (!location) {
        if (access("/etc/debian_version", F_OK) == 0) {
            install_dir = "/usr/local/presto";
        } else if (access("/etc/redhat-release", F_OK) == 0) {
            install_dir = "/opt/presto";
        } else {
            FPRINTF(global_client_socket, "Unsupported Linux distribution\n");
            exit(EXIT_FAILURE);
        }
    } else {
        install_dir = location;
    }

    // Create installation directory
    snprintf(command, sizeof(command), "mkdir -p %s >/dev/null 2>&1", install_dir);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket, "Failed to create directory %s\n", install_dir);
        exit(EXIT_FAILURE);
    }

    // Set ownership
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        FPRINTF(global_client_socket, "Error: Could not determine current user\n");
        exit(EXIT_FAILURE);
    }
    snprintf(command, sizeof(command), "chown -R %s:%s %s >/dev/null 2>&1",
             pwd->pw_name, pwd->pw_name, install_dir);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "Error: Failed to set ownership of %s\n", install_dir);
        exit(EXIT_FAILURE);
    }

    // Extract directly to installation directory
    snprintf(command, sizeof(command),
             "tar -xvzf presto-server-%s.tar.gz --strip-components=1 -C %s >/dev/null 2>&1",
             actual_version, install_dir);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket, "Extraction failed\n");
        exit(EXIT_FAILURE);
    }

    // Remove downloaded archive
    snprintf(command, sizeof(command), "rm -f presto-server-%s.tar.gz >/dev/null 2>&1", actual_version);
    executeSystemCommand(command);  // Continue even if removal fails

    // Create configuration directories
    char config_dir[512];
    snprintf(config_dir, sizeof(config_dir), "%s/etc", install_dir);

    char dir_cmd[1024];
    snprintf(dir_cmd, sizeof(dir_cmd), "mkdir -p %s/{catalog,data}", config_dir);
    if (!executeSystemCommand(dir_cmd)) {
        FPRINTF(global_client_socket, "Failed to create config directories\n");
    }

    // Create configuration files
    char node_props[512], jvm_config[512], presto_config[512], catalog_tpch[512];

    snprintf(node_props, sizeof(node_props), "%s/node.properties", config_dir);
    create_properties_file(node_props, "node.properties");

    snprintf(jvm_config, sizeof(jvm_config), "%s/jvm.config", config_dir);
    create_properties_file(jvm_config, "jvm.config");

    snprintf(presto_config, sizeof(presto_config), "%s/config.properties", config_dir);
    create_properties_file(presto_config, "config.properties");

    snprintf(catalog_tpch, sizeof(catalog_tpch), "%s/catalog/tpch.properties", config_dir);
    create_properties_file(catalog_tpch, "tpch.properties");

    // Set environment variables
    char* home = getenv("HOME");
    if (!home) {
        FPRINTF(global_client_socket, "Could not determine home directory\n");
        exit(EXIT_FAILURE);
    }

    char bashrc_path[256];
    snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home);

    FILE* bashrc = fopen(bashrc_path, "a");
    if (bashrc) {
        fprintf(bashrc, "\n# Presto configuration\nexport PRESTO_HOME=%s\nexport PATH=$PATH:$PRESTO_HOME/bin\n",
                install_dir);
        fclose(bashrc);
    }

    // Update current session
    if (sourceBashrc() != 0) {
        FPRINTF(global_client_socket,  "Failed to source .bashrc\n");
    }

    PRINTF(global_client_socket, "Presto %s successfully installed to %s\n", actual_version, install_dir);
}

void install_hive(char* version, char *location) {
    char url[512];
    char command[1024];

    if(version)
        snprintf(url, sizeof(url), "https://dlcdn.apache.org/hive/hive-%s/apache-hive-%s-bin.tar.gz", version, version);
    else
        snprintf(url, sizeof(url), "https://dlcdn.apache.org/hive/hive-4.0.1/apache-hive-4.0.1-bin.tar.gz");

    // Download Hive archive
    char wget_cmd[512];
    long unsigned int ret= snprintf(wget_cmd, sizeof(wget_cmd), "wget %s", url);
    if (ret >= sizeof(wget_cmd)) { // Truncation occurred
        FPRINTF(global_client_socket,  "Error: Command truncated.\n");
    }

    if (!executeSystemCommand(wget_cmd)) {
        FPRINTF(global_client_socket,  "Error: Failed to download Hive archive.\n");
        exit(EXIT_FAILURE);
    }

    if (version)
        snprintf(command, sizeof(command), "tar -xvzf apache-hive-%s-bin.tar.gz > /dev/null", version);
    else
        snprintf(command, sizeof(command), "tar -xvzf apache-hive-4.0.1-bin.tar.gz");

    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "taring  faild\n");
        return;
    }

    // Remove downloaded archive after extraction
    // PRINTF(global_client_socket, "Removing archive...\n");
    if (version)
        snprintf(command, sizeof(command), "rm -f apache-hive-%s-bin.tar.gz", version);
    else
        snprintf(command, sizeof(command), "rm -f apache-hive-4.0.1-bin.tar.gz");
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "Failed to remove archive.\n");
    }

    // Determine OS type
    int os_type;
    if (executeSystemCommand("command -v apt-get > /dev/null 2>&1")) {
        os_type = 0; // Debian-based
    } else if (executeSystemCommand("command -v yum > /dev/null 2>&1 || command -v dnf > /dev/null 2>&1")) {
        os_type = 1; // Red Hat-based
    } else {
        FPRINTF(global_client_socket,  "Error: Unsupported Linux distribution.\n");
        exit(EXIT_FAILURE);
    }

    char* installation_dir;
    if (!location)
    {
        if (os_type == 0) {
            installation_dir = "/usr/local";
        } else {
            installation_dir = "/opt";
        }
    }
    else
        installation_dir = location;


    char dir_name[256];
    snprintf(dir_name, sizeof(dir_name), "apache-hive-%s-bin",
             version ? version : "4.0.1");
    char rename_cmd[512];
    snprintf(rename_cmd, sizeof(rename_cmd), "mv %s hive", dir_name);
    if (!executeSystemCommand(rename_cmd)) {
        FPRINTF(global_client_socket,  "Failed to rename HBase directory from %s to hive\n", dir_name);
        exit(EXIT_FAILURE);
    }

    snprintf(command, sizeof(command), "mv hive %s"
             , installation_dir);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "Failed to move hive directory\n");
        exit(EXIT_FAILURE);
    }

    // Adjust ownership of install_dir to current user
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        FPRINTF(global_client_socket,  "Error: Could not determine current user\n");
        return;
    }
    char chown_cmd[512];
    snprintf(chown_cmd, sizeof(chown_cmd), "chown -R %s:%s %s", pwd->pw_name, pwd->pw_name, installation_dir);
    if (!executeSystemCommand(chown_cmd)) {
        FPRINTF(global_client_socket,  "Error: Failed to set ownership of %s\n", installation_dir);
        return;
    }

    // Update .bashrc with HIVE_HOME and PATH
    const char* home_dir = getenv("HOME");
    if (!home_dir) {
        home_dir = "/root";
    }

    char bashrc_path[256];
    snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home_dir);

    FILE* bashrc = fopen(bashrc_path, "a");
    if (!bashrc) {
        PERROR(global_client_socket,"Error: Failed to open .bashrc");
        exit(EXIT_FAILURE);
    }

    fprintf(bashrc, "\n# Apache Hive environment variables\nexport HIVE_HOME=%s/hive\nexport PATH=\"$PATH:$HIVE_HOME/bin\"\n", installation_dir);
    fclose(bashrc);

    // Source the .bashrc
    if (sourceBashrc() != 0) {
        FPRINTF(global_client_socket,  "bashing failed.\n");
        return;
    }

    char candidate_path[PATH_MAX];
    snprintf(candidate_path, sizeof(candidate_path), "%s/hive/conf", installation_dir);

    if (create_properties_file(candidate_path, "beeline-log4j2.properties") !=0)
        FPRINTF(global_client_socket,   "Failed to create XML file\n");

    if (create_properties_file(candidate_path, "atlas-application.properties") !=0)
        FPRINTF(global_client_socket,   "Failed to create XML file\n");

    if (create_properties_file(candidate_path, "hive-exec-log4j2.properties") !=0)
        FPRINTF(global_client_socket,  "Failed to create XML file\n");

    if (create_properties_file(candidate_path, "llap-cli-log4j2.properties") !=0)
        FPRINTF(global_client_socket,   "Failed to create XML file\n");



    if (create_properties_file(candidate_path, "hive-log4j2.properties") !=0)
        FPRINTF(global_client_socket,  "Failed to create XML file\n");

    if (create_properties_file(candidate_path, "llap-daemon-log4j2.properties") !=0)
        FPRINTF(global_client_socket,  "Failed to create XML file\n");

    if (create_xml_file(candidate_path, "hive-site.xml") !=0)
        FPRINTF(global_client_socket, "Failed to create XML file\n");

    if (create_xml_file(candidate_path, "hivemetastore-site.xml") !=0)
        FPRINTF(global_client_socket,  "Failed to create XML file\n");

    if (create_xml_file(candidate_path, "hiveserver2-site.xml") !=0)
        FPRINTF(global_client_socket,   "Failed to create XML file\n");

    PRINTF(global_client_socket, "Hive has been successfully installed to %s.\n", installation_dir);
}


void install_spark(char* version, char *location) {
    // Generate download URL
    char url[256];
    if(version)
        // Add "-bin-hadoop3" to download pre-built binaries
        snprintf(url, sizeof(url),
                 "https://dlcdn.apache.org/spark/spark-%s/spark-%s-bin-hadoop3.tgz",
                 version, version);
    else
        // Default to a pre-built binary version
        snprintf(url, sizeof(url),
                 "https://dlcdn.apache.org/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz");


    // Download Spark archive
    char wget_cmd[512];
    snprintf(wget_cmd, sizeof(wget_cmd), "wget -q %s", url);

    if (!executeSystemCommand(wget_cmd)) {
        FPRINTF(global_client_socket,  "Downloading failed\n");
        return;
    }

    // Extract archive
    char tar_cmd[512];
    if (version)
        snprintf(tar_cmd, sizeof(tar_cmd), "tar -xvzf spark-%s-bin-hadoop3.tgz", version);
    else
        snprintf(tar_cmd, sizeof(tar_cmd), "tar -xvzf spark-3.5.6-bin-hadoop3.tgz");
    if (!executeSystemCommand(tar_cmd)) {
        FPRINTF(global_client_socket,  "Error: Failed to extract archive\n");
        return;
    }

    // Remove the downloaded archive
    if (version) {
        snprintf(tar_cmd, sizeof(tar_cmd), "rm -f spark-%s-bin-hadoop3.tgz", version);
    } else {
        snprintf(tar_cmd, sizeof(tar_cmd), "rm -f spark-3.5.6-bin-hadoop3.tgz");
    }
    if (!executeSystemCommand(tar_cmd)) {
        FPRINTF(global_client_socket,  "Error: Failed to remove archive\n");
        return;
    }

    // Determine installation path
    char* install_path = NULL;
    if (!location) {
        if (access("/etc/debian_version", F_OK) == 0) {
            install_path = "/usr/local/spark";
        } else if (access("/etc/redhat-release", F_OK) == 0 ||
                   access("/etc/system-release", F_OK) == 0) {
            install_path = "/opt/spark";
        } else {
            FPRINTF(global_client_socket,  "Error: Unsupported Linux distribution\n");
            return;
        }
    } else {
        install_path = location;
    }

    // Move extracted directory
    char mv_cmd[512];
    if (version) {
        snprintf(mv_cmd, sizeof(mv_cmd),
                 "mv spark-%s-bin-hadoop3 %s", version, install_path);
    } else {
        snprintf(mv_cmd, sizeof(mv_cmd),
                 "mv spark-3.5.6-bin-hadoop3 %s", install_path);
    }
    if (!executeSystemCommand(mv_cmd)) {
        FPRINTF(global_client_socket,  "Moving failed\n");
        return;
    }

    // Configure environment variables
    const char* home = getenv("HOME");
    if (!home) {
        FPRINTF(global_client_socket,  "Error: HOME environment variable not set\n");
        return;
    }

    char bashrc_path[256];
    snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home);

    // Append SPARK_HOME to .bashrc
    char env_cmd[512];
    snprintf(env_cmd, sizeof(env_cmd),
             "echo 'export SPARK_HOME=%s' >> %s", install_path, bashrc_path);

    if (!executeSystemCommand(env_cmd)) {
        FPRINTF(global_client_socket,  "Environment setup failed\n");
        return;
    }

    // Append PATH update to .bashrc
    snprintf(env_cmd, sizeof(env_cmd),
             "echo 'export PATH=$PATH:$SPARK_HOME/bin' >> %s", bashrc_path);

    if (!executeSystemCommand(env_cmd)) {
        FPRINTF(global_client_socket,  "Environment setup failed\n");
        return;
    }

    // Source the updated configuration
    if (sourceBashrc() != 0) {
        FPRINTF(global_client_socket,  "Sourcing .bashrc failed\n");
        return;
    }
    FPRINTF(global_client_socket,  "Spark installed successfully\n");
}

void install_Zeppelin(char *version, char *location) {
    // Step 1: Generate download URL
    char url[512];
    if (version)
        snprintf(url, sizeof(url),
                 "https://downloads.apache.org/zeppelin/zeppelin-%s/zeppelin-%s-bin-all.tgz",
                 version, version);
    else
        snprintf(url, sizeof(url),
                 "https://downloads.apache.org/zeppelin/zeppelin-0.10.1/zeppelin-0.10.1-bin-all.tgz");

    // Step 2: Download using wget
    char wget_cmd[512];
    long unsigned int ret = snprintf(wget_cmd, sizeof(wget_cmd), "wget %s", url);

    if (ret >= sizeof(wget_cmd)) { // Truncation occurred
        FPRINTF(global_client_socket,  "Error: Command truncated.\n");
    }

    if (!executeSystemCommand(wget_cmd)) {
        FPRINTF(global_client_socket,  "Download failed. Invalid version or network issue.\n");
        exit(EXIT_FAILURE);
    }

    // Step 3: Extract archive
    char tar_cmd[512];
    if (version)
        snprintf(tar_cmd, sizeof(tar_cmd), "tar -xvzf zeppelin-%s-bin-all.tgz", version);
    else
        snprintf(tar_cmd, sizeof(tar_cmd), "tar -xvzf zeppelin-0.10.1-bin-all.tgz");
    if (!executeSystemCommand(tar_cmd)) {
        FPRINTF(global_client_socket,  "Extraction failed. Corrupted download?\n");
        exit(EXIT_FAILURE);
    }
    // Remove downloaded archive after extraction
    // PRINTF(global_client_socket, "Removing archive...\n");
    if (version)
        snprintf(tar_cmd, sizeof(tar_cmd), "rm -f zeppelin-%s-bin-all.tgz", version);
    else
        snprintf(tar_cmd, sizeof(tar_cmd), "rm -f zeppelin-0.10.1-bin-all.tgz");
    if (!executeSystemCommand(tar_cmd)) {
        FPRINTF(global_client_socket,  "Failed to remove archive.\n");
    }
    // Step 4: Determine OS type
    char *target_dir;
    if (!location)
    {
        if (access("/etc/debian_version", F_OK) == 0) {
            target_dir = "/usr/local/zeppelin";
        } else if (access("/etc/redhat-release", F_OK) == 0 ||
                   access("/etc/system-release", F_OK) == 0) {
            target_dir = "/opt/zeppelin";
        } else {
            FPRINTF(global_client_socket,  "Unsupported Linux distribution\n");
            exit(EXIT_FAILURE);
        }
    }
    else
        target_dir = location;
    // Step 5: Move extracted directory
    char dir_name[256];
    if (version)
        snprintf(dir_name, sizeof(dir_name), "zeppelin-%s-bin-all", version);
    else
        snprintf(dir_name, sizeof(dir_name), "zeppelin-0.10.1-bin-all");
    char mv_cmd[512];
    snprintf(mv_cmd, sizeof(mv_cmd), "mv %s %s", dir_name, target_dir);
    if (!executeSystemCommand(mv_cmd)) {
        FPRINTF(global_client_socket,  "Failed to move installation directory\n");
        exit(EXIT_FAILURE);
    }
    // Adjust ownership of install_dir to current user
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        FPRINTF(global_client_socket,  "Error: Could not determine current user\n");
        return;
    }
    char chown_cmd[512];
    snprintf(chown_cmd, sizeof(chown_cmd), "chown -R %s:%s %s", pwd->pw_name, pwd->pw_name, target_dir);
    if (!executeSystemCommand(chown_cmd)) {
        FPRINTF(global_client_socket,  "Error: Failed to set ownership of %s\n", target_dir);
        return;
    }

    // Step 6: Set ZEPPELIN_HOME in .bashrc
    const char *home = getenv("HOME");
    if (!home) {
        FPRINTF(global_client_socket,  "Could not determine home directory\n");
        exit(EXIT_FAILURE);
    }

    char bashrc_path[256];
    snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home);

    FILE *bashrc = fopen(bashrc_path, "a");
    if (!bashrc) {
        PERROR(global_client_socket,"Failed to update .bashrc");
        exit(EXIT_FAILURE);
    }

    fprintf(bashrc, "\nexport ZEPPELIN_HOME=%s\n", target_dir);
    fclose(bashrc);

    // Step 7: Source the updated .bashrc
    if (sourceBashrc() != 0) {
        FPRINTF(global_client_socket,  "bashing failed.\n");
        return;
    }

    char candidate_path[PATH_MAX];
    snprintf(candidate_path, sizeof(candidate_path), "%s/config", target_dir);


    if (create_properties_file(candidate_path, "zeppelin-shiro.ini") !=0)
        FPRINTF(global_client_socket,    "Failed to create properties file\n");
    PRINTF(global_client_socket, "Apache Zeppelin installed successfully at %s\n", target_dir);
}


void install_Livy(char* version, char *location) {
    char command[1024];
    char* install_dir;
    char* home_dir = getenv("HOME");

    // Validate home directory
    if (!home_dir) {
        FPRINTF(global_client_socket,  "Error: Unable to determine home directory\n");
        return;
    }

    // 1. Construct download URL
    char url[256];
    if(version)
        snprintf(url, sizeof(url),
                 "https://downloads.apache.org/incubator/livy/%s-incubating/apache-livy-%s-incubating-bin.zip",
                 version, version);
    else
        snprintf(url, sizeof(url),
                 "https://downloads.apache.org/incubator/livy/0.7.1-incubating/apache-livy-0.7.1-incubating-bin.zip");

    // 2. Download using wget
    snprintf(command, sizeof(command), "wget -q %s", url);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "Error downloading Livy archive\n");
        return;
    }

    // 3. Extract the archive
    char zip_file[256];
    if (version)
        snprintf(zip_file, sizeof(zip_file), "apache-livy-%s-incubating-bin.zip", version);
    else
        snprintf(zip_file, sizeof(zip_file), "apache-livy-0.7.1-incubating-bin.zip");

    snprintf(command, sizeof(command), "unzip -q %s", zip_file);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "Error extracting archive\n");
        return;
    }
    // Remove downloaded archive after extraction
    // PRINTF(global_client_socket, "Removing archive...\n");
    if (version)
        snprintf(command, sizeof(command), "rm -f apache-livy-%s-incubating-bin.zip", version);
    else
        snprintf(command, sizeof(command), "rm -f apache-livy-0.7.1-incubating-bin.zip");
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "Failed to remove archive.\n");
    }
    // 4. Determine package manager and installation directory
    if (!location)
    {
        if (access("/etc/debian_version", F_OK) == 0) {
            install_dir = "/usr/local";
        } else if (access("/etc/redhat-release", F_OK) == 0) {
            install_dir = "/opt";
        } else {
            install_dir = "/opt";
        }
    }
    else
        install_dir = location;
    // 5. Move to installation directory and set permissions
    char source_dir[256];
    if (version)
        snprintf(source_dir, sizeof(source_dir), "apache-livy-%s-incubating-bin", version);
    else
        snprintf(source_dir, sizeof(source_dir), "apache-livy-0.7.1-incubating-bin");

    snprintf(command, sizeof(command), "mv %s %s/livy && chmod -R 755 %s/livy",
             source_dir, install_dir, install_dir);
    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "Error moving Livy to installation directory\n");
        return;
    }

    // Adjust ownership of install_dir to current user
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        FPRINTF(global_client_socket,  "Error: Could not determine current user\n");
        return;
    }
    char chown_cmd[512];
    snprintf(chown_cmd, sizeof(chown_cmd), "chown -R %s:%s %s", pwd->pw_name, pwd->pw_name, install_dir);
    if (!executeSystemCommand(chown_cmd)) {
        FPRINTF(global_client_socket,  "Error: Failed to set ownership of %s\n", install_dir);
        return;
    }

    // 6. Update environment variables
    char bashrc_path[256];
    snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home_dir);

    FILE *bashrc = fopen(bashrc_path, "a");
    if (bashrc) {
        fprintf(bashrc, "\n# Apache Livy configuration\n");
        fprintf(bashrc, "export LIVY_HOME=%s/livy\n", install_dir);
        fprintf(bashrc, "export PATH=$PATH:$LIVY_HOME/bin\n");
        fclose(bashrc);
    } else {
        FPRINTF(global_client_socket,  "Error updating .bashrc file\n");
        return;
    }

    // 7. Update current session environment
    setenv("LIVY_HOME", install_dir , 1);

    char path_env[1024];
    snprintf(path_env, sizeof(path_env), "%s:%s/livy/bin", getenv("PATH"), install_dir);
    setenv("PATH", path_env, 1);

    // 8. Cleanup temporary files
    snprintf(command, sizeof(command), "rm %s", zip_file);

    if (!executeSystemCommand(command)) {
        FPRINTF(global_client_socket,  "rm  faild\n");
        return;
    }

    if (sourceBashrc() != 0) {
        FPRINTF(global_client_socket,  "bashing failed.\n");
        return;
    }

    char candidate_path[PATH_MAX];
    snprintf(candidate_path, sizeof(candidate_path), "%s/conf", install_dir);

    //if (create_conf_file(candidate_path, "livy.conf") !=0)
    //   FPRINTF(global_client_socket,   "Failed to create XML file\n");

    PRINTF(global_client_socket, "Livy  installed successfully to %s/livy\n", install_dir);
}
