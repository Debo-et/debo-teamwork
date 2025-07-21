#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <ctype.h>
#include <pwd.h>
#include "utiles.h"
#include <limits.h>

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
        fprintf(stderr,   "Error: Unsupported OS distribution (no apt/yum/dnf found)\n");
        return;
    }

    // Install wget if missing
    if (!executeSystemCommand("command -v wget >/dev/null 2>&1")) {
        char cmd[256];
        if (debian) {
            snprintf(cmd, sizeof(cmd), "sudo apt update && sudo apt install -y wget");
        } else {
            const char *pm = (executeSystemCommand("command -v dnf >/dev/null 2>&1") == 0) ? "dnf" : "yum";
            snprintf(cmd, sizeof(cmd), "sudo %s install -y wget", pm);
        }
        if (!executeSystemCommand(cmd)) {
            fprintf(stderr,   "Error: Failed to install wget. Check network or permissions.\n");
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
        fprintf(stderr,   "Error: Hadoop version string too long\n");
        return;
    }

    // Download archive
    char wget_cmd[512];
    size_t ret = snprintf(wget_cmd, sizeof(wget_cmd), "wget -q %s >/dev/null 2>&1", url); // -q for quiet mode
    if (ret >= sizeof(wget_cmd)) {
        fprintf(stderr,   "Error: size error  sizeof(wget_cmd)\n");
        return;
    }
    if (!executeSystemCommand(wget_cmd)) {
        fprintf(stderr,   "Error: Failed to download Hadoop %s (invalid version?)\n", hadoop_version);
        return;
    }

    // Extract archive
    char tar_cmd[512];
    snprintf(tar_cmd, sizeof(tar_cmd), "tar -xvzf hadoop-%s.tar.gz >/dev/null 2>&1", hadoop_version);
    if (!executeSystemCommand(tar_cmd)) {
        fprintf(stderr,   "Error: Extraction failed. Corrupted download?\n");
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
    snprintf(mkdir_cmd, sizeof(mkdir_cmd), "sudo mkdir -p %s >/dev/null 2>&1", install_dir);
    if (!executeSystemCommand(mkdir_cmd)) {
        fprintf(stderr,   "Error: Failed to create %s. Check permissions.\n", install_dir);
        return;
    }

    // Move extracted files
    char mv_cmd[512];
    snprintf(mv_cmd, sizeof(mv_cmd),
             "sudo mv hadoop-%s/* %s && sudo rm -rf hadoop-%s && sudo rm -f hadoop-%s.tar.gz >/dev/null 2>&1",
             hadoop_version, install_dir, hadoop_version, hadoop_version);
    if (!executeSystemCommand(mv_cmd)) {
        fprintf(stderr,   "Error: Failed to move files to %s\n", install_dir);
        return;
    }

    // Adjust ownership of install_dir to current user
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        fprintf(stderr,   "Error: Could not determine current user\n");
        return;
    }
    char chown_cmd[512];
    snprintf(chown_cmd, sizeof(chown_cmd), "sudo chown -R %s:%s %s >/dev/null 2>&1", pwd->pw_name, pwd->pw_name, install_dir);
    if (!executeSystemCommand(chown_cmd)) {
        fprintf(stderr,   "Error: Failed to set ownership of %s\n", install_dir);
        return;
    }

    // Update .bashrc (with duplication check)
    char *home = getenv("HOME");
    if (!home) {
        fprintf(stderr,   "Error: $HOME not set\n");
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
            fprintf(stderr,   "Warning: Could not update .bashrc. Manual configuration needed.\n");
        }
    }

    // Source .bashrc
    if (sourceBashrc() != 0) {
        fprintf(stderr,   "Warning: Failed to source .bashrc. Run 'source ~/.bashrc' manually.\n");
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
        fprintf(stderr,   "Error: JAVA_HOME not found. Install Java JDK.\n");
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
        fprintf(stderr,   "Error: Could not configure Hadoop environment. Check permissions.\n");
        return;
    }

    // Create HDFS directories
    char nameNode[512], dataNode[512];
    snprintf(nameNode, sizeof(nameNode), "%s/hdfs/namenode", home);
    snprintf(dataNode, sizeof(dataNode), "%s/hdfs/datanode", home);
    char mkdir_hdfs_cmd[512];
    size_t ret2 =  snprintf(mkdir_hdfs_cmd, sizeof(mkdir_hdfs_cmd), "mkdir -p %s %s >/dev/null 2>&1", nameNode, dataNode);
    if (ret2 >= sizeof(mkdir_hdfs_cmd)) {
        fprintf(stderr,   "Error: size error  \n");
    }
    if (!executeSystemCommand(mkdir_hdfs_cmd)) {
        fprintf(stderr,   "Error: Failed to create HDFS directories\n");
        return;
    }


    // Format NameNode
    char format_cmd[512];
    snprintf(format_cmd, sizeof(format_cmd), "%s/bin/hdfs namenode -format -force >/dev/null 2>&1", install_dir);
    if (!executeSystemCommand(format_cmd)) {
        fprintf(stderr,   "Error: NameNode format failed. Check HDFS config.\n");
        return;
    }

    char candidate_path[PATH_MAX];
    snprintf(candidate_path, sizeof(candidate_path), "%s/etc/hadoop/", install_dir);

    if (create_xml_file(candidate_path, "ranger-hdfs-audit.xml") !=0)
        fprintf(stderr,  "Failed to create XML file\n");


    if (create_xml_file(candidate_path, "ranger-policymgr-ssl.xml") !=0)
        fprintf(stderr,     "Failed to create XML file\n");

    if (create_xml_file(candidate_path, "ranger-hdfs-policymgr-ssl.xml") !=0)
        fprintf(stderr,     "Failed to create XML file\n");

    if (create_xml_file(candidate_path, "ssl-server.xml") !=0)
        fprintf(stderr,     "Failed to create XML file\n");


    if (create_xml_file(candidate_path, "ranger-hdfs-security.xml") !=0)
        fprintf(stderr,   "Failed to create XML file\n");

    if (create_xml_file(candidate_path, "ssl-client.xml") !=0)
        fprintf(stderr,   "Failed to create XML file\n");


    if (create_xml_file(candidate_path, "ranger-yarn-audit.xml") !=0)
        fprintf(stderr,  "Failed to create XML file\n");

    if (create_xml_file(candidate_path, "ranger-yarn-policymgr-ssl.xml") !=0)
        fprintf(stderr,  "Failed to create XML file\n");

    if (create_xml_file(candidate_path, "ranger-yarn-security.xml") !=0)
        fprintf(stderr,  "Failed to create XML file\n");

    if (create_xml_file(candidate_path, "resource-types.xml") !=0)
        fprintf(stderr,  "Failed to create XML file\n");

    if (create_xml_file(candidate_path, "container-executor.xml") !=0)
        fprintf(stderr,  "Failed to create XML file\n");


    if (create_properties_file(candidate_path, "ranger-yarn-plugin.properties") !=0)
        fprintf(stderr,   "Failed to create XML file\n");


    if (create_properties_file(candidate_path, "ranger-hdfs-plugin.properties") !=0)
        fprintf(stderr,   "Failed to create XML file\n");


    printf( "Hadoop installed successfully to %s\n", install_dir);
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
    snprintf(command, sizeof(command), "wget -q %s -O %s >/dev/null 2>&1", url, filename);
    if (!executeSystemCommand(command)) {
        fprintf(stderr, "Download failed. Check version availability.\n");
        return;
    }

    // Extract source archive
    snprintf(command, sizeof(command), "tar -xvzf %s >/dev/null 2>&1", filename);
    if (!executeSystemCommand(command)) {
        fprintf(stderr, "Source extraction failed.\n");
        return;
    }

    // Remove source archive
    snprintf(command, sizeof(command), "sudo rm -f %s >/dev/null 2>&1", filename);
    executeSystemCommand(command);  // Optional error handling

    // Build with Maven
    snprintf(command, sizeof(command),
             "cd apache-atlas-sources-%s && "
             "mvn clean -DskipTests package -Pdist,embedded-hbase-solr >/dev/null 2>&1",
             actual_version);
    if (!executeSystemCommand(command)) {
        fprintf(stderr,"Maven build failed.\n");
        return;
    }

    // Extract binary distribution
    char binary_tarball[512];
    snprintf(binary_tarball, sizeof(binary_tarball),
             "apache-atlas-sources-%s/distro/target/apache-atlas-%s-bin.tar.gz",
             actual_version, actual_version);
    snprintf(command, sizeof(command), "tar -xvzf %s >/dev/null 2>&1", binary_tarball);
    if (!executeSystemCommand(command)) {
        fprintf(stderr, "Binary extraction failed.\n");
        return;
    }

    // Cleanup source directory
    snprintf(command, sizeof(command), "sudo rm -rf apache-atlas-sources-%s >/dev/null 2>&1", actual_version);
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
    snprintf(command, sizeof(command), "sudo mv %s %s/atlas-%s >/dev/null 2>&1",
             extracted_dir, install_path, actual_version);
    if (!executeSystemCommand(command)) {
        fprintf(stderr,"Moving installation failed.\n");
        return;
    }

    // Set ownership
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        fprintf(stderr, "Could not determine current user\n");
        return;
    }
    snprintf(command, sizeof(command), "sudo chown -R %s:%s %s/atlas-%s >/dev/null 2>&1",
             pwd->pw_name, pwd->pw_name, install_path, actual_version);
    if (!executeSystemCommand(command)) {
        fprintf(stderr, "Ownership change failed\n");
        return;
    }

    // Configure Hadoop integration
    char *hadoop_home = getenv("HADOOP_HOME");
    if (!hadoop_home) {
        fprintf(stderr, "HADOOP_HOME not set!\n");
        return;
    }

    char atlas_conf_dir[512];
    snprintf(atlas_conf_dir, sizeof(atlas_conf_dir),
             "%s/atlas-%s/conf", install_path, actual_version);

    // Link core-site.xml
    snprintf(command, sizeof(command), "ln -sf %s/etc/hadoop/core-site.xml %s/ >/dev/null 2>&1",
             hadoop_home, atlas_conf_dir);
    if (!executeSystemCommand(command)) {
        fprintf(stderr,"core-site.xml linking failed\n");
    }

    // Link hdfs-site.xml
    snprintf(command, sizeof(command), "ln -sf %s/etc/hadoop/hdfs-site.xml %s/ >/dev/null 2>&1",
             hadoop_home, atlas_conf_dir);
    if (!executeSystemCommand(command)) {
        fprintf(stderr, "hdfs-site.xml linking failed\n");
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
        fprintf(stderr, "Sourcing .bashrc failed\n");
    }

    // Configure Atlas properties
    char properties_file[512];
    snprintf(properties_file, sizeof(properties_file),
             "%s/atlas-%s/conf/atlas-application.properties",
             install_path, actual_version);

    // Initialize HBase schema
    snprintf(command, sizeof(command),
             "%s/atlas-%s/bin/atlas_start.py --setup >/dev/null 2>&1",
             install_path, actual_version);
    if (!executeSystemCommand(command)) {
        fprintf(stderr, "HBase schema initialization failed\n");
    }

    printf("Apache Atlas %s installed successfully at %s/atlas-%s\n",
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
    // printf( "Downloading Storm ...\n");
    char wget_cmd[512];
    snprintf(wget_cmd, sizeof(wget_cmd), "wget -q %s >/dev/null 2>&1", url);
    if (!executeSystemCommand(wget_cmd)) {
        fprintf(stderr,   "Download failed. Check version availability.\n");
        return;
    }

    // Extract the archive
    // printf( "Extracting archive...\n");
    char tar_cmd[512];
    if (version)
        snprintf(tar_cmd, sizeof(tar_cmd),
                 "tar -xzf apache-storm-%s.tar.gz >/dev/null 2>&1", version);
    else
        snprintf(tar_cmd, sizeof(tar_cmd),
                 "tar -xzf apache-storm-1.2.4.tar.gz >/dev/null 2>&1");

    if (!executeSystemCommand(tar_cmd)) {
        fprintf(stderr,   "Extraction failed\n");
        return;
    }

    // Remove downloaded archive after extraction
    //printf( "Removing archive...\n");
    char rm_cmd[512];
    if (version)
        snprintf(rm_cmd, sizeof(rm_cmd), "sudo rm -f apache-storm-%s.tar.gz >/dev/null 2>&1", version);
    else
        snprintf(rm_cmd, sizeof(rm_cmd), "sudo rm -f apache-storm-1.2.4.tar.gz >/dev/null 2>&1");

    if (!executeSystemCommand(rm_cmd)) {
        fprintf(stderr,   "Failed to remove archive.\n");
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
            fprintf(stderr,   "Unsupported Linux distribution\n");
            return;
        }
    }
    else
        install_dir = location;
    // Move extracted directory (with sudo if needed)
    // printf( "Installing to %s...\n", install_dir);
    char mv_cmd[512];
    int is_root = (geteuid() == 0);
    if (version)
        snprintf(mv_cmd, sizeof(mv_cmd), "%smv apache-storm-%s %s >/dev/null 2>&1",
                 is_root ? "" : "sudo ",
                 version,
                 install_dir);
    else
        snprintf(mv_cmd, sizeof(mv_cmd), "%smv apache-storm-1.2.4 %s >/dev/null 2>&1",
                 is_root ? "" : "sudo ",
                 install_dir);

    if (!executeSystemCommand(mv_cmd)) {
        fprintf(stderr,   "Installation directory error. Check permissions.\n");
        return;
    }

    // Adjust ownership of install_dir to current user
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        fprintf(stderr,   "Error: Could not determine current user\n");
        return;
    }
    char chown_cmd[512];
    snprintf(chown_cmd, sizeof(chown_cmd), "sudo chown -R %s:%s %s >/dev/null 2>&1", pwd->pw_name, pwd->pw_name, install_dir);
    if (!executeSystemCommand(chown_cmd)) {
        fprintf(stderr,   "Error: Failed to set ownership of %s\n", install_dir);
        return;
    }

    // Configure environment variables
    //printf( "Configuring environment...\n");
    char *home = getenv("HOME");
    if (!home) {
        fprintf(stderr,   "Could not determine user home directory\n");
        return;
    }

    char bashrc_path[512];
    snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home);

    FILE *bashrc = fopen(bashrc_path, "a");
    if (!bashrc) {
        perror("Error updating bashrc");
        return;
    }

    fprintf(bashrc, "\n# Apache Storm configuration\n");
    fprintf(bashrc, "export STORM_HOME=%s\n", install_dir);
    fprintf(bashrc, "export PATH=$PATH:$STORM_HOME/bin\n");
    fclose(bashrc);

    // Source the updated bashrc
    if (sourceBashrc() != 0) {
        fprintf(stderr,   "Failed to source .bashrc\n");
        return;
    }

    // Construct path to storm.yaml
    char storm_yaml_path[512];
    snprintf(storm_yaml_path, sizeof(storm_yaml_path), "%s/conf/storm.yaml", install_dir);

    // Configure Storm settings with sudo if needed
    char sudo_cmd[256];
    snprintf(sudo_cmd, sizeof(sudo_cmd), "%s", is_root ? "" : "sudo ");


    // Create Storm data directory with proper permissions
    printf( "Creating Storm data directory...\n");
    char mkdir_cmd[512];
    snprintf(mkdir_cmd, sizeof(mkdir_cmd), "%smkdir -p /var/lib/storm-data && %schown -R storm:storm /var/lib/storm-data >/dev/null 2>&1",
             is_root ? "" : "sudo ",
             is_root ? "" : "sudo ");
    if (!executeSystemCommand(mkdir_cmd)) {
        fprintf(stderr,   "Failed to create Storm data directory. Check permissions.\n");
        return;
    }

    // Final instructions
    printf( "\nInstallation complete!\n");
    // printf( "Ensure user 'storm' exists and has proper permissions on data directory.\n");
}

void install_Ranger(char* version, char *location) {
    char url[512];
    char filename[256];
    char extracted_dir[256];
    char command[1024];
    char* install_dir = NULL;

    // Generate URL and filenames for source distribution
    if (version) {
        snprintf(url, sizeof(url), "https://dlcdn.apache.org/ranger/%s/apache-ranger-%s-src.tar.gz", version, version);
        snprintf(filename, sizeof(filename), "apache-ranger-%s-src.tar.gz", version);
        snprintf(extracted_dir, sizeof(extracted_dir), "apache-ranger-%s-src", version);
    } else {
        snprintf(filename, sizeof(filename), "apache-ranger-2.0.0.tar.gz ");
        snprintf(extracted_dir, sizeof(extracted_dir), "apache-ranger-2.0.0");
        snprintf(url, sizeof(url), "https://dlcdn.apache.org/ranger/2.0.0/apache-ranger-2.0.0.tar.gz ");
    }

    // Download the source archive
    snprintf(command, sizeof(command), "wget -O %s %s >/dev/null 2>&1", filename, url);
    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "Failed to download Ranger source archive.\n");
        exit(EXIT_FAILURE);
    }

    // Extract the source archive
    snprintf(command, sizeof(command), "tar -xvzf %s >/dev/null 2>&1", filename);
    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "Failed to extract the source archive.\n");
        exit(EXIT_FAILURE);
    }

    // Build Apache Ranger with Maven
    snprintf(command, sizeof(command), "cd %s && mvn clean compile package install assembly:assembly -DskipTests >/dev/null 2>&1", extracted_dir);
    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "Failed to build Ranger.\n");
        exit(EXIT_FAILURE);
    }

    // Locate the built binary archive
    char built_filename[512];
    if (version) {
        snprintf(built_filename, sizeof(built_filename), "%s/target/apache-ranger-%s-bin.tar.gz", extracted_dir, version);
    } else {
        snprintf(built_filename, sizeof(built_filename), "%s/target/apache-ranger-2.0.0-bin.tar.gz", extracted_dir);
    }

    // Check if the built archive exists
    //  if (access(built_filename, F_OK) != 0) {
    //    fprintf(stderr,   "Built Ranger archive not found.\n");
    //  exit(EXIT_FAILURE);
    // }

    // Extract the built binary archive
    snprintf(command, sizeof(command), "tar -xvzf %s >/dev/null 2>&1", built_filename);
    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "Failed to extract the built Ranger archive.\n");
        exit(EXIT_FAILURE);
    }

    // Determine the extracted directory name of the built binary
    char built_extracted_dir[256];
    if (version) {
        snprintf(built_extracted_dir, sizeof(built_extracted_dir), "apache-ranger-%s", version);
    } else {
        snprintf(built_extracted_dir, sizeof(built_extracted_dir), "apache-ranger-2.0.0");
    }

    // Remove downloaded source archive and extracted source directory
    snprintf(command, sizeof(command), "sudo rm -f %s >/dev/null 2>&1", filename);
    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "Failed to remove source archive.\n");
    }

    snprintf(command, sizeof(command), "sudo rm -rf %s >/dev/null 2>&1", extracted_dir);
    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "Failed to remove extracted source directory.\n");
    }

    // Determine installation directory based on OS
    if (!location) {
        if (access("/etc/debian_version", F_OK) == 0) {
            install_dir = "/usr/local/ranger";
        } else if (access("/etc/redhat-release", F_OK) == 0 || access("/etc/system-release", F_OK) == 0) {
            install_dir = "/opt/ranger";
        } else {
            fprintf(stderr,   "Unsupported Linux distribution.\n");
            exit(EXIT_FAILURE);
        }
    } else {
        install_dir = location;
    }

    // Move the built extracted directory to the installation path
    snprintf(command, sizeof(command), "sudo mv %s %s >/dev/null 2>&1", built_extracted_dir, install_dir);
    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "Failed to move directory to %s. Check permissions.\n", install_dir);
        exit(EXIT_FAILURE);
    }

    // Adjust ownership of install_dir to current user
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        fprintf(stderr,   "Error: Could not determine current user\n");
        return;
    }
    char chown_cmd[512];
    snprintf(chown_cmd, sizeof(chown_cmd), "sudo chown -R %s:%s %s >/dev/null 2>&1", pwd->pw_name, pwd->pw_name, install_dir);
    if (!executeSystemCommand(chown_cmd)) {
        fprintf(stderr,   "Error: Failed to set ownership of %s\n", install_dir);
        return;
    }

    // Update .bashrc with RANGER_HOME and PATH
    const char* home = getenv("HOME");
    if (!home) {
        fprintf(stderr,   "HOME environment variable not set.\n");
        exit(EXIT_FAILURE);
    }

    char bashrc_path[256];
    snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home);
    FILE* bashrc = fopen(bashrc_path, "a");
    if (!bashrc) {
        perror("Failed to open .bashrc");
        exit(EXIT_FAILURE);
    }

    fprintf(bashrc, "\n# Apache Ranger environment variables\n");
    fprintf(bashrc, "export RANGER_HOME=%s\n", install_dir);
    fprintf(bashrc, "export PATH=\"$PATH:$RANGER_HOME/bin\"\n");
    fclose(bashrc);

    // Inform user to source the .bashrc or restart the shell
    printf( "Installation completed successfully.\n");

    if (sourceBashrc() != 0) {
        fprintf(stderr,   "Sourcing .bashrc failed.\n");
        return;
    }
}

void install_phoenix(char *version, char *location) {

    // Determine HBASE_HOME
    char *hbase_home = getenv("HBASE_HOME");
    struct stat buffer;
    if (hbase_home == NULL) {
        if (stat("/usr/local/hbase", &buffer) == 0) {
            hbase_home = "/usr/local/hbase";
        } else if (stat("/opt/hbase", &buffer) == 0) {
            hbase_home = "/opt/hbase";
        } else {
            fprintf(stderr,   "HBase installation not found. Set HBASE_HOME or install HBase.\n");
            exit(1);
        }
    }

    // Get HBase version
    char command[512];
    snprintf(command, sizeof(command), "%s/bin/hbase version 2>&1", hbase_home);
    FILE *fp = popen(command, "r");
    if (!fp) {
        perror("Failed to get HBase version");
        exit(1);
    }

    char hbase_version[256] = "2.5"; // Default if parsing fails
    char line[256];
    while (fgets(line, sizeof(line), fp) != NULL) {
        if (strstr(line, "HBase ")) {
            sscanf(line, "HBase %s", hbase_version);
            // Truncate to major.minor (e.g., 2.5.0 -> 2.5)
            char *second_dot = strrchr(hbase_version, '.');
            if (second_dot) *second_dot = '\0';
            break;
        }
    }
    pclose(fp);

    // Generate URL for Phoenix binary
    char url[256], filename[256], dirname[256];
    snprintf(url, sizeof(url), "https://downloads.apache.org/phoenix/phoenix-%s-HBase-%s/phoenix-%s-HBase-%s-bin.tar.gz",
             version, hbase_version, version, hbase_version);
    snprintf(filename, sizeof(filename), "phoenix-%s-HBase-%s-bin.tar.gz", version, hbase_version);
    snprintf(dirname, sizeof(dirname), "phoenix-%s-HBase-%s-bin", version, hbase_version);

    // Download the binary archive
    size_t ret = snprintf(command, sizeof(command), "wget -q %s -O %s >/dev/null 2>&1", url, filename);
    if (ret >= sizeof(command)) {
        fprintf(stderr,   "size error\n");
        exit(1);
    }
    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "Failed to download Phoenix. Check version or network.\n");
        exit(1);
    }

    // Extract the archive
    snprintf(command, sizeof(command), "tar -xvzf %s >/dev/null 2>&1", filename);
    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "Extraction failed. File may be corrupt.\n");
        exit(1);
    }

    // Remove the downloaded archive
    snprintf(command, sizeof(command), "rm -f %s >/dev/null 2>&1", filename);
    executeSystemCommand(command);

    // Determine installation directory based on OS
    char* install_dir = NULL;
    if (!location)
    {
        if (stat("/etc/debian_version", &buffer) == 0) {
            install_dir =  "/usr/local/phoenix";
        } else if (stat("/etc/redhat-release", &buffer) == 0) {
            install_dir =  "/opt/phoenix";
        } else {
            fprintf(stderr,   "Unsupported OS. Exiting.\n");
            exit(1);
        }
    }
    else
        install_dir = location;
    // Move extracted directory to install path
    snprintf(command, sizeof(command), "sudo mv %s %s >/dev/null 2>&1", dirname, install_dir);
    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "Failed to move Phoenix to %s. Check permissions.\n", install_dir);
        exit(1);
    }

    // Copy Phoenix server JAR to HBase's lib directory
    char server_jar[256];
    snprintf(server_jar, sizeof(server_jar), "phoenix-server-hbase-%s-%s.jar", hbase_version, version);
    char source_jar[512], dest_jar[512];
    snprintf(source_jar, sizeof(source_jar), "%s/%s", install_dir, server_jar);
    snprintf(dest_jar, sizeof(dest_jar), "%s/lib/%s", hbase_home, server_jar);
    size_t ret2 = snprintf(command, sizeof(command), "sudo cp %s %s >/dev/null 2>&1", source_jar, dest_jar);
    if (ret2 >= sizeof(command)) {
        fprintf(stderr,   "size error\n");
        exit(1);
    }
    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "Failed to copy server JAR to HBase.\n");
        exit(1);
    }

    // Configure Phoenix client JAR in hbase-env.sh
    char client_jar[256];
    snprintf(client_jar, sizeof(client_jar), "phoenix-client-hbase-%s-%s.jar", hbase_version, version);
    char hbase_env_path[512];
    snprintf(hbase_env_path, sizeof(hbase_env_path), "%s/conf/hbase-env.sh", hbase_home);
    FILE *hbase_env = fopen(hbase_env_path, "a");
    if (hbase_env) {
        fprintf(hbase_env, "\nexport HBASE_CLASSPATH=$HBASE_CLASSPATH:%s/%s\n", install_dir, client_jar);
        fclose(hbase_env);
    } else {
        perror("Failed to update hbase-env.sh");
        exit(1);
    }

    // Adjust ownership of install_dir to current user
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        fprintf(stderr,   "Error: Could not determine current user\n");
        return;
    }
    char chown_cmd[512];
    snprintf(chown_cmd, sizeof(chown_cmd), "sudo chown -R %s:%s %s >/dev/null 2>&1", pwd->pw_name, pwd->pw_name, install_dir);
    if (!executeSystemCommand(chown_cmd)) {
        fprintf(stderr,   "Error: Failed to set ownership of %s\n", install_dir);
        return;
    }

    // Update .bashrc with PHOENIX_HOME
    char bashrc_path[256];
    char *home = getenv("HOME");
    if (!home) {
        fprintf(stderr,   "Home directory not found.\n");
        exit(1);
    }
    snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home);
    FILE *bashrc = fopen(bashrc_path, "a");
    if (bashrc) {
        fprintf(bashrc, "\nexport PHOENIX_HOME=%s\n", install_dir);
        fclose(bashrc);
    } else {
        perror("Failed to update .bashrc");
        exit(1);
    }

    sourceBashrc();
    printf( "Apache Phoenix %s installed. Server JAR copied to HBase and client configured.\n", version);
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
        fprintf(stderr,   "Unsupported OS. Exiting.\n");
        exit(1);
    }

    // 1. Download Solr archive
    if (version)
    {
        snprintf(command, sizeof(command),
                 "wget https://downloads.apache.org/solr/solr/9.8.1/solr-%s.tgz >/dev/null 2>&1", version);
    }
    else
        snprintf(command, sizeof(command),
                 "wget https://downloads.apache.org/solr/solr/9.8.1/solr-9.8.1.tgz >/dev/null 2>&1");

    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "downloading faild\n");
        return;
    }
    // 2. Extract the archive
    if (version)
        snprintf(command, sizeof(command),
                 "tar xzf solr-%s.tgz >/dev/null 2>&1", version);
    else
        snprintf(command, sizeof(command),
                 "tar xzf solr-9.8.1.tgz >/dev/null 2>&1");

    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "download failed.\n");
        return;
    }

    // Remove downloaded archive after extraction
    //  printf( "Removing archive...\n");
    if (version)
        snprintf(command, sizeof(command), "sudo rm -f solr-%s.tgz >/dev/null 2>&1", version);
    else
        snprintf(command, sizeof(command), "sudo rm -f solr-9.8.1.tgz >/dev/null 2>&1");

    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "Failed to remove archive.\n");
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
                 "sudo mv solr-%s %s && sudo chown -R $(whoami): %s >/dev/null 2>&1",
                 version, install_dir, install_dir);
    else
        snprintf(command, sizeof(command),
                 "sudo mv solr-9.8.1 %s && sudo chown -R $(whoami): %s >/dev/null 2>&1", install_dir, install_dir);

    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "move failed.\n");
        return;
    }

    // Adjust ownership of install_dir to current user
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        fprintf(stderr,   "Error: Could not determine current user\n");
        return;
    }
    char chown_cmd[512];
    snprintf(chown_cmd, sizeof(chown_cmd), "sudo chown -R %s:%s %s >/dev/null 2>&1", pwd->pw_name, pwd->pw_name, install_dir);
    if (!executeSystemCommand(chown_cmd)) {
        fprintf(stderr,   "Error: Failed to set ownership of %s\n", install_dir);
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
                fprintf(stderr,   "bashing failed.\n");
                return;
            }

        }
    }

    printf( "Solr installed successfully at %s\n", install_dir);
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
    snprintf(command, sizeof(command), "wget -q %s >/dev/null 2>&1", url);
    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "Failed to download Kafka\n");
        exit(EXIT_FAILURE);
    }

    // Extract archive
    if (version)
        snprintf(filename, sizeof(filename), "kafka_2.13-%s.tgz", version);
    else
        snprintf(filename, sizeof(filename), "kafka_2.13-3.7.2.tgz");

    snprintf(command, sizeof(command), "tar -xvzf %s >/dev/null 2>&1", filename);
    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "Failed to extract archive\n");
        exit(EXIT_FAILURE);
    }

    // Remove downloaded archive after extraction
    //printf( "Removing archive...\n");
    snprintf(command, sizeof(command), "sudo rm -f %s >/dev/null 2>&1", filename);

    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "Failed to remove archive.\n");
    }

    // Determine OS type
    int is_debian = access("/etc/debian_version", F_OK) == 0;
    int is_redhat = access("/etc/redhat-release", F_OK) == 0;

    if (!is_debian && !is_redhat) {
        fprintf(stderr,   "Unsupported Linux distribution\n");
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

    snprintf(command, sizeof(command), "sudo mv %s %s >/dev/null 2>&1"
             , dir_name, install_dir);
    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "Failed to move Kafka directory\n");
        exit(EXIT_FAILURE);
    }

    // Adjust ownership of install_dir to current user
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        fprintf(stderr,   "Error: Could not determine current user\n");
        return;
    }
    char chown_cmd[512];
    snprintf(chown_cmd, sizeof(chown_cmd), "sudo chown -R %s:%s %s >/dev/null 2>&1", pwd->pw_name, pwd->pw_name, install_dir);
    if (!executeSystemCommand(chown_cmd)) {
        fprintf(stderr,   "Error: Failed to set ownership of %s\n", install_dir);
        return;
    }


    // Configure environment variables
    char* home = getenv("HOME");
    if (!home) {
        fprintf(stderr,   "Could not determine home directory\n");
        exit(EXIT_FAILURE);
    }

    char bashrc_path[512];
    snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home);

    FILE* bashrc = fopen(bashrc_path, "a");
    if (!bashrc) {
        perror("Failed to update .bashrc");
        exit(EXIT_FAILURE);
    }

    fprintf(bashrc, "\n# Apache Kafka configuration\n");
    fprintf(bashrc, "export KAFKA_HOME=%s\n", install_dir);
    fprintf(bashrc, "export PATH=\"$KAFKA_HOME/bin:$PATH\"\n");
    fclose(bashrc);

    char candidate_path[PATH_MAX];
    snprintf(candidate_path, sizeof(candidate_path), "%s/config", install_dir);

    if (create_xml_file(candidate_path, "ranger-kafka-audit.xml") !=0)
        fprintf(stderr,   "Failed to create XML file\n");


    if (create_xml_file(candidate_path, "ranger-kafka-policymgr-ssl.xml") !=0)
        fprintf(stderr,   "Failed to create XML file\n");


    if (create_xml_file(candidate_path, "ranger-kafka-security.xml") !=0)
        fprintf(stderr,  "Failed to create XML file\n");


    if (create_properties_file(candidate_path, "ranger-kafka-plugin.properties") !=0)
        fprintf(stderr,    "Failed to create XML file\n");



    printf( "\nKafka installed successfully to %s\n", install_dir);

    if (sourceBashrc() != 0) {
        fprintf(stderr,   "bashing failed.\n");
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
    snprintf(command, sizeof(command), "wget %s >/dev/null 2>&1", url);
    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "download failed\n");
        exit(EXIT_FAILURE);
    }

    if (!executeSystemCommand("tar -xvzf pig-*.tar.gz >/dev/null 2>&1")) {
        fprintf(stderr,   "taring failed\n");
        exit(EXIT_FAILURE);
    }

    // Remove downloaded archive after extraction
    //printf( "Removing archive...\n");
    if (version)
        snprintf(command, sizeof(command), "sudo rm -f pig-%s.tar.gz >/dev/null 2>&1", version);
    else
        snprintf(command, sizeof(command), "sudo rm -f pig-0.17.0.tar.gz >/dev/null 2>&1");

    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "Failed to remove archive.\n");
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
        snprintf(command, sizeof(command), "mv pig-%s %s >/dev/null 2>&1", version, install_dir);
    else
        snprintf(command, sizeof(command), "mv pig-0.17.0 %s >/dev/null 2>&1", install_dir);

    //if (ret1 >= sizeof(command))  {// Truncation occurred
    //  fprintf(stderr,   "Error: Command truncated.\n");
    //}
    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "moving failed\n");
        exit(EXIT_FAILURE);
    }

    // Adjust ownership of install_dir to current user
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        fprintf(stderr,   "Error: Could not determine current user\n");
        return;
    }
    char chown_cmd[512];
    snprintf(chown_cmd, sizeof(chown_cmd), "sudo chown -R %s:%s %s >/dev/null 2>&1", pwd->pw_name, pwd->pw_name, install_dir);
    if (!executeSystemCommand(chown_cmd)) {
        fprintf(stderr,   "Error: Failed to set ownership of %s\n", install_dir);
        return;
    }

    char bashrc_path[256];
    char* home = getenv("HOME");
    if (home == NULL) {
        fprintf(stderr,   "Error: HOME environment variable not set.\n");
        return;
    }
    snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home);

    long unsigned int ret2 =  snprintf(command, sizeof(command), "echo 'export PIG_HOME=\"%s\"' >> %s >/dev/null 2>&1", install_dir, bashrc_path);
    if (ret2 >= sizeof(command)) { // Truncation occurred
        fprintf(stderr,   "Error: Command truncated.\n");
    }
    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "echo failed\n");
        exit(EXIT_FAILURE);
    }

    snprintf(command, sizeof(command), "echo 'export PATH=$PATH:$PIG_HOME/bin' >> %s >/dev/null 2>&1", bashrc_path);
    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "echo failed\n");
        exit(EXIT_FAILURE);
    }
    snprintf(command, sizeof(command), "echo 'export PIG_CLASSPATH=$HADOOP_HOME/etc/hadoop' >> >/dev/null 2>&1 ");
    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "echo failed\n");
        exit(EXIT_FAILURE);
    }
    if (sourceBashrc() != 0) {
        fprintf(stderr,   "bashing failed.\n");
        return;
    }

    fprintf(stderr,   "Pig Installed successfully\n");

}
// Helper function to execute a command and capture output
char* executeCommandAndGetOutput(const char* command) {
    char buffer[128];
    char* result = malloc(256);
    if (!result) {
        perror("malloc failed");
        return NULL;
    }
    result[0] = '\0';

    FILE* fp = popen(command, "r");
    if (!fp) {
        perror("popen failed");
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
    const char* default_version = "2.5.11";

    // Create download URL
    snprintf(url, sizeof(url), "https://downloads.apache.org/hbase/stable/hbase-%s-bin.tar.gz",
             version ? version : default_version);

    // Download the archive
    char wget_cmd[512];
    snprintf(wget_cmd, sizeof(wget_cmd), "wget %s >/dev/null 2>&1", url);
    if (!executeSystemCommand(wget_cmd)) {
        fprintf(stderr,   "Download failed for version: %s\n", version ? version : default_version);
        exit(EXIT_FAILURE);
    }

    // Extract archive
    char tar_cmd[512];
    snprintf(tar_cmd, sizeof(tar_cmd), "tar -xvzf hbase-%s-bin.tar.gz >/dev/null 2>&1",
             version ? version : default_version);
    if (!executeSystemCommand(tar_cmd)) {
        fprintf(stderr,   "Extraction failed\n");
        exit(EXIT_FAILURE);
    }

    // Remove downloaded archive
    char rm_cmd[512];
    snprintf(rm_cmd, sizeof(rm_cmd), "rm -f hbase-%s-bin.tar.gz >/dev/null 2>&1",
             version ? version : default_version);
    if (!executeSystemCommand(rm_cmd)) {
        fprintf(stderr,   "Failed to remove archive\n");
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
            fprintf(stderr,   "Unsupported OS\n");
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
    snprintf(rename_cmd, sizeof(rename_cmd), "mv %s hbase >/dev/null 2>&1", dir_name);
    if (!executeSystemCommand(rename_cmd)) {
        fprintf(stderr,   "Failed to rename HBase directory from %s to hbase\n", dir_name);
        exit(EXIT_FAILURE);
    }

    // Move HBase directory to target_dir
    snprintf(command, sizeof(command), "sudo mv hbase %s >/dev/null 2>&1", target_dir);
    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "Failed to move HBase to %s\n", target_dir);
        exit(EXIT_FAILURE);
    }

    // Adjust ownership
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        fprintf(stderr,   "Error: Could not determine current user\n");
        exit(EXIT_FAILURE);
    }
    char chown_cmd[512];
    snprintf(chown_cmd, sizeof(chown_cmd), "sudo chown -R %s:%s %s/hbase >/dev/null 2>&1",
             pwd->pw_name, pwd->pw_name, target_dir);
    if (!executeSystemCommand(chown_cmd)) {
        fprintf(stderr,   "Error: Failed to set ownership of %s/hbase\n", target_dir);
        exit(EXIT_FAILURE);
    }

    // Update environment variables
    char *home = getenv("HOME");
    if (!home) {
        fprintf(stderr,   "Home directory not found\n");
        exit(EXIT_FAILURE);
    }

    char bashrc_path[256];
    snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home);
    FILE *bashrc = fopen(bashrc_path, "a");
    if (!bashrc) {
        perror("Failed to open .bashrc");
        exit(EXIT_FAILURE);
    }
    fprintf(bashrc, "\n# HBase Configuration\nexport HBASE_HOME=%s/hbase\n", target_dir);
    fprintf(bashrc, "export PATH=$HBASE_HOME/bin:$PATH\n");
    fclose(bashrc);

    // Set JAVA_HOME in hbase-env.sh
    char* java_home = executeCommandAndGetOutput("bash -c 'source ~/.bashrc && echo $JAVA_HOME'");
    if (!java_home || strlen(java_home) == 0) {
        fprintf(stderr,   "JAVA_HOME not set in .bashrc\n");
        exit(EXIT_FAILURE);
    }

    char hbase_env_path[512];
    snprintf(hbase_env_path, sizeof(hbase_env_path), "%s/hbase/conf/hbase-env.sh", target_dir);

    // Update JAVA_HOME configuration
    char sed_cmd[1024];
    snprintf(sed_cmd, sizeof(sed_cmd),
             "sed -i.bak '/^#[[:space:]]*export JAVA_HOME=/c\\export JAVA_HOME=\"%s\"' %s >/dev/null 2>&1",
             java_home, hbase_env_path);
    if (!executeSystemCommand(sed_cmd)) {
        fprintf(stderr,   "Failed to set JAVA_HOME in hbase-env.sh\n");
        free(java_home);
        exit(EXIT_FAILURE);
    }
    free(java_home);

    fprintf(stderr,  "HBase successfully installed to %s/hbase\n", target_dir);
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
    snprintf(command, sizeof(command), "wget -q %s >/dev/null 2>&1", url);

    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "downloading faild\n");
        return;
    }
    // Extract downloaded archive
    if (version)
        snprintf(command, sizeof(command),
                 "tar -xvzf apache-tez-%s-bin.tar.gz >/dev/null 2>&1", version);
    else
        snprintf(command, sizeof(command),
                 "tar -xvzf apache-tez-0.10.1-bin.tar.gz >/dev/null 2>&1");


    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "taring  faild\n");
        return;
    }
    // Remove downloaded archive after extraction
    //printf( "Removing archive...\n");
    if (version)
        snprintf(command, sizeof(command), "sudo rm -f apache-tez-%s-bin.tar.gz >/dev/null 2>&1", version);
    else
        snprintf(command, sizeof(command), "sudo rm -f apache-tez-0.10.1-bin.tar.gz >/dev/null 2>&1");

    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "Failed to remove archive.\n");
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
                 "sudo mv -f apache-tez-%s-bin %s >/dev/null 2>&1", version, install_dir);
    else
        snprintf(command, sizeof(command),
                 "sudo mv -f apache-tez-0.10.1-bin %s >/dev/null 2>&1", install_dir);

    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "moving   faild\n");
        return;
    }

    // Adjust ownership of install_dir to current user
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        fprintf(stderr,   "Error: Could not determine current user\n");
        return;
    }
    char chown_cmd[512];
    snprintf(chown_cmd, sizeof(chown_cmd), "sudo chown -R %s:%s %s >/dev/null 2>&1", pwd->pw_name, pwd->pw_name, install_dir);
    if (!executeSystemCommand(chown_cmd)) {
        fprintf(stderr,   "Error: Failed to set ownership of %s\n", install_dir);
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
        fprintf(stderr,   "bashing failed.\n");
        return;
    }

    printf( "Apache Tez installed successfully at %s\n", install_dir);
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
    //printf( "Downloading Flink ...\n");
    snprintf(cmd, sizeof(cmd), "wget -q %s >/dev/null 2>&1", url);
    ret = executeSystemCommand(cmd);
    if (ret == -1) {
        fprintf(stderr,   "Failed to download Flink archive\n");
        exit(EXIT_FAILURE);
    }

    // Extract the archive
    //printf( "Extracting archive...\n");
    snprintf(cmd, sizeof(cmd), "tar -xvzf %s >/dev/null", filename);
    ret = executeSystemCommand(cmd);
    if (ret == -1) {
        fprintf(stderr,   "Failed to extract archive\n");
        exit(EXIT_FAILURE);
    }

    // Remove downloaded archive after extraction
    //printf( "Removing archive...\n");
    if (version)
        snprintf(cmd, sizeof(cmd), "sudo rm -f %s >/dev/null 2>&1", filename);

    if (!executeSystemCommand(cmd)) {
        fprintf(stderr,   "Failed to remove archive.\n");
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
            fprintf(stderr,   "Unsupported Linux distribution\n");
            exit(EXIT_FAILURE);
        }
    }
    else
        install_dir = location;
    // Move Flink directory
    printf( "Installing to %s...\n", install_dir);
    char src_dir[256];
    if (version)
        snprintf(src_dir, sizeof(src_dir), "flink-%s", version);
    else
        snprintf(src_dir, sizeof(src_dir), "flink-2.0.0");

    snprintf(cmd, sizeof(cmd), "sudo mv %s %s >/dev/null 2>&1", src_dir, install_dir);
    ret = executeSystemCommand(cmd);
    if (ret == -1) {
        fprintf(stderr,   "Failed to move installation directory\n");
        exit(EXIT_FAILURE);
    }

    // Adjust ownership of install_dir to current user
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        fprintf(stderr,   "Error: Could not determine current user\n");
        return;
    }
    char chown_cmd[512];
    snprintf(chown_cmd, sizeof(chown_cmd), "sudo chown -R %s:%s %s >/dev/null 2>&1", pwd->pw_name, pwd->pw_name, install_dir);
    if (!executeSystemCommand(chown_cmd)) {
        fprintf(stderr,   "Error: Failed to set ownership of %s\n", install_dir);
        return;
    }

    // Update environment variables
    // printf( "Configuring environment...\n");
    const char* home = getenv("HOME");
    if (!home) {
        fprintf(stderr,   "HOME environment variable not set\n");
        exit(EXIT_FAILURE);
    }

    char bashrc_path[256];
    snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home);
    FILE* bashrc = fopen(bashrc_path, "a");
    if (!bashrc) {
        perror("Failed to open .bashrc");
        exit(EXIT_FAILURE);
    }

    fprintf(bashrc, "\n# Apache Flink configuration\nexport FLINK_HOME=%s\nexport PATH=\"$PATH:$FLINK_HOME/bin\"\n", install_dir);
    fclose(bashrc);

    printf( "Installation completed successfully.\n");

    if (sourceBashrc() != 0) {
        fprintf(stderr,   "bashing failed.\n");
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
        snprintf(url, sizeof(url), "https://downloads.apache.org/zookeeper/stable/apache-zookeeper-%s-bin.tar.gz", version);
    else
        snprintf(url, sizeof(url), "https://downloads.apache.org/zookeeper/stable/apache-zookeeper-3.8.4-bin.tar.gz");

    // Download using wget
    snprintf(command, sizeof(command), "wget -q %s >/dev/null 2>&1", url);

    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "downloading  faild\n");
        return;
    }
    // Extract the archive
    if (version)
        snprintf(command, sizeof(command), "tar -xvzf apache-zookeeper-%s-bin.tar.gz > /dev/null", version);
    else
        snprintf(command, sizeof(command), "tar -xvzf apache-zookeeper-3.8.4-bin.tar.gz >/dev/null 2>&1");

    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "taring  faild\n");
        return;
    }

    // Remove downloaded archive after extraction
    // printf( "Removing archive...\n");
    if (version)
        snprintf(command, sizeof(command), "sudo rm -f apache-zookeeper-%s-bin.tar.gz >/dev/null 2>&1", version);
    else
        snprintf(command, sizeof(command), "sudo rm -f apache-zookeeper-3.8.4-bin.tar.gz >/dev/null 2>&1");
    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "Failed to remove archive.\n");
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
            fprintf(stderr,   "Unsupported OS. Defaulting to /usr/local.\n");
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

    snprintf(command, sizeof(command), "sudo mv -f %s %s/zookeeper 2> /dev/null", src_dir, install_dir);

    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "moving  faild\n");
        return;
    }

    // Adjust ownership of install_dir to current user
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        fprintf(stderr,   "Error: Could not determine current user\n");
        return;
    }
    char chown_cmd[512];
    snprintf(chown_cmd, sizeof(chown_cmd), "sudo chown -R %s:%s %s >/dev/null 2>&1", pwd->pw_name, pwd->pw_name, install_dir);
    if (!executeSystemCommand(chown_cmd)) {
        fprintf(stderr,   "Error: Failed to set ownership of %s\n", install_dir);
        return;
    }

    // Copy configuration file
    snprintf(command, sizeof(command), "cp %s/zookeeper/conf/zoo_sample.cfg %s/zookeeper/conf/zoo.cfg >/dev/null 2>&1", install_dir, install_dir);

    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "copying  faild\n");
        return;
    }

    if (!executeSystemCommand("sudo mkdir -p /var/lib/zookeeper > /dev/null 2>&1")) {
        fprintf(stderr,   "make directory failed\n");
        return;
    }


    user = getenv("USER");
    if (user) {
        snprintf(command, sizeof(command), "sudo chown -R %s:%s /var/lib/zookeeper > /dev/null 2>&1", user, user);

        if (!executeSystemCommand(command)) {
            fprintf(stderr,   "copying  faild\n");
            return;
        }
    } else {
        fprintf(stderr,   "Failed to get current user.\n");
    }

    // Update .bashrc with environment variables
    home = getenv("HOME");
    if (home) {
        snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home);

        // Append ZOOKEEPER_HOME and PATH
        snprintf(command, sizeof(command), "echo 'export ZOOKEEPER_HOME=%s/zookeeper' >> %s >/dev/null 2>&1", install_dir, bashrc_path);
        if (!executeSystemCommand(command)) {
            fprintf(stderr,   "eko  faild\n");
            return;
        }
        snprintf(command, sizeof(command), "echo 'export PATH=\"$PATH:$ZOOKEEPER_HOME/bin\"' >> %s >/dev/null 2>&1", bashrc_path);
        if (!executeSystemCommand(command)) {
            fprintf(stderr,   "eko  faild\n");
            return;
        }
    }
    // Source the .bashrc

    if (sourceBashrc() != 0) {
        fprintf(stderr,   "bashing failed.\n");
        return;
    }

    char candidate_path[PATH_MAX];
    snprintf(candidate_path, sizeof(candidate_path), "%s/zookeeper/conf", install_dir);

    if (create_properties_file(candidate_path, "log4j.properties") !=0)
        fprintf(stderr,   "Failed to create properties file\n");

    if (create_properties_file(candidate_path, "zookeeper-env.properties") !=0)
        fprintf(stderr,   "Failed to create properties file\n");

    fprintf(stderr,   "Zookeeper installed successfully\n");

}


void install_Presto(char* version, char *location) {
    char url[512];
    char command[1024];
    char* install_dir = NULL;

    // Construct download URL
    if(version)
        snprintf(url, sizeof(url),
                 "https://repo1.maven.org/maven2/com/facebook/presto/presto-server/%s/presto-server-%s.tar.gz",
                 version, version);
    else
        snprintf(url, sizeof(url),
                 "https://repo1.maven.org/maven2/com/facebook/presto/presto-server/0.282/presto-server-0.282.tar.gz");

    // Download the archive
    snprintf(command, sizeof(command), "wget -q %s >/dev/null 2>&1", url);
    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "Failed to download Presto version %s\n", version);
        exit(EXIT_FAILURE);
    }


    // Extract the archive
    if (version)
        snprintf(command, sizeof(command), "tar -xvzf presto-server-%s.tar.gz > /dev/null", version);
    else
        snprintf(command, sizeof(command), "tar -xvzf presto-server-0.282.tar.gz >/dev/null 2>&1");

    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "taring  faild\n");
        return;
    }

    // Remove downloaded archive after extraction
    // printf( "Removing archive...\n");
    if (version)
        snprintf(command, sizeof(command), "sudo rm -f presto-server-%s.tar.gz >/dev/null 2>&1", version);
    else
        snprintf(command, sizeof(command), "sudo rm -f presto-server-0.282.tar.gz >/dev/null 2>&1");
    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "Failed to remove archive.\n");
    }

    // Determine OS family
    if (!location)
    {
        if (access("/etc/debian_version", F_OK) == 0) {
            install_dir = "/usr/local/presto";
        } else if (access("/etc/redhat-release", F_OK) == 0) {
            install_dir = "/opt/presto";
        } else {
            fprintf(stderr,   "Unsupported Linux distribution\n");
            exit(EXIT_FAILURE);
        }
    }
    else
        install_dir = location;
    // Create installation directory
    snprintf(command, sizeof(command), "sudo mkdir -p %s >/dev/null 2>&1", install_dir);
    // Adjust ownership of install_dir to current user
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        fprintf(stderr,   "Error: Could not determine current user\n");
        return;
    }
    char chown_cmd[512];
    snprintf(chown_cmd, sizeof(chown_cmd), "sudo chown -R %s:%s %s >/dev/null 2>&1", pwd->pw_name, pwd->pw_name, install_dir);
    if (!executeSystemCommand(chown_cmd)) {
        fprintf(stderr,   "Error: Failed to set ownership of %s\n", install_dir);
        return;
    }


    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "making directory  faild\n");
        return;
    }
    // Move extracted files
    if (version)
        snprintf(command, sizeof(command), "sudo mv presto-server-%s %s >/dev/null 2>&1", version, install_dir);
    else
        snprintf(command, sizeof(command), "sudo mv presto-server-0.282 %s >/dev/null 2>&1", install_dir);

    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "making directory  faild\n");
        return;
    }
    // Set environment variables
    char* home = getenv("HOME");
    if (!home) {
        fprintf(stderr,   "Could not determine home directory\n");
        exit(EXIT_FAILURE);
    }

    char bashrc_path[256];
    snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home);

    FILE* bashrc = fopen(bashrc_path, "a");
    if (bashrc) {
        fprintf(bashrc, "\nexport PRESTO_HOME=%s\n", install_dir);
        fprintf(bashrc, "export PATH=$PATH:$PRESTO_HOME/bin\n");
        fclose(bashrc);
    }

    // Update current session
    if (sourceBashrc() != 0) {
        fprintf(stderr,   "bashing failed.\n");
        return;
    }


    if (!executeSystemCommand("rm -f presto-server-*.tar.gz >/dev/null 2>&1")) {
        fprintf(stderr,   "moving failed %s\n", version);
        exit(EXIT_FAILURE);
    }
    //printf( "Presto  successfully installed to %s\n", install_dir);
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
    long unsigned int ret= snprintf(wget_cmd, sizeof(wget_cmd), "wget %s >/dev/null 2>&1", url);
    if (ret >= sizeof(wget_cmd)) { // Truncation occurred
        fprintf(stderr,   "Error: Command truncated.\n");
    }

    if (!executeSystemCommand(wget_cmd)) {
        fprintf(stderr,   "Error: Failed to download Hive archive.\n");
        exit(EXIT_FAILURE);
    }

    if (version)
        snprintf(command, sizeof(command), "tar -xvzf apache-hive-%s-bin.tar.gz > /dev/null", version);
    else
        snprintf(command, sizeof(command), "tar -xvzf apache-hive-4.0.1-bin.tar.gz >/dev/null 2>&1");

    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "taring  faild\n");
        return;
    }

    // Remove downloaded archive after extraction
    // printf( "Removing archive...\n");
    if (version)
        snprintf(command, sizeof(command), "sudo rm -f apache-hive-%s-bin.tar.gz >/dev/null 2>&1", version);
    else
        snprintf(command, sizeof(command), "sudo rm -f apache-hive-4.0.1-bin.tar.gz >/dev/null 2>&1");
    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "Failed to remove archive.\n");
    }

    // Determine OS type
    int os_type;
    if (executeSystemCommand("command -v apt-get > /dev/null 2>&1")) {
        os_type = 0; // Debian-based
    } else if (executeSystemCommand("command -v yum > /dev/null 2>&1 || command -v dnf > /dev/null 2>&1")) {
        os_type = 1; // Red Hat-based
    } else {
        fprintf(stderr,   "Error: Unsupported Linux distribution.\n");
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
    snprintf(rename_cmd, sizeof(rename_cmd), "mv %s hive >/dev/null 2>&1", dir_name);
    if (!executeSystemCommand(rename_cmd)) {
        fprintf(stderr,   "Failed to rename HBase directory from %s to hive\n", dir_name);
        exit(EXIT_FAILURE);
    }

    snprintf(command, sizeof(command), "sudo mv hive %s >/dev/null 2>&1"
             , installation_dir);
    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "Failed to move hive directory\n");
        exit(EXIT_FAILURE);
    }

    // Adjust ownership of install_dir to current user
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        fprintf(stderr,   "Error: Could not determine current user\n");
        return;
    }
    char chown_cmd[512];
    snprintf(chown_cmd, sizeof(chown_cmd), "sudo chown -R %s:%s %s >/dev/null 2>&1", pwd->pw_name, pwd->pw_name, installation_dir);
    if (!executeSystemCommand(chown_cmd)) {
        fprintf(stderr,   "Error: Failed to set ownership of %s\n", installation_dir);
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
        perror("Error: Failed to open .bashrc");
        exit(EXIT_FAILURE);
    }

    fprintf(bashrc, "\n# Apache Hive environment variables\nexport HIVE_HOME=%s/hive\nexport PATH=\"$PATH:$HIVE_HOME/bin\"\n", installation_dir);
    fclose(bashrc);

    // Source the .bashrc
    if (sourceBashrc() != 0) {
        fprintf(stderr,   "bashing failed.\n");
        return;
    }

    char candidate_path[PATH_MAX];
    snprintf(candidate_path, sizeof(candidate_path), "%s/hive/conf", installation_dir);

    if (create_properties_file(candidate_path, "beeline-log4j2.properties") !=0)
        fprintf(stderr,   "Failed to create XML file\n");

    if (create_properties_file(candidate_path, "atlas-application.properties") !=0)
        fprintf(stderr,   "Failed to create XML file\n");

    if (create_properties_file(candidate_path, "hive-exec-log4j2.properties") !=0)
        fprintf(stderr,   "Failed to create XML file\n");

    if (create_properties_file(candidate_path, "llap-cli-log4j2.properties") !=0)
        fprintf(stderr,   "Failed to create XML file\n");

    if (create_properties_file(candidate_path, "llap-daemon-log4j2.properties") !=0)
        fprintf(stderr,   "Failed to create XML file\n");


    if (create_properties_file(candidate_path, "hive-log4j2.properties") !=0)
        fprintf(stderr,   "Failed to create XML file\n");


    if (create_xml_file(candidate_path, "hive-site.xml") !=0)
        fprintf(stderr,   "Failed to create XML file\n");

    if (create_xml_file(candidate_path, "hivemetastore-site.xml") !=0)
        fprintf(stderr,   "Failed to create XML file\n");

    if (create_xml_file(candidate_path, "hiveserver2-site.xml") !=0)
        fprintf(stderr,   "Failed to create XML file\n");

    printf( "Hive has been successfully installed to %s.\n", installation_dir);
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
    snprintf(wget_cmd, sizeof(wget_cmd), "wget -q %s >/dev/null 2>&1", url);

    if (!executeSystemCommand(wget_cmd)) {
        fprintf(stderr,   "Downloading failed\n");
        return;
    }

    // Extract archive
    char tar_cmd[512];
    if (version)
        snprintf(tar_cmd, sizeof(tar_cmd), "tar -xvzf spark-%s-bin-hadoop3.tgz >/dev/null 2>&1", version);
    else
        snprintf(tar_cmd, sizeof(tar_cmd), "tar -xvzf spark-3.5.6-bin-hadoop3.tgz >/dev/null 2>&1");
    if (!executeSystemCommand(tar_cmd)) {
        fprintf(stderr,   "Error: Failed to extract archive\n");
        return;
    }

    // Remove the downloaded archive
    if (version) {
        snprintf(tar_cmd, sizeof(tar_cmd), "sudo rm -f spark-%s-bin-hadoop3.tgz >/dev/null 2>&1", version);
    } else {
        snprintf(tar_cmd, sizeof(tar_cmd), "sudo rm -f spark-3.5.6-bin-hadoop3.tgz >/dev/null 2>&1");
    }
    if (!executeSystemCommand(tar_cmd)) {
        fprintf(stderr,   "Error: Failed to remove archive\n");
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
            fprintf(stderr,   "Error: Unsupported Linux distribution\n");
            return;
        }
    } else {
        install_path = location;
    }

    // Move extracted directory
    char mv_cmd[512];
    if (version) {
        snprintf(mv_cmd, sizeof(mv_cmd),
                 "sudo mv spark-%s-bin-hadoop3 %s >/dev/null 2>&1", version, install_path);
    } else {
        snprintf(mv_cmd, sizeof(mv_cmd),
                 "sudo mv spark-3.5.6-bin-hadoop3 %s >/dev/null 2>&1", install_path);
    }
    if (!executeSystemCommand(mv_cmd)) {
        fprintf(stderr,   "Moving failed\n");
        return;
    }

    // Configure environment variables
    const char* home = getenv("HOME");
    if (!home) {
        fprintf(stderr,   "Error: HOME environment variable not set\n");
        return;
    }

    char bashrc_path[256];
    snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home);

    // Append SPARK_HOME to .bashrc
    char env_cmd[512];
    snprintf(env_cmd, sizeof(env_cmd),
             "echo 'export SPARK_HOME=%s' >> %s >/dev/null 2>&1", install_path, bashrc_path);

    if (!executeSystemCommand(env_cmd)) {
        fprintf(stderr,   "Environment setup failed\n");
        return;
    }

    // Append PATH update to .bashrc
    snprintf(env_cmd, sizeof(env_cmd),
             "echo 'export PATH=$PATH:$SPARK_HOME/bin' >> %s >/dev/null 2>&1", bashrc_path);

    if (!executeSystemCommand(env_cmd)) {
        fprintf(stderr,   "Environment setup failed\n");
        return;
    }

    // Source the updated configuration
    if (sourceBashrc() != 0) {
        fprintf(stderr,   "Sourcing .bashrc failed\n");
        return;
    }
    fprintf(stderr,   "Spark installed successfully\n");
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
    long unsigned int ret = snprintf(wget_cmd, sizeof(wget_cmd), "wget %s >/dev/null 2>&1", url);

    if (ret >= sizeof(wget_cmd)) { // Truncation occurred
        fprintf(stderr,   "Error: Command truncated.\n");
    }

    if (!executeSystemCommand(wget_cmd)) {
        fprintf(stderr,   "Download failed. Invalid version or network issue.\n");
        exit(EXIT_FAILURE);
    }

    // Step 3: Extract archive
    char tar_cmd[512];
    if (version)
        snprintf(tar_cmd, sizeof(tar_cmd), "tar -xvzf zeppelin-%s-bin-all.tgz >/dev/null 2>&1", version);
    else
        snprintf(tar_cmd, sizeof(tar_cmd), "tar -xvzf zeppelin-0.10.1-bin-all.tgz >/dev/null 2>&1");
    if (!executeSystemCommand(tar_cmd)) {
        fprintf(stderr,   "Extraction failed. Corrupted download?\n");
        exit(EXIT_FAILURE);
    }
    // Remove downloaded archive after extraction
    // printf( "Removing archive...\n");
    if (version)
        snprintf(tar_cmd, sizeof(tar_cmd), "sudo rm -f zeppelin-%s-bin-all.tgz >/dev/null 2>&1", version);
    else
        snprintf(tar_cmd, sizeof(tar_cmd), "sudo rm -f zeppelin-0.10.1-bin-all.tgz >/dev/null 2>&1");
    if (!executeSystemCommand(tar_cmd)) {
        fprintf(stderr,   "Failed to remove archive.\n");
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
            fprintf(stderr,   "Unsupported Linux distribution\n");
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
    snprintf(mv_cmd, sizeof(mv_cmd), "sudo mv %s %s >/dev/null 2>&1", dir_name, target_dir);
    if (!executeSystemCommand(mv_cmd)) {
        fprintf(stderr,   "Failed to move installation directory\n");
        exit(EXIT_FAILURE);
    }
    // Adjust ownership of install_dir to current user
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        fprintf(stderr,   "Error: Could not determine current user\n");
        return;
    }
    char chown_cmd[512];
    snprintf(chown_cmd, sizeof(chown_cmd), "sudo chown -R %s:%s %s >/dev/null 2>&1", pwd->pw_name, pwd->pw_name, target_dir);
    if (!executeSystemCommand(chown_cmd)) {
        fprintf(stderr,   "Error: Failed to set ownership of %s\n", target_dir);
        return;
    }

    // Step 6: Set ZEPPELIN_HOME in .bashrc
    const char *home = getenv("HOME");
    if (!home) {
        fprintf(stderr,   "Could not determine home directory\n");
        exit(EXIT_FAILURE);
    }

    char bashrc_path[256];
    snprintf(bashrc_path, sizeof(bashrc_path), "%s/.bashrc", home);

    FILE *bashrc = fopen(bashrc_path, "a");
    if (!bashrc) {
        perror("Failed to update .bashrc");
        exit(EXIT_FAILURE);
    }

    fprintf(bashrc, "\nexport ZEPPELIN_HOME=%s\n", target_dir);
    fclose(bashrc);

    // Step 7: Source the updated .bashrc
    if (sourceBashrc() != 0) {
        fprintf(stderr,   "bashing failed.\n");
        return;
    }

    char candidate_path[PATH_MAX];
    snprintf(candidate_path, sizeof(candidate_path), "%s/config", target_dir);



    if (create_properties_file(candidate_path, "zeppelin-shiro.ini") !=0)
        fprintf(stderr,    "Failed to create properties file\n");
    printf( "Apache Zeppelin installed successfully at %s\n", target_dir);
}


void install_Livy(char* version, char *location) {
    char command[1024];
    char* install_dir;
    char* home_dir = getenv("HOME");

    // Validate home directory
    if (!home_dir) {
        fprintf(stderr,   "Error: Unable to determine home directory\n");
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
    snprintf(command, sizeof(command), "wget -q %s >/dev/null 2>&1", url);
    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "Error downloading Livy archive\n");
        return;
    }

    // 3. Extract the archive
    char zip_file[256];
    if (version)
        snprintf(zip_file, sizeof(zip_file), "apache-livy-%s-incubating-bin.zip", version);
    else
        snprintf(zip_file, sizeof(zip_file), "apache-livy-0.7.1-incubating-bin.zip");

    snprintf(command, sizeof(command), "unzip -q %s >/dev/null 2>&1", zip_file);
    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "Error extracting archive\n");
        return;
    }
    // Remove downloaded archive after extraction
    printf( "Removing archive...\n");
    if (version)
        snprintf(command, sizeof(command), "sudo rm -f apache-livy-%s-incubating-bin.zip >/dev/null 2>&1", version);
    else
        snprintf(command, sizeof(command), "sudo rm -f apache-livy-0.7.1-incubating-bin.zip >/dev/null 2>&1");
    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "Failed to remove archive.\n");
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

    snprintf(command, sizeof(command), "sudo mv %s %s/livy && sudo chmod -R 755 %s/livy >/dev/null 2>&1",
             source_dir, install_dir, install_dir);
    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "Error moving Livy to installation directory\n");
        return;
    }

    // Adjust ownership of install_dir to current user
    struct passwd *pwd = getpwuid(getuid());
    if (!pwd) {
        fprintf(stderr,   "Error: Could not determine current user\n");
        return;
    }
    char chown_cmd[512];
    snprintf(chown_cmd, sizeof(chown_cmd), "sudo chown -R %s:%s %s >/dev/null 2>&1", pwd->pw_name, pwd->pw_name, install_dir);
    if (!executeSystemCommand(chown_cmd)) {
        fprintf(stderr,   "Error: Failed to set ownership of %s\n", install_dir);
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
        fprintf(stderr,   "Error updating .bashrc file\n");
        return;
    }

    // 7. Update current session environment
    setenv("LIVY_HOME", install_dir , 1);

    char path_env[1024];
    snprintf(path_env, sizeof(path_env), "%s:%s/livy/bin", getenv("PATH"), install_dir);
    setenv("PATH", path_env, 1);

    // 8. Cleanup temporary files
    snprintf(command, sizeof(command), "rm %s >/dev/null 2>&1", zip_file);

    if (!executeSystemCommand(command)) {
        fprintf(stderr,   "rm  faild\n");
        return;
    }

    if (sourceBashrc() != 0) {
        fprintf(stderr,   "bashing failed.\n");
        return;
    }

    char candidate_path[PATH_MAX];
    snprintf(candidate_path, sizeof(candidate_path), "%s/conf", install_dir);

    if (create_conf_file(candidate_path, "livy.conf") !=0)
        fprintf(stderr,   "Failed to create XML file\n");

    printf( "Livy  installed successfully to %s/livy\n", install_dir);
}
