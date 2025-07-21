#include "utiles.h"

ConfigParam flink_params[] = {
    // Core Configuration
    { "jobmanager.rpc.address", "^jobmanager[._-]rpc[._-]address$", "config.yaml" },
    { "jobmanager.rpc.port", "^jobmanager[._-]rpc[._-]port$", "config.yaml" },
    { "jobmanager.heap.size", "^jobmanager[._-]heap[._-]size$", "config.yaml" },
    { "taskmanager.heap.size", "^taskmanager[._-]heap[._-]size$", "config.yaml" },
    { "taskmanager.numberOfTaskSlots", "^taskmanager[._-]numberOfTaskSlots$", "config.yaml" },
    { "parallelism.default", "^parallelism[._-]default$", "config.yaml" },
    { "io.tmp.dirs", "^io[._-]tmp[._-]dirs$", "config.yaml" },
    { "classloader.resolve-order", "^classloader[._-]resolve[._-]order$", "config.yaml" },

    // State Backend & Checkpointing
    { "state.backend", "^state[._-]backend$", "config.yaml" },
    { "state.checkpoints.dir", "^state[._-]checkpoints[._-]dir$", "config.yaml" },
    { "state.savepoints.dir", "^state[._-]savepoints[._-]dir$", "config.yaml" },
    { "checkpoint.interval", "^checkpoint[._-]interval$", "config.yaml" },
    { "execution.checkpointing.interval", "^execution[._-]checkpointing[._-]interval$", "config.yaml" },
    { "state.backend.incremental", "^state[._-]backend[._-]incremental$", "config.yaml" },
    { "state.backend.async", "^state[._-]backend[._-]async$", "config.yaml" },
    { "state.backend.rocksdb.ttl-compaction-filter.enabled", "^state[._-]backend[._-]rocksdb[._-]ttl[._-]compaction[._-]filter[._-]enabled$", "config.yaml" },

    // Rest & Web UI
    { "rest.port", "^rest[._-]port$", "config.yaml" },
    { "rest.address", "^rest[._-]address$", "config.yaml" },
    { "web.timeout", "^web[._-]timeout$", "config.yaml" },
    { "web.submit.enable", "^web[._-]submit[._-]enable$", "config.yaml" },
    { "web.upload.dir", "^web[._-]upload[._-]dir$", "config.yaml" },
    { "web.access-control-allow-origin", "^web[._-]access[._-]control[._-]allow[._-]origin$", "config.yaml" },

    // High Availability
    { "high-availability", "^high[._-]availability$", "config.yaml" },
    { "high-availability.storageDir", "^high[._-]availability[._-]storageDir$", "config.yaml" },
    { "high-availability.zookeeper.quorum", "^high[._-]availability[._-]zookeeper[._-]quorum$", "config.yaml" },
    { "high-availability.zookeeper.path.root", "^high[._-]availability[._-]zookeeper[._-]path[._-]root$", "config.yaml" },
    { "high-availability.cluster-id", "^high[._-]availability[._-]cluster[._-]id$", "config.yaml" },

    // Security
    { "security.ssl.enabled", "^security[._-]ssl[._-]enabled$", "config.yaml" },
    { "security.ssl.keystore", "^security[._-]ssl[._-]keystore$", "config.yaml" },
    { "security.ssl.keystore-password", "^security[._-]ssl[._-]keystore[._-]password$", "config.yaml" },
    { "security.ssl.key-password", "^security[._-]ssl[._-]key[._-]password$", "config.yaml" },
    { "security.ssl.truststore", "^security[._-]ssl[._-]truststore$", "config.yaml" },
    { "security.ssl.truststore-password", "^security[._-]ssl[._-]truststore[._-]password$", "config.yaml" },
    { "security.kerberos.login.keytab", "^security[._-]kerberos[._-]login[._-]keytab$", "config.yaml" },

    // Network & Communication
    { "taskmanager.data.port", "^taskmanager[._-]data[._-]port$", "config.yaml" },
    { "taskmanager.data.ssl.enabled", "^taskmanager[._-]data[._-]ssl[._-]enabled$", "config.yaml" },
    { "blob.server.port", "^blob[._-]server[._-]port$", "config.yaml" },
    { "queryable-state.proxy.ports", "^queryable[._-]state[._-]proxy[._-]ports$", "config.yaml" },
    { "akka.ask.timeout", "^akka[._-]ask[._-]timeout$", "config.yaml" },
    { "akka.framesize", "^akka[._-]framesize$", "config.yaml" },

    // Memory Management
    { "taskmanager.memory.framework.heap.size", "^taskmanager[._-]memory[._-]framework[._-]heap[._-]size$", "config.yaml" },
    { "taskmanager.memory.network.min", "^taskmanager[._-]memory[._-]network[._-]min$", "config.yaml" },
    { "taskmanager.memory.managed.size", "^taskmanager[._-]memory[._-]managed[._-]size$", "config.yaml" },
    { "taskmanager.memory.managed.fraction", "^taskmanager[._-]memory[._-]managed[._-]fraction$", "config.yaml" },
    { "jobmanager.memory.off-heap.size", "^jobmanager[._-]memory[._-]off[._-]heap[._-]size$", "config.yaml" },

    // YARN Deployment
    { "yarn.application.name", "^yarn[._-]application[._-]name$", "config.yaml" },
    { "yarn.application.queue", "^yarn[._-]application[._-]queue$", "config.yaml" },
    { "yarn.containers.vcores", "^yarn[._-]containers[._-]vcores$", "config.yaml" },
    { "yarn.containers.memory", "^yarn[._-]containers[._-]memory$", "config.yaml" },
    { "yarn.ship-files", "^yarn[._-]ship[._-]files$", "config.yaml" },

    // Kubernetes Deployment
    { "kubernetes.cluster-id", "^kubernetes[._-]cluster[._-]id$", "config.yaml" },
    { "kubernetes.namespace", "^kubernetes[._-]namespace$", "config.yaml" },
    { "kubernetes.service.account", "^kubernetes[._-]service[._-]account$", "config.yaml" },
    { "kubernetes.container.image", "^kubernetes[._-]container[._-]image$", "config.yaml" },

    // Mesos Deployment
    { "mesos.resourcemanager.tasks.cpus", "^mesos[._-]resourcemanager[._-]tasks[._-]cpus$", "config.yaml" },
    { "mesos.resourcemanager.tasks.mem", "^mesos[._-]resourcemanager[._-]tasks[._-]mem$", "config.yaml" },

    // Failover & Recovery
    { "jobmanager.execution.failover-strategy", "^jobmanager[._-]execution[._-]failover[._-]strategy$", "config.yaml" },
    { "restart-strategy", "^restart[._-]strategy$", "config.yaml" },
    { "restart-strategy.fixed-delay.attempts", "^restart[._-]strategy[._-]fixed[._-]delay[._-]attempts$", "config.yaml" },

    // Metrics & Monitoring
    { "metrics.reporter.prom.class", "^metrics[._-]reporter[._-]prom[._-]class$", "config.yaml" },
    { "metrics.reporter.prom.port", "^metrics[._-]reporter[._-]prom[._-]port$", "config.yaml" },
    { "metrics.system-resource", "^metrics[._-]system[._-]resource$", "config.yaml" },
    // Parameters from log4j-cli.properties (28 entries)
    { "monitorInterval", "^monitorInterval$", "log4j-cli.properties" },
    { "rootLogger.level", "^rootLogger[._-]level$", "log4j-cli.properties" },
    { "rootLogger.appenderRef.file.ref", "^rootLogger[._-]appenderRef[._-]file[._-]ref$", "log4j-cli.properties" },
    { "appender.file.name", "^appender[._-]file[._-]name$", "log4j-cli.properties" },
    { "appender.file.type", "^appender[._-]file[._-]type$", "log4j-cli.properties" },
    { "appender.file.append", "^appender[._-]file[._-]append$", "log4j-cli.properties" },
    { "appender.file.fileName", "^appender[._-]file[._-]fileName$", "log4j-cli.properties" },
    { "appender.file.layout.type", "^appender[._-]file[._-]layout[._-]type$", "log4j-cli.properties" },
    { "appender.file.layout.pattern", "^appender[._-]file[._-]layout[._-]pattern$", "log4j-cli.properties" },
    { "logger.yarn.name", "^logger[._-]yarn[._-]name$", "log4j-cli.properties" },
    { "logger.yarn.level", "^logger[._-]yarn[._-]level$", "log4j-cli.properties" },
    { "logger.yarn.appenderRef.console.ref", "^logger[._-]yarn[._-]appenderRef[._-]console[._-]ref$", "log4j-cli.properties" },
    { "logger.yarncli.name", "^logger[._-]yarncli[._-]name$", "log4j-cli.properties" },
    { "logger.yarncli.level", "^logger[._-]yarncli[._-]level$", "log4j-cli.properties" },
    { "logger.yarncli.appenderRef.console.ref", "^logger[._-]yarncli[._-]appenderRef[._-]console[._-]ref$", "log4j-cli.properties" },
    { "logger.hadoop.name", "^logger[._-]hadoop[._-]name$", "log4j-cli.properties" },
    { "logger.hadoop.level", "^logger[._-]hadoop[._-]level$", "log4j-cli.properties" },
    { "logger.hadoop.appenderRef.console.ref", "^logger[._-]hadoop[._-]appenderRef[._-]console[._-]ref$", "log4j-cli.properties" },
    { "logger.hive.name", "^logger[._-]hive[._-]name$", "log4j-cli.properties" },
    { "logger.hive.level", "^logger[._-]hive[._-]level$", "log4j-cli.properties" },
    { "logger.hive.additivity", "^logger[._-]hive[._-]additivity$", "log4j-cli.properties" },
    { "logger.hive.appenderRef.file.ref", "^logger[._-]hive[._-]appenderRef[._-]file[._-]ref$", "log4j-cli.properties" },
    { "logger.kubernetes.name", "^logger[._-]kubernetes[._-]name$", "log4j-cli.properties" },
    { "logger.kubernetes.level", "^logger[._-]kubernetes[._-]level$", "log4j-cli.properties" },
    { "logger.kubernetes.appenderRef.console.ref", "^logger[._-]kubernetes[._-]appenderRef[._-]console[._-]ref$", "log4j-cli.properties" },
    { "appender.console.name", "^appender[._-]console[._-]name$", "log4j-cli.properties" },
    { "appender.console.type", "^appender[._-]console[._-]type$", "log4j-cli.properties" },
    { "appender.console.layout.type", "^appender[._-]console[._-]layout[._-]type$", "log4j-cli.properties" },
    { "appender.console.layout.pattern", "^appender[._-]console[._-]layout[._-]pattern$", "log4j-cli.properties" },
    { "logger.hadoopnative.name", "^logger[._-]hadoopnative[._-]name$", "log4j-cli.properties" },
    { "logger.hadoopnative.level", "^logger[._-]hadoopnative[._-]level$", "log4j-cli.properties" },
    { "logger.netty.name", "^logger[._-]netty[._-]name$", "log4j-cli.properties" },
    { "logger.netty.level", "^logger[._-]netty[._-]level$", "log4j-cli.properties" },

    // New parameters from log4j-console.properties (35 entries)
    { "monitorInterval", "^monitorInterval$", "log4j-console.properties" },
    { "rootLogger.level", "^rootLogger[._-]level$", "log4j-console.properties" },
    { "rootLogger.appenderRef.console.ref", "^rootLogger[._-]appenderRef[._-]console[._-]ref$", "log4j-console.properties" },
    { "rootLogger.appenderRef.rolling.ref", "^rootLogger[._-]appenderRef[._-]rolling[._-]ref$", "log4j-console.properties" },
    { "appender.console.name", "^appender[._-]console[._-]name$", "log4j-console.properties" },
    { "appender.console.type", "^appender[._-]console[._-]type$", "log4j-console.properties" },
    { "appender.console.layout.type", "^appender[._-]console[._-]layout[._-]type$", "log4j-console.properties" },
    { "appender.console.layout.pattern", "^appender[._-]console[._-]layout[._-]pattern$", "log4j-console.properties" },
    { "appender.console.filter.threshold.type", "^appender[._-]console[._-]filter[._-]threshold[._-]type$", "log4j-console.properties" },
    { "appender.console.filter.threshold.level", "^appender[._-]console[._-]filter[._-]threshold[._-]level$", "log4j-console.properties" },
    { "appender.rolling.name", "^appender[._-]rolling[._-]name$", "log4j-console.properties" },
    { "appender.rolling.type", "^appender[._-]rolling[._-]type$", "log4j-console.properties" },
    { "appender.rolling.append", "^appender[._-]rolling[._-]append$", "log4j-console.properties" },
    { "appender.rolling.fileName", "^appender[._-]rolling[._-]fileName$", "log4j-console.properties" },
    { "appender.rolling.filePattern", "^appender[._-]rolling[._-]filePattern$", "log4j-console.properties" },
    { "appender.rolling.layout.type", "^appender[._-]rolling[._-]layout[._-]type$", "log4j-console.properties" },
    { "appender.rolling.layout.pattern", "^appender[._-]rolling[._-]layout[._-]pattern$", "log4j-console.properties" },
    { "appender.rolling.policies.type", "^appender[._-]rolling[._-]policies[._-]type$", "log4j-console.properties" },
    { "appender.rolling.policies.size.type", "^appender[._-]rolling[._-]policies[._-]size[._-]type$", "log4j-console.properties" },
    { "appender.rolling.policies.size.size", "^appender[._-]rolling[._-]policies[._-]size[._-]size$", "log4j-console.properties" },
    { "appender.rolling.policies.startup.type", "^appender[._-]rolling[._-]policies[._-]startup[._-]type$", "log4j-console.properties" },
    { "appender.rolling.strategy.type", "^appender[._-]rolling[._-]strategy[._-]type$", "log4j-console.properties" },
    { "appender.rolling.strategy.max", "^appender[._-]rolling[._-]strategy[._-]max$", "log4j-console.properties" },
    { "logger.pekko.name", "^logger[._-]pekko[._-]name$", "log4j-console.properties" },
    { "logger.pekko.level", "^logger[._-]pekko[._-]level$", "log4j-console.properties" },
    { "logger.kafka.name", "^logger[._-]kafka[._-]name$", "log4j-console.properties" },
    { "logger.kafka.level", "^logger[._-]kafka[._-]level$", "log4j-console.properties" },
    { "logger.hadoop.name", "^logger[._-]hadoop[._-]name$", "log4j-console.properties" },
    { "logger.hadoop.level", "^logger[._-]hadoop[._-]level$", "log4j-console.properties" },
    { "logger.zookeeper.name", "^logger[._-]zookeeper[._-]name$", "log4j-console.properties" },
    { "logger.zookeeper.level", "^logger[._-]zookeeper[._-]level$", "log4j-console.properties" },
    { "logger.shaded_zookeeper.name", "^logger[._-]shaded_zookeeper[._-]name$", "log4j-console.properties" },
    { "logger.shaded_zookeeper.level", "^logger[._-]shaded_zookeeper[._-]level$", "log4j-console.properties" },
    { "logger.netty.name", "^logger[._-]netty[._-]name$", "log4j-console.properties" },
    { "logger.netty.level", "^logger[._-]netty[._-]level$", "log4j-console.properties" },
    { "monitorInterval", "^monitorInterval$", "log4j.properties" },
    { "rootLogger.level", "^rootLogger[._-]level$", "log4j.properties" },
    { "rootLogger.appenderRef.file.ref", "^rootLogger[._-]appenderRef[._-]file[._-]ref$", "log4j.properties" },
    { "logger.pekko.name", "^logger[._-]pekko[._-]name$", "log4j.properties" },
    { "logger.pekko.level", "^logger[._-]pekko[._-]level$", "log4j.properties" },
    { "logger.kafka.name", "^logger[._-]kafka[._-]name$", "log4j.properties" },
    { "logger.kafka.level", "^logger[._-]kafka[._-]level$", "log4j.properties" },
    { "logger.hadoop.name", "^logger[._-]hadoop[._-]name$", "log4j.properties" },
    { "logger.hadoop.level", "^logger[._-]hadoop[._-]level$", "log4j.properties" },
    { "logger.zookeeper.name", "^logger[._-]zookeeper[._-]name$", "log4j.properties" },
    { "logger.zookeeper.level", "^logger[._-]zookeeper[._-]level$", "log4j.properties" },
    { "logger.shaded_zookeeper.name", "^logger[._-]shaded_zookeeper[._-]name$", "log4j.properties" },
    { "logger.shaded_zookeeper.level", "^logger[._-]shaded_zookeeper[._-]level$", "log4j.properties" },
    { "appender.main.name", "^appender[._-]main[._-]name$", "log4j.properties" },
    { "appender.main.type", "^appender[._-]main[._-]type$", "log4j.properties" },
    { "appender.main.append", "^appender[._-]main[._-]append$", "log4j.properties" },
    { "appender.main.fileName", "^appender[._-]main[._-]fileName$", "log4j.properties" },
    { "appender.main.filePattern", "^appender[._-]main[._-]filePattern$", "log4j.properties" },
    { "appender.main.layout.type", "^appender[._-]main[._-]layout[._-]type$", "log4j.properties" },
    { "appender.main.layout.pattern", "^appender[._-]main[._-]layout[._-]pattern$", "log4j.properties" },
    { "appender.main.policies.type", "^appender[._-]main[._-]policies[._-]type$", "log4j.properties" },
    { "appender.main.policies.size.type", "^appender[._-]main[._-]policies[._-]size[._-]type$", "log4j.properties" },
    { "appender.main.policies.size.size", "^appender[._-]main[._-]policies[._-]size[._-]size$", "log4j.properties" },
    { "appender.main.policies.startup.type", "^appender[._-]main[._-]policies[._-]startup[._-]type$", "log4j.properties" },
    { "appender.main.strategy.type", "^appender[._-]main[._-]strategy[._-]type$", "log4j.properties" },
    { "appender.main.strategy.max", "^appender[._-]main[._-]strategy[._-]max$", "log4j.properties" },
    { "logger.netty.name", "^logger[._-]netty[._-]name$", "log4j.properties" },
    { "logger.netty.level", "^logger[._-]netty[._-]level$", "log4j.properties" },
    { "monitorInterval", "^monitorInterval$", "log4j-session.properties" },
    { "rootLogger.level", "^rootLogger[._-]level$", "log4j-session.properties" },
    { "rootLogger.appenderRef.console.ref", "^rootLogger[._-]appenderRef[._-]console[._-]ref$", "log4j-session.properties" },
    { "appender.console.name", "^appender[._-]console[._-]name$", "log4j-session.properties" },
    { "appender.console.type", "^appender[._-]console[._-]type$", "log4j-session.properties" },
    { "appender.console.layout.type", "^appender[._-]console[._-]layout[._-]type$", "log4j-session.properties" },
    { "appender.console.layout.pattern", "^appender[._-]console[._-]layout[._-]pattern$", "log4j-session.properties" },
    { "logger.netty.name", "^logger[._-]netty[._-]name$", "log4j-session.properties" },
    { "logger.netty.level", "^logger[._-]netty[._-]level$", "log4j-session.properties" },
    { "logger.zookeeper.name", "^logger[._-]zookeeper[._-]name$", "log4j-session.properties" },
    { "logger.zookeeper.level", "^logger[._-]zookeeper[._-]level$", "log4j-session.properties" },
    { "logger.shaded_zookeeper.name", "^logger[._-]shaded_zookeeper[._-]name$", "log4j-session.properties" },
    { "logger.shaded_zookeeper.level", "^logger[._-]shaded_zookeeper[._-]level$", "log4j-session.properties" },
    { "logger.curator.name", "^logger[._-]curator[._-]name$", "log4j-session.properties" },
    { "logger.curator.level", "^logger[._-]curator[._-]level$", "log4j-session.properties" },
    { "logger.runtimeutils.name", "^logger[._-]runtimeutils[._-]name$", "log4j-session.properties" },
    { "logger.runtimeutils.level", "^logger[._-]runtimeutils[._-]level$", "log4j-session.properties" },
    { "logger.runtimeleader.name", "^logger[._-]runtimeleader[._-]name$", "log4j-session.properties" },
    { "logger.runtimeleader.level", "^logger[._-]runtimeleader[._-]level$", "log4j-session.properties" },

};

ValidationResult validateFlinkConfigParam(const char *param_name, const char *value) {
    // Check if parameter exists
    bool param_exists = false;
    for (size_t i = 0; i < sizeof(flink_params)/sizeof(flink_params[0]); i++) {
        if (strcmp(param_name, flink_params[i].canonicalName) == 0) {
            param_exists = true;
            break;
        }
    }
    if (!param_exists) return ERROR_PARAM_NOT_FOUND;

    // Check value presence
    if (!value || strlen(value) == 0) return ERROR_VALUE_EMPTY;

    // Parameter-specific validation
    if (strcmp(param_name, "jobmanager.rpc.port") == 0 ||
        strcmp(param_name, "rest.port") == 0 ||
        strcmp(param_name, "blob.server.port") == 0 ||
        strcmp(param_name, "metrics.reporter.prom.port") == 0) {
        return isValidPort(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "jobmanager.heap.size") == 0 ||
             strcmp(param_name, "taskmanager.heap.size") == 0 ||
             strcmp(param_name, "taskmanager.memory.framework.heap.size") == 0 ||
             strcmp(param_name, "taskmanager.memory.managed.size") == 0 ||
             strcmp(param_name, "jobmanager.memory.off-heap.size") == 0) {
        return isMemorySize(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "state.backend") == 0) {
        const char *valid[] = {"rocksdb", "filesystem", "hashmap", NULL};
        for (int i = 0; valid[i]; i++)
            if (strcmp(value, valid[i]) == 0) return VALIDATION_OK;
        return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "taskmanager.numberOfTaskSlots") == 0 ||
             strcmp(param_name, "parallelism.default") == 0 ||
             strcmp(param_name, "restart-strategy.fixed-delay.attempts") == 0 ||
             strcmp(param_name, "yarn.containers.vcores") == 0) {
        return isPositiveInteger(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "checkpoint.interval") == 0 ||
             strcmp(param_name, "execution.checkpointing.interval") == 0 ||
             strcmp(param_name, "web.timeout") == 0 ||
             strcmp(param_name, "akka.ask.timeout") == 0) {
        return isValidDuration(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "io.tmp.dirs") == 0 ||
             strcmp(param_name, "state.checkpoints.dir") == 0 ||
             strcmp(param_name, "state.savepoints.dir") == 0 ||
             strcmp(param_name, "web.upload.dir") == 0) {
        return (value[0] != '\0') ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "high-availability.zookeeper.quorum") == 0) {
        char *copy = strdup(value);
        char *token = strtok(copy, ",");
        while (token) {
            if (!isHostPortPair(token)) {
                free(copy);
                return ERROR_INVALID_FORMAT;
            }
            token = strtok(NULL, ",");
        }
        free(copy);
    }
    else if (strcmp(param_name, "taskmanager.memory.managed.fraction") == 0) {
        return isFraction(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "security.ssl.enabled") == 0 ||
             strcmp(param_name, "state.backend.incremental") == 0 ||
             strcmp(param_name, "state.backend.async") == 0 ||
             strcmp(param_name, "web.submit.enable") == 0 ||
             strcmp(param_name, "taskmanager.data.ssl.enabled") == 0) {
        return isValidBoolean(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "restart-strategy") == 0) {
        const char *valid[] = {"fixed-delay", "failure-rate", "none", NULL};
        for (int i = 0; valid[i]; i++)
            if (strcmp(value, valid[i]) == 0) return VALIDATION_OK;
        return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "high-availability") == 0) {
        return strcmp(value, "NONE") == 0 || strcmp(value, "ZOOKEEPER") == 0
            ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "akka.framesize") == 0) {
        char *end;
        strtol(value, &end, 10);
        if (*end != '\0' && !(end[0] == 'b' || end[0] == 'k' || end[0] == 'm' || end[0] == 'g'))
            return ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "security.ssl.keystore") == 0 ||
             strcmp(param_name, "security.ssl.truststore") == 0 ||
             strcmp(param_name, "security.kerberos.login.keytab") == 0) {
        return (strlen(value) > 0) ? VALIDATION_OK : ERROR_VALUE_EMPTY;
    }
    else if (strcmp(param_name, "kubernetes.namespace") == 0 ||
             strcmp(param_name, "yarn.application.queue") == 0) {
        return (strlen(value) > 0) ? VALIDATION_OK : ERROR_VALUE_EMPTY;
    }

    return VALIDATION_OK;
}


ConfigResult* set_flink_config(const char *param, const char *value) {

    if (!param || !value) return NULL;

    for (size_t i = 0; i < sizeof(flink_params)/sizeof(flink_params[0]); i++) {
        regex_t regex;
        int reti = regcomp(&regex, flink_params[i].normalizedName, REG_EXTENDED | REG_NOSUB);
        if (reti) {
            // Handle regex compilation error if needed
            continue;
        }

        reti = regexec(&regex, param, 0, NULL, 0);
        regfree(&regex); // Always free compiled regex

        if (reti == 0) { // Match found
            ConfigResult *result = malloc(sizeof(ConfigResult));
            if (!result) return NULL;

            result->canonical_name = strdup(flink_params[i].canonicalName);
            result->value = strdup(value);
            result->config_file = strdup(flink_params[i].configFile);

            if (!result->canonical_name || !result->value || !result->config_file) {
                free(result->canonical_name);
                free(result->value);
                free(result->config_file);
                free(result);
                return NULL;
            }
            return result;
        }
    }

    return NULL;
}
// Remember to free the returned FlinkConfigResult and its members after use.
void free_flink_config_result(ConfigResult *result) {
    if (result) {
        free(result->canonical_name);
        free(result->value);
        free(result->config_file);
        free(result);
    }
}

int find_flink_config(char *config_path, const char *config_filename) {
    // Check FLINK_HOME environment variable
    char *flink_home = getenv("FLINK_HOME");
    if (flink_home != NULL) {
        snprintf(config_path, PATH_MAX, "%s/conf/%s", flink_home, config_filename);
        if (access(config_path, F_OK) == 0) {
            return 1;
        }
    }

    // Common Flink configuration directories
    const char *conf_dirs[] = {
        "/etc/flink/conf",
        "/usr/local/flink/conf",
        "/usr/share/flink/conf",
        "/opt/flink/conf",
        NULL
    };

    for (int i = 0; conf_dirs[i] != NULL; i++) {
        snprintf(config_path, PATH_MAX, "%s/%s", conf_dirs[i], config_filename);
        if (access(config_path, F_OK) == 0) {
            return 1;
        }
    }

    return 0;
}

ConfigStatus update_flink_config(const char *param, const char *value , const char *filename) {
    char config_path[PATH_MAX];
    if (!find_flink_config(config_path, filename)) {
        return FILE_NOT_FOUND; // Config file not found
    }

    if (strcmp(filename, "log4j-cli.properties") == 0 ||
        strcmp(filename, "log4j-console.properties") == 0 ||
        strcmp(filename, "log4j-session.properties") == 0 ||
        strcmp(filename, "log4j.properties")) {
        configure_hadoop_property(config_path, param, value);
        return SUCCESS;
    }
    FILE *file = fopen(config_path, "r");
    if (!file) {
        return FILE_READ_ERROR; // Failed to open file for reading
    }

    char **lines = NULL;
    size_t line_count = 0;
    char buffer[1024];
    int param_found = 0;

    // Read and process lines
    while (fgets(buffer, sizeof(buffer), file) != NULL) {
        trim_whitespace(buffer);
        size_t param_len = strlen(param);

        // Check if line starts with the parameter and has a colon immediately after
        if (strncmp(buffer, param, param_len) == 0 && buffer[param_len] == ':') {
            char new_line[1024];
            snprintf(new_line, sizeof(new_line), "%s: %s\n", param, value);

            // Allocate space for new line
            char **temp = realloc(lines, (line_count + 1) * sizeof(char *));
            if (!temp) {
                fclose(file);
                return SAVE_FAILED; // Memory allocation failed
            }
            lines = temp;

            lines[line_count] = strdup(new_line);
            if (!lines[line_count]) {
                fclose(file);
                free(lines);
                return SAVE_FAILED;
            }
            line_count++;
            param_found = 1;
        } else {
            // Keep original line
            char **temp = realloc(lines, (line_count + 1) * sizeof(char *));
            if (!temp) {
                fclose(file);
                return SAVE_FAILED;
            }
            lines = temp;

            lines[line_count] = strdup(buffer);
            if (!lines[line_count]) {
                fclose(file);
                free(lines);
                return SAVE_FAILED;
            }
            line_count++;
        }
    }
    fclose(file);

    // Add new parameter if not found
    if (!param_found) {
        char new_line[1024];
        snprintf(new_line, sizeof(new_line), "%s: %s\n", param, value);

        char **temp = realloc(lines, (line_count + 1) * sizeof(char *));
        if (!temp) return SAVE_FAILED;
        lines = temp;

        lines[line_count] = strdup(new_line);
        if (!lines[line_count]) {
            for (size_t i = 0; i < line_count; i++) free(lines[i]);
            free(lines);
            return SAVE_FAILED;
        }
        line_count++;
    }

    // Write to file
    file = fopen(config_path, "w");
    if (!file) {
        for (size_t i = 0; i < line_count; i++) free(lines[i]);
        free(lines);
        return FILE_WRITE_ERROR; // Failed to open for writing
    }

    for (size_t i = 0; i < line_count; i++) {
        if (fputs(lines[i], file) == EOF) {
            fclose(file);
            for (size_t j = 0; j < line_count; j++) free(lines[j]);
            free(lines);
            return FILE_WRITE_ERROR; // Write error
        }
    }

    // Cleanup
    fclose(file);
    for (size_t i = 0; i < line_count; i++) free(lines[i]);
    free(lines);

    return SUCCESS; // Success/
}

