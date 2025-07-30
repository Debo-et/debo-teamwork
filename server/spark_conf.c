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


#include "utiles.h"


// Predefined list of Spark configuration parameters with regex patterns and config files
ConfigParam spark_param_configs[] = {
    // Original configurations
    { "spark.master", "^(spark[._-])?master$", "spark-defaults.conf" },
    { "spark.app.name", "^(spark[._-])?app[._-]name$", "spark-defaults.conf" },
    { "spark.executor.memory", "^(spark[._-])?executor[._-]memory$", "spark-defaults.conf" },
    { "spark.driver.memory", "^(spark[._-])?driver[._-]memory$", "spark-defaults.conf" },
    { "spark.serializer", "^(spark[._-])?serializer$", "spark-defaults.conf" },
    { "spark.sql.shuffle.partitions", "^(spark[._-])?sql[._-]shuffle[._-]partitions$", "spark-defaults.conf" },
    { "spark.default.parallelism", "^(spark[._-])?default[._-]parallelism$", "spark-defaults.conf" },
    { "spark.executor.cores", "^(spark[._-])?executor[._-]cores$", "spark-defaults.conf" },
    { "spark.shuffle.service.enabled", "^(spark[._-])?shuffle[._-]service[._-]enabled$", "spark-defaults.conf" },
    { "spark.dynamicAllocation.enabled", "^(spark[._-])?dynamicAllocation[._-]enabled$", "spark-defaults.conf" },
    { "spark.eventLog.enabled", "^(spark[._-])?eventLog[._-]enabled$", "spark-defaults.conf" },
    { "spark.yarn.queue", "^(spark[._-])?yarn[._-]queue$", "spark-defaults.conf" },
    { "spark.submit.deployMode", "^(spark[._-])?submit[._-]deployMode$", "spark-defaults.conf" },
    { "spark.network.timeout", "^(spark[._-])?network[._-]timeout$", "spark-defaults.conf" },
    { "spark.ui.port", "^(spark[._-])?ui[._-]port$", "spark-defaults.conf" },
    { "spark.driver.maxResultSize", "^(spark[._-])?driver[._-]maxResultSize$", "spark-defaults.conf" },
    { "spark.executor.instances", "^(spark[._-])?executor[._-]instances$", "spark-defaults.conf" },
    { "spark.sql.autoBroadcastJoinThreshold", "^(spark[._-])?sql[._-]autoBroadcastJoinThreshold$", "spark-defaults.conf" },
    { "spark.memory.fraction", "^(spark[._-])?memory[._-]fraction$", "spark-defaults.conf" },
    { "spark.locality.wait", "^(spark[._-])?locality[._-]wait$", "spark-defaults.conf" },

    // Additional comprehensive configurations
    { "spark.driver.cores", "^(spark[._-])?driver[._-]cores$", "spark-defaults.conf" },
    { "spark.memory.offHeap.enabled", "^(spark[._-])?memory[._-]offHeap[._-]enabled$", "spark-defaults.conf" },
    { "spark.memory.offHeap.size", "^(spark[._-])?memory[._-]offHeap[._-]size$", "spark-defaults.conf" },
    { "spark.executor.memoryOverhead", "^(spark[._-])?executor[._-]memoryOverhead$", "spark-defaults.conf" },
    { "spark.driver.memoryOverhead", "^(spark[._-])?driver[._-]memoryOverhead$", "spark-defaults.conf" },
    { "spark.shuffle.compress", "^(spark[._-])?shuffle[._-]compress$", "spark-defaults.conf" },
    { "spark.shuffle.spill.compress", "^(spark[._-])?shuffle[._-]spill[._-]compress$", "spark-defaults.conf" },
    { "spark.io.compression.codec", "^(spark[._-])?io[._-]compression[._-]codec$", "spark-defaults.conf" },
    { "spark.shuffle.file.buffer", "^(spark[._-])?shuffle[._-]file[._-]buffer$", "spark-defaults.conf" },
    { "spark.reducer.maxSizeInFlight", "^(spark[._-])?reducer[._-]maxSizeInFlight$", "spark-defaults.conf" },
    { "spark.dynamicAllocation.minExecutors", "^(spark[._-])?dynamicAllocation[._-]minExecutors$", "spark-defaults.conf" },
    { "spark.dynamicAllocation.maxExecutors", "^(spark[._-])?dynamicAllocation[._-]maxExecutors$", "spark-defaults.conf" },
    { "spark.dynamicAllocation.initialExecutors", "^(spark[._-])?dynamicAllocation[._-]initialExecutors$", "spark-defaults.conf" },
    { "spark.dynamicAllocation.executorIdleTimeout", "^(spark[._-])?dynamicAllocation[._-]executorIdleTimeout$", "spark-defaults.conf" },
    { "spark.sql.adaptive.enabled", "^(spark[._-])?sql[._-]adaptive[._-]enabled$", "spark-defaults.conf" },
    { "spark.sql.files.maxPartitionBytes", "^(spark[._-])?sql[._-]files[._-]maxPartitionBytes$", "spark-defaults.conf" },
    { "spark.sql.sources.partitionOverwriteMode", "^(spark[._-])?sql[._-]sources[._-]partitionOverwriteMode$", "spark-defaults.conf" },
    { "spark.sql.cbo.enabled", "^(spark[._-])?sql[._-]cbo[._-]enabled$", "spark-defaults.conf" },
    { "spark.streaming.backpressure.enabled", "^(spark[._-])?streaming[._-]backpressure[._-]enabled$", "spark-defaults.conf" },
    { "spark.streaming.kafka.maxRatePerPartition", "^(spark[._-])?streaming[._-]kafka[._-]maxRatePerPartition$", "spark-defaults.conf" },
    { "spark.ui.enabled", "^(spark[._-])?ui[._-]enabled$", "spark-defaults.conf" },
    { "spark.eventLog.dir", "^(spark[._-])?eventLog[._-]dir$", "spark-defaults.conf" },
    { "spark.eventLog.compress", "^(spark[._-])?eventLog[._-]compress$", "spark-defaults.conf" },
    { "spark.authenticate", "^(spark[._-])?authenticate$", "spark-defaults.conf" },
    { "spark.ssl.enabled", "^(spark[._-])?ssl[._-]enabled$", "spark-defaults.conf" },
    { "spark.yarn.am.memory", "^(spark[._-])?yarn[._-]am[._-]memory$", "spark-defaults.conf" },
    { "spark.yarn.executor.memoryOverhead", "^(spark[._-])?yarn[._-]executor[._-]memoryOverhead$", "spark-defaults.conf" },
    { "spark.yarn.driver.memoryOverhead", "^(spark[._-])?yarn[._-]driver[._-]memoryOverhead$", "spark-defaults.conf" },
    { "spark.rpc.message.maxSize", "^(spark[._-])?rpc[._-]message[._-]maxSize$", "spark-defaults.conf" },
    { "spark.blockManager.port", "^(spark[._-])?blockManager[._-]port$", "spark-defaults.conf" },
    { "spark.scheduler.mode", "^(spark[._-])?scheduler[._-]mode$", "spark-defaults.conf" },
    { "spark.checkpoint.compress", "^(spark[._-])?checkpoint[._-]compress$", "spark-defaults.conf" },
    { "spark.pyspark.python", "^(spark[._-])?pyspark[._-]python$", "spark-defaults.conf" },
    { "rootLogger.level", "^rootLogger[._-]level$", "spark-log4j.properties" },
    { "rootLogger.appenderRef.stdout.ref", "^rootLogger[._-]appenderRef[._-]stdout[._-]ref$", "spark-log4j.properties" },
    { "appender.console.type", "^appender[._-]console[._-]type$", "spark-log4j.properties" },
    { "appender.console.name", "^appender[._-]console[._-]name$", "spark-log4j.properties" },
    { "appender.console.target", "^appender[._-]console[._-]target$", "spark-log4j.properties" },
    { "appender.console.layout.type", "^appender[._-]console[._-]layout[._-]type$", "spark-log4j.properties" },
    { "appender.console.layout.pattern", "^appender[._-]console[._-]layout[._-]pattern$", "spark-log4j.properties" },
    { "logger.repl.name", "^logger[._-]repl[._-]name$", "spark-log4j.properties" },
    { "logger.repl.level", "^logger[._-]repl[._-]level$", "spark-log4j.properties" },
    { "logger.thriftserver.name", "^logger[._-]thriftserver[._-]name$", "spark-log4j.properties" },
    { "logger.thriftserver.level", "^logger[._-]thriftserver[._-]level$", "spark-log4j.properties" },
    { "logger.jetty1.name", "^logger[._-]jetty1[._-]name$", "spark-log4j.properties" },
    { "logger.jetty1.level", "^logger[._-]jetty1[._-]level$", "spark-log4j.properties" },
    { "logger.jetty2.name", "^logger[._-]jetty2[._-]name$", "spark-log4j.properties" },
    { "logger.jetty2.level", "^logger[._-]jetty2[._-]level$", "spark-log4j.properties" },
    { "logger.replexprTyper.name", "^logger[._-]replexprTyper[._-]name$", "spark-log4j.properties" },

    { "logger.replexprTyper.level", "^logger[._-]replexprTyper[._-]level$", "spark-log4j.properties" },
    { "logger.replSparkILoopInterpreter.name", "^logger[._-]replSparkILoopInterpreter[._-]name$", "spark-log4j.properties" },
    { "logger.replSparkILoopInterpreter.level", "^logger[._-]replSparkILoopInterpreter[._-]level$", "spark-log4j.properties" },
    { "logger.parquet1.name", "^logger[._-]parquet1[._-]name$", "spark-log4j.properties" },
    { "logger.parquet1.level", "^logger[._-]parquet1[._-]level$", "spark-log4j.properties" },
    { "logger.parquet2.name", "^logger[._-]parquet2[._-]name$", "spark-log4j.properties" },
    { "logger.parquet2.level", "^logger[._-]parquet2[._-]level$", "spark-log4j.properties" },
    { "logger.RetryingHMSHandler.name", "^logger[._-]RetryingHMSHandler[._-]name$", "spark-log4j.properties" },
    { "logger.RetryingHMSHandler.level", "^logger[._-]RetryingHMSHandler[._-]level$", "spark-log4j.properties" },
    { "logger.FunctionRegistry.name", "^logger[._-]FunctionRegistry[._-]name$", "spark-log4j.properties" },
    { "logger.FunctionRegistry.level", "^logger[._-]FunctionRegistry[._-]level$", "spark-log4j.properties" },

    // Metrics.properties configurations (144 new entries)
    // Class properties for sinks
    { "*.sink.console.class", "^\\*[._-]sink[._-]console[._-]class$", "metrics.properties" },
    { "master.sink.console.class", "^master[._-]sink[._-]console[._-]class$", "metrics.properties" },
    { "worker.sink.console.class", "^worker[._-]sink[._-]console[._-]class$", "metrics.properties" },
    { "executor.sink.console.class", "^executor[._-]sink[._-]console[._-]class$", "metrics.properties" },
    { "driver.sink.console.class", "^driver[._-]sink[._-]console[._-]class$", "metrics.properties" },
    { "applications.sink.console.class", "^applications[._-]sink[._-]console[._-]class$", "metrics.properties" },
    { "*.sink.csv.class", "^\\*[._-]sink[._-]csv[._-]class$", "metrics.properties" },
    { "master.sink.csv.class", "^master[._-]sink[._-]csv[._-]class$", "metrics.properties" },
    { "worker.sink.csv.class", "^worker[._-]sink[._-]csv[._-]class$", "metrics.properties" },
    { "executor.sink.csv.class", "^executor[._-]sink[._-]csv[._-]class$", "metrics.properties" },
    { "driver.sink.csv.class", "^driver[._-]sink[._-]csv[._-]class$", "metrics.properties" },
    { "applications.sink.csv.class", "^applications[._-]sink[._-]csv[._-]class$", "metrics.properties" },
    { "*.sink.ganglia.class", "^\\*[._-]sink[._-]ganglia[._-]class$", "metrics.properties" },
    { "master.sink.ganglia.class", "^master[._-]sink[._-]ganglia[._-]class$", "metrics.properties" },
    { "worker.sink.ganglia.class", "^worker[._-]sink[._-]ganglia[._-]class$", "metrics.properties" },
    { "executor.sink.ganglia.class", "^executor[._-]sink[._-]ganglia[._-]class$", "metrics.properties" },
    { "driver.sink.ganglia.class", "^driver[._-]sink[._-]ganglia[._-]class$", "metrics.properties" },
    { "applications.sink.ganglia.class", "^applications[._-]sink[._-]ganglia[._-]class$", "metrics.properties" },
    { "*.sink.jmx.class", "^\\*[._-]sink[._-]jmx[._-]class$", "metrics.properties" },
    { "master.sink.jmx.class", "^master[._-]sink[._-]jmx[._-]class$", "metrics.properties" },
    { "worker.sink.jmx.class", "^worker[._-]sink[._-]jmx[._-]class$", "metrics.properties" },
    { "executor.sink.jmx.class", "^executor[._-]sink[._-]jmx[._-]class$", "metrics.properties" },
    { "driver.sink.jmx.class", "^driver[._-]sink[._-]jmx[._-]class$", "metrics.properties" },
    { "applications.sink.jmx.class", "^applications[._-]sink[._-]jmx[._-]class$", "metrics.properties" },
    { "*.sink.graphite.class", "^\\*[._-]sink[._-]graphite[._-]class$", "metrics.properties" },
    { "master.sink.graphite.class", "^master[._-]sink[._-]graphite[._-]class$", "metrics.properties" },
    { "worker.sink.graphite.class", "^worker[._-]sink[._-]graphite[._-]class$", "metrics.properties" },
    { "executor.sink.graphite.class", "^executor[._-]sink[._-]graphite[._-]class$", "metrics.properties" },
    { "driver.sink.graphite.class", "^driver[._-]sink[._-]graphite[._-]class$", "metrics.properties" },
    { "applications.sink.graphite.class", "^applications[._-]sink[._-]graphite[._-]class$", "metrics.properties" },

    // Console sink options
    { "*.sink.console.period", "^\\*[._-]sink[._-]console[._-]period$", "metrics.properties" },
    { "master.sink.console.period", "^master[._-]sink[._-]console[._-]period$", "metrics.properties" },
    { "worker.sink.console.period", "^worker[._-]sink[._-]console[._-]period$", "metrics.properties" },
    { "executor.sink.console.period", "^executor[._-]sink[._-]console[._-]period$", "metrics.properties" },
    { "driver.sink.console.period", "^driver[._-]sink[._-]console[._-]period$", "metrics.properties" },
    { "applications.sink.console.period", "^applications[._-]sink[._-]console[._-]period$", "metrics.properties" },
    { "*.sink.console.unit", "^\\*[._-]sink[._-]console[._-]unit$", "metrics.properties" },
    { "master.sink.console.unit", "^master[._-]sink[._-]console[._-]unit$", "metrics.properties" },
    { "worker.sink.console.unit", "^worker[._-]sink[._-]console[._-]unit$", "metrics.properties" },
    { "executor.sink.console.unit", "^executor[._-]sink[._-]console[._-]unit$", "metrics.properties" },
    { "driver.sink.console.unit", "^driver[._-]sink[._-]console[._-]unit$", "metrics.properties" },
    { "applications.sink.console.unit", "^applications[._-]sink[._-]console[._-]unit$", "metrics.properties" },

    // CSV sink options
    { "*.sink.csv.period", "^\\*[._-]sink[._-]csv[._-]period$", "metrics.properties" },
    { "master.sink.csv.period", "^master[._-]sink[._-]csv[._-]period$", "metrics.properties" },
    { "worker.sink.csv.period", "^worker[._-]sink[._-]csv[._-]period$", "metrics.properties" },
    { "executor.sink.csv.period", "^executor[._-]sink[._-]csv[._-]period$", "metrics.properties" },
    { "driver.sink.csv.period", "^driver[._-]sink[._-]csv[._-]period$", "metrics.properties" },
    { "applications.sink.csv.period", "^applications[._-]sink[._-]csv[._-]period$", "metrics.properties" },
    { "*.sink.csv.unit", "^\\*[._-]sink[._-]csv[._-]unit$", "metrics.properties" },
    { "master.sink.csv.unit", "^master[._-]sink[._-]csv[._-]unit$", "metrics.properties" },
    { "worker.sink.csv.unit", "^worker[._-]sink[._-]csv[._-]unit$", "metrics.properties" },
    { "executor.sink.csv.unit", "^executor[._-]sink[._-]csv[._-]unit$", "metrics.properties" },
    { "driver.sink.csv.unit", "^driver[._-]sink[._-]csv[._-]unit$", "metrics.properties" },
    { "applications.sink.csv.unit", "^applications[._-]sink[._-]csv[._-]unit$", "metrics.properties" },
    { "*.sink.csv.directory", "^\\*[._-]sink[._-]csv[._-]directory$", "metrics.properties" },
    { "master.sink.csv.directory", "^master[._-]sink[._-]csv[._-]directory$", "metrics.properties" },
    { "worker.sink.csv.directory", "^worker[._-]sink[._-]csv[._-]directory$", "metrics.properties" },
    { "executor.sink.csv.directory", "^executor[._-]sink[._-]csv[._-]directory$", "metrics.properties" },
    { "driver.sink.csv.directory", "^driver[._-]sink[._-]csv[._-]directory$", "metrics.properties" },
    { "applications.sink.csv.directory", "^applications[._-]sink[._-]csv[._-]directory$", "metrics.properties" },

    // Ganglia sink options
    { "*.sink.ganglia.host", "^\\*[._-]sink[._-]ganglia[._-]host$", "metrics.properties" },
    { "master.sink.ganglia.host", "^master[._-]sink[._-]ganglia[._-]host$", "metrics.properties" },
    { "worker.sink.ganglia.host", "^worker[._-]sink[._-]ganglia[._-]host$", "metrics.properties" },
    { "executor.sink.ganglia.host", "^executor[._-]sink[._-]ganglia[._-]host$", "metrics.properties" },
    { "driver.sink.ganglia.host", "^driver[._-]sink[._-]ganglia[._-]host$", "metrics.properties" },
    { "applications.sink.ganglia.host", "^applications[._-]sink[._-]ganglia[._-]host$", "metrics.properties" },
    { "*.sink.ganglia.port", "^\\*[._-]sink[._-]ganglia[._-]port$", "metrics.properties" },
    { "master.sink.ganglia.port", "^master[._-]sink[._-]ganglia[._-]port$", "metrics.properties" },
    { "worker.sink.ganglia.port", "^worker[._-]sink[._-]ganglia[._-]port$", "metrics.properties" },
    { "executor.sink.ganglia.port", "^executor[._-]sink[._-]ganglia[._-]port$", "metrics.properties" },
    { "driver.sink.ganglia.port", "^driver[._-]sink[._-]ganglia[._-]port$", "metrics.properties" },
    { "applications.sink.ganglia.port", "^applications[._-]sink[._-]ganglia[._-]port$", "metrics.properties" },
    { "*.sink.ganglia.period", "^\\*[._-]sink[._-]ganglia[._-]period$", "metrics.properties" },
    { "master.sink.ganglia.period", "^master[._-]sink[._-]ganglia[._-]period$", "metrics.properties" },
    { "worker.sink.ganglia.period", "^worker[._-]sink[._-]ganglia[._-]period$", "metrics.properties" },
    { "executor.sink.ganglia.period", "^executor[._-]sink[._-]ganglia[._-]period$", "metrics.properties" },
    { "driver.sink.ganglia.period", "^driver[._-]sink[._-]ganglia[._-]period$", "metrics.properties" },
    { "applications.sink.ganglia.period", "^applications[._-]sink[._-]ganglia[._-]period$", "metrics.properties" },
    { "*.sink.ganglia.unit", "^\\*[._-]sink[._-]ganglia[._-]unit$", "metrics.properties" },
    { "master.sink.ganglia.unit", "^master[._-]sink[._-]ganglia[._-]unit$", "metrics.properties" },
    { "worker.sink.ganglia.unit", "^worker[._-]sink[._-]ganglia[._-]unit$", "metrics.properties" },
    { "executor.sink.ganglia.unit", "^executor[._-]sink[._-]ganglia[._-]unit$", "metrics.properties" },
    { "driver.sink.ganglia.unit", "^driver[._-]sink[._-]ganglia[._-]unit$", "metrics.properties" },
    { "applications.sink.ganglia.unit", "^applications[._-]sink[._-]ganglia[._-]unit$", "metrics.properties" },
    { "*.sink.ganglia.ttl", "^\\*[._-]sink[._-]ganglia[._-]ttl$", "metrics.properties" },
    { "master.sink.ganglia.ttl", "^master[._-]sink[._-]ganglia[._-]ttl$", "metrics.properties" },
    { "worker.sink.ganglia.ttl", "^worker[._-]sink[._-]ganglia[._-]ttl$", "metrics.properties" },
    { "executor.sink.ganglia.ttl", "^executor[._-]sink[._-]ganglia[._-]ttl$", "metrics.properties" },
    { "driver.sink.ganglia.ttl", "^driver[._-]sink[._-]ganglia[._-]ttl$", "metrics.properties" },
    { "applications.sink.ganglia.ttl", "^applications[._-]sink[._-]ganglia[._-]ttl$", "metrics.properties" },
    { "*.sink.ganglia.mode", "^\\*[._-]sink[._-]ganglia[._-]mode$", "metrics.properties" },
    { "master.sink.ganglia.mode", "^master[._-]sink[._-]ganglia[._-]mode$", "metrics.properties" },
    { "worker.sink.ganglia.mode", "^worker[._-]sink[._-]ganglia[._-]mode$", "metrics.properties" },
    { "executor.sink.ganglia.mode", "^executor[._-]sink[._-]ganglia[._-]mode$", "metrics.properties" },
    { "driver.sink.ganglia.mode", "^driver[._-]sink[._-]ganglia[._-]mode$", "metrics.properties" },
    { "applications.sink.ganglia.mode", "^applications[._-]sink[._-]ganglia[._-]mode$", "metrics.properties" },

    // Graphite sink options
    { "*.sink.graphite.host", "^\\*[._-]sink[._-]graphite[._-]host$", "metrics.properties" },
    { "master.sink.graphite.host", "^master[._-]sink[._-]graphite[._-]host$", "metrics.properties" },
    { "worker.sink.graphite.host", "^worker[._-]sink[._-]graphite[._-]host$", "metrics.properties" },
    { "executor.sink.graphite.host", "^executor[._-]sink[._-]graphite[._-]host$", "metrics.properties" },
    { "driver.sink.graphite.host", "^driver[._-]sink[._-]graphite[._-]host$", "metrics.properties" },
    { "applications.sink.graphite.host", "^applications[._-]sink[._-]graphite[._-]host$", "metrics.properties" },
    { "*.sink.graphite.port", "^\\*[._-]sink[._-]graphite[._-]port$", "metrics.properties" },
    { "master.sink.graphite.port", "^master[._-]sink[._-]graphite[._-]port$", "metrics.properties" },
    { "worker.sink.graphite.port", "^worker[._-]sink[._-]graphite[._-]port$", "metrics.properties" },
    { "executor.sink.graphite.port", "^executor[._-]sink[._-]graphite[._-]port$", "metrics.properties" },
    { "driver.sink.graphite.port", "^driver[._-]sink[._-]graphite[._-]port$", "metrics.properties" },
    { "applications.sink.graphite.port", "^applications[._-]sink[._-]graphite[._-]port$", "metrics.properties" },
    { "*.sink.graphite.period", "^\\*[._-]sink[._-]graphite[._-]period$", "metrics.properties" },
    { "master.sink.graphite.period", "^master[._-]sink[._-]graphite[._-]period$", "metrics.properties" },
    { "worker.sink.graphite.period", "^worker[._-]sink[._-]graphite[._-]period$", "metrics.properties" },
    { "executor.sink.graphite.period", "^executor[._-]sink[._-]graphite[._-]period$", "metrics.properties" },
    { "driver.sink.graphite.period", "^driver[._-]sink[._-]graphite[._-]period$", "metrics.properties" },
    { "applications.sink.graphite.period", "^applications[._-]sink[._-]graphite[._-]period$", "metrics.properties" },
    { "*.sink.graphite.unit", "^\\*[._-]sink[._-]graphite[._-]unit$", "metrics.properties" },
    { "master.sink.graphite.unit", "^master[._-]sink[._-]graphite[._-]unit$", "metrics.properties" },
    { "worker.sink.graphite.unit", "^worker[._-]sink[._-]graphite[._-]unit$", "metrics.properties" },
    { "executor.sink.graphite.unit", "^executor[._-]sink[._-]graphite[._-]unit$", "metrics.properties" },
    { "driver.sink.graphite.unit", "^driver[._-]sink[._-]graphite[._-]unit$", "metrics.properties" },
    { "applications.sink.graphite.unit", "^applications[._-]sink[._-]graphite[._-]unit$", "metrics.properties" },
    { "*.sink.graphite.prefix", "^\\*[._-]sink[._-]graphite[._-]prefix$", "metrics.properties" },
    { "master.sink.graphite.prefix", "^master[._-]sink[._-]graphite[._-]prefix$", "metrics.properties" },
    { "worker.sink.graphite.prefix", "^worker[._-]sink[._-]graphite[._-]prefix$", "metrics.properties" },
    { "executor.sink.graphite.prefix", "^executor[._-]sink[._-]graphite[._-]prefix$", "metrics.properties" },
    { "driver.sink.graphite.prefix", "^driver[._-]sink[._-]graphite[._-]prefix$", "metrics.properties" },
    { "applications.sink.graphite.prefix", "^applications[._-]sink[._-]graphite[._-]prefix$", "metrics.properties" },

    // MetricsServlet options
    { "*.sink.MetricsServlet.path", "^\\*[._-]sink[._-]metricsservlet[._-]path$", "metrics.properties" },
    { "master.sink.MetricsServlet.path", "^master[._-]sink[._-]metricsservlet[._-]path$", "metrics.properties" },
    { "worker.sink.MetricsServlet.path", "^worker[._-]sink[._-]metricsservlet[._-]path$", "metrics.properties" },
    { "executor.sink.MetricsServlet.path", "^executor[._-]sink[._-]metricsservlet[._-]path$", "metrics.properties" },
    { "driver.sink.MetricsServlet.path", "^driver[._-]sink[._-]metricsservlet[._-]path$", "metrics.properties" },
    { "applications.sink.MetricsServlet.path", "^applications[._-]sink[._-]metricsservlet[._-]path$", "metrics.properties" },
    { "*.sink.MetricsServlet.sample", "^\\*[._-]sink[._-]metricsservlet[._-]sample$", "metrics.properties" },
    { "master.sink.MetricsServlet.sample", "^master[._-]sink[._-]metricsservlet[._-]sample$", "metrics.properties" },
    { "worker.sink.MetricsServlet.sample", "^worker[._-]sink[._-]metricsservlet[._-]sample$", "metrics.properties" },
    { "executor.sink.MetricsServlet.sample", "^executor[._-]sink[._-]metricsservlet[._-]sample$", "metrics.properties" },
    { "driver.sink.MetricsServlet.sample", "^driver[._-]sink[._-]metricsservlet[._-]sample$", "metrics.properties" },
    { "applications.sink.MetricsServlet.sample", "^applications[._-]sink[._-]metricsservlet[._-]sample$", "metrics.properties" },

    // JVM source class
    { "*.source.jvm.class", "^\\*[._-]source[._-]jvm[._-]class$", "metrics.properties" },
    { "master.source.jvm.class", "^master[._-]source[._-]jvm[._-]class$", "metrics.properties" },
    { "worker.source.jvm.class", "^worker[._-]source[._-]jvm[._-]class$", "metrics.properties" },
    { "executor.source.jvm.class", "^executor[._-]source[._-]jvm[._-]class$", "metrics.properties" },
    { "driver.source.jvm.class", "^driver[._-]source[._-]jvm[._-]class$", "metrics.properties" },
    { "applications.source.jvm.class", "^applications[._-]source[._-]jvm[._-]class$", "metrics.properties" },
};

ValidationResult validateSparkConfigParam(const char *param_name, const char *value) {
    // Check if parameter exists
    bool param_exists = false;
    for (size_t i = 0; i < sizeof(spark_param_configs)/sizeof(spark_param_configs[0]); i++) {
        if (strcmp(param_name, spark_param_configs[i].canonicalName) == 0) {
            param_exists = true;
            break;
        }
    }
    if (!param_exists) return ERROR_PARAM_NOT_FOUND;

    // Check value presence
    if (!value || strlen(value) == 0) return ERROR_VALUE_EMPTY;

    // Parameter-specific validation
    if (strcmp(param_name, "spark.executor.memory") == 0 ||
        strcmp(param_name, "spark.driver.memory") == 0 ||
        strcmp(param_name, "spark.memory.offHeap.size") == 0 ||
        strcmp(param_name, "spark.executor.memoryOverhead") == 0 ||
        strcmp(param_name, "spark.driver.memoryOverhead") == 0 ||
        strcmp(param_name, "spark.yarn.am.memory") == 0 ||
        strcmp(param_name, "spark.yarn.executor.memoryOverhead") == 0 ||
        strcmp(param_name, "spark.yarn.driver.memoryOverhead") == 0) {
        return isDataSizeWithUnit(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "spark.ui.port") == 0 ||
             strcmp(param_name, "spark.blockManager.port") == 0) {
        return isValidPort(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "spark.shuffle.service.enabled") == 0 ||
             strcmp(param_name, "spark.dynamicAllocation.enabled") == 0 ||
             strcmp(param_name, "spark.eventLog.enabled") == 0 ||
             strcmp(param_name, "spark.memory.offHeap.enabled") == 0 ||
             strcmp(param_name, "spark.shuffle.compress") == 0 ||
             strcmp(param_name, "spark.shuffle.spill.compress") == 0 ||
             strcmp(param_name, "spark.sql.adaptive.enabled") == 0 ||
             strcmp(param_name, "spark.sql.cbo.enabled") == 0 ||
             strcmp(param_name, "spark.streaming.backpressure.enabled") == 0 ||
             strcmp(param_name, "spark.eventLog.compress") == 0 ||
             strcmp(param_name, "spark.authenticate") == 0 ||
             strcmp(param_name, "spark.ssl.enabled") == 0 ||
             strcmp(param_name, "spark.checkpoint.compress") == 0 ||
             strcmp(param_name, "spark.ui.enabled") == 0) {
        return isValidBoolean(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "spark.network.timeout") == 0 ||
             strcmp(param_name, "spark.locality.wait") == 0 ||
             strcmp(param_name, "spark.dynamicAllocation.executorIdleTimeout") == 0) {
        return isValidSparkDuration(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "spark.executor.cores") == 0 ||
             strcmp(param_name, "spark.driver.cores") == 0 ||
             strcmp(param_name, "spark.sql.shuffle.partitions") == 0 ||
             strcmp(param_name, "spark.default.parallelism") == 0 ||
             strcmp(param_name, "spark.executor.instances") == 0 ||
             strcmp(param_name, "spark.dynamicAllocation.minExecutors") == 0 ||
             strcmp(param_name, "spark.dynamicAllocation.maxExecutors") == 0 ||
             strcmp(param_name, "spark.dynamicAllocation.initialExecutors") == 0 ||
             strcmp(param_name, "spark.reducer.maxSizeInFlight") == 0 ||
             strcmp(param_name, "spark.rpc.message.maxSize") == 0) {
        return isPositiveInteger(value) ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "spark.master") == 0) {
        return isValidSparkMasterFormat(value) ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "spark.serializer") == 0) {
        return strstr(value, "KryoSerializer") != NULL ||
            strstr(value, "JavaSerializer") != NULL
            ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "spark.submit.deployMode") == 0) {
        return strcmp(value, "client") == 0 || strcmp(value, "cluster") == 0
            ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "spark.io.compression.codec") == 0) {
        return strstr(value, "lz4") != NULL || strstr(value, "snappy") != NULL ||
            strstr(value, "zstd") != NULL || strstr(value, "lzf") != NULL
            ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "spark.sql.autoBroadcastJoinThreshold") == 0) {
        return isDataSizeWithUnit(value) || (atoi(value) == -1)
            ? VALIDATION_OK : ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "spark.memory.fraction") == 0) {
        char *end;
        float fraction = strtof(value, &end);
        return *end == '\0' && fraction >= 0.0f && fraction <= 1.0f
            ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "spark.scheduler.mode") == 0) {
        return strcmp(value, "FIFO") == 0 || strcmp(value, "FAIR") == 0
            ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "spark.sql.sources.partitionOverwriteMode") == 0) {
        return strcmp(value, "static") == 0 || strcmp(value, "dynamic") == 0
            ? VALIDATION_OK : ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "spark.pyspark.python") == 0) {
        return (strlen(value) > 0) ? VALIDATION_OK : ERROR_VALUE_EMPTY;
    }

    // Default validation for parameters without specific rules
    return VALIDATION_OK;
}


ConfigResult *get_spark_config(const char *param_name, const char *param_value) {
    regex_t regex;
    int reti;

    for (size_t i = 0; i < sizeof(spark_param_configs)/sizeof(spark_param_configs[0]); i++) {
        reti = regcomp(&regex, spark_param_configs[i].normalizedName, REG_ICASE | REG_NOSUB | REG_EXTENDED);
        if (reti) {
            fprintf(stderr, "Failed to compile regex for %s\n", spark_param_configs[i].canonicalName);
            continue;

        }

        reti = regexec(&regex, param_name, 0, NULL, 0);
        regfree(&regex);

        if (reti == 0) {
            ConfigResult *result = malloc(sizeof(ConfigResult));
            if (!result) return NULL;

            result->canonical_name = strdup(spark_param_configs[i].canonicalName);
            result->value = strdup(param_value);
            result->config_file = strdup(spark_param_configs[i].configFile);

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

void free_spark_config_result(ConfigResult *result) {
    if (result) {
        free(result->canonical_name);
        free(result->value);
        free(result->config_file);
        free(result);
    }
}

/*

 * Update Spark configuration parameter in the configuration file.

 *

 * Searches for the given parameter and updates its value. If the parameter

 * does not exist, it is appended to the configuration file.

 *

 * Returns ConfigStatus indicating success or specific error.

 */
ConfigStatus
update_spark_config(const char *param, const char *value, const char* configuration_file)
{
    char        config_path[PATH_MAX] = {0};
    FILE       *fp = NULL;
    FILE       *tmp_fp = NULL;
    int         found = 0;
    ConfigStatus status = INVALID_CONFIG_FILE;
    struct stat st;

    /* Validate input parameters */
    if (param == NULL || value == NULL)
    {
        return INVALID_CONFIG_FILE;
    }

    /* Determine the configuration file path */
    char *spark_home_env = getenv("SPARK_HOME");
    if (spark_home_env != NULL)
    {
        snprintf(config_path, sizeof(config_path), "%s/conf/%s", spark_home_env, configuration_file);
    }
    else
    {
        const char *spark_home_candidates[] = {"/usr/local/spark", "/opt/spark", NULL};
        struct stat st;
        const char *selected_spark_home = NULL;
        for (int i = 0; spark_home_candidates[i] != NULL; i++)
        {
            if (stat(spark_home_candidates[i], &st) == 0 && S_ISDIR(st.st_mode))
            {
                selected_spark_home = spark_home_candidates[i];
                break;
            }
        }
        if (selected_spark_home == NULL)
        {
            /* Default to Red Hat path if none found */
            selected_spark_home = "/usr/local/spark";
        }
        snprintf(config_path, sizeof(config_path), "%s/conf/%s", selected_spark_home, configuration_file);
    }

    if (strcmp(configuration_file, "spark-log4j.properties") == 0 ||
        strcmp(configuration_file, "metrics.properties") == 0 ) {
        configure_hadoop_property(config_path, param, value);
        return SUCCESS;
    }

    /*
     * Ensure parent directory exists. If not, attempt to create it with
     * appropriate permissions.
     */
    char *last_slash = strrchr(config_path, '/');
    if (last_slash != NULL)
    {
        *last_slash = '\0';
        /* Check if directory exists; if not, create it */
        if (stat(config_path, &st) == -1)
        {
            if (mkdir(config_path, 0755) != 0)
            {
                status = FILE_WRITE_ERROR;
                goto cleanup;
            }
        }
        *last_slash = '/';
    }

    /*
     * Open existing configuration file for reading. If it doesn't exist,
     * proceed with an empty temporary file.
     */
    fp = fopen(config_path, "r");
    if (fp != NULL)
    {
        /* Create a temporary file to hold updated configuration */
        tmp_fp = tmpfile();
        if (tmp_fp == NULL)
        {
            fclose(fp);
            status = FILE_WRITE_ERROR;
            goto cleanup;
        }
    }
    else
    {
        /* Existing file doesn't exist; create a new temporary file */
        tmp_fp = tmpfile();
        if (tmp_fp == NULL)
        {
            status = FILE_WRITE_ERROR;
            goto cleanup;
        }
        fp = NULL; /* Ensure fp remains NULL */
    }

    /* Process each line in the existing configuration file */
    if (fp != NULL)
    {
        char        line[MAX_LINE_LENGTH];

        while (fgets(line, sizeof(line), fp) != NULL)
        {
            char       *key = line;
            char       *value_start;

            /* Trim leading whitespace */
            while (*key == ' ' || *key == '\t')
            {
                key++;
            }

            /* Skip comments and empty lines */
            if (*key == '#' || *key == '\n')
            {
                if (fputs(line, tmp_fp) == EOF)
                {
                    status = FILE_WRITE_ERROR;
                    goto cleanup;
                }
                continue;
            }

            /* Locate the value separator (space or tab) */
            value_start = strchr(key, ' ');
            if (value_start == NULL)
            {
                value_start = strchr(key, '\t');
            }

            if (value_start != NULL)
            {
                *value_start = '\0'; /* Terminate key string */
                value_start++;
                /* Trim leading whitespace from value */
                while (*value_start == ' ' || *value_start == '\t')
                {
                    value_start++;
                }

                /* Check if current line matches target parameter */
                if (strcmp(key, param) == 0)
                {
                    /* Replace with new value */
                    if (fprintf(tmp_fp, "%s %s\n", param, value) < 0)
                    {
                        status = FILE_WRITE_ERROR;
                        goto cleanup;
                    }
                    found = 1;
                    continue;
                }
            }

            /* Write the original line to temporary file */
            if (fputs(line, tmp_fp) == EOF)
            {
                status = FILE_WRITE_ERROR;
                goto cleanup;
            }
        }

        /* Check for read errors */
        if (ferror(fp))
        {
            status = FILE_READ_ERROR;
            goto cleanup;
        }

        fclose(fp);
        fp = NULL;
    }

    /* If parameter was not found, append it to the end */
    if (!found)
    {
        if (fprintf(tmp_fp, "%s %s\n", param, value) < 0)
        {
            status = FILE_WRITE_ERROR;
            goto cleanup;
        }
    }

    /* Write updated configuration back to the original file */
    rewind(tmp_fp);
    fp = fopen(config_path, "w");
    if (fp == NULL)
    {
        status = FILE_WRITE_ERROR;
        goto cleanup;
    }

    {
        char        buffer[MAX_LINE_LENGTH];

        while (fgets(buffer, sizeof(buffer), tmp_fp) != NULL)
        {
            if (fputs(buffer, fp) == EOF)
            {
                fclose(fp);
                status = FILE_WRITE_ERROR;
                goto cleanup;
            }
        }

        /* Check for write errors */
        if (ferror(tmp_fp))
        {
            fclose(fp);
            status = FILE_READ_ERROR;
            goto cleanup;
        }

        fclose(fp);
        fp = NULL;
    }

    status = SUCCESS;

cleanup:
    /* Cleanup resources */
    if (fp != NULL)
    {
        fclose(fp);
    }
    if (tmp_fp != NULL)
    {
        fclose(tmp_fp);
    }

    return status;
}

