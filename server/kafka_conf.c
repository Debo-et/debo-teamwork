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

ConfigParam kafka_param_configs[] = {
    // Core Connection Settings
    { "bootstrap.servers", "^(kafka[._-])?bootstrap[._-]servers$", "producer.properties" },
    { "bootstrap.servers", "^(kafka[._-])?bootstrap[._-]servers$", "consumer.properties" },
    { "zookeeper.connect", "^(kafka[._-])?zookeeper[._-]connect$", "server.properties" },
    { "client.id", "^(kafka[._-])?client[._-]id$", "producer.properties" },
    { "client.id", "^(kafka[._-])?client[._-]id$", "consumer.properties" },
    { "listeners", "^(kafka[._-])?listeners$", "server.properties" },
    { "advertised.listeners", "^(kafka[._-])?advertised[._-]listeners$", "server.properties" },

    // Producer Configurations
    { "acks", "^(kafka[._-])?acks$", "producer.properties" },
    { "retries", "^(kafka[._-])?retries$", "producer.properties" },
    { "batch.size", "^(kafka[._-])?batch[._-]size$", "producer.properties" },
    { "linger.ms", "^(kafka[._-])?linger[._-]ms$", "producer.properties" },
    { "compression.type", "^(kafka[._-])?compression[._-]type$", "producer.properties" },
    { "max.request.size", "^(kafka[._-])?max[._-]request[._-]size$", "producer.properties" },
    { "enable.idempotence", "^(kafka[._-])?enable[._-]idempotence$", "producer.properties" },
    { "buffer.memory", "^(kafka[._-])?buffer[._-]memory$", "producer.properties" },
    { "max.block.ms", "^(kafka[._-])?max[._-]block[._-]ms$", "producer.properties" },
    { "delivery.timeout.ms", "^(kafka[._-])?delivery[._-]timeout[._-]ms$", "producer.properties" },
    { "request.timeout.ms", "^(kafka[._-])?request[._-]timeout[._-]ms$", "producer.properties" },
    { "max.in.flight.requests.per.connection", "^(kafka[._-])?max[._-]in[._-]flight[._-]requests[._-]per[._-]connection$", "producer.properties" },
    { "metadata.max.age.ms", "^(kafka[._-])?metadata[._-]max[._-]age[._-]ms$", "producer.properties" },
    { "send.buffer.bytes", "^(kafka[._-])?send[._-]buffer[._-]bytes$", "producer.properties" },
    { "transactional.id", "^(kafka[._-])?transactional[._-]id$", "producer.properties" },

    // Consumer Configurations
    { "group.id", "^(kafka[._-])?group[._-]id$", "consumer.properties" },
    { "auto.offset.reset", "^(kafka[._-])?auto[._-]offset[._-]reset$", "consumer.properties" },
    { "enable.auto.commit", "^(kafka[._-])?enable[._-]auto[._-]commit$", "consumer.properties" },
    { "max.poll.records", "^(kafka[._-])?max[._-]poll[._-]records$", "consumer.properties" },
    { "fetch.min.bytes", "^(kafka[._-])?fetch[._-]min[._-]bytes$", "consumer.properties" },
    { "fetch.max.bytes", "^(kafka[._-])?fetch[._-]max[._-]bytes$", "consumer.properties" },
    { "heartbeat.interval.ms", "^(kafka[._-])?heartbeat[._-]interval[._-]ms$", "consumer.properties" },
    { "max.partition.fetch.bytes", "^(kafka[._-])?max[._-]partition[._-]fetch[._-]bytes$", "consumer.properties" },
    { "receive.buffer.bytes", "^(kafka[._-])?receive[._-]buffer[._-]bytes$", "consumer.properties" },
    { "partition.assignment.strategy", "^(kafka[._-])?partition[._-]assignment[._-]strategy$", "consumer.properties" },
    { "fetch.max.wait.ms", "^(kafka[._-])?fetch[._-]max[._-]wait[._-]ms$", "consumer.properties" },
    { "max.poll.interval.ms", "^(kafka[._-])?max[._-]poll[._-]interval[._-]ms$", "consumer.properties" },

    // Broker Configurations
    { "log.dirs", "^(kafka[._-])?log[._-]dirs$", "server.properties" },
    { "num.partitions", "^(kafka[._-])?num[._-]partitions$", "server.properties" },
    { "default.replication.factor", "^(kafka[._-])?default[._-]replication[._-]factor$", "server.properties" },
    { "offsets.topic.replication.factor", "^(kafka[._-])?offsets[._-]topic[._-]replication[._-]factor$", "server.properties" },
    { "auto.create.topics.enable", "^(kafka[._-])?auto[._-]create[._-]topics[._-]enable$", "server.properties" },
    { "log.retention.ms", "^(kafka[._-])?log[._-]retention[._-]ms$", "server.properties" },
    { "log.segment.bytes", "^(kafka[._-])?log[._-]segment[._-]bytes$", "server.properties" },
    { "controlled.shutdown.enable", "^(kafka[._-])?controlled[._-]shutdown[._-]enable$", "server.properties" },
    { "unclean.leader.election.enable", "^(kafka[._-])?unclean[._-]leader[._-]election[._-]enable$", "server.properties" },
    { "socket.send.buffer.bytes", "^(kafka[._-])?socket[._-]send[._-]buffer[._-]bytes$", "server.properties" },
    { "socket.receive.buffer.bytes", "^(kafka[._-])?socket[._-]receive[._-]buffer[._-]bytes$", "server.properties" },
    { "num.recovery.threads.per.data.dir", "^(kafka[._-])?num[._-]recovery[._-]threads[._-]per[._-]data[._-]dir$", "server.properties" },
    { "log.flush.interval.messages", "^(kafka[._-])?log[._-]flush[._-]interval[._-]messages$", "server.properties" },
    { "log.flush.interval.ms", "^(kafka[._-])?log[._-]flush[._-]interval[._-]ms$", "server.properties" },
    { "message.max.bytes", "^(kafka[._-])?message[._-]max[._-]bytes$", "server.properties" },
    { "auto.leader.rebalance.enable", "^(kafka[._-])?auto[._-]leader[._-]rebalance[._-]enable$", "server.properties" },

    // Security (SSL/SASL)
    { "security.protocol", "^(kafka[._-])?security[._-]protocol$", "producer.properties" },
    { "security.protocol", "^(kafka[._-])?security[._-]protocol$", "consumer.properties" },
    { "security.protocol", "^(kafka[._-])?security[._-]protocol$", "server.properties" },
    { "ssl.keystore.location", "^(kafka[._-])?ssl[._-]keystore[._-]location$", "producer.properties" },
    { "ssl.keystore.location", "^(kafka[._-])?ssl[._-]keystore[._-]location$", "consumer.properties" },
    { "ssl.keystore.location", "^(kafka[._-])?ssl[._-]keystore[._-]location$", "server.properties" },
    { "ssl.truststore.location", "^(kafka[._-])?ssl[._-]truststore[._-]location$", "producer.properties" },
    { "ssl.truststore.location", "^(kafka[._-])?ssl[._-]truststore[._-]location$", "consumer.properties" },
    { "ssl.truststore.location", "^(kafka[._-])?ssl[._-]truststore[._-]location$", "server.properties" },
    { "ssl.keystore.password", "^(kafka[._-])?ssl[._-]keystore[._-]password$", "producer.properties" },
    { "ssl.keystore.password", "^(kafka[._-])?ssl[._-]keystore[._-]password$", "consumer.properties" },
    { "ssl.keystore.password", "^(kafka[._-])?ssl[._-]keystore[._-]password$", "server.properties" },
    { "ssl.truststore.password", "^(kafka[._-])?ssl[._-]truststore[._-]password$", "producer.properties" },
    { "ssl.truststore.password", "^(kafka[._-])?ssl[._-]truststore[._-]password$", "consumer.properties" },
    { "ssl.truststore.password", "^(kafka[._-])?ssl[._-]truststore[._-]password$", "server.properties" },
    { "ssl.key.password", "^(kafka[._-])?ssl[._-]key[._-]password$", "producer.properties" },
    { "ssl.key.password", "^(kafka[._-])?ssl[._-]key[._-]password$", "consumer.properties" },
    { "ssl.key.password", "^(kafka[._-])?ssl[._-]key[._-]password$", "server.properties" },
    { "ssl.endpoint.identification.algorithm", "^(kafka[._-])?ssl[._-]endpoint[._-]identification[._-]algorithm$", "producer.properties" },
    { "ssl.endpoint.identification.algorithm", "^(kafka[._-])?ssl[._-]endpoint[._-]identification[._-]algorithm$", "consumer.properties" },
    { "ssl.endpoint.identification.algorithm", "^(kafka[._-])?ssl[._-]endpoint[._-]identification[._-]algorithm$", "server.properties" },
    { "sasl.mechanism", "^(kafka[._-])?sasl[._-]mechanism$", "producer.properties" },
    { "sasl.mechanism", "^(kafka[._-])?sasl[._-]mechanism$", "consumer.properties" },
    { "sasl.mechanism", "^(kafka[._-])?sasl[._-]mechanism$", "server.properties" },
    { "sasl.jaas.config", "^(kafka[._-])?sasl[._-]jaas[._-]config$", "producer.properties" },
    { "sasl.jaas.config", "^(kafka[._-])?sasl[._-]jaas[._-]config$", "consumer.properties" },
    { "sasl.jaas.config", "^(kafka[._-])?sasl[._-]jaas[._-]config$", "server.properties" },

    // Performance Tuning
    { "log.retention.hours", "^(kafka[._-])?log[._-]retention[._-]hours$", "server.properties" },
    { "log.retention.bytes", "^(kafka[._-])?log[._-]retention[._-]bytes$", "server.properties" },
    { "num.io.threads", "^(kafka[._-])?num[._-]io[._-]threads$", "server.properties" },
    { "num.network.threads", "^(kafka[._-])?num[._-]network[._-]threads$", "server.properties" },
    { "log.retention.ms", "^(kafka[._-])?log[._-]retention[._-]ms$", "server.properties" },
    { "log.segment.bytes", "^(kafka[._-])?log[._-]segment[._-]bytes$", "server.properties" },
    { "xasecure.audit.is.enabled", "^(kafka[._-])?xasecure[._-]audit[._-]is[._-]enabled$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.hdfs.is.enabled", "^(kafka[._-])?xasecure[._-]audit[._-]hdfs[._-]is[._-]enabled$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.hdfs.is.async", "^(kafka[._-])?xasecure[._-]audit[._-]hdfs[._-]is[._-]async$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.hdfs.async.max.queue.size", "^(kafka[._-])?xasecure[._-]audit[._-]hdfs[._-]async[._-]max[._-]queue[._-]size$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.hdfs.async.max.flush.interval.ms", "^(kafka[._-])?xasecure[._-]audit[._-]hdfs[._-]async[._-]max[._-]flush[._-]interval[._-]ms$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.hdfs.config.encoding", "^(kafka[._-])?xasecure[._-]audit[._-]hdfs[._-]config[._-]encoding$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.hdfs.config.destination.directory", "^(kafka[._-])?xasecure[._-]audit[._-]hdfs[._-]config[._-]destination[._-]directory$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.hdfs.config.destination.file", "^(kafka[._-])?xasecure[._-]audit[._-]hdfs[._-]config[._-]destination[._-]file$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.hdfs.config.destination.flush.interval.seconds", "^(kafka[._-])?xasecure[._-]audit[._-]hdfs[._-]config[._-]destination[._-]flush[._-]interval[._-]seconds$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.hdfs.config.destination.rollover.interval.seconds", "^(kafka[._-])?xasecure[._-]audit[._-]hdfs[._-]config[._-]destination[._-]rollover[._-]interval[._-]seconds$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.hdfs.config.destination.open.retry.interval.seconds", "^(kafka[._-])?xasecure[._-]audit[._-]hdfs[._-]config[._-]destination[._-]open[._-]retry[._-]interval[._-]seconds$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.hdfs.config.local.buffer.directory", "^(kafka[._-])?xasecure[._-]audit[._-]hdfs[._-]config[._-]local[._-]buffer[._-]directory$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.hdfs.config.local.buffer.file", "^(kafka[._-])?xasecure[._-]audit[._-]hdfs[._-]config[._-]local[._-]buffer[._-]file$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.hdfs.config.local.buffer.file.buffer.size.bytes", "^(kafka[._-])?xasecure[._-]audit[._-]hdfs[._-]config[._-]local[._-]buffer[._-]file[._-]buffer[._-]size[._-]bytes$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.hdfs.config.local.buffer.flush.interval.seconds", "^(kafka[._-])?xasecure[._-]audit[._-]hdfs[._-]config[._-]local[._-]buffer[._-]flush[._-]interval[._-]seconds$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.hdfs.config.local.buffer.rollover.interval.seconds", "^(kafka[._-])?xasecure[._-]audit[._-]hdfs[._-]config[._-]local[._-]buffer[._-]rollover[._-]interval[._-]seconds$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.hdfs.config.local.archive.directory", "^(kafka[._-])?xasecure[._-]audit[._-]hdfs[._-]config[._-]local[._-]archive[._-]directory$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.hdfs.config.local.archive.max.file.count", "^(kafka[._-])?xasecure[._-]audit[._-]hdfs[._-]config[._-]local[._-]archive[._-]max[._-]file[._-]count$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.log4j.is.enabled", "^(kafka[._-])?xasecure[._-]audit[._-]log4j[._-]is[._-]enabled$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.log4j.is.async", "^(kafka[._-])?xasecure[._-]audit[._-]log4j[._-]is[._-]async$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.log4j.async.max.queue.size", "^(kafka[._-])?xasecure[._-]audit[._-]log4j[._-]async[._-]max[._-]queue[._-]size$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.log4j.async.max.flush.interval.ms", "^(kafka[._-])?xasecure[._-]audit[._-]log4j[._-]async[._-]max[._-]flush[._-]interval[._-]ms$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.kafka.is.enabled", "^(kafka[._-])?xasecure[._-]audit[._-]kafka[._-]is[._-]enabled$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.kafka.async.max.queue.size", "^(kafka[._-])?xasecure[._-]audit[._-]kafka[._-]async[._-]max[._-]queue[._-]size$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.kafka.async.max.flush.interval.ms", "^(kafka[._-])?xasecure[._-]audit[._-]kafka[._-]async[._-]max[._-]flush[._-]interval[._-]ms$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.kafka.broker_list", "^(kafka[._-])?xasecure[._-]audit[._-]kafka[._-]broker[._-]list$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.kafka.topic_name", "^(kafka[._-])?xasecure[._-]audit[._-]kafka[._-]topic[._-]name$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.solr.is.enabled", "^(kafka[._-])?xasecure[._-]audit[._-]solr[._-]is[._-]enabled$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.solr.async.max.queue.size", "^(kafka[._-])?xasecure[._-]audit[._-]solr[._-]async[._-]max[._-]queue[._-]size$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.solr.async.max.flush.interval.ms", "^(kafka[._-])?xasecure[._-]audit[._-]solr[._-]async[._-]max[._-]flush[._-]interval[._-]ms$", "ranger-kafka-audit.xml" },
    { "xasecure.audit.solr.solr_url", "^(kafka[._-])?xasecure[._-]audit[._-]solr[._-]solr[._-]url$", "ranger-kafka-audit.xml" },
    // New parameters from ranger-kafka-security.xml
    { "ranger.plugin.kafka.service.name", "^(kafka[._-])?ranger[._-]plugin[._-]kafka[._-]service[._-]name$", "ranger-kafka-security.xml" },
    { "ranger.plugin.kafka.policy.source.impl", "^(kafka[._-])?ranger[._-]plugin[._-]kafka[._-]policy[._-]source[._-]impl$", "ranger-kafka-security.xml" },
    { "ranger.plugin.kafka.policy.rest.url", "^(kafka[._-])?ranger[._-]plugin[._-]kafka[._-]policy[._-]rest[._-]url$", "ranger-kafka-security.xml" },
    { "ranger.plugin.kafka.policy.rest.ssl.config.file", "^(kafka[._-])?ranger[._-]plugin[._-]kafka[._-]policy[._-]rest[._-]ssl[._-]config[._-]file$", "ranger-kafka-security.xml" },
    { "ranger.plugin.kafka.policy.pollIntervalMs", "^(kafka[._-])?ranger[._-]plugin[._-]kafka[._-]policy[._-]pollIntervalMs$", "ranger-kafka-security.xml" },
    { "ranger.plugin.kafka.policy.cache.dir", "^(kafka[._-])?ranger[._-]plugin[._-]kafka[._-]policy[._-]cache[._-]dir$", "ranger-kafka-security.xml" },
    { "ranger.plugin.kafka.policy.rest.client.connection.timeoutMs", "^(kafka[._-])?ranger[._-]plugin[._-]kafka[._-]policy[._-]rest[._-]client[._-]connection[._-]timeoutMs$", "ranger-kafka-security.xml" },
    { "ranger.plugin.kafka.policy.rest.client.read.timeoutMs", "^(kafka[._-])?ranger[._-]plugin[._-]kafka[._-]policy[._-]rest[._-]client[._-]read[._-]timeoutMs$", "ranger-kafka-security.xml" },
    // New parameters from ranger-kafka-policymgr-ssl.xml
    { "xasecure.policymgr.clientssl.keystore", "^(kafka[._-])?xasecure[._-]policymgr[._-]clientssl[._-]keystore$", "ranger-kafka-policymgr-ssl.xml" },
    { "xasecure.policymgr.clientssl.keystore.password", "^(kafka[._-])?xasecure[._-]policymgr[._-]clientssl[._-]keystore[._-]password$", "ranger-kafka-policymgr-ssl.xml" },
    { "xasecure.policymgr.clientssl.truststore", "^(kafka[._-])?xasecure[._-]policymgr[._-]clientssl[._-]truststore$", "ranger-kafka-policymgr-ssl.xml" },
    { "xasecure.policymgr.clientssl.truststore.password", "^(kafka[._-])?xasecure[._-]policymgr[._-]clientssl[._-]truststore[._-]password$", "ranger-kafka-policymgr-ssl.xml" },
    { "xasecure.policymgr.clientssl.keystore.credential.file", "^(kafka[._-])?xasecure[._-]policymgr[._-]clientssl[._-]keystore[._-]credential[._-]file$", "ranger-kafka-policymgr-ssl.xml" },
    { "xasecure.policymgr.clientssl.truststore.credential.file", "^(kafka[._-])?xasecure[._-]policymgr[._-]clientssl[._-]truststore[._-]credential[._-]file$", "ranger-kafka-policymgr-ssl.xml" },

    // New Settings from log4j.properties
    { "log4j.rootLogger", "^log4j[._-]rootLogger$", "log4j.properties" },
    { "log4j.appender.stdout", "^log4j[._-]appender[._-]stdout$", "log4j.properties" },
    { "log4j.appender.stdout.layout", "^log4j[._-]appender[._-]stdout[._-]layout$", "log4j.properties" },
    { "log4j.appender.stdout.layout.ConversionPattern", "^log4j[._-]appender[._-]stdout[._-]layout[._-]ConversionPattern$", "log4j.properties" },
    { "log4j.appender.kafkaAppender", "^log4j[._-]appender[._-]kafkaAppender$", "log4j.properties" },
    { "log4j.appender.kafkaAppender.DatePattern", "^log4j[._-]appender[._-]kafkaAppender[._-]DatePattern$", "log4j.properties" },
    { "log4j.appender.kafkaAppender.File", "^log4j[._-]appender[._-]kafkaAppender[._-]File$", "log4j.properties" },
    { "log4j.appender.kafkaAppender.layout", "^log4j[._-]appender[._-]kafkaAppender[._-]layout$", "log4j.properties" },
    { "log4j.appender.kafkaAppender.layout.ConversionPattern", "^log4j[._-]appender[._-]kafkaAppender[._-]layout[._-]ConversionPattern$", "log4j.properties" },
    { "log4j.appender.stateChangeAppender", "^log4j[._-]appender[._-]stateChangeAppender$", "log4j.properties" },
    { "log4j.appender.stateChangeAppender.DatePattern", "^log4j[._-]appender[._-]stateChangeAppender[._-]DatePattern$", "log4j.properties" },
    { "log4j.appender.stateChangeAppender.File", "^log4j[._-]appender[._-]stateChangeAppender[._-]File$", "log4j.properties" },
    { "log4j.appender.stateChangeAppender.layout", "^log4j[._-]appender[._-]stateChangeAppender[._-]layout$", "log4j.properties" },
    { "log4j.appender.stateChangeAppender.layout.ConversionPattern", "^log4j[._-]appender[._-]stateChangeAppender[._-]layout[._-]ConversionPattern$", "log4j.properties" },
    { "log4j.appender.requestAppender", "^log4j[._-]appender[._-]requestAppender$", "log4j.properties" },
    { "log4j.appender.requestAppender.DatePattern", "^log4j[._-]appender[._-]requestAppender[._-]DatePattern$", "log4j.properties" },
    { "log4j.appender.requestAppender.File", "^log4j[._-]appender[._-]requestAppender[._-]File$", "log4j.properties" },
    { "log4j.appender.requestAppender.layout", "^log4j[._-]appender[._-]requestAppender[._-]layout$", "log4j.properties" },
    { "log4j.appender.requestAppender.layout.ConversionPattern", "^log4j[._-]appender[._-]requestAppender[._-]layout[._-]ConversionPattern$", "log4j.properties" },
    { "log4j.appender.cleanerAppender", "^log4j[._-]appender[._-]cleanerAppender$", "log4j.properties" },
    { "log4j.appender.cleanerAppender.DatePattern", "^log4j[._-]appender[._-]cleanerAppender[._-]DatePattern$", "log4j.properties" },
    { "log4j.appender.cleanerAppender.File", "^log4j[._-]appender[._-]cleanerAppender[._-]File$", "log4j.properties" },
    { "log4j.appender.cleanerAppender.layout", "^log4j[._-]appender[._-]cleanerAppender[._-]layout$", "log4j.properties" },
    { "log4j.appender.cleanerAppender.layout.ConversionPattern", "^log4j[._-]appender[._-]cleanerAppender[._-]layout[._-]ConversionPattern$", "log4j.properties" },
    { "log4j.appender.controllerAppender", "^log4j[._-]appender[._-]controllerAppender$", "log4j.properties" },
    { "log4j.appender.controllerAppender.DatePattern", "^log4j[._-]appender[._-]controllerAppender[._-]DatePattern$", "log4j.properties" },
    { "log4j.appender.controllerAppender.File", "^log4j[._-]appender[._-]controllerAppender[._-]File$", "log4j.properties" },
    { "log4j.appender.controllerAppender.layout", "^log4j[._-]appender[._-]controllerAppender[._-]layout$", "log4j.properties" },
    { "log4j.appender.controllerAppender.layout.ConversionPattern", "^log4j[._-]appender[._-]controllerAppender[._-]layout[._-]ConversionPattern$", "log4j.properties" },
    { "log4j.appender.authorizerAppender", "^log4j[._-]appender[._-]authorizerAppender$", "log4j.properties" },
    { "log4j.appender.authorizerAppender.DatePattern", "^log4j[._-]appender[._-]authorizerAppender[._-]DatePattern$", "log4j.properties" },
    { "log4j.appender.authorizerAppender.File", "^log4j[._-]appender[._-]authorizerAppender[._-]File$", "log4j.properties" },
    { "log4j.appender.authorizerAppender.layout", "^log4j[._-]appender[._-]authorizerAppender[._-]layout$", "log4j.properties" },
    { "log4j.appender.authorizerAppender.layout.ConversionPattern", "^log4j[._-]appender[._-]authorizerAppender[._-]layout[._-]ConversionPattern$", "log4j.properties" },
    { "log4j.logger.org.apache.zookeeper", "^log4j[._-]logger[._-]org[._-]apache[._-]zookeeper$", "log4j.properties" },
    { "log4j.logger.kafka", "^log4j[._-]logger[._-]kafka$", "log4j.properties" },
    { "log4j.logger.org.apache.kafka", "^log4j[._-]logger[._-]org[._-]apache[._-]kafka$", "log4j.properties" },
    { "log4j.logger.kafka.request.logger", "^log4j[._-]logger[._-]kafka[._-]request[._-]logger$", "log4j.properties" },
    { "log4j.additivity.kafka.request.logger", "^log4j[._-]additivity[._-]kafka[._-]request[._-]logger$", "log4j.properties" },
    { "log4j.logger.kafka.network.RequestChannel$", "^log4j[._-]logger[._-]kafka[._-]network[._-]RequestChannel\\$$", "log4j.properties" },
    { "log4j.additivity.kafka.network.RequestChannel$", "^log4j[._-]additivity[._-]kafka[._-]network[._-]RequestChannel\\$$", "log4j.properties" },
    { "log4j.logger.org.apache.kafka.controller", "^log4j[._-]logger[._-]org[._-]apache[._-]kafka[._-]controller$", "log4j.properties" },
    { "log4j.additivity.org.apache.kafka.controller", "^log4j[._-]additivity[._-]org[._-]apache[._-]kafka[._-]controller$", "log4j.properties" },
    { "log4j.logger.kafka.controller", "^log4j[._-]logger[._-]kafka[._-]controller$", "log4j.properties" },
    { "log4j.additivity.kafka.controller", "^log4j[._-]additivity[._-]kafka[._-]controller$", "log4j.properties" },
    { "log4j.logger.kafka.log.LogCleaner", "^log4j[._-]logger[._-]kafka[._-]log[._-]LogCleaner$", "log4j.properties" },
    { "log4j.additivity.kafka.log.LogCleaner", "^log4j[._-]additivity[._-]kafka[._-]log[._-]LogCleaner$", "log4j.properties" },
    { "log4j.logger.state.change.logger", "^log4j[._-]logger[._-]state[._-]change[._-]logger$", "log4j.properties" },
    { "log4j.additivity.state.change.logger", "^log4j[._-]additivity[._-]state[._-]change[._-]logger$", "log4j.properties" },
    { "log4j.logger.kafka.authorizer.logger", "^log4j[._-]logger[._-]kafka[._-]authorizer[._-]logger$", "log4j.properties" },
    { "log4j.additivity.kafka.authorizer.logger", "^log4j[._-]additivity[._-]kafka[._-]authorizer[._-]logger$", "log4j.properties" },

};

ValidationResult validateKafkaConfigParam(const char *param_name, const char *value) {
    // Check if parameter exists
    bool param_exists = true;
    //   for (size_t i = 0; i < sizeof(kafka_param_configs)/sizeof(kafka_param_configs[0]); i++) {
    //     if (strcmp(param_name, kafka_param_configs[i].normalized_name) == 0) {
    //       param_exists = true;
    //     break;
    // }
    //}
    if (!param_exists) return ERROR_PARAM_NOT_FOUND;

    // Check value presence
    if (!value || strlen(value) == 0) return ERROR_VALUE_EMPTY;

    // Parameter-specific validation
    if (strcmp(param_name, "bootstrap.servers") == 0) {
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
    else if (strcmp(param_name, "acks") == 0) {
        if (strcmp(value, "all") != 0 &&
            strcmp(value, "0") != 0 &&
            strcmp(value, "1") != 0)
            return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "compression.type") == 0) {
        if (!isValidCompressionType(value))
            return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "security.protocol") == 0) {
        if (!isSecurityProtocolValid(value))
            return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "sasl.mechanism") == 0) {
        if (!isSaslMechanismValid(value))
            return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "auto.offset.reset") == 0) {
        if (!isAutoOffsetResetValid(value))
            return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "enable.idempotence") == 0 ||
             strcmp(param_name, "enable.auto.commit") == 0 ||
             strcmp(param_name, "auto.create.topics.enable") == 0 ||
             strcmp(param_name, "controlled.shutdown.enable") == 0 ||
             strcmp(param_name, "unclean.leader.election.enable") == 0) {
        if (!isValidBoolean(value))
            return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "retries") == 0 ||
             strcmp(param_name, "max.poll.records") == 0 ||
             strcmp(param_name, "num.partitions") == 0 ||
             strcmp(param_name, "default.replication.factor") == 0) {
        if (!isPositiveInteger(value))
            return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strcmp(param_name, "batch.size") == 0 ||
             strcmp(param_name, "buffer.memory") == 0 ||
             strcmp(param_name, "max.request.size") == 0 ||
             strcmp(param_name, "fetch.max.bytes") == 0 ||
             strcmp(param_name, "message.max.bytes") == 0) {
        char *end;
        strtoll(value, &end, 10);
        if (*end != '\0' && !(end[0] == 'k' || end[0] == 'K' ||
                              end[0] == 'm' || end[0] == 'M' ||
                              end[0] == 'g' || end[0] == 'G'))
            return ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "linger.ms") == 0 ||
             strcmp(param_name, "max.block.ms") == 0 ||
             strcmp(param_name, "request.timeout.ms") == 0 ||
             strcmp(param_name, "max.poll.interval.ms") == 0) {
        char *end;
        long ms = strtol(value, &end, 10);
        if (*end != '\0' || ms < 0)
            return ERROR_INVALID_FORMAT;
    }
    else if (strcmp(param_name, "ssl.keystore.location") == 0 ||
             strcmp(param_name, "ssl.truststore.location") == 0) {
        if (strlen(value) == 0)
            return ERROR_VALUE_EMPTY;
    }
    else if (strcmp(param_name, "partition.assignment.strategy") == 0) {
        const char *valid[] = {"range", "roundrobin", "sticky", NULL};
        bool valid_strategy = false;
        for (int i = 0; valid[i]; i++)
            if (strstr(value, valid[i])) valid_strategy = true;
        if (!valid_strategy)
            return ERROR_CONSTRAINT_VIOLATED;
    }
    else if (strstr(param_name, ".is.enabled") != NULL ||
             strstr(param_name, ".is.async") != NULL) {
        if (!isValidBoolean(value))
            return ERROR_CONSTRAINT_VIOLATED;
    }
    // Positive integers
    else if (strstr(param_name, ".max.queue.size") != NULL ||
             strstr(param_name, ".max.file.count") != NULL ||
             strstr(param_name, ".buffer.size.bytes") != NULL) {
        if (!isPositiveInteger(value))
            return ERROR_CONSTRAINT_VIOLATED;
    }
    // Time intervals (ms/seconds)
    else if (strstr(param_name, ".interval.ms") != NULL ||
             strstr(param_name, ".interval.seconds") != NULL ||
             strstr(param_name, ".timeoutMs") != NULL) {
        if (!isNonNegativeInteger(value))
            return ERROR_CONSTRAINT_VIOLATED;
    }
    // Host:port pairs
    else if (strcmp(param_name, "xasecure.audit.kafka.broker_list") == 0) {
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
    // URL formats
    else if (strcmp(param_name, "xasecure.audit.solr.solr_url") == 0 ||
             strcmp(param_name, "ranger.plugin.kafka.policy.rest.url") == 0) {
        if (!isValidUrl(value))
            return ERROR_INVALID_FORMAT;
    }
    // Credential files
    else if (strstr(param_name, ".credential.file") != NULL) {
        if (!isValidCredentialFile(value))
            return ERROR_INVALID_FORMAT;
    }
    // File paths
    else if (strstr(param_name, ".keystore") != NULL ||
             strstr(param_name, ".truststore") != NULL ||
             strstr(param_name, ".config.file") != NULL ||
             strstr(param_name, ".cache.dir") != NULL ||
             strstr(param_name, ".directory") != NULL) {
        // Basic path format check
        if (strlen(value) < 2 || value[0] != '/')
            return ERROR_INVALID_FORMAT;
    }
    // Service name format (alphanumeric + underscores)
    else if (strcmp(param_name, "ranger.plugin.kafka.service.name") == 0) {
        for (const char *p = value; *p; p++) {
            if (!isalnum(*p) && *p != '_' && *p != '-') {
                return ERROR_INVALID_FORMAT;
            }
        }
    }
    // Topic name format
    else if (strcmp(param_name, "xasecure.audit.kafka.topic_name") == 0) {
        if (strlen(value) > 255 || strchr(value, ' ') != NULL)
            return ERROR_INVALID_FORMAT;
    }
    return VALIDATION_OK;
}

#define NUM_CONFIGS (sizeof(kafka_param_configs) / sizeof(kafka_param_configs[0]))

char *normalize_kafka_param_name(const char *param) {
    if (!param) return NULL;
    size_t len = strlen(param);
    char *normalized = malloc(len + 1);
    if (!normalized) return NULL;

    size_t j = 0;
    for (size_t i = 0; i < len; i++) {
        unsigned char c = param[i];
        if (isalnum(c)) {
            normalized[j++] = tolower(c);
        }
    }
    normalized[j] = '\0';
    return normalized;
}

ConfigResult *validate_kafka_config_param(const char *param_name, const char *param_value) {
    if (!param_name || !param_value) return NULL;

    ConfigResult *result = NULL;

    for (size_t i = 0; i < NUM_CONFIGS; i++) {
        regex_t regex;
        int ret = regcomp(&regex, kafka_param_configs[i].normalizedName, REG_EXTENDED | REG_NOSUB | REG_ICASE);
        if (ret != 0) continue;

        ret = regexec(&regex, param_name, 0, NULL, 0);
        regfree(&regex);

        if (ret == 0) {
            result = malloc(sizeof(ConfigResult));
            if (!result) break;

            result->canonical_name = strdup(kafka_param_configs[i].canonicalName);
            result->value = strdup(param_value);
            result->config_file = strdup(kafka_param_configs[i].configFile);

            if (!result->canonical_name || !result->value || !result->config_file) {
                free(result->canonical_name);
                free(result->value);
                free(result->config_file);
                free(result);
                result = NULL;
            }
            break;
        }
    }

    return result;
}

void free_kafka_config_result(ConfigResult *result) {
    if (result) {
        free(result->canonical_name);
        free(result->value);
        free(result->config_file);
        free(result);
    }
}
ConfigStatus modify_kafka_config(const char *param, const char *value, const char *config_file) {

    const char *kafka_home = getenv("KAFKA_HOME");
    char filepath[1024];
    bool found = false;

    // Check KAFKA_HOME/config directory first
    if (kafka_home != NULL) {
        snprintf(filepath, sizeof(filepath), "%s/config/%s", kafka_home, config_file);
        if (access(filepath, F_OK) == 0) {
            found = true;
        }
    }

    // If not found, check common configuration paths
    if (!found) {
        const char *common_paths[] = {
            "/etc/kafka/%s",               // Common default
            "/etc/kafka/conf/%s",          // Subdirectory for some installations
            "/opt/kafka/config/%s",  // Red Hat-based
            "/usr/local/kafka/config/%s",    // Debian-based
            NULL
        };

        for (int i = 0; common_paths[i] != NULL; i++) {
            snprintf(filepath, sizeof(filepath), common_paths[i], config_file);
            if (access(filepath, F_OK) == 0) {
                found = true;
                break;
            }
        }
    }

    if (!found) {
        return FILE_NOT_FOUND;
    }

    if (strcmp(config_file, "ranger-kafka-audit.xml") == 0 ||
        strcmp(config_file, "ranger-kafka-security.xml") == 0 ||
        strcmp(config_file, "ranger-kafka-policymgr-ssl.xml") == 0) {
        updateHadoopConfigXML(filepath, param, value);
        return SUCCESS;
    }

    if (strcmp(config_file, "log4j.properties") ==0 ||
        strcmp(config_file, "ranger-kafka-plugin.properties") ==0) {
        configure_hadoop_property(filepath, param, value);
        return SUCCESS;
    }

    // Read the entire file into memory
    FILE *file = fopen(filepath, "r");
    if (!file) {
        return FILE_NOT_FOUND;
    }

    char **lines = NULL;
    size_t line_count = 0;
    char buffer[1024];
    bool param_found = false;

    while (fgets(buffer, sizeof(buffer), file) != NULL) {
        char *line = strdup(buffer);
        if (!line) {
            fclose(file);
            for (size_t i = 0; i < line_count; i++) free(lines[i]);
            free(lines);
            return SAVE_FAILED;
        }

        char *trimmed_line = line;
        trim_whitespace(trimmed_line);

        // Skip empty lines and comments
        if (trimmed_line[0] == '#' || trimmed_line[0] == '\0') {
            lines = realloc(lines, (line_count + 1) * sizeof(char *));
            if (!lines) {
                fclose(file);
                free(line);
                return SAVE_FAILED;
            }
            lines[line_count++] = line;
            continue;
        }

        char *equals = strchr(trimmed_line, '=');
        if (equals) {
            *equals = '\0';
            char *current_key = trimmed_line;
            trim_whitespace(current_key);

            if (strcmp(current_key, param) == 0) {
                // Replace this line with the new key=value
                free(line);
                size_t new_line_len = strlen(param) + strlen(value) + 2; // '=' and '\n'
                line = malloc(new_line_len + 1);
                if (!line) {
                    fclose(file);
                    for (size_t i = 0; i < line_count; i++) free(lines[i]);
                    free(lines);
                    return SAVE_FAILED;
                }
                snprintf(line, new_line_len + 1, "%s=%s\n", param, value);
                param_found = true;
            } else {
                // Restore the '=' character
                *equals = '=';
            }
        }

        lines = realloc(lines, (line_count + 1) * sizeof(char *));
        if (!lines) {
            fclose(file);
            free(line);
            return SAVE_FAILED;
        }
        lines[line_count++] = line;
    }

    fclose(file);

    // Add new parameter if not found
    if (!param_found) {
        char *new_line = malloc(strlen(param) + strlen(value) + 3); // '=', '\n', and null terminator
        if (!new_line) {
            for (size_t i = 0; i < line_count; i++) free(lines[i]);
            free(lines);
            return SAVE_FAILED;
        }
        snprintf(new_line, strlen(param) + strlen(value) + 3, "%s=%s\n", param, value);
        lines = realloc(lines, (line_count + 1) * sizeof(char *));
        if (!lines) {
            free(new_line);
            for (size_t i = 0; i < line_count; i++) free(lines[i]);
            free(lines);
            return SAVE_FAILED;
        }
        lines[line_count++] = new_line;
    }

    // Write back to the file
    file = fopen(filepath, "w");
    if (!file) {
        for (size_t i = 0; i < line_count; i++) free(lines[i]);
        free(lines);
        return SAVE_FAILED;
    }

    for (size_t i = 0; i < line_count; i++) {
        if (fputs(lines[i], file) == EOF) {
            fclose(file);
            for (size_t j = 0; j < line_count; j++) free(lines[j]);
            free(lines);
            return SAVE_FAILED;
        }
        free(lines[i]);
    }

    free(lines);
    fclose(file);

    return SUCCESS;
}


