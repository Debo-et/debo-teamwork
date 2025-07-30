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


#ifndef PROTOCOL_H
#define PROTOCOL_H

/* Core Commands */
#define CliMsg_Finish         'V'   /* Get version information */
#define CliMsg_All             'A'   /* Apply to all components */

/* Component Identifiers */
#define CliMsg_Hdfs            0xC3   /* HDFS component */
#define CliMsg_HBase           0xC5   /* HBase component */
#define CliMsg_Spark           0xC6   /* Spark component */
#define CliMsg_Kafka           0xC7   /* Kafka component */
#define CliMsg_ZooKeeper       0xC8   /* ZooKeeper component */
#define CliMsg_Flink           0xC9   /* Flink component */
#define CliMsg_Storm           0xCA   /* Storm component */
#define CliMsg_Hive            0xCB   /* Hive component */
#define CliMsg_Pig             0xCC   /* Pig component */
#define CliMsg_Presto          0xCD   /* Presto component */
#define CliMsg_Tez             0xCE   /* Tez component */
#define CliMsg_Atlas           0xCF   /* Atlas component */
#define CliMsg_Ranger          0xD0   /* Ranger component */
#define CliMsg_Livy            0xD1   /* Livy component */
#define CliMsg_Phoenix         0xD2   /* Phoenix component */
#define CliMsg_Solr            0xD3   /* Solr component */
#define CliMsg_Zeppelin        0xD4   /* Zeppelin component */

/* HDFS Operations */
#define CliMsg_Hdfs_Start       'H'
#define CliMsg_Hdfs_Stop        'J'
#define CliMsg_Hdfs_Restart     'K'
#define CliMsg_Hdfs_Uninstall   'N'
#define CliMsg_Hdfs_Install_Version  'i'
#define CliMsg_Hdfs_Install     'l'
#define CliMsg_Hdfs_Configure   0xB0

/* HBase Operations */
#define CliMsg_HBase_Start      'B'
#define CliMsg_HBase_Stop       'C'
#define CliMsg_HBase_Restart    'D'
#define CliMsg_HBase_Uninstall  'F'
#define CliMsg_HBase_Install_Version  'r'
#define CliMsg_HBase_Install    'u'
#define CliMsg_HBase_Configure  0xB2

/* Spark Operations */
#define CliMsg_Spark_Start      'S'
#define CliMsg_Spark_Stop       'P'
#define CliMsg_Spark_Restart    'T'
#define CliMsg_Spark_Uninstall  'U'
#define CliMsg_Spark_Install_Version  'a'
#define CliMsg_Spark_Install    'd'
#define CliMsg_Spark_Configure  0xB3

/* Kafka Operations */
#define CliMsg_Kafka_Start      'k'
#define CliMsg_Kafka_Stop       'L'
#define CliMsg_Kafka_Restart    'O'
#define CliMsg_Kafka_Uninstall  'X'
#define CliMsg_Kafka_Install_Version  'e'
#define CliMsg_Kafka_Install    'p'
#define CliMsg_Kafka_Configure  0xB4

/* ZooKeeper Operations */
#define CliMsg_ZooKeeper_Start  'Z'
#define CliMsg_ZooKeeper_Stop   'z'
#define CliMsg_ZooKeeper_Restart 'y'
#define CliMsg_ZooKeeper_Uninstall 'x'
#define CliMsg_ZooKeeper_Install_Version  'w'
#define CliMsg_ZooKeeper_Install  '2'
#define CliMsg_ZooKeeper_Configure 0xB5

/* Flink Operations */
#define CliMsg_Flink_Start      '3'
#define CliMsg_Flink_Stop       '4'
#define CliMsg_Flink_Restart    '5'
#define CliMsg_Flink_Uninstall  '6'
#define CliMsg_Flink_Install_Version  '7'
#define CliMsg_Flink_Install    '0'
#define CliMsg_Flink_Configure  0xB6

/* Storm Operations */
#define CliMsg_Storm_Start      '!'
#define CliMsg_Storm_Stop       '@'
#define CliMsg_Storm_Restart    '#'
#define CliMsg_Storm_Uninstall  '$'
#define CliMsg_Storm_Install_Version  '%'
#define CliMsg_Storm_Install    '*'
#define CliMsg_Storm_Configure  0xB7

/* Hive Operations */
#define CliMsg_Hive_Start       '{'
#define CliMsg_Hive_Stop        '}'
#define CliMsg_Hive_Restart     '['
#define CliMsg_Hive_Uninstall   ']'
#define CliMsg_Hive_Install_Version  ':'
#define CliMsg_Hive_Install     '\\'
#define CliMsg_Hive_Configure   0xB8

/* Pig Operations */
#define CliMsg_Pig_Start        '<'
#define CliMsg_Pig_Stop         '>'
#define CliMsg_Pig_Restart      '?'
#define CliMsg_Pig_Uninstall    '/'
#define CliMsg_Pig_Install_Version  '='
#define CliMsg_Pig_Install      '.'
#define CliMsg_Pig_Configure    0xB9

/* Presto Operations (Conflict Fixed) */
#define CliMsg_Presto_Start     '-'
#define CliMsg_Presto_Stop      '`'
#define CliMsg_Presto_Restart   '_'
#define CliMsg_Presto_Uninstall '|'
#define CliMsg_Presto_Install_Version  '~'
#define CliMsg_Presto_Install   0xC4   /* Changed from 0xC7 */
#define CliMsg_Presto_Configure 0xBA

/* Tez Operations (Conflicts Fixed) */
#define CliMsg_Tez_Start        0xE9
#define CliMsg_Tez_Stop         0xD5   /* Changed from 0xC9 */
#define CliMsg_Tez_Restart      0xEA
#define CliMsg_Tez_Uninstall    0xD6   /* Changed from 0xCA */
#define CliMsg_Tez_Install_Version  0xE0
#define CliMsg_Tez_Install      0xDD
#define CliMsg_Tez_Configure    0xBB

/* Atlas Operations */
#define CliMsg_Atlas_Start      0x80
#define CliMsg_Atlas_Stop       0x81
#define CliMsg_Atlas_Restart    0x82
#define CliMsg_Atlas_Uninstall  0x83
#define CliMsg_Atlas_Install_Version  0x84
#define CliMsg_Atlas_Install    0x87
#define CliMsg_Atlas_Configure  0xBC

/* Ranger Operations */
#define CliMsg_Ranger_Start     0x88
#define CliMsg_Ranger_Stop      0x89
#define CliMsg_Ranger_Restart   0x8A
#define CliMsg_Ranger_Uninstall 0x8B
#define CliMsg_Ranger_Install_Version  0x8C
#define CliMsg_Ranger_Install   0x8F
#define CliMsg_Ranger_Configure 0xBD

/* Livy Operations */
#define CliMsg_Livy_Start       0x90
#define CliMsg_Livy_Stop        0x91
#define CliMsg_Livy_Restart     0x92
#define CliMsg_Livy_Uninstall   0x93
#define CliMsg_Livy_Install_Version  0x94
#define CliMsg_Livy_Install     0x97
#define CliMsg_Livy_Configure   0xBE

/* Phoenix Operations */
#define CliMsg_Phoenix_Start    0x98
#define CliMsg_Phoenix_Stop     0x99
#define CliMsg_Phoenix_Restart  0x9A
#define CliMsg_Phoenix_Uninstall 0x9B
#define CliMsg_Phoenix_Install_Version 0x9C
#define CliMsg_Phoenix_Install 0x9F
#define CliMsg_Phoenix_Configure 0xBF

/* Solr Operations */
#define CliMsg_Solr_Start       0xA0
#define CliMsg_Solr_Stop        0xA1
#define CliMsg_Solr_Restart     0xA2
#define CliMsg_Solr_Uninstall   0xA3
#define CliMsg_Solr_Install_Version  0xA4
#define CliMsg_Solr_Install     0xA7
#define CliMsg_Solr_Configure   0xC1

/* Zeppelin Operations */
#define CliMsg_Zeppelin_Start   0xA8
#define CliMsg_Zeppelin_Stop    0xA9
#define CliMsg_Zeppelin_Restart 0xAA
#define CliMsg_Zeppelin_Uninstall 0xAB
#define CliMsg_Zeppelin_Install_Version 0xAC
#define CliMsg_Zeppelin_Install 0xAF
#define CliMsg_Zeppelin_Configure 0xC2

/* Shared Parameters */
#define CliMsg_Value            0xFF   /* Changed from '~' */

#endif /* PROTOCOL_H */
