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

#ifndef ACTION_H
#define ACTION_H

#include "utiles.h"

void hadoop_action(Action a);
void Presto_action(Action a);
void spark_action(Action a);
void hive_action(Action a);
void Zeppelin_action(Action a);
void livy_action(Action a);
void pig_action(Action a);
void HBase_action(Action a);
void tez_action(Action a);
void kafka_action(Action a);
void Solr_action(Action a);
void phoenix_action(Action a);
void ranger_action(Action a);
void livy_action(Action a);
int atlas_action(Action a);
void storm_action(Action a);
void flink_action(Action a);
void zookeeper_action(Action a);

#endif							/* IP_ACTION */
