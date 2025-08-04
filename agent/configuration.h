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

ConfigStatus modify_hdfs_config(const char* param, const char* value, const char *filename);
ConfigStatus set_ranger_config(const char *param, const char *value, const char *config_file);
ConfigStatus modify_yarn_config(const char* param, const char* value);
ConfigStatus update_hbase_config(const char* param, const char* value, const char *config_file);
ConfigStatus update_spark_config(const char *param, const char *value, const char* configuration_file);
ConfigStatus modify_kafka_config(const char* param, const char* value, const char *config_file);
ConfigStatus modify_zookeeper_config(const char* param, const char* value, const char *filename);
ConfigStatus modify_storm_config(const char* param, const char* value, const char *filename);
ConfigStatus modify_hive_config(const char* config_param, const char* value, const char* configuration_file);
ConfigStatus set_hue_config(const char* param, const char* value);
ConfigStatus modify_oozie_config(const char* param, const char* value);
ConfigStatus set_livy_config(const char *param, const char *value, const char *filename);
ConfigStatus update_pig_config(char *param,  char *value);
ConfigStatus update_phoenix_config(const char* param, const char* value);
ConfigStatus update_solr_config(const char* param, const char* value, const char* configuration_file);
ConfigStatus set_zeppelin_config(const char *config_file, const char* param, const char* value);
ConfigResult *get_hdfs_config(const char *param, const char *value);
ConfigResult* process_hbase_config(const char *param_name, const char *param_value);
ConfigResult *get_spark_config(const char *param_name, const char *param_value);
ConfigResult *validate_kafka_config_param(const char *param_name, const char *param_value);
ConfigStatus update_flink_config(const char *param, const char *value , const char *config_file);
ConfigResult *parse_zookeeper_param(const char *param_name, const char *param_value);
ConfigResult* validate_storm_config_param(const char* param_name, const char* param_value);
ConfigResult* process_hive_parameter(const char* param_name, const char* param_value);
ConfigResult* parse_livy_config_param(const char *input_key, const char *input_value);
ConfigResult* set_flink_config(const char* param, const char* value);
ConfigStatus set_ranger_config(const char *param, const char *value, const char *config_file);
ConfigResult* validate_solr_parameter(const char *param_name, const char *param_value);
ConfigResult *process_zeppelin_config_param(const char *param_name, const char *param_value);
ConfigResult* get_ranger_config(char *param_name, char *param_value);
ConfigResult *parse_tez_config_param(const char *param_name, const char *param_value);
ConfigStatus modify_tez_config(const char *param, const char *value, const char *config_file);
ConfigResult *validate_pig_config_param(const char *param_name, const char *param_value);
ConfigResult *get_presto_config_setting(const char *param_name, const char *param_value);
ConfigResult *validate_config_param(const char *paramName, const char *paramValue);
ConfigStatus  set_presto_config(const char *param, const char *value, const char *config_file);
int update_atlas_config(const char *param, const char *value, const char *filename);
ConfigResult* find_hdfs_config(const char *param);
ValidationResult validateHdfsConfigParam(const char *param_name, const char *value);
ValidationResult validateFlinkConfigParam(const char *param_name, const char *value);
ValidationResult validateZooKeeperConfigParam(const char *param_name, const char *value);
ValidationResult validateTezConfigParam(const char *param_name, const char *value);
ValidationResult validateZeppelinConfigParam(const char *param_name, const char *value);
ValidationResult validateSolrConfigParam(const char *param_name, const char *value);
ValidationResult validateLivyConfigParam(const char *param_name, const char *value);
ValidationResult validateHiveConfigParam(const char *param_name, const char *value);
ValidationResult validateStormConfigParam(const char *param_name, const char *value);
ValidationResult validateKafkaConfigParam(const char *param_name, const char *value);
ValidationResult validateSparkConfigParam(const char *param_name, const char *value);
ValidationResult validateHBaseConfigParam(const char *param_name, const char *value);
ValidationResult validatePigConfigParam(const char *param_name, const char *value);
void configure_target_component(Component target);
ValidationResult validatePrestoConfigParam(const char *param_name, const char *value);
