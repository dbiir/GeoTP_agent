/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dbiir.harp.backend.config.sharding.yaml.swapper.rule;


import org.dbiir.harp.backend.config.sharding.yaml.config.rule.YamlTableRuleConfiguration;
import org.dbiir.harp.backend.config.sharding.yaml.rule.ShardingTableRuleConfiguration;
import org.dbiir.harp.utils.common.yaml.swapper.YamlConfigurationSwapper;
import org.dbiir.harp.utils.exceptions.Preconditions;

/**
 * YAML sharding table rule configuration swapper.
 */
public final class YamlShardingTableRuleConfigurationSwapper implements YamlConfigurationSwapper<YamlTableRuleConfiguration, ShardingTableRuleConfiguration> {
    

    @Override
    public YamlTableRuleConfiguration swapToYamlConfiguration(final ShardingTableRuleConfiguration data) {
        YamlTableRuleConfiguration result = new YamlTableRuleConfiguration();
        result.setLogicTable(data.getLogicTable());
        result.setActualDataNodes(data.getActualDataNodes());
        return result;
    }
    
    @Override
    public ShardingTableRuleConfiguration swapToObject(final YamlTableRuleConfiguration yamlConfig) {
        Preconditions.checkNotNull(yamlConfig.getLogicTable(), () -> new RuntimeException("Sharding Logic table"));
        return new ShardingTableRuleConfiguration(yamlConfig.getLogicTable(), yamlConfig.getActualDataNodes());
    }
}
