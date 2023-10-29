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

package org.dbiir.harp.backend.config.sharding.yaml;

import lombok.Getter;
import lombok.Setter;
import org.dbiir.harp.backend.config.sharding.yaml.rule.ShardingAutoTableRuleConfiguration;
import org.dbiir.harp.backend.config.sharding.yaml.rule.ShardingTableReferenceRuleConfiguration;
import org.dbiir.harp.backend.config.sharding.yaml.rule.ShardingTableRuleConfiguration;
import org.dbiir.harp.utils.common.config.algorithm.AlgorithmConfiguration;
import org.dbiir.harp.utils.common.config.rule.function.DistributedRuleConfiguration;
import org.dbiir.harp.utils.common.config.rule.scope.DatabaseRuleConfiguration;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Sharding rule configuration.
 */
@Getter
@Setter
public final class ShardingRuleConfiguration implements DatabaseRuleConfiguration, DistributedRuleConfiguration {
    
    private Collection<ShardingTableRuleConfiguration> tables = new LinkedList<>();
    
    private Collection<ShardingAutoTableRuleConfiguration> autoTables = new LinkedList<>();
    
    private Collection<ShardingTableReferenceRuleConfiguration> bindingTableGroups = new LinkedList<>();
    
    private Collection<String> broadcastTables = new LinkedList<>();

//    private ShardingStrategyConfiguration defaultDatabaseShardingStrategy;
//
//    private ShardingStrategyConfiguration defaultTableShardingStrategy;
//
//    private KeyGenerateStrategyConfiguration defaultKeyGenerateStrategy;
//
//    private ShardingAuditStrategyConfiguration defaultAuditStrategy;

    private String defaultShardingColumn;
    
    private Map<String, AlgorithmConfiguration> shardingAlgorithms = new LinkedHashMap<>();
    
    private Map<String, AlgorithmConfiguration> keyGenerators = new LinkedHashMap<>();
    
    private Map<String, AlgorithmConfiguration> auditors = new LinkedHashMap<>();
}
