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

package org.dbiir.harp.backend.config.sharding.builder;


import org.dbiir.harp.backend.config.sharding.rule.ShardingRule;
import org.dbiir.harp.backend.config.sharding.yaml.ShardingRuleConfiguration;
import org.dbiir.harp.utils.common.instance.InstanceContext;
import org.dbiir.harp.utils.common.rule.AgentRule;
import org.dbiir.harp.utils.common.rule.builder.database.DatabaseRuleBuilder;
import org.dbiir.harp.utils.exceptions.Preconditions;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.Map;

/**
 * Sharding rule builder.
 */
public final class ShardingRuleBuilder implements DatabaseRuleBuilder<ShardingRuleConfiguration> {
    
    @Override
    public ShardingRule build(final ShardingRuleConfiguration config, final String databaseName,
                              final Map<String, DataSource> dataSources, final Collection<AgentRule> builtRules, final InstanceContext instanceContext) {
        Preconditions.checkState(null != dataSources && !dataSources.isEmpty(), () -> new RuntimeException("Data source" + databaseName));
        return new ShardingRule(config, dataSources.keySet(), instanceContext);
    }
    
    @Override
    public int getOrder() {
        return -10;
    }
    
    @Override
    public Class<ShardingRuleConfiguration> getTypeClass() {
        return ShardingRuleConfiguration.class;
    }
}
