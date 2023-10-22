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

package org.dbiir.harp.utils.common.datanode;

import lombok.RequiredArgsConstructor;
import org.dbiir.harp.utils.common.rule.AgentRule;
import org.dbiir.harp.utils.common.rule.identifier.type.DataNodeContainedRule;
import org.dbiir.harp.utils.common.spi.type.ordered.OrderedSPILoader;

import java.util.*;
import java.util.Map.Entry;

/**
 * Data nodes.
 */
@RequiredArgsConstructor
public final class DataNodes {
    
    private final Collection<AgentRule> rules;
    
    @SuppressWarnings("rawtypes")
    private final Map<AgentRule, DataNodeBuilder> dataNodeBuilders;
    
    public DataNodes(final Collection<AgentRule> rules) {
        this.rules = rules;
        dataNodeBuilders = OrderedSPILoader.getServices(DataNodeBuilder.class, rules);
    }
    
    /**
     * Get data nodes.
     * 
     * @param tableName table name
     * @return data nodes
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Collection<DataNode> getDataNodes(final String tableName) {
        Optional<DataNodeContainedRule> dataNodeContainedRule = findDataNodeContainedRule(tableName);
        if (!dataNodeContainedRule.isPresent()) {
            return Collections.emptyList();
        }
        Collection<DataNode> result = new LinkedList<>(dataNodeContainedRule.get().getDataNodesByTableName(tableName));
        for (Entry<AgentRule, DataNodeBuilder> entry : dataNodeBuilders.entrySet()) {
            result = entry.getValue().build(result, entry.getKey());
        }
        return result;
    }
    
    private Optional<DataNodeContainedRule> findDataNodeContainedRule(final String tableName) {
        return rules.stream().filter(each -> isDataNodeContainedRuleContainsTable(each, tableName)).findFirst().map(optional -> (DataNodeContainedRule) optional);
    }
    
    private boolean isDataNodeContainedRuleContainsTable(final AgentRule each, final String tableName) {
        return each instanceof DataNodeContainedRule && !((DataNodeContainedRule) each).getDataNodesByTableName(tableName).isEmpty();
    }
}
