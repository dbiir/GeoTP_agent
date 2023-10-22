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

package org.dbiir.harp.utils.common.yaml.schema.swapper;



import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentColumn;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentConstraint;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentIndex;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentTable;
import org.dbiir.harp.utils.common.yaml.schema.pojo.YamlAgentColumn;
import org.dbiir.harp.utils.common.yaml.schema.pojo.YamlAgentConstraint;
import org.dbiir.harp.utils.common.yaml.schema.pojo.YamlAgentIndex;
import org.dbiir.harp.utils.common.yaml.schema.pojo.YamlAgentTable;
import org.dbiir.harp.utils.common.yaml.swapper.YamlConfigurationSwapper;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * YAML table swapper.
 */
public final class YamlTableSwapper implements YamlConfigurationSwapper<YamlAgentTable, AgentTable> {
    
    @Override
    public YamlAgentTable swapToYamlConfiguration(final AgentTable table) {
        YamlAgentTable result = new YamlAgentTable();
        result.setColumns(swapYamlColumns(table.getColumns()));
        result.setIndexes(swapYamlIndexes(table.getIndexes()));
        result.setConstraints(swapYamlConstraints(table.getConstrains()));
        result.setName(table.getName());
        return result;
    }
    
    @Override
    public AgentTable swapToObject(final YamlAgentTable yamlConfig) {
        return new AgentTable(yamlConfig.getName(), swapColumns(yamlConfig.getColumns()), swapIndexes(yamlConfig.getIndexes()), swapConstraints(yamlConfig.getConstraints()));
    }
    
    private Collection<AgentConstraint> swapConstraints(final Map<String, YamlAgentConstraint> constraints) {
        return null == constraints ? Collections.emptyList() : constraints.values().stream().map(this::swapConstraint).collect(Collectors.toList());
    }
    
    private AgentConstraint swapConstraint(final YamlAgentConstraint constraint) {
        return new AgentConstraint(constraint.getName(), constraint.getReferencedTableName());
    }
    
    private Collection<AgentIndex> swapIndexes(final Map<String, YamlAgentIndex> indexes) {
        return null == indexes ? Collections.emptyList() : indexes.values().stream().map(this::swapIndex).collect(Collectors.toList());
    }
    
    private AgentIndex swapIndex(final YamlAgentIndex index) {
        return new AgentIndex(index.getName());
    }
    
    private Collection<AgentColumn> swapColumns(final Map<String, YamlAgentColumn> indexes) {
        return null == indexes ? Collections.emptyList() : indexes.values().stream().map(this::swapColumn).collect(Collectors.toList());
    }
    
    private AgentColumn swapColumn(final YamlAgentColumn column) {
        return new AgentColumn(column.getName(), column.getDataType(), column.isPrimaryKey(), column.isGenerated(), column.isCaseSensitive(), column.isVisible(), column.isUnsigned());
    }
    
    private Map<String, YamlAgentConstraint> swapYamlConstraints(final Map<String, AgentConstraint> constrains) {
        return constrains.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> swapYamlConstraint(entry.getValue()), (oldValue, currentValue) -> oldValue, LinkedHashMap::new));
    }
    
    private YamlAgentConstraint swapYamlConstraint(final AgentConstraint constraint) {
        YamlAgentConstraint result = new YamlAgentConstraint();
        result.setName(constraint.getName());
        result.setReferencedTableName(constraint.getReferencedTableName());
        return result;
    }
    
    private Map<String, YamlAgentIndex> swapYamlIndexes(final Map<String, AgentIndex> indexes) {
        return indexes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> swapYamlIndex(entry.getValue()), (oldValue, currentValue) -> oldValue, LinkedHashMap::new));
    }
    
    private YamlAgentIndex swapYamlIndex(final AgentIndex index) {
        YamlAgentIndex result = new YamlAgentIndex();
        result.setName(index.getName());
        return result;
    }
    
    private Map<String, YamlAgentColumn> swapYamlColumns(final Map<String, AgentColumn> columns) {
        return columns.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> swapYamlColumn(entry.getValue()), (oldValue, currentValue) -> oldValue, LinkedHashMap::new));
    }
    
    private YamlAgentColumn swapYamlColumn(final AgentColumn column) {
        YamlAgentColumn result = new YamlAgentColumn();
        result.setName(column.getName());
        result.setCaseSensitive(column.isCaseSensitive());
        result.setGenerated(column.isGenerated());
        result.setPrimaryKey(column.isPrimaryKey());
        result.setDataType(column.getDataType());
        result.setVisible(column.isVisible());
        result.setUnsigned(column.isUnsigned());
        return result;
    }
}
