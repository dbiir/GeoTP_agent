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

package org.dbiir.harp.utils.common.metadata.database.schema.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.*;

/**
 * ShardingSphere table.
 */
@RequiredArgsConstructor
@Getter
@EqualsAndHashCode
@ToString
public final class AgentTable {
    
    private final String name;
    
    private final Map<String, AgentColumn> columns;
    
    private final Map<String, AgentIndex> indexes;
    
    private final Map<String, AgentConstraint> constrains;
    
    private final List<String> columnNames = new ArrayList<>();
    
    private final List<String> visibleColumns = new ArrayList<>();
    
    private final List<String> primaryKeyColumns = new ArrayList<>();
    
    public AgentTable() {
        this("", Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }
    
    public AgentTable(final String name, final Collection<AgentColumn> columnList,
                      final Collection<AgentIndex> indexList, final Collection<AgentConstraint> constraintList) {
        this.name = name;
        columns = getColumns(columnList);
        indexes = getIndexes(indexList);
        constrains = getConstrains(constraintList);
    }
    
    private Map<String, AgentColumn> getColumns(final Collection<AgentColumn> columnList) {
        Map<String, AgentColumn> result = new LinkedHashMap<>(columnList.size(), 1);
        for (AgentColumn each : columnList) {
            String lowerColumnName = each.getName().toLowerCase();
            result.put(lowerColumnName, each);
            columnNames.add(each.getName());
            if (each.isPrimaryKey()) {
                primaryKeyColumns.add(lowerColumnName);
            }
            if (each.isVisible()) {
                visibleColumns.add(each.getName());
            }
        }
        return result;
    }
    
    private Map<String, AgentIndex> getIndexes(final Collection<AgentIndex> indexList) {
        Map<String, AgentIndex> result = new LinkedHashMap<>(indexList.size(), 1);
        for (AgentIndex each : indexList) {
            result.put(each.getName().toLowerCase(), each);
        }
        return result;
    }
    
    private Map<String, AgentConstraint> getConstrains(final Collection<AgentConstraint> constraintList) {
        Map<String, AgentConstraint> result = new LinkedHashMap<>(constraintList.size(), 1);
        for (AgentConstraint each : constraintList) {
            result.put(each.getName().toLowerCase(), each);
        }
        return result;
    }
}
