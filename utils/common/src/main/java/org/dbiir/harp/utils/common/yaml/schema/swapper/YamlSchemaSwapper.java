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


import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentSchema;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentTable;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentView;
import org.dbiir.harp.utils.common.yaml.schema.pojo.YamlAgentSchema;
import org.dbiir.harp.utils.common.yaml.schema.pojo.YamlAgentTable;
import org.dbiir.harp.utils.common.yaml.schema.pojo.YamlAgentView;
import org.dbiir.harp.utils.common.yaml.swapper.YamlConfigurationSwapper;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * YAML schema swapper.
 */
public final class YamlSchemaSwapper implements YamlConfigurationSwapper<YamlAgentSchema, AgentSchema> {
    
    @Override
    public YamlAgentSchema swapToYamlConfiguration(final AgentSchema schema) {
        Map<String, YamlAgentTable> tables = schema.getAllTableNames().stream()
                .collect(Collectors.toMap(each -> each, each -> swapYamlTable(schema.getTable(each)), (oldValue, currentValue) -> oldValue, LinkedHashMap::new));
        Map<String, YamlAgentView> views = schema.getAllViewNames().stream()
                .collect(Collectors.toMap(each -> each, each -> swapYamlView(schema.getView(each)), (oldValue, currentValue) -> oldValue, LinkedHashMap::new));
        YamlAgentSchema result = new YamlAgentSchema();
        result.setTables(tables);
        result.setViews(views);
        return result;
    }
    
    @Override
    public AgentSchema swapToObject(final YamlAgentSchema yamlConfig) {
        return Optional.ofNullable(yamlConfig).map(this::swapSchema).orElseGet(AgentSchema::new);
    }
    
    private AgentSchema swapSchema(final YamlAgentSchema schema) {
        Map<String, AgentTable> tables = null == schema.getTables() || schema.getTables().isEmpty() ? new LinkedHashMap<>()
                : schema.getTables().entrySet().stream().collect(Collectors.toMap(Entry::getKey, entry -> swapTable(entry.getValue()), (oldValue, currentValue) -> oldValue, LinkedHashMap::new));
        Map<String, AgentView> views = null == schema.getViews() || schema.getViews().isEmpty() ? new LinkedHashMap<>()
                : schema.getViews().entrySet().stream().collect(Collectors.toMap(Entry::getKey, entry -> swapView(entry.getValue()), (oldValue, currentValue) -> oldValue, LinkedHashMap::new));
        return new AgentSchema(tables, views);
    }
    
    private AgentTable swapTable(final YamlAgentTable table) {
        return new YamlTableSwapper().swapToObject(table);
    }
    
    private YamlAgentTable swapYamlTable(final AgentTable table) {
        return new YamlTableSwapper().swapToYamlConfiguration(table);
    }
    
    private AgentView swapView(final YamlAgentView view) {
        return new YamlViewSwapper().swapToObject(view);
    }
    
    private YamlAgentView swapYamlView(final AgentView view) {
        return new YamlViewSwapper().swapToYamlConfiguration(view);
    }
}
