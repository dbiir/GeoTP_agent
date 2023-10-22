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

package org.dbiir.harp.utils.common.metadata.database.schema.builder;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.dbiir.harp.utils.common.database.type.DatabaseType;
import org.dbiir.harp.utils.common.database.type.SchemaSupportedDatabaseType;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentSchema;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentTable;
import org.dbiir.harp.utils.common.yaml.schema.pojo.YamlAgentTable;
import org.dbiir.harp.utils.common.yaml.schema.swapper.YamlTableSwapper;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.*;

/**
 * System schema builder.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SystemSchemaBuilder {
    
    /**
     * Build system schema.
     * 
     * @param databaseName database name
     * @param databaseType database type
     * @return Agent system schema map
     */
    public static Map<String, AgentSchema> build(final String databaseName, final DatabaseType databaseType) {
        Map<String, AgentSchema> result = new LinkedHashMap<>(databaseType.getSystemSchemas().size(), 1);
        YamlTableSwapper swapper = new YamlTableSwapper();
        for (String each : getSystemSchemas(databaseName, databaseType)) {
            result.put(each.toLowerCase(), createSchema(getSchemaStreams(each, databaseType), swapper));
        }
        return result;
    }
    
    private static Collection<String> getSystemSchemas(final String originalDatabaseName, final DatabaseType databaseType) {
        String databaseName = databaseType instanceof SchemaSupportedDatabaseType ? "postgres" : originalDatabaseName;
        return databaseType.getSystemDatabaseSchemaMap().getOrDefault(databaseName, Collections.emptyList());
    }
    
    private static Collection<InputStream> getSchemaStreams(final String schemaName, final DatabaseType databaseType) {
        SystemSchemaBuilderRule builderRule = SystemSchemaBuilderRule.valueOf(databaseType.getType(), schemaName);
        Collection<InputStream> result = new LinkedList<>();
        for (String each : builderRule.getTables()) {
            result.add(SystemSchemaBuilder.class.getClassLoader().getResourceAsStream("schema/" + databaseType.getType().toLowerCase() + "/" + schemaName + "/" + each + ".yaml"));
        }
        return result;
    }
    
    private static AgentSchema createSchema(final Collection<InputStream> schemaStreams, final YamlTableSwapper swapper) {
        Map<String, AgentTable> tables = new LinkedHashMap<>(schemaStreams.size(), 1);
        for (InputStream each : schemaStreams) {
            YamlAgentTable metaData = new Yaml().loadAs(each, YamlAgentTable.class);
            tables.put(metaData.getName(), swapper.swapToObject(metaData));
        }
        return new AgentSchema(tables, Collections.emptyMap());
    }
}
