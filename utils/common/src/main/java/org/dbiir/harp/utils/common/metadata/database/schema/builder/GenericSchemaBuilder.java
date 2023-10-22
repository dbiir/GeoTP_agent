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
import org.dbiir.harp.utils.common.config.props.ConfigurationPropertyKey;
import org.dbiir.harp.utils.common.database.type.DatabaseType;
import org.dbiir.harp.utils.common.database.type.DatabaseTypeEngine;
import org.dbiir.harp.utils.common.metadata.database.schema.loader.metadata.SchemaMetaDataLoaderEngine;
import org.dbiir.harp.utils.common.metadata.database.schema.loader.metadata.SchemaMetaDataLoaderMaterial;
import org.dbiir.harp.utils.common.metadata.database.schema.loader.model.*;
import org.dbiir.harp.utils.common.metadata.database.schema.model.*;
import org.dbiir.harp.utils.common.metadata.database.schema.reviser.MetaDataReviseEngine;
import org.dbiir.harp.utils.common.metadata.database.schema.util.SchemaMetaDataUtils;
import org.dbiir.harp.utils.common.rule.AgentRule;
import org.dbiir.harp.utils.common.rule.identifier.type.TableContainedRule;

import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Generic schema builder.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class GenericSchemaBuilder {
    
    /**
     * Build generic schema.
     *
     * @param material generic schema builder material
     * @return generic schema map
     * @throws SQLException SQL exception
     */
    public static Map<String, AgentSchema> build(final GenericSchemaBuilderMaterial material) throws SQLException {
        return build(getAllTableNames(material.getRules()), material);
    }

    /**
     * Build generic schema.
     *
     * @param tableNames table names
     * @param material generic schema builder material
     * @return generic schema map
     * @throws SQLException SQL exception
     */
    public static Map<String, AgentSchema> build(final Collection<String> tableNames, final GenericSchemaBuilderMaterial material) throws SQLException {
        Map<String, SchemaMetaData> result = loadSchemas(tableNames, material);
        if (!isProtocolTypeSameWithStorageType(material)) {
            result = translate(result, material);
        }
        return revise(result, material);
    }

    private static boolean isProtocolTypeSameWithStorageType(final GenericSchemaBuilderMaterial material) {
        for (DatabaseType each : material.getStorageTypes().values()) {
            if (!material.getProtocolType().equals(each)) {
                return false;
            }
        }
        return true;
    }

    private static Collection<String> getAllTableNames(final Collection<AgentRule> rules) {
        return rules.stream().filter(each -> each instanceof TableContainedRule).flatMap(each -> ((TableContainedRule) each).getTables().stream()).collect(Collectors.toSet());
    }


    private static Map<String, SchemaMetaData> loadSchemas(final Collection<String> tableNames, final GenericSchemaBuilderMaterial material) throws SQLException {
        boolean checkMetaDataEnable = material.getProps().getValue(ConfigurationPropertyKey.CHECK_TABLE_META_DATA_ENABLED);
        Collection<SchemaMetaDataLoaderMaterial> schemaMetaDataLoaderMaterials = SchemaMetaDataUtils.getSchemaMetaDataLoaderMaterials(tableNames, material, checkMetaDataEnable);
        if (schemaMetaDataLoaderMaterials.isEmpty()) {
            return Collections.emptyMap();
        }
        return SchemaMetaDataLoaderEngine.load(schemaMetaDataLoaderMaterials);
    }
    
    private static Map<String, SchemaMetaData> translate(final Map<String, SchemaMetaData> schemaMetaDataMap, final GenericSchemaBuilderMaterial material) {
        Collection<TableMetaData> tableMetaDataList = new LinkedList<>();
        for (DatabaseType each : material.getStorageTypes().values()) {
            String defaultSchemaName = DatabaseTypeEngine.getDefaultSchemaName(each, material.getDefaultSchemaName());
            tableMetaDataList.addAll(Optional.ofNullable(schemaMetaDataMap.get(defaultSchemaName)).map(SchemaMetaData::getTables).orElseGet(Collections::emptyList));
        }
        String frontendSchemaName = DatabaseTypeEngine.getDefaultSchemaName(material.getProtocolType(), material.getDefaultSchemaName());
        Map<String, SchemaMetaData> result = new LinkedHashMap<>();
        result.put(frontendSchemaName, new SchemaMetaData(frontendSchemaName, tableMetaDataList));
        return result;
    }
    
    private static Map<String, AgentSchema> revise(final Map<String, SchemaMetaData> schemaMetaDataMap, final GenericSchemaBuilderMaterial material) {
        Map<String, SchemaMetaData> result = new LinkedHashMap<>(schemaMetaDataMap);
        result.putAll(new MetaDataReviseEngine(material.getRules().stream().filter(each -> each instanceof TableContainedRule).collect(Collectors.toList())).revise(result, material));
        return convertToSchemaMap(result, material);
    }
    
    private static Map<String, AgentSchema> convertToSchemaMap(final Map<String, SchemaMetaData> schemaMetaDataMap, final GenericSchemaBuilderMaterial material) {
        if (schemaMetaDataMap.isEmpty()) {
            return Collections.singletonMap(material.getDefaultSchemaName(), new AgentSchema());
        }
        Map<String, AgentSchema> result = new ConcurrentHashMap<>(schemaMetaDataMap.size(), 1);
        for (Entry<String, SchemaMetaData> entry : schemaMetaDataMap.entrySet()) {
            Map<String, AgentTable> tables = convertToTableMap(entry.getValue().getTables());
            result.put(entry.getKey().toLowerCase(), new AgentSchema(tables, new LinkedHashMap<>()));
        }
        return result;
    }
    
    private static Map<String, AgentTable> convertToTableMap(final Collection<TableMetaData> tableMetaDataList) {
        Map<String, AgentTable> result = new LinkedHashMap<>(tableMetaDataList.size(), 1);
        for (TableMetaData each : tableMetaDataList) {
            Collection<AgentColumn> columns = convertToColumns(each.getColumns());
            Collection<AgentIndex> indexes = convertToIndexes(each.getIndexes());
            Collection<AgentConstraint> constraints = convertToConstraints(each.getConstrains());
            result.put(each.getName(), new AgentTable(each.getName(), columns, indexes, constraints));
        }
        return result;
    }
    
    private static Collection<AgentColumn> convertToColumns(final Collection<ColumnMetaData> columnMetaDataList) {
        Collection<AgentColumn> result = new LinkedList<>();
        for (ColumnMetaData each : columnMetaDataList) {
            result.add(new AgentColumn(each.getName(), each.getDataType(), each.isPrimaryKey(), each.isGenerated(), each.isCaseSensitive(), each.isVisible(), each.isUnsigned()));
        }
        return result;
    }
    
    private static Collection<AgentIndex> convertToIndexes(final Collection<IndexMetaData> indexMetaDataList) {
        Collection<AgentIndex> result = new LinkedList<>();
        for (IndexMetaData each : indexMetaDataList) {
            result.add(new AgentIndex(each.getName()));
        }
        return result;
    }
    
    private static Collection<AgentConstraint> convertToConstraints(final Collection<ConstraintMetaData> constraintMetaDataList) {
        Collection<AgentConstraint> result = new LinkedList<>();
        for (ConstraintMetaData each : constraintMetaDataList) {
            result.add(new AgentConstraint(each.getName(), each.getReferencedTableName()));
        }
        return result;
    }
}
