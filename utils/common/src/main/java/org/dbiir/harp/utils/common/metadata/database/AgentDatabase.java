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

package org.dbiir.harp.utils.common.metadata.database;

import lombok.Getter;
import org.dbiir.harp.utils.common.config.database.DatabaseConfiguration;
import org.dbiir.harp.utils.common.config.database.impl.DataSourceProvidedDatabaseConfiguration;
import org.dbiir.harp.utils.common.config.props.ConfigurationProperties;
import org.dbiir.harp.utils.common.database.type.DatabaseType;
import org.dbiir.harp.utils.common.database.type.DatabaseTypeEngine;
import org.dbiir.harp.utils.common.datasource.state.DataSourceStateManager;
import org.dbiir.harp.utils.common.instance.InstanceContext;
import org.dbiir.harp.utils.common.metadata.database.resource.AgentResourceMetaData;
import org.dbiir.harp.utils.common.metadata.database.rule.AgentRuleMetaData;
import org.dbiir.harp.utils.common.metadata.database.schema.builder.GenericSchemaBuilderMaterial;
import org.dbiir.harp.utils.common.metadata.database.schema.builder.SystemSchemaBuilder;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentSchema;
import org.dbiir.harp.utils.common.rule.AgentRule;
import org.dbiir.harp.utils.common.rule.builder.database.DatabaseRulesBuilder;
import org.dbiir.harp.utils.common.metadata.database.schema.builder.GenericSchemaBuilder;


import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Agent database.
 */
@Getter
public final class AgentDatabase {
    
    private final String name;
    
    private final DatabaseType protocolType;
    
    private final AgentResourceMetaData resourceMetaData;

    private final AgentRuleMetaData ruleMetaData;

    private final Map<String, AgentSchema> schemas;
    
    public AgentDatabase(final String name, final DatabaseType protocolType, final AgentResourceMetaData resourceMetaData,
                         final AgentRuleMetaData ruleMetaData, final Map<String, AgentSchema> schemas) {
        this.name = name;
        this.protocolType = protocolType;
        this.resourceMetaData = resourceMetaData;
        this.ruleMetaData = ruleMetaData;
        this.schemas = new ConcurrentHashMap<>(schemas.size(), 1);
        schemas.forEach((key, value) -> this.schemas.put(key.toLowerCase(), value));
    }


//    /**
//     * Create database meta data.
//     *
//     * @param name database name
//     * @param protocolType database protocol type
//     * @param databaseConfig database configuration
//     * @param props configuration properties
//     * @return database meta data
//     * @throws SQLException SQL exception
//     */
//    public static AgentDatabase create(final String name, final DatabaseType protocolType, final DatabaseConfiguration databaseConfig,
//                                       final ConfigurationProperties props) throws SQLException {
//        Map<String, AgentSchema> schemas = new ConcurrentHashMap<>(GenericSchemaBuilder
//                .build(new GenericSchemaBuilderMaterial(protocolType, DataSourceStateManager.getInstance().getEnabledDataSourceMap(name, databaseConfig.getDataSources()),
//                        props, DatabaseTypeEngine.getDefaultSchemaName(protocolType, name))));
//        SystemSchemaBuilder.build(name, protocolType).forEach(schemas::putIfAbsent);
//        return create(name, protocolType, databaseConfig, schemas);
//    }

    /**
     * Create system database meta data.
     * 
     * @param name system database name
     * @param protocolType protocol database type
     * @return system database meta data
     */
    public static AgentDatabase create(final String name, final DatabaseType protocolType) {
        DatabaseConfiguration databaseConfig = new DataSourceProvidedDatabaseConfiguration(new LinkedHashMap<>(), new LinkedList<>());
        return create(name, protocolType, databaseConfig, new LinkedList<>(), SystemSchemaBuilder.build(name, protocolType));
    }

    /**
     * Create database meta data.
     *
     * @param name database name
     * @param protocolType database protocol type
     * @param storageTypes storage types
     * @param databaseConfig database configuration
     * @param props configuration properties
     * @param instanceContext instance context
     * @return database meta data
     * @throws SQLException SQL exception
     */
    public static AgentDatabase create(final String name, final DatabaseType protocolType, final Map<String, DatabaseType> storageTypes,
                                                final DatabaseConfiguration databaseConfig, final ConfigurationProperties props, final InstanceContext instanceContext) throws SQLException {
        Collection<AgentRule> databaseRules = DatabaseRulesBuilder.build(name, databaseConfig, instanceContext);
        Map<String, AgentSchema> schemas = new ConcurrentHashMap<>(GenericSchemaBuilder
                .build(new GenericSchemaBuilderMaterial(protocolType, storageTypes, DataSourceStateManager.getInstance().getEnabledDataSourceMap(name, databaseConfig.getDataSources()), databaseRules,
                        props, DatabaseTypeEngine.getDefaultSchemaName(protocolType, name))));
        SystemSchemaBuilder.build(name, protocolType).forEach(schemas::putIfAbsent);
        return create(name, protocolType, databaseConfig, databaseRules, schemas);
    }


    /**
     * Create database meta data.
     *
     * @param name database name
     * @param protocolType database protocol type
     * @param databaseConfig database configuration
     * @param rules rules
     * @param schemas schemas
     * @return database meta data
     */
    public static AgentDatabase create(final String name, final DatabaseType protocolType, final DatabaseConfiguration databaseConfig,
                                       final Collection<AgentRule> rules, final Map<String, AgentSchema> schemas) {
        AgentResourceMetaData resourceMetaData = createResourceMetaData(name, databaseConfig.getDataSources());
        AgentRuleMetaData ruleMetaData = new AgentRuleMetaData(rules);
        return new AgentDatabase(name, protocolType, resourceMetaData, ruleMetaData, schemas);
    }
    
    private static AgentResourceMetaData createResourceMetaData(final String databaseName, final Map<String, DataSource> dataSourceMap) {
        return new AgentResourceMetaData(databaseName, dataSourceMap);
    }
    
    /**
     * Get schema.
     *
     * @param schemaName schema name
     * @return schema
     */
    public AgentSchema getSchema(final String schemaName) {
        return schemas.get(schemaName.toLowerCase());
    }
    
    /**
     * Put schema.
     *
     * @param schemaName schema name
     * @param schema schema
     */
    public void putSchema(final String schemaName, final AgentSchema schema) {
        schemas.put(schemaName.toLowerCase(), schema);
    }
    
    /**
     * Remove schema.
     *
     * @param schemaName schema name
     */
    public void removeSchema(final String schemaName) {
        schemas.remove(schemaName.toLowerCase());
    }
    
    /**
     * Judge contains schema from database or not.
     *
     * @param schemaName schema name
     * @return contains schema from database or not
     */
    public boolean containsSchema(final String schemaName) {
        return schemas.containsKey(schemaName.toLowerCase());
    }
    
    /**
     * Judge whether is completed.
     *
     * @return is completed or not
     */
    public boolean isComplete() {
        return !resourceMetaData.getDataSources().isEmpty();
    }
    
    /**
     * Judge whether contains data source.
     *
     * @return contains data source or not
     */
    public boolean containsDataSource() {
        return !resourceMetaData.getDataSources().isEmpty();
    }
}
