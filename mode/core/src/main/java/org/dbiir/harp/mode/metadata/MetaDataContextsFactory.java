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

package org.dbiir.harp.mode.metadata;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.dbiir.harp.kernel.metadata.MetaDataFactory;
import org.dbiir.harp.kernel.metadata.persist.MetaDataPersistService;
import org.dbiir.harp.mode.event.storage.StorageNodeDataSource;
import org.dbiir.harp.mode.manager.ContextManagerBuilderParameter;
import org.dbiir.harp.utils.common.config.database.DatabaseConfiguration;
import org.dbiir.harp.utils.common.config.database.impl.DataSourceProvidedDatabaseConfiguration;
import org.dbiir.harp.utils.common.config.props.ConfigurationProperties;
import org.dbiir.harp.utils.common.config.rule.RuleConfiguration;
import org.dbiir.harp.utils.common.datasource.state.DataSourceState;
import org.dbiir.harp.utils.common.datasource.state.DataSourceStateManager;
import org.dbiir.harp.utils.common.instance.InstanceContext;
import org.dbiir.harp.utils.common.instance.metadata.jdbc.JDBCInstanceMetaData;
import org.dbiir.harp.utils.common.metadata.AgentMetaData;
import org.dbiir.harp.utils.common.metadata.database.AgentDatabase;
import org.dbiir.harp.utils.common.metadata.database.rule.AgentRuleMetaData;
import org.dbiir.harp.utils.common.rule.builder.global.GlobalRulesBuilder;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * Meta data contexts.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class MetaDataContextsFactory {
    
    /**
     * Create meta data contexts.
     *
     * @param persistService persist service
     * @param param context manager builder parameter
     * @param instanceContext instance context
     * @return meta data contexts
     * @throws SQLException SQL exception
     */
    public static MetaDataContexts create(final MetaDataPersistService persistService, final ContextManagerBuilderParameter param, final InstanceContext instanceContext) throws SQLException {
        return create(persistService, param, instanceContext, Collections.emptyMap());
    }
    
    /**
     * Create meta data contexts.
     *
     * @param persistService persist service
     * @param param context manager builder parameter
     * @param instanceContext instance context
     * @param storageNodes storage nodes
     * @return meta data contexts
     * @throws SQLException SQL exception
     */
    public static MetaDataContexts create(final MetaDataPersistService persistService, final ContextManagerBuilderParameter param,
                                          final InstanceContext instanceContext, final Map<String, StorageNodeDataSource> storageNodes) throws SQLException {
        boolean databaseMetaDataExisted = databaseMetaDataExisted(persistService);
        Map<String, DatabaseConfiguration> effectiveDatabaseConfigs = getDatabaseConfigurations(databaseMetaDataExisted,
                getDatabaseNames(instanceContext, param.getDatabaseConfigs(), persistService), param.getDatabaseConfigs(), persistService);
        checkDataSourceStates(effectiveDatabaseConfigs, storageNodes, param.isForce());
        Collection<RuleConfiguration> globalRuleConfigs = getGlobalRuleConfigs(databaseMetaDataExisted, persistService, param.getGlobalRuleConfigs());
        ConfigurationProperties props = getConfigurationProperties(databaseMetaDataExisted, persistService, param.getProps());
        Map<String, AgentDatabase> databases = getDatabases(databaseMetaDataExisted, persistService, effectiveDatabaseConfigs, props, instanceContext);
        AgentRuleMetaData globalMetaData = new AgentRuleMetaData(GlobalRulesBuilder.buildRules(globalRuleConfigs, databases, props));
        MetaDataContexts result = new MetaDataContexts(persistService, new AgentMetaData(databases, globalMetaData, props));
        persistDatabaseConfigurations(databaseMetaDataExisted, param, result);
        persistMetaData(databaseMetaDataExisted, result);
        return result;
    }
    
    private static boolean databaseMetaDataExisted(final MetaDataPersistService persistService) {
        return !persistService.getDatabaseMetaDataService().loadAllDatabaseNames().isEmpty();
    }
    
    private static Collection<String> getDatabaseNames(final InstanceContext instanceContext, final Map<String, DatabaseConfiguration> databaseConfigs, final MetaDataPersistService persistService) {
        return instanceContext.getInstance().getMetaData() instanceof JDBCInstanceMetaData ? databaseConfigs.keySet() : persistService.getDatabaseMetaDataService().loadAllDatabaseNames();
    }
    
    private static ConfigurationProperties getConfigurationProperties(final boolean databaseMetaDataExisted, final MetaDataPersistService persistService, final Properties props) {
        return databaseMetaDataExisted ? new ConfigurationProperties(persistService.getPropsService().load()) : new ConfigurationProperties(props);
    }
    
    private static Map<String, DatabaseConfiguration> getDatabaseConfigurations(final boolean databaseMetaDataExisted, final Collection<String> databaseNames,
                                                                                final Map<String, DatabaseConfiguration> databaseConfigs, final MetaDataPersistService persistService) {
        return databaseMetaDataExisted ? createEffectiveDatabaseConfigurations(databaseNames, databaseConfigs, persistService) : databaseConfigs;
    }
    
    private static Map<String, DatabaseConfiguration> createEffectiveDatabaseConfigurations(final Collection<String> databaseNames,
                                                                                            final Map<String, DatabaseConfiguration> databaseConfigs, final MetaDataPersistService persistService) {
        return databaseNames.stream().collect(
                Collectors.toMap(each -> each, each -> createEffectiveDatabaseConfiguration(each, databaseConfigs, persistService), (a, b) -> b, () -> new HashMap<>(databaseNames.size(), 1)));
    }
    
    private static DatabaseConfiguration createEffectiveDatabaseConfiguration(final String databaseName,
                                                                              final Map<String, DatabaseConfiguration> databaseConfigs, final MetaDataPersistService persistService) {
        Map<String, DataSource> effectiveDataSources = persistService.getEffectiveDataSources(databaseName, databaseConfigs);
        Collection<RuleConfiguration> databaseRuleConfigs = persistService.getDatabaseRulePersistService().load(databaseName);
        return new DataSourceProvidedDatabaseConfiguration(effectiveDataSources, databaseRuleConfigs);
    }
    
    private static void checkDataSourceStates(final Map<String, DatabaseConfiguration> databaseConfigs, final Map<String, StorageNodeDataSource> storageNodes, final boolean force) {
        Map<String, DataSourceState> storageDataSourceStates = getStorageDataSourceStates(storageNodes);
        databaseConfigs.forEach((key, value) -> {
            if (!value.getDataSources().isEmpty()) {
                DataSourceStateManager.getInstance().initStates(key, value.getDataSources(), storageDataSourceStates, force);
            }
        });
    }
    
    private static Map<String, DataSourceState> getStorageDataSourceStates(final Map<String, StorageNodeDataSource> storageDataSourceStates) {
        Map<String, DataSourceState> result = new HashMap<>(storageDataSourceStates.size(), 1);
        storageDataSourceStates.forEach((key, value) -> {
            List<String> values = Splitter.on(".").splitToList(key);
            Preconditions.checkArgument(3 == values.size(), "Illegal data source of storage node.");
            String databaseName = values.get(0);
            String dataSourceName = values.get(2);
            result.put(databaseName + "." + dataSourceName, DataSourceState.valueOf(value.getStatus().name()));
        });
        return result;
    }
    
    private static Collection<RuleConfiguration> getGlobalRuleConfigs(final boolean databaseMetaDataExisted, final MetaDataPersistService persistService,
                                                                      final Collection<RuleConfiguration> globalRuleConfigs) {
        return databaseMetaDataExisted ? persistService.getGlobalRuleService().load() : globalRuleConfigs;
    }
    
    private static Map<String, AgentDatabase> getDatabases(final boolean databaseMetaDataExisted, final MetaDataPersistService persistService,
                                                                    final Map<String, DatabaseConfiguration> databaseConfigMap, final ConfigurationProperties props,
                                                                    final InstanceContext instanceContext) throws SQLException {
        return MetaDataFactory.create(databaseMetaDataExisted, persistService, databaseConfigMap, props, instanceContext);
    }
    
    private static void persistDatabaseConfigurations(final boolean databaseMetaDataExisted, final ContextManagerBuilderParameter param, final MetaDataContexts metadataContexts) {
        if (!databaseMetaDataExisted) {
            persistDatabaseConfigurations(metadataContexts, param);
        }
    }
    
    private static void persistDatabaseConfigurations(final MetaDataContexts metadataContexts, final ContextManagerBuilderParameter param) {
        metadataContexts.getPersistService().persistGlobalRuleConfiguration(param.getGlobalRuleConfigs(), param.getProps());
        for (Entry<String, ? extends DatabaseConfiguration> entry : param.getDatabaseConfigs().entrySet()) {
            String databaseName = entry.getKey();
            metadataContexts.getPersistService().persistConfigurations(entry.getKey(), entry.getValue(),
                    metadataContexts.getMetaData().getDatabase(databaseName).getResourceMetaData().getDataSources(),
                    metadataContexts.getMetaData().getDatabase(databaseName).getRuleMetaData().getRules());
        }
    }
    
    private static void persistMetaData(final boolean databaseMetaDataExisted, final MetaDataContexts metaDataContexts) {
        if (!databaseMetaDataExisted) {
            metaDataContexts.getMetaData().getDatabases().values().forEach(each -> each.getSchemas()
                    .forEach((schemaName, schema) -> metaDataContexts.getPersistService().getDatabaseMetaDataService().persist(each.getName(), schemaName, schema)));
            metaDataContexts.getAgentData().getDatabaseData().forEach((databaseName, databaseData) -> databaseData.getSchemaData().forEach((schemaName, schemaData) -> metaDataContexts
                    .getPersistService().getAgentDataPersistService().persist(databaseName, schemaName, schemaData, metaDataContexts.getMetaData().getDatabases())));
        }
    }
}
