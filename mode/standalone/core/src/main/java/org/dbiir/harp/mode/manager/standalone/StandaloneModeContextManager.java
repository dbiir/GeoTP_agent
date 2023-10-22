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

package org.dbiir.harp.mode.manager.standalone;

import com.google.common.base.Strings;
import org.dbiir.harp.kernel.metadata.persist.service.DatabaseMetaDataPersistService;
import org.dbiir.harp.mode.manager.ContextManager;
import org.dbiir.harp.mode.manager.ContextManagerAware;
import org.dbiir.harp.mode.manager.switcher.ResourceSwitchManager;
import org.dbiir.harp.mode.manager.switcher.SwitchingResource;
import org.dbiir.harp.mode.metadata.MetaDataContexts;
import org.dbiir.harp.utils.common.config.rule.RuleConfiguration;
import org.dbiir.harp.utils.common.datasource.props.DataSourceProperties;
import org.dbiir.harp.utils.common.instance.mode.ModeContextManager;
import org.dbiir.harp.utils.common.metadata.database.AgentDatabase;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentSchema;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentTable;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentView;
import org.dbiir.harp.utils.common.metadata.database.schema.pojo.AlterSchemaMetaDataPOJO;
import org.dbiir.harp.utils.common.metadata.database.schema.pojo.AlterSchemaPOJO;
import org.dbiir.harp.utils.common.rule.identifier.type.DataNodeContainedRule;
import org.dbiir.harp.utils.common.rule.identifier.type.MutableDataNodeRule;
import org.dbiir.harp.utils.common.rule.identifier.type.ResourceHeldRule;
import org.dbiir.harp.utils.common.spi.type.ordered.cache.OrderedServicesCache;

import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * Standalone mode context manager.
 */
public final class StandaloneModeContextManager implements ModeContextManager, ContextManagerAware {
    
    private ContextManager contextManager;
    
    @Override
    public void createDatabase(final String databaseName) {
        contextManager.addDatabase(databaseName);
        contextManager.getMetaDataContexts().getPersistService().getDatabaseMetaDataService().addDatabase(databaseName);
        clearServiceCache();
    }
    
    @Override
    public void dropDatabase(final String databaseName) {
        contextManager.dropDatabase(databaseName);
        contextManager.getMetaDataContexts().getPersistService().getDatabaseMetaDataService().dropDatabase(databaseName);
        clearServiceCache();
    }
    
    @Override
    public void createSchema(final String databaseName, final String schemaName) {
        AgentSchema schema = new AgentSchema();
        contextManager.getMetaDataContexts().getMetaData().getDatabase(databaseName).putSchema(schemaName, schema);
        contextManager.getMetaDataContexts().getPersistService().getDatabaseMetaDataService().persist(databaseName, schemaName, schema);
    }
    
    @Override
    public void alterSchema(final AlterSchemaPOJO alterSchemaPOJO) {
        AgentDatabase database = contextManager.getMetaDataContexts().getMetaData().getDatabase(alterSchemaPOJO.getDatabaseName());
        putSchemaMetaData(database, alterSchemaPOJO.getSchemaName(), alterSchemaPOJO.getRenameSchemaName(), alterSchemaPOJO.getLogicDataSourceName());
        removeSchemaMetaData(database, alterSchemaPOJO.getSchemaName());
        DatabaseMetaDataPersistService databaseMetaDataService = contextManager.getMetaDataContexts().getPersistService().getDatabaseMetaDataService();
        databaseMetaDataService.persist(alterSchemaPOJO.getDatabaseName(), alterSchemaPOJO.getRenameSchemaName(), database.getSchema(alterSchemaPOJO.getRenameSchemaName()));
        databaseMetaDataService.getViewMetaDataPersistService().persist(alterSchemaPOJO.getDatabaseName(), alterSchemaPOJO.getRenameSchemaName(),
                database.getSchema(alterSchemaPOJO.getRenameSchemaName()).getViews());
        databaseMetaDataService.dropSchema(alterSchemaPOJO.getDatabaseName(), alterSchemaPOJO.getSchemaName());
    }
    
    private void putSchemaMetaData(final AgentDatabase database, final String schemaName, final String renameSchemaName, final String logicDataSourceName) {
        AgentSchema schema = database.getSchema(schemaName);
        database.putSchema(renameSchemaName, schema);
        addDataNode(database, logicDataSourceName, schemaName, schema.getAllTableNames());
    }
    
    private void addDataNode(final AgentDatabase database, final String logicDataSourceName, final String schemaName, final Collection<String> tobeAddedTableNames) {
        tobeAddedTableNames.forEach(each -> {
            if (!Strings.isNullOrEmpty(logicDataSourceName) && !containsInImmutableDataNodeContainedRule(each, database)) {
                database.getRuleMetaData().findRules(MutableDataNodeRule.class).forEach(rule -> rule.put(logicDataSourceName, schemaName, each));
            }
        });
    }
    
    private void addDataNode(final AgentDatabase database, final String logicDataSourceName, final String schemaName, final Map<String, AgentTable> toBeAddedTables,
                             final Map<String, AgentView> toBeAddedViews) {
        addTablesToDataNode(database, schemaName, logicDataSourceName, toBeAddedTables);
        addViewsToDataNode(database, schemaName, logicDataSourceName, toBeAddedTables, toBeAddedViews);
    }
    
    private void addTablesToDataNode(final AgentDatabase database, final String schemaName, final String logicDataSourceName, final Map<String, AgentTable> toBeAddedTables) {
        for (Entry<String, AgentTable> entry : toBeAddedTables.entrySet()) {
            if (!Strings.isNullOrEmpty(logicDataSourceName) && !containsInImmutableDataNodeContainedRule(entry.getKey(), database)) {
                database.getRuleMetaData().findRules(MutableDataNodeRule.class).forEach(rule -> rule.put(logicDataSourceName, schemaName, entry.getKey()));
            }
            database.getSchema(schemaName).putTable(entry.getKey(), entry.getValue());
        }
    }
    
    private void addViewsToDataNode(final AgentDatabase database, final String schemaName, final String logicDataSourceName,
                                    final Map<String, AgentTable> toBeAddedTables, final Map<String, AgentView> toBeAddedViews) {
        for (Entry<String, AgentView> entry : toBeAddedViews.entrySet()) {
            if (!Strings.isNullOrEmpty(logicDataSourceName) && !containsInImmutableDataNodeContainedRule(entry.getKey(), database)) {
                database.getRuleMetaData().findRules(MutableDataNodeRule.class).forEach(rule -> rule.put(logicDataSourceName, schemaName, entry.getKey()));
            }
            database.getSchema(schemaName).putTable(entry.getKey(), toBeAddedTables.get(entry.getKey().toLowerCase()));
            database.getSchema(schemaName).putView(entry.getKey(), entry.getValue());
        }
    }
    
    private boolean containsInImmutableDataNodeContainedRule(final String tableName, final AgentDatabase database) {
        return database.getRuleMetaData().findRules(DataNodeContainedRule.class).stream()
                .filter(each -> !(each instanceof MutableDataNodeRule)).anyMatch(each -> each.getAllTables().contains(tableName));
    }
    
    private void removeSchemaMetaData(final AgentDatabase database, final String schemaName) {
        AgentSchema schema = new AgentSchema(database.getSchema(schemaName).getTables(), database.getSchema(schemaName).getViews());
        database.removeSchema(schemaName);
        removeDataNode(database.getRuleMetaData().findRules(MutableDataNodeRule.class), Collections.singletonList(schemaName), schema.getAllTableNames());
    }
    
    private void removeDataNode(final Collection<MutableDataNodeRule> rules, final Collection<String> schemaNames, final Collection<String> tobeRemovedTables) {
        tobeRemovedTables.forEach(each -> rules.forEach(rule -> rule.remove(schemaNames, each)));
    }
    
    private void removeDataNode(final AgentDatabase database, final String schemaName, final Collection<String> tobeRemovedTables, final Collection<String> tobeRemovedViews) {
        removeTablesToDataNode(database, schemaName, tobeRemovedTables);
        removeViewsToDataNode(database, schemaName, tobeRemovedTables, tobeRemovedViews);
    }
    
    private void removeDataNode(final Collection<MutableDataNodeRule> rules, final String schemaName, final Collection<String> tobeRemovedTables) {
        tobeRemovedTables.forEach(each -> rules.forEach(rule -> rule.remove(schemaName, each)));
    }
    
    private void removeTablesToDataNode(final AgentDatabase database, final String schemaName, final Collection<String> toBeDroppedTables) {
        removeDataNode(database.getRuleMetaData().findRules(MutableDataNodeRule.class), schemaName, toBeDroppedTables);
        toBeDroppedTables.forEach(each -> database.getSchema(schemaName).removeTable(each));
    }
    
    private void removeViewsToDataNode(final AgentDatabase database, final String schemaName, final Collection<String> toBeDroppedTables, final Collection<String> toBeDroppedViews) {
        removeDataNode(database.getRuleMetaData().findRules(MutableDataNodeRule.class), schemaName, toBeDroppedViews);
        AgentSchema schema = database.getSchema(schemaName);
        toBeDroppedTables.forEach(schema::removeTable);
        toBeDroppedViews.forEach(schema::removeView);
    }
    
    @Override
    public void dropSchema(final String databaseName, final Collection<String> schemaNames) {
        Collection<String> tobeRemovedTables = new LinkedHashSet<>();
        Collection<String> tobeRemovedSchemas = new LinkedHashSet<>();
        AgentDatabase database = contextManager.getMetaDataContexts().getMetaData().getDatabase(databaseName);
        for (String each : schemaNames) {
            AgentSchema schema = new AgentSchema(database.getSchema(each).getTables(), database.getSchema(each).getViews());
            database.removeSchema(each);
            Optional.of(schema).ifPresent(optional -> tobeRemovedTables.addAll(optional.getAllTableNames()));
            tobeRemovedSchemas.add(each.toLowerCase());
        }
        removeDataNode(database.getRuleMetaData().findRules(MutableDataNodeRule.class), tobeRemovedSchemas, tobeRemovedTables);
    }
    
    @Override
    public void alterSchemaMetaData(final AlterSchemaMetaDataPOJO alterSchemaMetaDataPOJO) {
        String databaseName = alterSchemaMetaDataPOJO.getDatabaseName();
        String schemaName = alterSchemaMetaDataPOJO.getSchemaName();
        AgentDatabase database = contextManager.getMetaDataContexts().getMetaData().getDatabase(databaseName);
        Map<String, AgentTable> tables = alterSchemaMetaDataPOJO.getAlteredTables().stream().collect(Collectors.toMap(AgentTable::getName, table -> table));
        Map<String, AgentView> views = alterSchemaMetaDataPOJO.getAlteredViews().stream().collect(Collectors.toMap(AgentView::getName, view -> view));
        addDataNode(database, alterSchemaMetaDataPOJO.getLogicDataSourceName(), schemaName, tables, views);
        removeDataNode(database, schemaName, alterSchemaMetaDataPOJO.getDroppedTables(), alterSchemaMetaDataPOJO.getDroppedViews());
        DatabaseMetaDataPersistService databaseMetaDataService = contextManager.getMetaDataContexts().getPersistService().getDatabaseMetaDataService();
        databaseMetaDataService.getTableMetaDataPersistService().persist(databaseName, schemaName, tables);
        databaseMetaDataService.getViewMetaDataPersistService().persist(databaseName, schemaName, views);
        alterSchemaMetaDataPOJO.getDroppedTables().forEach(each -> databaseMetaDataService.getTableMetaDataPersistService().delete(databaseName, schemaName, each));
        alterSchemaMetaDataPOJO.getDroppedViews().forEach(each -> databaseMetaDataService.getViewMetaDataPersistService().delete(databaseName, schemaName, each));
    }
    
    @Override
    public void registerStorageUnits(final String databaseName, final Map<String, DataSourceProperties> toBeRegisterStorageUnitProps) throws SQLException {
        SwitchingResource switchingResource =
                new ResourceSwitchManager().create(contextManager.getMetaDataContexts().getMetaData().getDatabase(databaseName).getResourceMetaData(), toBeRegisterStorageUnitProps);
        contextManager.getMetaDataContexts().getMetaData().getDatabases().putAll(contextManager.createChangedDatabases(databaseName, false, switchingResource, null));
        contextManager.getMetaDataContexts().getMetaData().getGlobalRuleMetaData().findRules(ResourceHeldRule.class)
                .forEach(each -> each.addResource(contextManager.getMetaDataContexts().getMetaData().getDatabase(databaseName)));
        contextManager.getMetaDataContexts().getMetaData().getDatabase(databaseName).getSchemas()
                .forEach((schemaName, schema) -> contextManager.getMetaDataContexts().getPersistService().getDatabaseMetaDataService()
                        .persist(contextManager.getMetaDataContexts().getMetaData().getActualDatabaseName(databaseName), schemaName, schema));
        contextManager.getMetaDataContexts().getPersistService().getDataSourceService().append(contextManager.getMetaDataContexts().getMetaData().getActualDatabaseName(databaseName),
                toBeRegisterStorageUnitProps);
        clearServiceCache();
    }
    
    @Override
    public void alterStorageUnits(final String databaseName, final Map<String, DataSourceProperties> toBeUpdatedStorageUnitProps) throws SQLException {
        SwitchingResource switchingResource =
                new ResourceSwitchManager().create(contextManager.getMetaDataContexts().getMetaData().getDatabase(databaseName).getResourceMetaData(), toBeUpdatedStorageUnitProps);
        contextManager.getMetaDataContexts().getMetaData().getDatabases().putAll(contextManager.createChangedDatabases(databaseName, true, switchingResource, null));
        contextManager.getMetaDataContexts().getMetaData().getGlobalRuleMetaData().findRules(ResourceHeldRule.class)
                .forEach(each -> each.addResource(contextManager.getMetaDataContexts().getMetaData().getDatabase(databaseName)));
        contextManager.getMetaDataContexts().getPersistService().getDataSourceService().append(contextManager.getMetaDataContexts().getMetaData().getActualDatabaseName(databaseName),
                toBeUpdatedStorageUnitProps);
        switchingResource.closeStaleDataSources();
        clearServiceCache();
    }
    
    @Override
    public void unregisterStorageUnits(final String databaseName, final Collection<String> toBeDroppedStorageUnitNames) throws SQLException {
        Map<String, DataSourceProperties> dataSourcePropsMap = contextManager.getMetaDataContexts().getPersistService().getDataSourceService()
                .load(contextManager.getMetaDataContexts().getMetaData().getActualDatabaseName(databaseName));
        Map<String, DataSourceProperties> toBeDeletedDataSourcePropsMap = getToBeDeletedDataSourcePropsMap(dataSourcePropsMap, toBeDroppedStorageUnitNames);
        SwitchingResource switchingResource =
                new ResourceSwitchManager().createByDropResource(contextManager.getMetaDataContexts().getMetaData().getDatabase(databaseName).getResourceMetaData(), toBeDeletedDataSourcePropsMap);
        contextManager.getMetaDataContexts().getMetaData().getDatabases()
                .putAll(contextManager.renewDatabase(contextManager.getMetaDataContexts().getMetaData().getDatabase(databaseName), switchingResource));
        MetaDataContexts reloadMetaDataContexts = contextManager.createMetaDataContexts(databaseName, false, switchingResource, null);
        contextManager.alterSchemaMetaData(databaseName, reloadMetaDataContexts.getMetaData().getDatabase(databaseName), contextManager.getMetaDataContexts().getMetaData().getDatabase(databaseName));
        contextManager.deletedSchemaNames(databaseName, reloadMetaDataContexts.getMetaData().getDatabase(databaseName), contextManager.getMetaDataContexts().getMetaData().getDatabase(databaseName));
        contextManager.renewMetaDataContexts(reloadMetaDataContexts);
        Map<String, DataSourceProperties> toBeReversedDataSourcePropsMap = getToBeReversedDataSourcePropsMap(dataSourcePropsMap, toBeDroppedStorageUnitNames);
        contextManager.getMetaDataContexts().getPersistService().getDataSourceService().persist(contextManager.getMetaDataContexts().getMetaData().getActualDatabaseName(databaseName),
                toBeReversedDataSourcePropsMap);
        switchingResource.closeStaleDataSources();
        clearServiceCache();
    }
    
    private Map<String, DataSourceProperties> getToBeDeletedDataSourcePropsMap(final Map<String, DataSourceProperties> dataSourcePropsMap, final Collection<String> toBeDroppedResourceNames) {
        return dataSourcePropsMap.entrySet().stream().filter(entry -> toBeDroppedResourceNames.contains(entry.getKey())).collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }
    
    private Map<String, DataSourceProperties> getToBeReversedDataSourcePropsMap(final Map<String, DataSourceProperties> dataSourcePropsMap, final Collection<String> toBeDroppedResourceNames) {
        return dataSourcePropsMap.entrySet().stream().filter(entry -> !toBeDroppedResourceNames.contains(entry.getKey())).collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }
    
    @Override
    public void alterRuleConfiguration(final String databaseName, final Collection<RuleConfiguration> ruleConfigs) {
        contextManager.alterRuleConfiguration(databaseName, ruleConfigs);
        contextManager.getMetaDataContexts().getPersistService()
                .getDatabaseRulePersistService().persist(contextManager.getMetaDataContexts().getMetaData().getActualDatabaseName(databaseName), ruleConfigs);
        clearServiceCache();
    }
    
    @Override
    public void alterGlobalRuleConfiguration(final Collection<RuleConfiguration> globalRuleConfigs) {
        contextManager.alterGlobalRuleConfiguration(globalRuleConfigs);
        contextManager.getMetaDataContexts().getPersistService().getGlobalRuleService().persist(contextManager.getMetaDataContexts().getMetaData().getGlobalRuleMetaData().getConfigurations());
        clearServiceCache();
    }
    
    @Override
    public void alterProperties(final Properties props) {
        contextManager.alterProperties(props);
        if (null != contextManager.getMetaDataContexts().getPersistService().getPropsService()) {
            contextManager.getMetaDataContexts().getPersistService().getPropsService().persist(props);
        }
        clearServiceCache();
    }
    
    private void clearServiceCache() {
        OrderedServicesCache.clearCache();
    }
    
    @Override
    public void setContextManagerAware(final ContextManager contextManager) {
        this.contextManager = contextManager;
    }
}
