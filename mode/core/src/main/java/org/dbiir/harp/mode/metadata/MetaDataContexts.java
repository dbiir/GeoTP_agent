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

import lombok.Getter;
import org.dbiir.harp.kernel.metadata.persist.MetaDataPersistService;
import org.dbiir.harp.utils.common.database.type.SchemaSupportedDatabaseType;
import org.dbiir.harp.utils.common.metadata.AgentMetaData;
import org.dbiir.harp.utils.common.metadata.data.AgentData;
import org.dbiir.harp.utils.common.metadata.data.AgentDatabaseData;
import org.dbiir.harp.utils.common.metadata.data.AgentSchemaData;
import org.dbiir.harp.utils.common.metadata.data.AgentTableData;
import org.dbiir.harp.utils.common.metadata.data.builder.AgentDataBuilder;
import org.dbiir.harp.utils.common.spi.type.typed.TypedSPILoader;

import java.util.Map.Entry;
import java.util.Optional;

/**
 * Meta data contexts.
 */
@Getter
public final class MetaDataContexts implements AutoCloseable {

    private final MetaDataPersistService persistService;

    private final AgentMetaData metaData;
    
    private final AgentData agentData;

    public MetaDataContexts(final MetaDataPersistService persistService, final AgentMetaData metaData) {
//    public MetaDataContexts(final AgentMetaData metaData) {
        this.persistService = persistService;
        this.metaData = metaData;
        this.agentData = initAgentData(metaData);
    }
    
    private AgentData initAgentData(final AgentMetaData metaData) {
        if (metaData.getDatabases().isEmpty()) {
            return new AgentData();
        }
        AgentData result = Optional.ofNullable(metaData.getDatabases().values().iterator().next().getProtocolType())
                // TODO can `protocolType instanceof SchemaSupportedDatabaseType ? "PostgreSQL" : protocolType.getType()` replace to trunk database type?
                .flatMap(protocolType -> TypedSPILoader.findService(AgentDataBuilder.class, protocolType instanceof SchemaSupportedDatabaseType ? "PostgreSQL" : protocolType.getType())
                        .map(builder -> builder.build(metaData)))
                .orElseGet(AgentData::new);
//        Optional<AgentData> loadedAgentData = Optional.ofNullable(persistService.getAgentDataPersistService())
//                .flatMap(shardingSphereDataPersistService -> shardingSphereDataPersistService.load(metaData));
//        loadedAgentData.ifPresent(optional -> useLoadedToReplaceInit(result, optional));
        return result;
    }
    
    private void useLoadedToReplaceInit(final AgentData initAgentData, final AgentData loadedAgentData) {
        for (Entry<String, AgentDatabaseData> entry : initAgentData.getDatabaseData().entrySet()) {
            if (loadedAgentData.getDatabaseData().containsKey(entry.getKey())) {
                useLoadedToReplaceInitByDatabaseData(entry.getValue(), loadedAgentData.getDatabaseData().get(entry.getKey()));
            }
        }
    }
    
    private void useLoadedToReplaceInitByDatabaseData(final AgentDatabaseData initDatabaseData, final AgentDatabaseData loadedDatabaseData) {
        for (Entry<String, AgentSchemaData> entry : initDatabaseData.getSchemaData().entrySet()) {
            if (loadedDatabaseData.getSchemaData().containsKey(entry.getKey())) {
                useLoadedToReplaceInitBySchemaData(entry.getValue(), loadedDatabaseData.getSchemaData().get(entry.getKey()));
            }
        }
    }
    
    private void useLoadedToReplaceInitBySchemaData(final AgentSchemaData initSchemaData, final AgentSchemaData loadedSchemaData) {
        for (Entry<String, AgentTableData> entry : initSchemaData.getTableData().entrySet()) {
            if (loadedSchemaData.getTableData().containsKey(entry.getKey())) {
                entry.setValue(loadedSchemaData.getTableData().get(entry.getKey()));
            }
        }
    }
    
    @Override
    public void close() {
//        persistService.getRepository().close();
//        metaData.getGlobalRuleMetaData().findRules(ResourceHeldRule.class).forEach(ResourceHeldRule::closeStaleResource);
//        metaData.getDatabases().values().forEach(each -> each.getRuleMetaData().findRules(ResourceHeldRule.class).forEach(ResourceHeldRule::closeStaleResource));
    }
}
