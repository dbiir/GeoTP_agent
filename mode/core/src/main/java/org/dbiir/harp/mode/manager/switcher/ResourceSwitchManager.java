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

package org.dbiir.harp.mode.manager.switcher;


import org.dbiir.harp.utils.common.datasource.pool.creator.DataSourcePoolCreator;
import org.dbiir.harp.utils.common.datasource.props.DataSourceProperties;
import org.dbiir.harp.utils.common.datasource.props.DataSourcePropertiesCreator;
import org.dbiir.harp.utils.common.metadata.database.resource.AgentResourceMetaData;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * Resource switch manager.
 */
public final class ResourceSwitchManager {
    
    /**
     * Create switching resource.
     * 
     * @param resourceMetaData resource meta data
     * @param toBeChangedDataSourceProps to be changed data source properties map
     * @return created switching resource
     */
    public SwitchingResource create(final AgentResourceMetaData resourceMetaData, final Map<String, DataSourceProperties> toBeChangedDataSourceProps) {
        return new SwitchingResource(resourceMetaData, createNewDataSources(resourceMetaData, toBeChangedDataSourceProps), getStaleDataSources(resourceMetaData, toBeChangedDataSourceProps));
    }
    
    /**
     * Create switching resource by drop resource.
     *
     * @param resourceMetaData resource meta data
     * @param toBeDeletedDataSourceProps to be deleted data source properties map
     * @return created switching resource
     */
    public SwitchingResource createByDropResource(final AgentResourceMetaData resourceMetaData, final Map<String, DataSourceProperties> toBeDeletedDataSourceProps) {
        return new SwitchingResource(resourceMetaData, Collections.emptyMap(), getStaleDataSources(resourceMetaData.getDataSources(), toBeDeletedDataSourceProps));
    }
    
    /**
     * Create switching resource by alter data source props.
     *
     * @param resourceMetaData resource meta data
     * @param toBeChangedDataSourceProps to be changed data source properties map
     * @return created switching resource
     */
    public SwitchingResource createByAlterDataSourceProps(final AgentResourceMetaData resourceMetaData, final Map<String, DataSourceProperties> toBeChangedDataSourceProps) {
        Map<String, DataSource> staleDataSources = getStaleDataSources(resourceMetaData, toBeChangedDataSourceProps);
        staleDataSources.putAll(getToBeDeletedDataSources(resourceMetaData, toBeChangedDataSourceProps));
        return new SwitchingResource(resourceMetaData, createNewDataSources(resourceMetaData, toBeChangedDataSourceProps), staleDataSources);
    }
    
    private Map<String, DataSource> createNewDataSources(final AgentResourceMetaData resourceMetaData, final Map<String, DataSourceProperties> toBeChangedDataSourceProps) {
        Map<String, DataSource> result = new LinkedHashMap<>(resourceMetaData.getDataSources());
        result.keySet().removeAll(getToBeDeletedDataSources(resourceMetaData, toBeChangedDataSourceProps).keySet());
        result.putAll(createToBeChangedDataSources(resourceMetaData, toBeChangedDataSourceProps));
        result.putAll(createToBeAddedDataSources(resourceMetaData, toBeChangedDataSourceProps));
        return result;
    }
    
    private Map<String, DataSource> createToBeChangedDataSources(final AgentResourceMetaData resourceMetaData, final Map<String, DataSourceProperties> toBeChangedDataSourceProps) {
        return DataSourcePoolCreator.create(getChangedDataSourceProperties(resourceMetaData, toBeChangedDataSourceProps));
    }
    
    private Map<String, DataSourceProperties> getChangedDataSourceProperties(final AgentResourceMetaData resourceMetaData,
                                                                             final Map<String, DataSourceProperties> toBeChangedDataSourceProps) {
        return toBeChangedDataSourceProps.entrySet().stream().filter(entry -> isModifiedDataSource(resourceMetaData.getDataSources(), entry.getKey(), entry.getValue()))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue, (oldValue, currentValue) -> oldValue, LinkedHashMap::new));
    }
    
    private boolean isModifiedDataSource(final Map<String, DataSource> originalDataSources, final String dataSourceName, final DataSourceProperties dataSourceProps) {
        return originalDataSources.containsKey(dataSourceName) && !dataSourceProps.equals(DataSourcePropertiesCreator.create(originalDataSources.get(dataSourceName)));
    }
    
    private Map<String, DataSource> createToBeAddedDataSources(final AgentResourceMetaData resourceMetaData, final Map<String, DataSourceProperties> toBeChangedDataSourceProps) {
        Map<String, DataSourceProperties> toBeAddedDataSourceProps = toBeChangedDataSourceProps.entrySet().stream()
                .filter(entry -> !resourceMetaData.getDataSources().containsKey(entry.getKey())).collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        return DataSourcePoolCreator.create(toBeAddedDataSourceProps);
    }
    
    private Map<String, DataSource> getStaleDataSources(final AgentResourceMetaData resourceMetaData, final Map<String, DataSourceProperties> toBeChangedDataSourceProps) {
        Map<String, DataSource> result = new LinkedHashMap<>(resourceMetaData.getDataSources().size(), 1);
        result.putAll(getToBeChangedDataSources(resourceMetaData, toBeChangedDataSourceProps));
        return result;
    }
    
    private Map<String, DataSource> getStaleDataSources(final Map<String, DataSource> dataSources, final Map<String, DataSourceProperties> toBeDeletedDataSourceProps) {
        return dataSources.entrySet().stream().filter(entry -> toBeDeletedDataSourceProps.containsKey(entry.getKey())).collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }
    
    private Map<String, DataSource> getToBeDeletedDataSources(final AgentResourceMetaData resourceMetaData, final Map<String, DataSourceProperties> toBeChangedDataSourceProps) {
        return resourceMetaData.getDataSources().entrySet().stream().filter(entry -> !toBeChangedDataSourceProps.containsKey(entry.getKey())).collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }
    
    private Map<String, DataSource> getToBeChangedDataSources(final AgentResourceMetaData resourceMetaData, final Map<String, DataSourceProperties> toBeChangedDataSourceProps) {
        Map<String, DataSourceProperties> changedDataSourceProps = getChangedDataSourceProperties(resourceMetaData, toBeChangedDataSourceProps);
        return resourceMetaData.getDataSources().entrySet().stream().filter(entry -> changedDataSourceProps.containsKey(entry.getKey())).collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }
}
