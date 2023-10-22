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

package org.dbiir.harp.kernel.metadata.persist.service.schema;

import com.google.common.base.Strings;
import lombok.RequiredArgsConstructor;
import org.dbiir.harp.kernel.metadata.persist.node.AgentDataNode;
import org.dbiir.harp.mode.spi.PersistRepository;
import org.dbiir.harp.utils.common.metadata.data.AgentTableData;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentTable;
import org.dbiir.harp.utils.common.yaml.YamlEngine;
import org.dbiir.harp.utils.common.yaml.data.pojo.YamlAgentRowData;
import org.dbiir.harp.utils.common.yaml.data.swapper.YamlAgentRowDataSwapper;


import java.util.ArrayList;
import java.util.Collection;

/**
 * Agent table row data persist service.
 */
@RequiredArgsConstructor
public final class AgentTableRowDataPersistService {
    
    private final PersistRepository repository;
    
    /**
     * Persist table row data.
     *
     * @param databaseName database name
     * @param schemaName schema name
     * @param tableName table name
     * @param rows rows
     */
    public void persist(final String databaseName, final String schemaName, final String tableName, final Collection<YamlAgentRowData> rows) {
        if (rows.isEmpty()) {
            persistTable(databaseName, schemaName, tableName);
        }
        rows.forEach(each -> repository.persist(AgentDataNode.getTableRowPath(databaseName, schemaName, tableName.toLowerCase(), each.getUniqueKey()), YamlEngine.marshal(each)));
    }
    
    private void persistTable(final String databaseName, final String schemaName, final String tableName) {
        repository.persist(AgentDataNode.getTablePath(databaseName, schemaName, tableName.toLowerCase()), "");
    }
    
    /**
     * Delete table row data.
     *
     * @param databaseName database name
     * @param schemaName schema name
     * @param tableName table name
     * @param rows rows
     */
    public void delete(final String databaseName, final String schemaName, final String tableName, final Collection<YamlAgentRowData> rows) {
        rows.forEach(each -> repository.delete(AgentDataNode.getTableRowPath(databaseName, schemaName, tableName.toLowerCase(), each.getUniqueKey())));
    }
    
    /**
     * Load table data.
     *
     * @param databaseName database name
     * @param schemaName schema name
     * @param tableName table name
     * @param table table
     * @return Agent table data
     */
    public AgentTableData load(final String databaseName, final String schemaName, final String tableName, final AgentTable table) {
        AgentTableData result = new AgentTableData(tableName);
        YamlAgentRowDataSwapper swapper = new YamlAgentRowDataSwapper(new ArrayList<>(table.getColumns().values()));
        for (String each : repository.getChildrenKeys(AgentDataNode.getTablePath(databaseName, schemaName, tableName))) {
            String yamlRow = repository.getDirectly(AgentDataNode.getTableRowPath(databaseName, schemaName, tableName, each));
            if (!Strings.isNullOrEmpty(yamlRow)) {
                result.getRows().add(swapper.swapToObject(YamlEngine.unmarshal(yamlRow, YamlAgentRowData.class)));
            }
        }
        return result;
    }
}
