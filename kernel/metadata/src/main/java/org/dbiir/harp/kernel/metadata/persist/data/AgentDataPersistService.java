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

package org.dbiir.harp.kernel.metadata.persist.data;

import lombok.Getter;
import org.dbiir.harp.kernel.metadata.persist.node.AgentDataNode;
import org.dbiir.harp.kernel.metadata.persist.service.schema.AgentTableRowDataPersistService;
import org.dbiir.harp.mode.spi.PersistRepository;
import org.dbiir.harp.utils.common.metadata.AgentMetaData;
import org.dbiir.harp.utils.common.metadata.data.AgentData;
import org.dbiir.harp.utils.common.metadata.data.AgentDatabaseData;
import org.dbiir.harp.utils.common.metadata.data.AgentSchemaData;
import org.dbiir.harp.utils.common.metadata.database.AgentDatabase;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentSchema;
import org.dbiir.harp.utils.common.yaml.data.pojo.YamlAgentRowData;
import org.dbiir.harp.utils.common.yaml.data.swapper.YamlAgentRowDataSwapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Agent data persist service.
 */
@Getter
public final class AgentDataPersistService {
    
    private final PersistRepository repository;
    
    private final AgentTableRowDataPersistService tableRowDataPersistService;
    
    public AgentDataPersistService(final PersistRepository repository) {
        this.repository = repository;
        tableRowDataPersistService = new AgentTableRowDataPersistService(repository);
    }
    
    /**
     * Load Agent data.
     *
     * @param metaData meta data
     * @return Agent data
     */
    public Optional<AgentData> load(final AgentMetaData metaData) {
        Collection<String> databaseNames = repository.getChildrenKeys(AgentDataNode.getAgentDataNodePath());
        if (databaseNames.isEmpty()) {
            return Optional.empty();
        }
        AgentData result = new AgentData();
        for (String each : databaseNames.stream().filter(metaData::containsDatabase).collect(Collectors.toList())) {
            result.getDatabaseData().put(each, load(each, metaData.getDatabase(each)));
        }
        return Optional.of(result);
    }
    
    private AgentDatabaseData load(final String databaseName, final AgentDatabase database) {
        Collection<String> schemaNames = repository.getChildrenKeys(AgentDataNode.getSchemasPath(databaseName));
        if (schemaNames.isEmpty()) {
            return new AgentDatabaseData();
        }
        AgentDatabaseData result = new AgentDatabaseData();
        for (String each : schemaNames.stream().filter(database::containsSchema).collect(Collectors.toList())) {
            result.getSchemaData().put(each, load(databaseName, each, database.getSchema(each)));
        }
        return result;
    }
    
    private AgentSchemaData load(final String databaseName, final String schemaName, final AgentSchema schema) {
        Collection<String> tableNames = repository.getChildrenKeys(AgentDataNode.getTablesPath(databaseName, schemaName));
        if (tableNames.isEmpty()) {
            return new AgentSchemaData();
        }
        AgentSchemaData result = new AgentSchemaData();
        for (String each : tableNames.stream().filter(schema::containsTable).collect(Collectors.toList())) {
            result.getTableData().put(each, tableRowDataPersistService.load(databaseName, schemaName, each, schema.getTable(each)));
            
        }
        return result;
    }
    
    /**
     * Persist table.
     * @param databaseName database name
     * @param schemaName schema name
     * @param schemaData schema data
     * @param databases databases
     */
    public void persist(final String databaseName, final String schemaName, final AgentSchemaData schemaData, final Map<String, AgentDatabase> databases) {
        if (schemaData.getTableData().isEmpty()) {
            persistSchema(databaseName, schemaName);
        }
        persistTableData(databaseName, schemaName, schemaData, databases);
    }
    
    private void persistSchema(final String databaseName, final String schemaName) {
        repository.persist(AgentDataNode.getSchemaDataPath(databaseName, schemaName), "");
    }
    
    private void persistTableData(final String databaseName, final String schemaName, final AgentSchemaData schemaData, final Map<String, AgentDatabase> databases) {
        schemaData.getTableData().values().forEach(each -> {
            YamlAgentRowDataSwapper swapper =
                    new YamlAgentRowDataSwapper(new ArrayList<>(databases.get(databaseName.toLowerCase()).getSchema(schemaName).getTable(each.getName()).getColumns().values()));
            persistTableData(databaseName, schemaName, each.getName(), each.getRows().stream().map(swapper::swapToYamlConfiguration).collect(Collectors.toList()));
        });
    }
    
    private void persistTableData(final String databaseName, final String schemaName, final String tableName, final Collection<YamlAgentRowData> rows) {
        tableRowDataPersistService.persist(databaseName, schemaName, tableName, rows);
    }
}
