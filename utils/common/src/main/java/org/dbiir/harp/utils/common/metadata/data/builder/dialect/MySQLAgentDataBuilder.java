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

package org.dbiir.harp.utils.common.metadata.data.builder.dialect;


import org.dbiir.harp.utils.common.metadata.AgentMetaData;
import org.dbiir.harp.utils.common.metadata.data.*;
import org.dbiir.harp.utils.common.metadata.data.builder.AgentDataBuilder;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentSchema;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentTable;

import java.util.Collections;
import java.util.Map.Entry;
import java.util.Optional;

/**
 * MySQL Agent data Builder.
 */

public final class MySQLAgentDataBuilder implements AgentDataBuilder {
    
    private static final String SHARDING_SPHERE = "shardingsphere";
    
    private static final String CLUSTER_INFORMATION = "cluster_information";
    
    @Override
    public AgentData build(final AgentMetaData metaData) {
        AgentData result = new AgentData();
        Optional<AgentSchema> shardingSphereSchema = Optional.ofNullable(metaData.getDatabase(SHARDING_SPHERE)).map(database -> database.getSchema(SHARDING_SPHERE));
        if (!shardingSphereSchema.isPresent()) {
            return result;
        }
        AgentSchemaData schemaData = new AgentSchemaData();
        for (Entry<String, AgentTable> entry : shardingSphereSchema.get().getTables().entrySet()) {
            AgentTableData tableData = new AgentTableData(entry.getValue().getName());
            schemaData.getTableData().put(entry.getKey(), tableData);
        }
        AgentDatabaseData databaseData = new AgentDatabaseData();
        databaseData.getSchemaData().put(SHARDING_SPHERE, schemaData);
        result.getDatabaseData().put(SHARDING_SPHERE, databaseData);
        return result;
    }
    
    @Override
    public String getType() {
        return "MySQL";
    }
}
