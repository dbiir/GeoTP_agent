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

package org.dbiir.harp.backend.response.header.query;


import org.dbiir.harp.backend.response.header.query.QueryHeader;
import org.dbiir.harp.backend.response.header.query.QueryHeaderBuilder;
import org.dbiir.harp.executor.sql.execute.result.query.QueryResultMetaData;
import org.dbiir.harp.utils.common.metadata.database.AgentDatabase;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentSchema;
import org.dbiir.harp.utils.common.rule.identifier.type.DataNodeContainedRule;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentColumn;

import java.sql.SQLException;
import java.util.Optional;

/**
 * Query header builder for MySQL.
 */
public final class MySQLQueryHeaderBuilder implements QueryHeaderBuilder {
    
    @Override
    public QueryHeader build(final QueryResultMetaData queryResultMetaData,
                             final AgentDatabase database, final String columnName, final String columnLabel, final int columnIndex) throws SQLException {
        String schemaName = null == database ? "" : database.getName();
        String actualTableName = queryResultMetaData.getTableName(columnIndex);
        String tableName;
        boolean primaryKey;
        if (null == actualTableName || null == database) {
            tableName = actualTableName;
            primaryKey = false;
        } else {
            tableName = getLogicTableName(database, actualTableName);
            AgentSchema schema = database.getSchema(schemaName);
            primaryKey = null != schema
                    && Optional.ofNullable(schema.getTable(tableName)).map(optional -> optional.getColumns().get(columnName.toLowerCase())).map(AgentColumn::isPrimaryKey).orElse(false);
        }
        int columnType = queryResultMetaData.getColumnType(columnIndex);
        String columnTypeName = queryResultMetaData.getColumnTypeName(columnIndex);
        int columnLength = queryResultMetaData.getColumnLength(columnIndex);
        int decimals = queryResultMetaData.getDecimals(columnIndex);
        boolean signed = queryResultMetaData.isSigned(columnIndex);
        boolean notNull = queryResultMetaData.isNotNull(columnIndex);
        boolean autoIncrement = queryResultMetaData.isAutoIncrement(columnIndex);
        return new QueryHeader(schemaName, tableName, columnLabel, columnName, columnType, columnTypeName, columnLength, decimals, signed, primaryKey, notNull, autoIncrement);
    }
    
    private String getLogicTableName(final AgentDatabase database, final String actualTableName) {
        for (DataNodeContainedRule each : database.getRuleMetaData().findRules(DataNodeContainedRule.class)) {
            Optional<String> logicTable = each.findLogicTableByActualTable(actualTableName);
            if (logicTable.isPresent()) {
                return logicTable.get();
            }
        }
        return actualTableName;
    }
    
    @Override
    public String getType() {
        return "MySQL";
    }
    
    // TODO to be confirmed, QueryHeaderBuilder should not has default value, just throw unsupported exception if database type missing
    @Override
    public boolean isDefault() {
        return true;
    }
}
