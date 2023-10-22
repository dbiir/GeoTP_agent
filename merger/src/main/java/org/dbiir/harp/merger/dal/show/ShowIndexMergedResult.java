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

package org.dbiir.harp.merger.dal.show;


import org.dbiir.harp.executor.sql.execute.result.query.QueryResult;
import org.dbiir.harp.merger.result.impl.memory.MemoryMergedResult;
import org.dbiir.harp.merger.result.impl.memory.MemoryQueryResultRow;
import org.dbiir.harp.utils.binder.statement.SQLStatementContext;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentSchema;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * Merged result for show index.
 */
public final class ShowIndexMergedResult extends MemoryMergedResult {
    
    public ShowIndexMergedResult(final SQLStatementContext<?> sqlStatementContext, final AgentSchema schema, final List<QueryResult> queryResults) throws SQLException {
        super(schema, sqlStatementContext, queryResults);
    }
    
    @Override
    protected List<MemoryQueryResultRow> init(final AgentSchema schema,
                                              final SQLStatementContext<?> sqlStatementContext, final List<QueryResult> queryResults) throws SQLException {
        List<MemoryQueryResultRow> result = new LinkedList<>();
        for (QueryResult each : queryResults) {
            while (each.next()) {
                MemoryQueryResultRow memoryResultSetRow = new MemoryQueryResultRow(each);
//                String actualTableName = memoryResultSetRow.getCell(1).toString();
//                String actualIndexName = memoryResultSetRow.getCell(3).toString();
//                Optional<TableRule> tableRule = shardingRule.findTableRuleByActualTable(actualTableName);
//                tableRule.ifPresent(optional -> memoryResultSetRow.setCell(1, optional.getLogicTable()));
//                memoryResultSetRow.setCell(3, IndexMetaDataUtils.getLogicIndexName(actualIndexName, actualTableName));
                result.add(memoryResultSetRow);
            }
        }
        return result;
    }
}
