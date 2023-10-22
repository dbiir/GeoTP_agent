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
import org.dbiir.harp.merger.result.impl.memory.MemoryQueryResultRow;
import org.dbiir.harp.utils.binder.statement.SQLStatementContext;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentConstraint;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentSchema;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentTable;

import java.sql.SQLException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;

/**
 * Merged result for show create table.
 */
public final class ShowCreateTableMergedResult extends LogicTablesMergedResult {
    
    public ShowCreateTableMergedResult(final SQLStatementContext<?> sqlStatementContext, final AgentSchema schema, final List<QueryResult> queryResults) throws SQLException {
        super(sqlStatementContext, schema, queryResults);
    }

    // not implement shardingRule
//    @Override
//    protected void setCellValue(final MemoryQueryResultRow memoryResultSetRow, final String logicTableName, final String actualTableName,
//                                final AgentTable table) {
//        memoryResultSetRow.setCell(2, memoryResultSetRow.getCell(2).toString().replaceFirst(actualTableName, logicTableName));
//        for (String each : table.getIndexes().keySet()) {
//            String actualIndexName = IndexMetaDataUtils.getActualIndexName(each, actualTableName);
//            memoryResultSetRow.setCell(2, memoryResultSetRow.getCell(2).toString().replace(actualIndexName, each));
//        }
//        for (Entry<String, AgentConstraint> entry : table.getConstrains().entrySet()) {
//            String actualIndexName = IndexMetaDataUtils.getActualIndexName(entry.getKey(), actualTableName);
//            memoryResultSetRow.setCell(2, memoryResultSetRow.getCell(2).toString().replace(actualIndexName, entry.getKey()));
//            Optional<TableRule> tableRule = shardingRule.findTableRule(entry.getValue().getReferencedTableName());
//            if (!tableRule.isPresent()) {
//                continue;
//            }
//            for (DataNode dataNode : tableRule.get().getActualDataNodes()) {
//                memoryResultSetRow.setCell(2, memoryResultSetRow.getCell(2).toString().replace(dataNode.getTableName(), entry.getValue().getReferencedTableName()));
//            }
//            assert (false);
//        }
//    }
}
