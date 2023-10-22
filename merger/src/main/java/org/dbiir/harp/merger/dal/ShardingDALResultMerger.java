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

package org.dbiir.harp.merger.dal;

import lombok.RequiredArgsConstructor;
import org.dbiir.harp.executor.sql.execute.result.query.QueryResult;
import org.dbiir.harp.merger.dal.show.LogicTablesMergedResult;
import org.dbiir.harp.merger.dal.show.ShowCreateTableMergedResult;
import org.dbiir.harp.merger.dal.show.ShowIndexMergedResult;
import org.dbiir.harp.merger.dal.show.ShowTableStatusMergedResult;
import org.dbiir.harp.merger.merger.ResultMerger;
import org.dbiir.harp.merger.result.MergedResult;
import org.dbiir.harp.merger.result.impl.local.LocalDataMergedResult;
import org.dbiir.harp.merger.result.impl.local.LocalDataQueryResultRow;
import org.dbiir.harp.merger.result.impl.transparent.TransparentMergedResult;
import org.dbiir.harp.utils.binder.statement.SQLStatementContext;
import org.dbiir.harp.utils.common.database.type.DatabaseTypeEngine;
import org.dbiir.harp.utils.common.metadata.database.AgentDatabase;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentSchema;
import org.dbiir.harp.utils.common.statement.SQLStatement;
import org.dbiir.harp.utils.common.statement.mysql.dal.*;
import org.dbiir.harp.utils.context.ConnectionContext;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 * DAL result merger for Sharding.
 */
@RequiredArgsConstructor
public final class ShardingDALResultMerger implements ResultMerger {
    
    private final String databaseName;
    
    @Override
    public MergedResult merge(final List<QueryResult> queryResults, final SQLStatementContext<?> sqlStatementContext,
                              final AgentDatabase database, final ConnectionContext connectionContext) throws SQLException {
        SQLStatement dalStatement = sqlStatementContext.getSqlStatement();
        String schemaName = sqlStatementContext.getTablesContext().getSchemaName().orElseGet(() -> DatabaseTypeEngine.getDefaultSchemaName(sqlStatementContext.getDatabaseType(), database.getName()));
        if (dalStatement instanceof MySQLShowDatabasesStatement) {
            return new LocalDataMergedResult(Collections.singleton(new LocalDataQueryResultRow(databaseName)));
        }
        AgentSchema schema = database.getSchema(schemaName);
        if (dalStatement instanceof MySQLShowTablesStatement) {
            return new LogicTablesMergedResult(sqlStatementContext, schema, queryResults);
        }
        if (dalStatement instanceof MySQLShowTableStatusStatement) {
            return new ShowTableStatusMergedResult(sqlStatementContext, schema, queryResults);
        }
        if (dalStatement instanceof MySQLShowIndexStatement) {
            return new ShowIndexMergedResult(sqlStatementContext, schema, queryResults);
        }
        if (dalStatement instanceof MySQLShowCreateTableStatement) {
            return new ShowCreateTableMergedResult(sqlStatementContext, schema, queryResults);
        }
        return new TransparentMergedResult(queryResults.get(0));
    }
}
