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

package org.dbiir.harp.backend.handler.admin.executor;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.dbiir.harp.backend.context.ProxyContext;
import org.dbiir.harp.backend.session.ConnectionSession;
import org.dbiir.harp.executor.sql.execute.result.query.QueryResult;
import org.dbiir.harp.executor.sql.execute.result.query.QueryResultMetaData;
import org.dbiir.harp.executor.sql.execute.result.query.impl.raw.metadata.RawQueryResultColumnMetaData;
import org.dbiir.harp.executor.sql.execute.result.query.impl.raw.metadata.RawQueryResultMetaData;
import org.dbiir.harp.executor.sql.execute.result.query.impl.raw.type.RawMemoryQueryResult;
import org.dbiir.harp.executor.sql.execute.result.query.type.memory.row.MemoryQueryResultDataRow;
import org.dbiir.harp.merger.result.MergedResult;
import org.dbiir.harp.merger.result.impl.transparent.TransparentMergedResult;
import org.dbiir.harp.utils.common.statement.mysql.dal.MySQLShowCreateDatabaseStatement;
import org.dbiir.harp.utils.exceptions.external.UnknownDatabaseException;

import java.sql.Types;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Show create database executor.
 */
@RequiredArgsConstructor
@Getter
public final class ShowCreateDatabaseExecutor implements DatabaseAdminQueryExecutor {
    
    private static final String CREATE_DATABASE_PATTERN = "CREATE DATABASE `%s`;";
    
    private static final String DATABASE = "Database";
    
    private static final String CREATE_DATABASE = "CREATE DATABASE ";
    
    private final MySQLShowCreateDatabaseStatement showCreateDatabaseStatement;
    
    private QueryResultMetaData queryResultMetaData;
    
    private MergedResult mergedResult;
    
    @Override
    public void execute(final ConnectionSession connectionSession) {
        queryResultMetaData = createQueryResultMetaData();
        mergedResult = new TransparentMergedResult(getQueryResult(showCreateDatabaseStatement.getDatabaseName()));
    }
    
    private QueryResult getQueryResult(final String databaseName) {
        if (!ProxyContext.getInstance().databaseExists(databaseName)) {
            throw new UnknownDatabaseException(databaseName);
        }
        List<MemoryQueryResultDataRow> rows = new LinkedList<>();
        rows.add(new MemoryQueryResultDataRow(Arrays.asList(databaseName, String.format(CREATE_DATABASE_PATTERN, databaseName))));
        return new RawMemoryQueryResult(queryResultMetaData, rows);
    }
    
    private QueryResultMetaData createQueryResultMetaData() {
        List<RawQueryResultColumnMetaData> columnMetaData = Arrays.asList(new RawQueryResultColumnMetaData("", DATABASE, DATABASE, Types.VARCHAR, "VARCHAR", 255, 0),
                new RawQueryResultColumnMetaData("", CREATE_DATABASE, CREATE_DATABASE, Types.VARCHAR, "VARCHAR", 255, 0));
        return new RawQueryResultMetaData(columnMetaData);
    }
}
