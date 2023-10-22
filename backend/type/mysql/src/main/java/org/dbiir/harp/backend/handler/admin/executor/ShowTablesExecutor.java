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
import org.dbiir.harp.backend.util.RegularUtils;
import org.dbiir.harp.executor.sql.execute.result.query.QueryResult;
import org.dbiir.harp.executor.sql.execute.result.query.QueryResultMetaData;
import org.dbiir.harp.executor.sql.execute.result.query.impl.raw.metadata.RawQueryResultColumnMetaData;
import org.dbiir.harp.executor.sql.execute.result.query.impl.raw.metadata.RawQueryResultMetaData;
import org.dbiir.harp.executor.sql.execute.result.query.impl.raw.type.RawMemoryQueryResult;
import org.dbiir.harp.executor.sql.execute.result.query.type.memory.row.MemoryQueryResultDataRow;
import org.dbiir.harp.merger.result.MergedResult;
import org.dbiir.harp.merger.result.impl.transparent.TransparentMergedResult;
import org.dbiir.harp.utils.common.database.type.DatabaseType;
import org.dbiir.harp.utils.common.statement.mysql.dal.MySQLShowTablesStatement;
import org.dbiir.harp.utils.common.util.SQLUtils;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentTable;

import java.sql.Types;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Show tables executor.
 */
@RequiredArgsConstructor
public final class ShowTablesExecutor implements DatabaseAdminQueryExecutor {
    
    private static final String TABLE_TYPE = "BASE TABLE";
    
    private final MySQLShowTablesStatement showTablesStatement;
    
    private final DatabaseType databaseType;
    
    @Getter
    private QueryResultMetaData queryResultMetaData;
    
    @Getter
    private MergedResult mergedResult;
    
    @Override
    public void execute(final ConnectionSession connectionSession) {
        String databaseName = showTablesStatement.getFromSchema().map(schema -> schema.getSchema().getIdentifier().getValue()).orElseGet(connectionSession::getDatabaseName);
        queryResultMetaData = createQueryResultMetaData(databaseName);
        mergedResult = new TransparentMergedResult(getQueryResult(databaseName));
    }
    
    private QueryResultMetaData createQueryResultMetaData(final String databaseName) {
        List<RawQueryResultColumnMetaData> columnNames = new LinkedList<>();
        String tableColumnName = String.format("Tables_in_%s", databaseName);
        columnNames.add(new RawQueryResultColumnMetaData("", tableColumnName, tableColumnName, Types.VARCHAR, "VARCHAR", 255, 0));
        columnNames.add(new RawQueryResultColumnMetaData("", "Table_type", "Table_type", Types.VARCHAR, "VARCHAR", 20, 0));
        return new RawQueryResultMetaData(columnNames);
    }
    
    private QueryResult getQueryResult(final String databaseName) {
        if (!databaseType.getSystemSchemas().contains(databaseName) && !ProxyContext.getInstance().getDatabase(databaseName).isComplete()) {
            return new RawMemoryQueryResult(queryResultMetaData, Collections.emptyList());
        }
        List<MemoryQueryResultDataRow> rows = getAllTableNames(databaseName).stream().map(each -> {
            List<Object> rowValues = new LinkedList<>();
            rowValues.add(each);
            rowValues.add(TABLE_TYPE);
            return new MemoryQueryResultDataRow(rowValues);
        }).collect(Collectors.toList());
        return new RawMemoryQueryResult(queryResultMetaData, rows);
    }
    
    private Collection<String> getAllTableNames(final String databaseName) {
        Collection<String> result = ProxyContext.getInstance()
                .getDatabase(databaseName).getSchema(databaseName).getTables().values().stream().map(AgentTable::getName).collect(Collectors.toList());
        if (showTablesStatement.getFilter().isPresent()) {
            Optional<String> pattern = showTablesStatement.getFilter().get().getLike().map(optional -> SQLUtils.convertLikePatternToRegex(optional.getPattern()));
            return pattern.isPresent() ? result.stream().filter(each -> RegularUtils.matchesCaseInsensitive(pattern.get(), each)).collect(Collectors.toList()) : result;
        }
        return result;
    }
}
