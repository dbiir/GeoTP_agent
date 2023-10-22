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
import org.dbiir.harp.backend.connector.DatabaseConnector;
import org.dbiir.harp.backend.connector.DatabaseConnectorFactory;
import org.dbiir.harp.backend.context.ProxyContext;
import org.dbiir.harp.backend.response.header.ResponseHeader;
import org.dbiir.harp.backend.response.header.query.QueryResponseHeader;
import org.dbiir.harp.backend.session.ConnectionSession;
import org.dbiir.harp.executor.sql.execute.result.query.QueryResult;
import org.dbiir.harp.executor.sql.execute.result.query.QueryResultMetaData;
import org.dbiir.harp.executor.sql.execute.result.query.impl.raw.metadata.RawQueryResultColumnMetaData;
import org.dbiir.harp.executor.sql.execute.result.query.impl.raw.metadata.RawQueryResultMetaData;
import org.dbiir.harp.executor.sql.execute.result.query.impl.raw.type.RawMemoryQueryResult;
import org.dbiir.harp.executor.sql.execute.result.query.type.memory.row.MemoryQueryResultDataRow;
import org.dbiir.harp.merger.result.MergedResult;
import org.dbiir.harp.merger.result.impl.transparent.TransparentMergedResult;
import org.dbiir.harp.utils.binder.QueryContext;
import org.dbiir.harp.utils.binder.SQLStatementContextFactory;
import org.dbiir.harp.utils.binder.statement.SQLStatementContext;
import org.dbiir.harp.utils.common.statement.dml.SelectStatement;
import org.dbiir.harp.utils.exceptions.external.NoDatabaseSelectedException;
import org.dbiir.harp.backend.response.header.query.QueryHeader;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Unicast resource show executor.
 */
@RequiredArgsConstructor
@Getter
public final class UnicastResourceShowExecutor implements DatabaseAdminQueryExecutor {
    
    private final DatabaseConnectorFactory databaseConnectorFactory = DatabaseConnectorFactory.getInstance();
    
    private final SelectStatement sqlStatement;
    
    private final String sql;
    
    private MergedResult mergedResult;
    
    private DatabaseConnector databaseConnector;
    
    private List<ResponseHeader> responseHeader;
    
    @Override
    public void execute(final ConnectionSession connectionSession) throws SQLException {
        String originDatabase = connectionSession.getDatabaseName();
        String databaseName = null == originDatabase ? getFirstDatabaseName() : originDatabase;
        try {
            connectionSession.setCurrentDatabase(databaseName);
            SQLStatementContext<?> sqlStatementContext = SQLStatementContextFactory.newInstance(ProxyContext.getInstance().getContextManager().getMetaDataContexts().getMetaData(),
                    sqlStatement, connectionSession.getDefaultDatabaseName());
            databaseConnector = databaseConnectorFactory.newInstance(new QueryContext(sqlStatementContext, sql, Collections.emptyList()),
                    connectionSession.getBackendConnection(), false);
            responseHeader = databaseConnector.execute();
            mergedResult = new TransparentMergedResult(createQueryResult());
        } finally {
            connectionSession.setCurrentDatabase(originDatabase);
            databaseConnector.close();
        }
    }
    
    private String getFirstDatabaseName() {
        Collection<String> databaseNames = ProxyContext.getInstance().getAllDatabaseNames();
        if (databaseNames.isEmpty()) {
            throw new NoDatabaseSelectedException();
        }
        Optional<String> result = databaseNames.stream().filter(each -> ProxyContext.getInstance().getDatabase(each).containsDataSource()).findFirst();
        return result.get();
    }
    
    @Override
    public QueryResultMetaData getQueryResultMetaData() {
        // TODO: get query result meta data by index(command id)
        List<RawQueryResultColumnMetaData> columns = ((QueryResponseHeader) responseHeader.get(0)).getQueryHeaders().stream().map(QueryHeader::getColumnLabel)
                .map(each -> new RawQueryResultColumnMetaData("", each, each, Types.VARCHAR, "VARCHAR", 100, 0))
                .collect(Collectors.toList());
        return new RawQueryResultMetaData(columns);
    }
    
    private QueryResult createQueryResult() throws SQLException {
        List<MemoryQueryResultDataRow> rows = new LinkedList<>();
        while (databaseConnector.next()) {
            List<Object> data = databaseConnector.getRowData().getData();
            rows.add(new MemoryQueryResultDataRow(data));
        }
        return new RawMemoryQueryResult(getQueryResultMetaData(), rows);
    }
}
