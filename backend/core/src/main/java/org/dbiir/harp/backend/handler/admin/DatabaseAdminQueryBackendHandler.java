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

package org.dbiir.harp.backend.handler.admin;

import lombok.RequiredArgsConstructor;
import org.dbiir.harp.backend.context.ProxyContext;
import org.dbiir.harp.backend.handler.ProxyBackendHandler;
import org.dbiir.harp.backend.handler.admin.executor.DatabaseAdminQueryExecutor;
import org.dbiir.harp.backend.response.data.QueryResponseCell;
import org.dbiir.harp.backend.response.data.QueryResponseRow;
import org.dbiir.harp.backend.response.header.ResponseHeader;
import org.dbiir.harp.backend.response.header.query.QueryHeader;
import org.dbiir.harp.backend.response.header.query.QueryHeaderBuilderEngine;
import org.dbiir.harp.backend.response.header.query.QueryResponseHeader;
import org.dbiir.harp.backend.session.ConnectionSession;
import org.dbiir.harp.executor.sql.execute.result.query.QueryResultMetaData;
import org.dbiir.harp.merger.result.MergedResult;
import org.dbiir.harp.utils.common.database.type.DatabaseType;
import org.dbiir.harp.utils.common.metadata.database.AgentDatabase;


import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Database admin query backend handler.
 */
@RequiredArgsConstructor
public final class DatabaseAdminQueryBackendHandler implements ProxyBackendHandler {
    
    private final ConnectionSession connectionSession;
    
    private final DatabaseAdminQueryExecutor executor;
    
    private QueryResultMetaData queryResultMetaData;

    private MergedResult mergedResult;

    @Override
    public List<ResponseHeader> execute() throws SQLException {
        List<ResponseHeader> result = new LinkedList<ResponseHeader>();
        executor.execute(connectionSession);
        queryResultMetaData = executor.getQueryResultMetaData();
        mergedResult = executor.getMergedResult();
        result.add(new QueryResponseHeader(createResponseHeader()));
        return result;
    }
    
    private List<QueryHeader> createResponseHeader() throws SQLException {
        List<QueryHeader> result = new ArrayList<>(queryResultMetaData.getColumnCount());
        AgentDatabase database = null == connectionSession.getDatabaseName() ? null : ProxyContext.getInstance().getDatabase(connectionSession.getDatabaseName());
        DatabaseType databaseType = null == database ? connectionSession.getProtocolType() : database.getProtocolType();
        QueryHeaderBuilderEngine queryHeaderBuilderEngine = new QueryHeaderBuilderEngine(databaseType);
        for (int columnIndex = 1; columnIndex <= queryResultMetaData.getColumnCount(); columnIndex++) {
            result.add(queryHeaderBuilderEngine.build(queryResultMetaData, database, columnIndex));
        }
        return result;
    }
    
    @Override
    public boolean next() throws SQLException {
        return mergedResult.next();
    }
    
    @Override
    public QueryResponseRow getRowData() throws SQLException {
        List<QueryResponseCell> result = new ArrayList<>(queryResultMetaData.getColumnCount());
        for (int columnIndex = 1; columnIndex <= queryResultMetaData.getColumnCount(); columnIndex++) {
            result.add(new QueryResponseCell(queryResultMetaData.getColumnType(columnIndex), mergedResult.getValue(columnIndex, Object.class)));
        }
        return new QueryResponseRow(result);
    }
}
