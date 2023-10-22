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

package org.dbiir.harp.backend.connector.jdbc.executor;

import lombok.RequiredArgsConstructor;

import org.dbiir.harp.backend.connector.DatabaseConnector;
import org.dbiir.harp.backend.connector.jdbc.executor.callback.ProxyJDBCExecutorCallbackFactory;
import org.dbiir.harp.backend.context.ProxyContext;
import org.dbiir.harp.backend.session.ConnectionSession;
import org.dbiir.harp.executor.kernel.model.ExecutionGroupContext;
import org.dbiir.harp.executor.sql.execute.engine.driver.jdbc.JDBCExecutionUnit;
import org.dbiir.harp.executor.sql.execute.engine.driver.jdbc.JDBCExecutor;
import org.dbiir.harp.executor.sql.execute.result.ExecuteResult;
import org.dbiir.harp.executor.sql.process.ExecuteProcessEngine;
import org.dbiir.harp.mode.metadata.MetaDataContexts;
import org.dbiir.harp.utils.binder.QueryContext;
import org.dbiir.harp.utils.binder.statement.SQLStatementContext;
import org.dbiir.harp.utils.common.database.type.DatabaseType;
import org.dbiir.harp.utils.common.metadata.database.AgentDatabase;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Proxy JDBC executor.
 */
@RequiredArgsConstructor
public final class ProxyJDBCExecutor {
    
    private final String type;
    
    private final ConnectionSession connectionSession;
    
    private final DatabaseConnector databaseConnector;
    
    private final JDBCExecutor jdbcExecutor;
    
    /**
     * Execute.
     * 
     * @param queryContext query context
     * @param executionGroupContext execution group context
     * @param isReturnGeneratedKeys is return generated keys
     * @param isExceptionThrown is exception thrown
     * @return execute results
     * @throws SQLException SQL exception
     */
    public List<ExecuteResult> execute(final QueryContext queryContext, final ExecutionGroupContext<JDBCExecutionUnit> executionGroupContext,
                                       final boolean isReturnGeneratedKeys, final boolean isExceptionThrown) throws SQLException {
        ExecuteProcessEngine executeProcessEngine = new ExecuteProcessEngine();
        try {
            MetaDataContexts metaDataContexts = ProxyContext.getInstance().getContextManager().getMetaDataContexts();
            AgentDatabase database = metaDataContexts.getMetaData().getDatabase(connectionSession.getDatabaseName());
            DatabaseType protocolType = database.getProtocolType();
            Map<String, DatabaseType> storageTypes = database.getResourceMetaData().getStorageTypes();
            executeProcessEngine.initializeExecution(executionGroupContext, queryContext);
            SQLStatementContext<?> context = queryContext.getSqlStatementContext();
            return jdbcExecutor.execute(executionGroupContext,
                    ProxyJDBCExecutorCallbackFactory.newInstance(type, protocolType, storageTypes, context.getSqlStatement(), databaseConnector, isReturnGeneratedKeys, isExceptionThrown,
                            true),
                    ProxyJDBCExecutorCallbackFactory.newInstance(type, protocolType, storageTypes, context.getSqlStatement(), databaseConnector, isReturnGeneratedKeys, isExceptionThrown,
                            false));
        } finally {
            executeProcessEngine.cleanExecution();
        }
    }
}
