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

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.dbiir.harp.backend.handler.ProxyBackendHandler;
import org.dbiir.harp.backend.handler.admin.executor.DatabaseAdminExecutor;
import org.dbiir.harp.backend.handler.admin.executor.DatabaseAdminExecutorCreator;
import org.dbiir.harp.backend.handler.admin.executor.DatabaseAdminQueryExecutor;
import org.dbiir.harp.backend.session.ConnectionSession;
import org.dbiir.harp.utils.binder.statement.SQLStatementContext;
import org.dbiir.harp.utils.common.database.type.DatabaseType;
import org.dbiir.harp.utils.common.spi.type.typed.TypedSPILoader;

import java.util.Optional;

/**
 * Database admin backend handler factory.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DatabaseAdminBackendHandlerFactory {
    
    /**
     * Create new instance of database admin backend handler, and this handler requires a connection containing a schema to be used.
     *
     * @param databaseType database type
     * @param sqlStatementContext SQL statement context
     * @param connectionSession connection session
     * @return created instance
     */
    public static Optional<ProxyBackendHandler> newInstance(final DatabaseType databaseType, final SQLStatementContext<?> sqlStatementContext, final ConnectionSession connectionSession) {
        Optional<DatabaseAdminExecutorCreator> executorCreator = TypedSPILoader.findService(DatabaseAdminExecutorCreator.class, databaseType.getType());
        if (!executorCreator.isPresent()) {
            return Optional.empty();
        }
        Optional<DatabaseAdminExecutor> executor = executorCreator.get().create(sqlStatementContext);
        return executor.map(optional -> createProxyBackendHandler(sqlStatementContext, connectionSession, optional));
    }
    
    /**
     * Create new instance of database admin backend handler.
     *
     * @param databaseType database type
     * @param sqlStatementContext SQL statement context
     * @param connectionSession connection session
     * @param sql SQL being executed
     * @return created instance
     */
    public static Optional<ProxyBackendHandler> newInstance(final DatabaseType databaseType,
                                                            final SQLStatementContext<?> sqlStatementContext, final ConnectionSession connectionSession, final String sql) {
        Optional<DatabaseAdminExecutorCreator> executorCreator = TypedSPILoader.findService(DatabaseAdminExecutorCreator.class, databaseType.getType());
        if (!executorCreator.isPresent()) {
            return Optional.empty();
        }
        Optional<DatabaseAdminExecutor> executor = executorCreator.get().create(sqlStatementContext, sql, connectionSession.getDatabaseName());
        return executor.map(optional -> createProxyBackendHandler(sqlStatementContext, connectionSession, optional));
    }
    
    private static ProxyBackendHandler createProxyBackendHandler(final SQLStatementContext<?> sqlStatementContext, final ConnectionSession connectionSession, final DatabaseAdminExecutor executor) {
        return executor instanceof DatabaseAdminQueryExecutor
                ? new DatabaseAdminQueryBackendHandler(connectionSession, (DatabaseAdminQueryExecutor) executor)
                : new DatabaseAdminUpdateBackendHandler(connectionSession, sqlStatementContext.getSqlStatement(), executor);
    }
}
