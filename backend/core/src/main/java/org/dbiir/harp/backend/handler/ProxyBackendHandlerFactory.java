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

package org.dbiir.harp.backend.handler;

import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.dbiir.harp.backend.context.ProxyContext;
import org.dbiir.harp.backend.handler.admin.DatabaseAdminBackendHandlerFactory;
import org.dbiir.harp.backend.handler.data.DatabaseBackendHandlerFactory;
import org.dbiir.harp.backend.handler.extra.ExtraProxyBackendHandler;
import org.dbiir.harp.backend.handler.skip.SkipBackendHandler;
import org.dbiir.harp.backend.handler.transaction.TransactionBackendHandlerFactory;
import org.dbiir.harp.backend.session.ConnectionSession;
import org.dbiir.harp.kernel.authority.AuthorityRule;
import org.dbiir.harp.kernel.authority.checker.AuthorityChecker;
import org.dbiir.harp.kernel.core.util.AutoCommitUtils;
import org.dbiir.harp.parser.rule.SQLParserRule;
import org.dbiir.harp.utils.binder.QueryContext;
import org.dbiir.harp.utils.binder.SQLStatementContextFactory;
import org.dbiir.harp.utils.binder.statement.SQLStatementContext;
import org.dbiir.harp.utils.common.database.type.DatabaseType;
import org.dbiir.harp.utils.common.hint.HintValueContext;
import org.dbiir.harp.utils.common.spi.HarpServiceLoader;
import org.dbiir.harp.utils.common.statement.SQLStatement;
import org.dbiir.harp.utils.common.statement.dal.EmptyStatement;
import org.dbiir.harp.utils.common.statement.dal.FlushStatement;
import org.dbiir.harp.utils.common.statement.mysql.dal.MySQLShowCreateUserStatement;
import org.dbiir.harp.utils.common.statement.tcl.TCLStatement;
import org.dbiir.harp.utils.common.util.SQLUtils;
import org.dbiir.harp.utils.exceptions.external.sql.type.generic.UnsupportedSQLOperationException;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Optional;

/**
 * Proxy backend handler factory.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ProxyBackendHandlerFactory {
    
    /**
     * Create new instance of backend handler.
     *
     * @param databaseType database type
     * @param sql SQL to be executed
     * @param connectionSession connection session
     * @return created instance
     * @throws SQLException SQL exception
     */
    public static ProxyBackendHandler newInstance(final DatabaseType databaseType, final String sql, final ConnectionSession connectionSession) throws SQLException {
        if (Strings.isNullOrEmpty(SQLUtils.trimComment(sql))) {
            return new SkipBackendHandler(new EmptyStatement());
        }
        SQLParserRule sqlParserRule = ProxyContext.getInstance().getContextManager().getMetaDataContexts().getMetaData().getGlobalRuleMetaData().getSingleRule(SQLParserRule.class);
        SQLStatement sqlStatement = sqlParserRule.getSQLParserEngine(getProtocolType(databaseType, connectionSession).getType()).parse(sql);
        return newInstance(databaseType, sql, sqlStatement, connectionSession, new HintValueContext());
    }
    
    /**
     * Create new instance of backend handler.
     *
     * @param databaseType database type
     * @param sql SQL to be executed
     * @param sqlStatement SQL statement
     * @param connectionSession connection session
     * @param hintValueContext hint query context
     * @return created instance
     * @throws SQLException SQL exception
     */
    public static ProxyBackendHandler newInstance(final DatabaseType databaseType, final String sql, final SQLStatement sqlStatement,
                                                  final ConnectionSession connectionSession, final HintValueContext hintValueContext) throws SQLException {
        if (sqlStatement instanceof EmptyStatement) {
            return new SkipBackendHandler(sqlStatement);
        }
        SQLStatementContext<?> sqlStatementContext =SQLStatementContextFactory.newInstance(ProxyContext.getInstance().getContextManager().getMetaDataContexts().getMetaData(), sqlStatement, connectionSession.getDefaultDatabaseName());
        QueryContext queryContext = new QueryContext(sqlStatementContext, sql, Collections.emptyList(), hintValueContext);
        connectionSession.setQueryContext(queryContext);
        return newInstance(databaseType, queryContext, connectionSession, false);
    }
    
    /**
     * Create new instance of backend handler.
     *
     * @param databaseType database type
     * @param queryContext query context
     * @param connectionSession connection session
     * @param preferPreparedStatement use prepared statement as possible
     * @return created instance
     * @throws SQLException SQL exception
     */
    @SuppressWarnings("unchecked")
    public static ProxyBackendHandler newInstance(final DatabaseType databaseType, final QueryContext queryContext, final ConnectionSession connectionSession,
                                                  final boolean preferPreparedStatement) throws SQLException {
        SQLStatementContext<?> sqlStatementContext = queryContext.getSqlStatementContext();
        SQLStatement sqlStatement = sqlStatementContext.getSqlStatement();
        databaseType.handleRollbackOnly(connectionSession.getTransactionStatus().isRollbackOnly(), sqlStatement);
        checkUnsupportedSQLStatement(sqlStatement);
        if (sqlStatement instanceof EmptyStatement) {
            return new SkipBackendHandler(sqlStatement);
        }
        String sql = queryContext.getSql();
        handleAutoCommit(sqlStatement, connectionSession);
        if (sqlStatement instanceof TCLStatement) {
            return TransactionBackendHandlerFactory.newInstance((SQLStatementContext<TCLStatement>) sqlStatementContext, sql, connectionSession);
        }
        Optional<ProxyBackendHandler> backendHandler = DatabaseAdminBackendHandlerFactory.newInstance(databaseType, sqlStatementContext, connectionSession, sql);
        if (backendHandler.isPresent()) {
            return backendHandler.get();
        }
        Optional<ExtraProxyBackendHandler> extraHandler = findExtraProxyBackendHandler(sqlStatement);
        if (extraHandler.isPresent()) {
            return extraHandler.get();
        }
        String databaseName = sqlStatementContext.getTablesContext().getDatabaseName().isPresent()
                ? sqlStatementContext.getTablesContext().getDatabaseName().get()
                : connectionSession.getDatabaseName();
        AuthorityRule authorityRule = ProxyContext.getInstance().getContextManager().getMetaDataContexts().getMetaData().getGlobalRuleMetaData().getSingleRule(AuthorityRule.class);
        new AuthorityChecker(authorityRule, connectionSession.getGrantee()).checkPrivileges(databaseName, sqlStatementContext.getSqlStatement());
        backendHandler = DatabaseAdminBackendHandlerFactory.newInstance(databaseType, sqlStatementContext, connectionSession);
        return backendHandler.orElseGet(() -> DatabaseBackendHandlerFactory.newInstance(queryContext, connectionSession, preferPreparedStatement));
    }

    private static DatabaseType getProtocolType(final DatabaseType defaultDatabaseType, final ConnectionSession connectionSession) {
        String databaseName = connectionSession.getDatabaseName();
        return Strings.isNullOrEmpty(databaseName) || !ProxyContext.getInstance().databaseExists(databaseName)
                ? defaultDatabaseType
                : ProxyContext.getInstance().getContextManager().getMetaDataContexts().getMetaData().getDatabase(databaseName).getProtocolType();
    }
    
    private static void handleAutoCommit(final SQLStatement sqlStatement, final ConnectionSession connectionSession) {
        if (AutoCommitUtils.needOpenTransaction(sqlStatement)) {
            connectionSession.getBackendConnection().handleAutoCommit();
        }
    }
    
    private static Optional<ExtraProxyBackendHandler> findExtraProxyBackendHandler(final SQLStatement sqlStatement) {
        for (ExtraProxyBackendHandler each : HarpServiceLoader.getServiceInstances(ExtraProxyBackendHandler.class)) {
            if (each.accept(sqlStatement)) {
                return Optional.of(each);
            }
        }
        return Optional.empty();
    }

    private static void checkUnsupportedSQLStatement(final SQLStatement sqlStatement) {
        if (sqlStatement instanceof FlushStatement || sqlStatement instanceof MySQLShowCreateUserStatement) {
            throw new UnsupportedSQLOperationException("Unsupported operation");
        }
    }
}
