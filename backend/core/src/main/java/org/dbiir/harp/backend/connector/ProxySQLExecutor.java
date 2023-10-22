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

package org.dbiir.harp.backend.connector;

import org.dbiir.harp.backend.connector.jdbc.executor.ProxyJDBCExecutor;
import org.dbiir.harp.backend.connector.jdbc.statement.JDBCBackendStatement;
import org.dbiir.harp.backend.connector.sane.SaneQueryResultEngine;
import org.dbiir.harp.backend.context.BackendExecutorContext;
import org.dbiir.harp.backend.context.ProxyContext;
import org.dbiir.harp.backend.session.ConnectionSession;
import org.dbiir.harp.executor.kernel.ExecutorEngine;
import org.dbiir.harp.executor.kernel.model.ExecutionGroupContext;
import org.dbiir.harp.executor.kernel.model.ExecutionGroupReportContext;
import org.dbiir.harp.executor.sql.context.ExecutionContext;
import org.dbiir.harp.executor.sql.execute.engine.SQLExecutorExceptionHandler;
import org.dbiir.harp.executor.sql.execute.engine.driver.jdbc.JDBCExecutionUnit;
import org.dbiir.harp.executor.sql.execute.engine.driver.jdbc.JDBCExecutor;
import org.dbiir.harp.executor.sql.execute.engine.raw.RawExecutor;
import org.dbiir.harp.executor.sql.execute.result.ExecuteResult;
import org.dbiir.harp.executor.sql.prepare.driver.DriverExecutionPrepareEngine;
import org.dbiir.harp.executor.sql.prepare.driver.jdbc.StatementOption;
import org.dbiir.harp.kernel.transaction.spi.TransactionHook;
import org.dbiir.harp.utils.common.config.props.ConfigurationPropertyKey;
import org.dbiir.harp.utils.common.database.type.DatabaseType;
import org.dbiir.harp.utils.common.rule.AgentRule;
import org.dbiir.harp.utils.common.spi.HarpServiceLoader;
import org.dbiir.harp.utils.common.spi.type.typed.TypedSPILoader;
import org.dbiir.harp.utils.common.statement.SQLStatement;
import org.dbiir.harp.utils.common.statement.mysql.dml.MySQLInsertStatement;
import org.dbiir.harp.utils.context.ConnectionContext;
import org.dbiir.harp.utils.context.transaction.TransactionConnectionContext;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Proxy SQL Executor.
 */
public final class ProxySQLExecutor {
    
    private final String type;
    
    private final BackendConnection backendConnection;
    
    private final ProxyJDBCExecutor jdbcExecutor;
    
    private final RawExecutor rawExecutor;
    
    private final Collection<TransactionHook> transactionHooks = HarpServiceLoader.getServiceInstances(TransactionHook.class);
    
    public ProxySQLExecutor(final String type, final BackendConnection backendConnection, final DatabaseConnector databaseConnector) {
        this.type = type;
        this.backendConnection = backendConnection;
        ExecutorEngine executorEngine = BackendExecutorContext.getInstance().getExecutorEngine();
        ConnectionContext connectionContext = backendConnection.getConnectionSession().getConnectionContext();
        jdbcExecutor = new ProxyJDBCExecutor(type, backendConnection.getConnectionSession(), databaseConnector, new JDBCExecutor(executorEngine, connectionContext));
        rawExecutor = new RawExecutor(executorEngine, connectionContext);
    }
    
//    private boolean isValidExecutePrerequisites(final ExecutionContext executionContext) {
//        return !isExecuteDDLInXATransaction(executionContext.getSqlStatementContext().getSqlStatement())
//                && !isExecuteDDLInPostgreSQLOpenGaussTransaction(executionContext.getSqlStatementContext().getSqlStatement());
//    }
    
//    private boolean isExecuteDDLInXATransaction(final SQLStatement sqlStatement) {
//        TransactionStatus transactionStatus = backendConnection.getConnectionSession().getTransactionStatus();
//        return TransactionType.XA == transactionStatus.getTransactionType() && transactionStatus.isInTransaction() && isUnsupportedDDLStatement(sqlStatement);
//    }
    
    private boolean isExecuteDDLInPostgreSQLOpenGaussTransaction(final SQLStatement sqlStatement) {
        return false;
    }
    
//    private static String getTableName(final ExecutionContext executionContext) {
//        return executionContext.getSqlStatementContext() instanceof TableAvailable && !((TableAvailable) executionContext.getSqlStatementContext()).getAllTables().isEmpty()
//                ? ((TableAvailable) executionContext.getSqlStatementContext()).getAllTables().iterator().next().getTableName().getIdentifier().getValue()
//                : "unknown_table";
//    }

    /**
     * Execute SQL.
     *
     * @param executionContext execution context
     * @return execute results
     * @throws SQLException SQL exception
     */
    public List<ExecuteResult> execute(final ExecutionContext executionContext) throws SQLException {
        String databaseName = backendConnection.getConnectionSession().getDatabaseName();
        Collection<AgentRule> rules = ProxyContext.getInstance().getContextManager().getMetaDataContexts().getMetaData().getDatabase(databaseName).getRuleMetaData().getRules();
        int maxConnectionsSizePerQuery = ProxyContext.getInstance()
                .getContextManager().getMetaDataContexts().getMetaData().getProps().<Integer>getValue(ConfigurationPropertyKey.MAX_CONNECTIONS_SIZE_PER_QUERY);
        boolean isReturnGeneratedKeys = executionContext.getSqlStatementContext().getSqlStatement() instanceof MySQLInsertStatement;
        return useDriverToExecute(executionContext, rules, maxConnectionsSizePerQuery, isReturnGeneratedKeys, SQLExecutorExceptionHandler.isExceptionThrown());
    }
    
//    private boolean hasRawExecutionRule(final Collection<ShardingSphereRule> rules) {
//        for (ShardingSphereRule each : rules) {
//            if (each instanceof RawExecutionRule) {
//                return true;
//            }
//        }
//        return false;
//    }
    
//    private List<ExecuteResult> rawExecute(final ExecutionContext executionContext, final Collection<ShardingSphereRule> rules, final int maxConnectionsSizePerQuery) throws SQLException {
//        RawExecutionPrepareEngine prepareEngine = new RawExecutionPrepareEngine(maxConnectionsSizePerQuery, rules);
//        ExecutionGroupContext<RawSQLExecutionUnit> executionGroupContext;
//        try {
//            executionGroupContext = prepareEngine.prepare(executionContext.getRouteContext(), executionContext.getExecutionUnits(), new ExecutionGroupReportContext(
//                    backendConnection.getConnectionSession().getDatabaseName(), backendConnection.getConnectionSession().getExecutionId()));
//        } catch (final SQLException ex) {
//            return getSaneExecuteResults(executionContext, ex);
//        }
//        // TODO handle query header
//        return rawExecutor.execute(executionGroupContext, executionContext.getQueryContext(), new RawSQLExecutorCallback());
//    }
    
    private List<ExecuteResult> useDriverToExecute(final ExecutionContext executionContext, final Collection<AgentRule> rules, final int maxConnectionsSizePerQuery,
                                                   final boolean isReturnGeneratedKeys, final boolean isExceptionThrown) throws SQLException {
        JDBCBackendStatement statementManager = (JDBCBackendStatement) backendConnection.getConnectionSession().getStatementManager();
        DriverExecutionPrepareEngine<JDBCExecutionUnit, Connection> prepareEngine = new DriverExecutionPrepareEngine<>(
                type, maxConnectionsSizePerQuery, backendConnection, statementManager, new StatementOption(isReturnGeneratedKeys), rules,
                ProxyContext.getInstance().getDatabase(backendConnection.getConnectionSession().getDatabaseName()).getResourceMetaData().getStorageTypes());
        ExecutionGroupContext<JDBCExecutionUnit> executionGroupContext;
        try {
            executionGroupContext = prepareEngine.prepare(executionContext.getRouteContext(), executionContext.getExecutionUnits(), new ExecutionGroupReportContext(
                    backendConnection.getConnectionSession().getDatabaseName(), backendConnection.getConnectionSession().getGrantee(), backendConnection.getConnectionSession().getExecutionId()));
        } catch (final SQLException ex) {
            return getSaneExecuteResults(executionContext, ex);
        }

        SQLStatement sqlStatement = executionContext.getQueryContext().getSqlStatementContext().getSqlStatement();

        executeTransactionHooksBeforeExecuteSQL(backendConnection.getConnectionSession());

        List<ExecuteResult> results;
        try {
            results = jdbcExecutor.execute(executionContext.getQueryContext(), executionGroupContext, isReturnGeneratedKeys, isExceptionThrown);
        } catch (Exception ex) {
            throw ex;
        }
        return results;
    }

    private void executeTransactionHooksBeforeExecuteSQL(final ConnectionSession connectionSession) throws SQLException {
        if (!getTransactionContext(connectionSession).isInTransaction()) {
            return;
        }
        for (TransactionHook each : transactionHooks) {
            each.beforeExecuteSQL(connectionSession.getBackendConnection().getCachedConnections().values(), getTransactionContext(connectionSession), connectionSession.getIsolationLevel());
        }
    }
    
    private TransactionConnectionContext getTransactionContext(final ConnectionSession connectionSession) {
        return connectionSession.getBackendConnection().getConnectionSession().getConnectionContext().getTransactionContext();
    }
    
    private List<ExecuteResult> getSaneExecuteResults(final ExecutionContext executionContext, final SQLException originalException) throws SQLException {
        DatabaseType databaseType = ProxyContext.getInstance().getDatabase(backendConnection.getConnectionSession().getDatabaseName()).getProtocolType();
        Optional<ExecuteResult> executeResult = TypedSPILoader.getService(SaneQueryResultEngine.class, databaseType.getType())
                .getSaneQueryResult(executionContext.getSqlStatementContext().getSqlStatement(), originalException);
        if (executeResult.isPresent()) {
            return Collections.singletonList(executeResult.get());
        }
        throw originalException;
    }
}
