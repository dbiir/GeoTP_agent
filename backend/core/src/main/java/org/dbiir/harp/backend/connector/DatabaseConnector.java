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

import org.dbiir.harp.backend.connector.jdbc.statement.JDBCBackendStatement;
import org.dbiir.harp.backend.connector.jdbc.transaction.BackendTransactionManager;
import org.dbiir.harp.backend.context.ProxyContext;
import org.dbiir.harp.backend.exception.RuleNotExistedException;
import org.dbiir.harp.backend.handler.data.DatabaseBackendHandler;
import org.dbiir.harp.backend.response.data.QueryResponseCell;
import org.dbiir.harp.backend.response.data.QueryResponseRow;
import org.dbiir.harp.backend.response.header.ResponseHeader;
import org.dbiir.harp.backend.response.header.query.QueryHeader;
import org.dbiir.harp.backend.response.header.query.QueryHeaderBuilderEngine;
import org.dbiir.harp.backend.response.header.query.QueryResponseHeader;
import org.dbiir.harp.backend.response.header.update.UpdateResponseHeader;
import org.dbiir.harp.backend.session.ConnectionSession;
import org.dbiir.harp.backend.session.transaction.TransactionStatus;
import org.dbiir.harp.executor.kernel.context.KernelProcessor;
import org.dbiir.harp.executor.sql.context.ExecutionContext;
import org.dbiir.harp.executor.sql.execute.engine.driver.jdbc.JDBCExecutionUnit;
import org.dbiir.harp.executor.sql.execute.result.query.QueryResult;
import org.dbiir.harp.executor.sql.execute.result.update.UpdateResult;
import org.dbiir.harp.executor.sql.prepare.driver.DriverExecutionPrepareEngine;
import org.dbiir.harp.executor.sql.prepare.driver.jdbc.StatementOption;
import org.dbiir.harp.kernel.transaction.api.TransactionType;
import org.dbiir.harp.merger.MergeEngine;
import org.dbiir.harp.merger.result.MergedResult;
import org.dbiir.harp.mode.metadata.MetaDataContexts;
import org.dbiir.harp.utils.binder.QueryContext;
import org.dbiir.harp.utils.binder.segment.insert.keygen.GeneratedKeyContext;
import org.dbiir.harp.utils.binder.statement.SQLStatementContext;
import org.dbiir.harp.utils.binder.statement.dml.InsertStatementContext;
import org.dbiir.harp.utils.common.config.props.ConfigurationPropertyKey;
import org.dbiir.harp.utils.common.metadata.database.AgentDatabase;
import org.dbiir.harp.utils.common.metadata.database.schema.util.SystemSchemaUtils;
import org.dbiir.harp.utils.common.rule.identifier.type.DataNodeContainedRule;
import org.dbiir.harp.utils.common.statement.SQLStatement;
import org.dbiir.harp.utils.common.statement.dml.DMLStatement;
import org.dbiir.harp.utils.common.statement.dml.SelectStatement;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Database connector.
 */
public final class DatabaseConnector implements DatabaseBackendHandler {
    
    private final ProxySQLExecutor proxySQLExecutor;
    
    private final Collection<Statement> cachedStatements = new CopyOnWriteArrayList<>();
    
    private final Collection<ResultSet> cachedResultSets = new CopyOnWriteArrayList<>();
    
    private final String driverType;
    
    private final AgentDatabase database;
    
    private final boolean transparentStatement;
    
    private final QueryContext queryContext;
    
    private final BackendConnection backendConnection;

    private List<QueryHeader> queryHeaders;

    private MergedResult mergedResult;

    public DatabaseConnector(final String driverType, final AgentDatabase database, final QueryContext queryContext, final BackendConnection backendConnection) {
        SQLStatementContext<?> sqlStatementContext = queryContext.getSqlStatementContext();
        failedIfBackendNotReady(backendConnection.getConnectionSession(), sqlStatementContext);
        this.driverType = driverType;
        this.database = database;
        this.transparentStatement = false;
        this.queryContext = queryContext;
        this.backendConnection = backendConnection;
        proxySQLExecutor = new ProxySQLExecutor(driverType, backendConnection, this);
    }
    
    private void failedIfBackendNotReady(final ConnectionSession connectionSession, final SQLStatementContext<?> sqlStatementContext) {
        AgentDatabase database = ProxyContext.getInstance().getDatabase(connectionSession.getDatabaseName());
        boolean isSystemSchema = SystemSchemaUtils.containsSystemSchema(sqlStatementContext.getDatabaseType(), sqlStatementContext.getTablesContext().getSchemaNames(), database);
        if (!isSystemSchema && !database.isComplete()) {
            throw new RuleNotExistedException(connectionSession.getDatabaseName());
        }
    }

    /**
     * Add statement.
     *
     * @param statement statement to be added
     */
    public void add(final Statement statement) {
        cachedStatements.add(statement);
    }
    
    /**
     * Add result set.
     *
     * @param resultSet result set to be added
     */
    public void add(final ResultSet resultSet) {
        cachedResultSets.add(resultSet);
    }
    
    /**
     * Execute to database.
     *
     * @return backend response
     * @throws SQLException SQL exception
     */
    @Override
    public List<ResponseHeader> execute() throws SQLException {
        Collection<ExecutionContext> executionContexts = generateExecutionContexts();
        return isNeedImplicitCommitTransaction(executionContexts) ? doExecuteWithImplicitCommitTransaction(executionContexts) : doExecute(executionContexts);
    }
    
    private Collection<ExecutionContext> generateExecutionContexts() {
        Collection<ExecutionContext> result = new LinkedList<>();
        MetaDataContexts metaDataContexts = ProxyContext.getInstance().getContextManager().getMetaDataContexts();
        ExecutionContext executionContext = new KernelProcessor().generateExecutionContext(queryContext, database, metaDataContexts.getMetaData().getGlobalRuleMetaData(),
                metaDataContexts.getMetaData().getProps(), backendConnection.getConnectionSession().getConnectionContext());
        result.add(executionContext);
        // TODO support logical SQL optimize to generate multiple logical SQL
        return result;
    }
    
    private boolean isNeedImplicitCommitTransaction(final Collection<ExecutionContext> executionContexts) {
        TransactionStatus transactionStatus = backendConnection.getConnectionSession().getTransactionStatus();
        if (!TransactionType.isDistributedTransaction(transactionStatus.getTransactionType()) || transactionStatus.isInTransaction()) {
            return false;
        }
        if (1 == executionContexts.size()) {
            SQLStatement sqlStatement = executionContexts.iterator().next().getSqlStatementContext().getSqlStatement();
            return isWriteDMLStatement(sqlStatement) && executionContexts.iterator().next().getExecutionUnits().size() > 1;
        }
        return executionContexts.stream().anyMatch(each -> isWriteDMLStatement(each.getSqlStatementContext().getSqlStatement()));
    }
    
    private static boolean isWriteDMLStatement(final SQLStatement sqlStatement) {
        return sqlStatement instanceof DMLStatement && !(sqlStatement instanceof SelectStatement);
    }
    
    private List<ResponseHeader> doExecuteWithImplicitCommitTransaction(final Collection<ExecutionContext> executionContexts) throws SQLException {
        List<ResponseHeader> result;
        BackendTransactionManager transactionManager = new BackendTransactionManager(backendConnection);
        try {
            transactionManager.begin();
            result = new LinkedList<>(doExecute(executionContexts));
            transactionManager.commit();
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            transactionManager.rollback();
            String databaseName = backendConnection.getConnectionSession().getDatabaseName();
            throw ex;
//            throw SQLExceptionTransformEngine.toSQLException(ex, ProxyContext.getInstance().getContextManager().getMetaDataContexts().getMetaData()
//                    .getDatabase(databaseName).getProtocolType().getType());
        }
        return result;
    }
    
    private List<ResponseHeader> doExecute(final Collection<ExecutionContext> executionContexts) throws SQLException {
        List<ResponseHeader> result = new LinkedList<>();
        for (ExecutionContext each : executionContexts) {
            ResponseHeader responseHeader = doExecute(each);
            if (result.size() == 0) {
                result.add(responseHeader);
            }
        }
        return result;
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    private ResponseHeader doExecute(final ExecutionContext executionContext) throws SQLException {
        if (executionContext.getExecutionUnits().isEmpty()) {
            return new UpdateResponseHeader(executionContext.getSqlStatementContext().getSqlStatement());
        }
        List result = proxySQLExecutor.execute(executionContext);

        Object executeResultSample = result.iterator().next();
        return executeResultSample instanceof QueryResult ? processExecuteQuery(executionContext, result, (QueryResult) executeResultSample) : processExecuteUpdate(executionContext, result);
    }

    private void checkAsyncPrepare() {

    }

//    private DriverExecutionPrepareEngine<JDBCExecutionUnit, Connection> createDriverExecutionPrepareEngine(final boolean isReturnGeneratedKeys, final MetaDataContexts metaData) {
//        int maxConnectionsSizePerQuery = metaData.getMetaData().getProps().<Integer>getValue(ConfigurationPropertyKey.MAX_CONNECTIONS_SIZE_PER_QUERY);
//        JDBCBackendStatement statementManager = (JDBCBackendStatement) backendConnection.getConnectionSession().getStatementManager();
//        return new DriverExecutionPrepareEngine<>(driverType, maxConnectionsSizePerQuery, backendConnection, statementManager,
//                new StatementOption(isReturnGeneratedKeys),
//                metaData.getMetaData().getDatabase(backendConnection.getConnectionSession().getDatabaseName()).getResourceMetaData().getStorageTypes());
//    }

    private QueryResponseHeader processExecuteQuery(final ExecutionContext executionContext, final List<QueryResult> queryResults, final QueryResult queryResultSample) throws SQLException {
        queryHeaders = createQueryHeaders(executionContext, queryResultSample);
        mergedResult = mergeQuery(executionContext.getSqlStatementContext(), queryResults);
        return new QueryResponseHeader(queryHeaders);
    }
    
    private List<QueryHeader> createQueryHeaders(final ExecutionContext executionContext, final QueryResult queryResultSample) throws SQLException {
        int columnCount = getColumnCount(executionContext, queryResultSample);
        List<QueryHeader> result = new ArrayList<>(columnCount);
        QueryHeaderBuilderEngine queryHeaderBuilderEngine = new QueryHeaderBuilderEngine(database.getProtocolType());
        for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
            result.add(createQueryHeader(queryHeaderBuilderEngine, executionContext, queryResultSample, database, columnIndex));
        }
        return result;
    }
    
    private int getColumnCount(final ExecutionContext executionContext, final QueryResult queryResultSample) throws SQLException {
        if (transparentStatement) {
            return queryResultSample.getMetaData().getColumnCount();
        }
        return queryResultSample.getMetaData().getColumnCount();
    }


    private QueryHeader createQueryHeader(final QueryHeaderBuilderEngine queryHeaderBuilderEngine, final ExecutionContext executionContext,
                                          final QueryResult queryResultSample, final AgentDatabase database, final int columnIndex) throws SQLException {
        return queryHeaderBuilderEngine.build(queryResultSample.getMetaData(), database, columnIndex);
    }

    private MergedResult mergeQuery(final SQLStatementContext<?> sqlStatementContext, final List<QueryResult> queryResults) throws SQLException {
        MergeEngine mergeEngine = new MergeEngine(database, ProxyContext.getInstance().getContextManager().getMetaDataContexts().getMetaData().getProps(),
                backendConnection.getConnectionSession().getConnectionContext());
        return mergeEngine.merge(queryResults, sqlStatementContext);
    }

    private UpdateResponseHeader processExecuteUpdate(final ExecutionContext executionContext, final Collection<UpdateResult> updateResults) {
        Optional<GeneratedKeyContext> generatedKeyContext = executionContext.getSqlStatementContext() instanceof InsertStatementContext
                ? ((InsertStatementContext) executionContext.getSqlStatementContext()).getGeneratedKeyContext()
                : Optional.empty();
        Collection<Comparable<?>> autoIncrementGeneratedValues =
                generatedKeyContext.filter(GeneratedKeyContext::isSupportAutoIncrement).map(GeneratedKeyContext::getGeneratedValues).orElseGet(Collections::emptyList);
        UpdateResponseHeader result = new UpdateResponseHeader(executionContext.getSqlStatementContext().getSqlStatement(), updateResults, autoIncrementGeneratedValues);
        mergeUpdateCount(executionContext.getSqlStatementContext(), result);
        return result;
    }

    private void mergeUpdateCount(final SQLStatementContext<?> sqlStatementContext, final UpdateResponseHeader response) {
        if (isNeedAccumulate(sqlStatementContext)) {
            response.mergeUpdateCount();
        }
    }

    private boolean isNeedAccumulate(final SQLStatementContext<?> sqlStatementContext) {
        Optional<DataNodeContainedRule> dataNodeContainedRule = database.getRuleMetaData().findSingleRule(DataNodeContainedRule.class);
        return dataNodeContainedRule.isPresent() && dataNodeContainedRule.get().isNeedAccumulate(sqlStatementContext.getTablesContext().getTableNames());
    }

    /**
     * Goto next result value.
     *
     * @return has more result value or not
     * @throws SQLException SQL exception
     */
    @Override
    public boolean next() throws SQLException {
        return null != mergedResult && mergedResult.next();
    }

    /**
     * Get query response row.
     *
     * @return query response row
     * @throws SQLException SQL exception
     */
    @Override
    public QueryResponseRow getRowData() throws SQLException {
        List<QueryResponseCell> cells = new ArrayList<>(queryHeaders.size());
        for (int columnIndex = 1; columnIndex <= queryHeaders.size(); columnIndex++) {
            Object data = mergedResult.getValue(columnIndex, Object.class);
            cells.add(new QueryResponseCell(queryHeaders.get(columnIndex - 1).getColumnType(), data));
        }
        return new QueryResponseRow(cells);
    }
    
    /**
     * Close database connector.
     *
     * @throws SQLException SQL exception
     */
    @Override
    public void close() throws SQLException {
        Collection<SQLException> result = new LinkedList<>();
        result.addAll(closeResultSets());
        result.addAll(closeStatements());
        if (result.isEmpty()) {
            return;
        }
        SQLException ex = new SQLException();
        result.forEach(ex::setNextException);
        throw ex;
    }

    public SQLStatement getSqlStatement() {
        return queryContext.getSqlStatementContext().getSqlStatement();
    }
    
    private Collection<SQLException> closeResultSets() {
        Collection<SQLException> result = new LinkedList<>();
        for (ResultSet each : cachedResultSets) {
            try {
                each.close();
            } catch (final SQLException ex) {
                result.add(ex);
            }
        }
        cachedResultSets.clear();
        return result;
    }
    
    private Collection<SQLException> closeStatements() {
        Collection<SQLException> result = new LinkedList<>();
        for (Statement each : cachedStatements) {
            try {
                each.cancel();
                each.close();
            } catch (final SQLException ex) {
                result.add(ex);
            }
        }
        cachedStatements.clear();
        return result;
    }
}
