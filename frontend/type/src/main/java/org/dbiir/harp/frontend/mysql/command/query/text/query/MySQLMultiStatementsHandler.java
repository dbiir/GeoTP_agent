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

package org.dbiir.harp.frontend.mysql.command.query.text.query;

import lombok.extern.slf4j.Slf4j;
import org.dbiir.harp.executor.kernel.context.KernelProcessor;
import org.dbiir.harp.mode.metadata.MetaDataContexts;
import org.dbiir.harp.parser.SQLParserEngine;
import org.dbiir.harp.parser.rule.SQLParserRule;
import org.dbiir.harp.utils.binder.QueryContext;
import org.dbiir.harp.utils.binder.SQLStatementContextFactory;
import org.dbiir.harp.utils.binder.statement.SQLStatementContext;
import org.dbiir.harp.utils.binder.statement.dml.SelectStatementContext;
import org.dbiir.harp.utils.binder.statement.dml.UpdateStatementContext;

import org.dbiir.harp.executor.kernel.model.ExecutionGroup;
import org.dbiir.harp.executor.kernel.model.ExecutionGroupContext;
import org.dbiir.harp.executor.kernel.model.ExecutionGroupReportContext;
import org.dbiir.harp.executor.sql.context.ExecutionContext;
import org.dbiir.harp.executor.sql.context.ExecutionUnit;
import org.dbiir.harp.executor.sql.execute.engine.ConnectionMode;
import org.dbiir.harp.executor.sql.execute.engine.SQLExecutorExceptionHandler;
import org.dbiir.harp.executor.sql.execute.engine.driver.jdbc.JDBCExecutionUnit;
import org.dbiir.harp.executor.sql.execute.engine.driver.jdbc.JDBCExecutor;
import org.dbiir.harp.executor.sql.execute.engine.driver.jdbc.JDBCExecutorCallback;
import org.dbiir.harp.executor.sql.execute.result.ExecuteResult;
import org.dbiir.harp.executor.sql.execute.result.query.QueryResult;
import org.dbiir.harp.executor.sql.execute.result.query.QueryResultMetaData;
import org.dbiir.harp.executor.sql.execute.result.query.impl.driver.jdbc.type.memory.JDBCMemoryQueryResult;
import org.dbiir.harp.executor.sql.execute.result.query.impl.driver.jdbc.type.stream.JDBCStreamQueryResult;
import org.dbiir.harp.executor.sql.execute.result.update.UpdateResult;
import org.dbiir.harp.executor.sql.prepare.driver.DriverExecutionPrepareEngine;
import org.dbiir.harp.executor.sql.prepare.driver.jdbc.JDBCDriverType;
import org.dbiir.harp.executor.sql.prepare.driver.jdbc.StatementOption;

import org.dbiir.harp.backend.connector.jdbc.statement.JDBCBackendStatement;
import org.dbiir.harp.backend.context.BackendExecutorContext;
import org.dbiir.harp.backend.context.ProxyContext;
import org.dbiir.harp.backend.handler.ProxyBackendHandler;
import org.dbiir.harp.backend.response.header.ResponseHeader;
import org.dbiir.harp.backend.response.header.query.QueryHeader;
import org.dbiir.harp.backend.response.header.query.QueryResponseHeader;
import org.dbiir.harp.backend.response.header.update.UpdateResponseHeader;
import org.dbiir.harp.backend.session.ConnectionSession;

import org.dbiir.harp.utils.common.config.props.ConfigurationPropertyKey;
import org.dbiir.harp.utils.common.database.type.DatabaseType;
import org.dbiir.harp.utils.common.metadata.database.AgentDatabase;
import org.dbiir.harp.utils.common.metadata.database.rule.AgentRuleMetaData;
import org.dbiir.harp.utils.common.rule.AgentRule;
import org.dbiir.harp.utils.common.spi.type.typed.TypedSPILoader;
import org.dbiir.harp.utils.common.statement.SQLStatement;
import org.dbiir.harp.utils.common.statement.dml.UpdateStatement;
import org.dbiir.harp.utils.common.util.SQLUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Handler for MySQL multi statements.
 */
@Slf4j
public final class MySQLMultiStatementsHandler implements ProxyBackendHandler {
    
    private static final Pattern MULTI_UPDATE_STATEMENTS = Pattern.compile(";(?=\\s*update)", Pattern.CASE_INSENSITIVE);
    
    private static final Pattern MULTI_DELETE_STATEMENTS = Pattern.compile(";(?=\\s*delete)", Pattern.CASE_INSENSITIVE);
    
    private final KernelProcessor kernelProcessor = new KernelProcessor();
    
    private final JDBCExecutor jdbcExecutor;
    
    private final ConnectionSession connectionSession;
    
    private final SQLStatement sqlStatementSample;
    
    private final List<SQLStatement> sqlStatements;
    
    private final MetaDataContexts metaDataContexts = ProxyContext.getInstance().getContextManager().getMetaDataContexts();
    
    private final Map<String, List<ExecutionUnit>> dataSourcesToExecutionUnits = new HashMap<>();
    
    private final Map<String, List<Integer>> dataSourcesToCommandId = new HashMap<>();
    
    private final Map<String, List<QueryContext>> dataSourcesToQueryContext = new HashMap<>();
    
    private ExecutionContext anyExecutionContext;
    
    private boolean isBatchInsert;
    
    public MySQLMultiStatementsHandler(final ConnectionSession connectionSession, final SQLStatement sqlStatementSample, final String sql, boolean isBatchInsert) {
        jdbcExecutor = new JDBCExecutor(BackendExecutorContext.getInstance().getExecutorEngine(), connectionSession.getConnectionContext());
        connectionSession.getBackendConnection().handleAutoCommit();
        this.connectionSession = connectionSession;
        this.sqlStatementSample = sqlStatementSample;
        Pattern pattern = sqlStatementSample instanceof UpdateStatement ? MULTI_UPDATE_STATEMENTS : MULTI_DELETE_STATEMENTS;
        SQLParserEngine sqlParserEngine = getSQLParserEngine();
        for (String each : extractMultiStatements(pattern, sql)) {
            SQLStatement eachSQLStatement = sqlParserEngine.parse(each);
            ExecutionContext executionContext = createExecutionContext(createQueryContext(each, eachSQLStatement));
            if (null == anyExecutionContext) {
                anyExecutionContext = executionContext;
            }
            for (ExecutionUnit eachExecutionUnit : executionContext.getExecutionUnits()) {
                dataSourcesToExecutionUnits.computeIfAbsent(eachExecutionUnit.getDataSourceName(), unused -> new LinkedList<>()).add(eachExecutionUnit);
            }
        }
        this.sqlStatements = null;
        this.isBatchInsert = isBatchInsert;
    }
    
    public MySQLMultiStatementsHandler(final ConnectionSession connectionSession, final List<SQLStatement> sqlStatements, final String sql, boolean isBatchInsert) {
        jdbcExecutor = new JDBCExecutor(BackendExecutorContext.getInstance().getExecutorEngine(), connectionSession.getConnectionContext());
        connectionSession.getBackendConnection().handleAutoCommit();
        this.connectionSession = connectionSession;
        this.sqlStatements = sqlStatements;
        this.sqlStatementSample = null;
        this.isBatchInsert = isBatchInsert;
        // Pattern pattern = sqlStatementSample instanceof UpdateStatement ? MULTI_UPDATE_STATEMENTS : MULTI_DELETE_STATEMENTS;
        List<String> sqls = SQLUtils.splitMultiSQL(sql);
        
        assert (sqlStatements.size() == sqls.size());
        
        Map<String, List<ExecutionUnit>> groupExecuteUnits = new HashMap<>();
        for (int i = 0; i < sqlStatements.size(); i++) {
            ExecutionContext executionContext = createExecutionContext(createQueryContext(sqls.get(i), sqlStatements.get(i)));
            String dataSourceName = "";
            if (null == anyExecutionContext) {
                anyExecutionContext = executionContext;
            }
            for (ExecutionUnit eachExecutionUnit : executionContext.getExecutionUnits()) {
                dataSourceName = eachExecutionUnit.getDataSourceName();
                groupExecuteUnits.computeIfAbsent(dataSourceName, unused -> new LinkedList<>()).add(eachExecutionUnit);
                dataSourcesToCommandId.computeIfAbsent(dataSourceName, unused -> new LinkedList<>()).add(i);
            }
            
            dataSourcesToQueryContext.computeIfAbsent(dataSourceName, unused -> new LinkedList<>()).add(executionContext.getQueryContext());
        }
        
        for (List<ExecutionUnit> each : groupExecuteUnits.values()) {
            ExecutionUnit first = each.get(0);
            for (int i = 1; i < each.size(); i++) {
                first.CombineExecutionUnit(each.get(i));
            }
            
            // TODO: add prepare stmt
            
            dataSourcesToExecutionUnits.computeIfAbsent(first.getDataSourceName(), unused -> new LinkedList<>()).add(first);
        }
    }
    
    private SQLParserEngine getSQLParserEngine() {
        MetaDataContexts metaDataContexts = ProxyContext.getInstance().getContextManager().getMetaDataContexts();
        SQLParserRule sqlParserRule = metaDataContexts.getMetaData().getGlobalRuleMetaData().getSingleRule(SQLParserRule.class);
        return sqlParserRule.getSQLParserEngine(TypedSPILoader.getService(DatabaseType.class, "MySQL").getType());
    }
    
    private List<String> extractMultiStatements(final Pattern pattern, final String sql) {
        // TODO Multi statements should be split by SQL Parser instead of simple regexp.
        return Arrays.asList(pattern.split(sql));
    }
    
    private QueryContext createQueryContext(final String sql, final SQLStatement sqlStatement) {
        SQLStatementContext<?> sqlStatementContext = SQLStatementContextFactory.newInstance(
                metaDataContexts.getMetaData(), Collections.emptyList(), sqlStatement, connectionSession.getDatabaseName());
        return new QueryContext(sqlStatementContext, sql, Collections.emptyList());
    }
    
    private ExecutionContext createExecutionContext(final QueryContext queryContext) {
        AgentRuleMetaData globalRuleMetaData = metaDataContexts.getMetaData().getGlobalRuleMetaData();
        AgentDatabase currentDatabase = metaDataContexts.getMetaData().getDatabase(connectionSession.getDatabaseName());
//        SQLAuditEngine.audit(queryContext.getSqlStatementContext(), queryContext.getParameters(), globalRuleMetaData, currentDatabase, null);
        return kernelProcessor.generateExecutionContext(queryContext, currentDatabase, globalRuleMetaData, metaDataContexts.getMetaData().getProps(), connectionSession.getConnectionContext());
    }
    
    @Override
    public List<ResponseHeader> execute() throws SQLException {
        Collection<AgentRule> rules = metaDataContexts.getMetaData().getDatabase(connectionSession.getDatabaseName()).getRuleMetaData().getRules();
        DriverExecutionPrepareEngine<JDBCExecutionUnit, Connection> prepareEngine = new DriverExecutionPrepareEngine<>(JDBCDriverType.STATEMENT, metaDataContexts.getMetaData().getProps()
                .<Integer>getValue(ConfigurationPropertyKey.MAX_CONNECTIONS_SIZE_PER_QUERY), connectionSession.getBackendConnection(),
                (JDBCBackendStatement) connectionSession.getStatementManager(), new StatementOption(false), rules,
                metaDataContexts.getMetaData().getDatabase(connectionSession.getDatabaseName()).getResourceMetaData().getStorageTypes());
        ExecutionGroupContext<JDBCExecutionUnit> executionGroupContext = prepareEngine.prepare(anyExecutionContext.getRouteContext(), samplingExecutionUnit(),
                new ExecutionGroupReportContext(connectionSession.getDatabaseName(), connectionSession.getGrantee(), connectionSession.getExecutionId()));
        if (isBatchInsert) {
            for (ExecutionGroup<JDBCExecutionUnit> eachGroup : executionGroupContext.getInputGroups()) {
                for (JDBCExecutionUnit each : eachGroup.getInputs()) {
                    prepareBatchedStatement(each);
                }
            }
        }
        
        return executeMultiStatements(executionGroupContext);
    }

    private String getTableNameFromSQLStatementContext(SQLStatementContext sqlStatementContext) {
        String tableName = "";
        if (sqlStatementContext instanceof SelectStatementContext) {
            SelectStatementContext selectStatementContext = (SelectStatementContext) sqlStatementContext;
            tableName = selectStatementContext.getTableName().get(0);
        } else if (sqlStatementContext instanceof UpdateStatementContext) {
            UpdateStatementContext updateStatementContext = (UpdateStatementContext) sqlStatementContext;
            tableName = updateStatementContext.getTableName().get(0);
        }
        return tableName;
    }
    
    private int getKeyFromSQLStatementContext(SQLStatementContext sqlStatementContext) {
        int key = -1;
        if (sqlStatementContext instanceof SelectStatementContext) {
            SelectStatementContext selectStatementContext = (SelectStatementContext) sqlStatementContext;
            key = selectStatementContext.getKey().get(0);
        } else if (sqlStatementContext instanceof UpdateStatementContext) {
            UpdateStatementContext updateStatementContext = (UpdateStatementContext) sqlStatementContext;
            key = updateStatementContext.getKey().get(0);
        }
        return key;
    }
    
    private Collection<ExecutionUnit> samplingExecutionUnit() {
        Collection<ExecutionUnit> result = new LinkedList<>();
        for (List<ExecutionUnit> each : dataSourcesToExecutionUnits.values()) {
            result.add(each.get(0));
        }
        return result;
    }
    
    private void prepareBatchedStatement(final JDBCExecutionUnit each) throws SQLException {
        Statement statement = each.getStorageResource();
        for (ExecutionUnit eachExecutionUnit : dataSourcesToExecutionUnits.get(each.getExecutionUnit().getDataSourceName())) {
            statement.addBatch(eachExecutionUnit.getSqlUnit().getSql());
        }
    }
    
    private QueryHeader generateQueryHeader(QueryResultMetaData meta, int colIndex) throws SQLException {
        String schemaName = connectionSession.getDatabaseName();
        
        return new QueryHeader(schemaName,
                meta.getTableName(colIndex),
                meta.getColumnLabel(colIndex),
                meta.getColumnName(colIndex),
                meta.getColumnType(colIndex),
                meta.getColumnTypeName(colIndex),
                meta.getColumnLength(colIndex),
                meta.getDecimals(colIndex),
                meta.isSigned(colIndex),
                colIndex == 0,
                meta.isNotNull(colIndex),
                meta.isAutoIncrement(colIndex));
    }
    
    private List<ResponseHeader> executeMultiStatements(final ExecutionGroupContext<JDBCExecutionUnit> executionGroupContext) throws SQLException {
        boolean isExceptionThrown = SQLExecutorExceptionHandler.isExceptionThrown();
        List<ResponseHeader> result = new LinkedList<>();
        Map<String, DatabaseType> storageTypes = metaDataContexts.getMetaData().getDatabase(connectionSession.getDatabaseName()).getResourceMetaData().getStorageTypes();
        if (isBatchInsert) {
            JDBCExecutorCallback<int[]> callback = new BatchedInsertJDBCExecutorCallback(storageTypes, sqlStatementSample, isExceptionThrown);
            List<int[]> executeResults = jdbcExecutor.execute(executionGroupContext, callback);
            int updated = 0;
            for (int[] eachResult : executeResults) {
                for (int each : eachResult) {
                    updated += each;
                }
            }
            result.add(new UpdateResponseHeader(sqlStatementSample, Collections.singletonList(new UpdateResult(updated, 0L))));
        } else {
            JDBCExecutorCallback<List<ExecuteResult>> callback = new BatchedJDBCExecutorCallback(storageTypes, sqlStatementSample, isExceptionThrown);
            try {
                List<List<ExecuteResult>> executeResults = jdbcExecutor.execute(executionGroupContext, callback);
                boolean first = false;
                for (List<ExecuteResult> each : executeResults) {
                    for (ExecuteResult obj : each) {
                        if (obj instanceof QueryResult) {
                            QueryResultMetaData meta = ((QueryResult) obj).getMetaData();
                            int columnCount = meta.getColumnCount();
                            List<QueryHeader> headers = new ArrayList<>(columnCount);
                            
                            for (int i = 1; i <= columnCount; i++) {
                                headers.add(generateQueryHeader(meta, i));
                            }
                            
                            if (!first) {
                                result.add(new QueryResponseHeader(headers));
                                first = true;
                            }
                            try {
                                ((QueryResult) obj).close();
                            } catch (SQLException ignore) {
                            }
                        } else {
                            if (!first) {
                                result.add(new UpdateResponseHeader(sqlStatementSample,
                                        Collections.singletonList(new UpdateResult(((UpdateResult) obj).getUpdateCount(), ((UpdateResult) obj).getLastInsertId()))));
                                first = true;
                            }
                        }
                    }
                    each.clear();
                }
            } catch (Exception ex) {
                throw ex;
            }
        }
        
        return result;
    }

    private static class BatchedJDBCExecutorCallback extends JDBCExecutorCallback<List<ExecuteResult>> {
        
        BatchedJDBCExecutorCallback(final Map<String, DatabaseType> storageTypes, final SQLStatement sqlStatement, final boolean isExceptionThrown) {
            super(TypedSPILoader.getService(DatabaseType.class, "MySQL"), storageTypes, sqlStatement, isExceptionThrown);
        }
        
        @Override
        protected List<ExecuteResult> executeSQL(final String sql, final Statement statement, final ConnectionMode connectionMode, final DatabaseType storageType) throws SQLException {
            boolean resultsAvailable = false;
            try {
                resultsAvailable = statement.execute(sql);
                
                List<ExecuteResult> list = new ArrayList<>();
                while (true) {
                    if (resultsAvailable) {
                        ResultSet rs = statement.getResultSet();
                        list.add(createQueryResult(rs, connectionMode, storageType));
                    } else {
                        int update_cnt = statement.getUpdateCount();
                        // TODO:
                        if (update_cnt != -1) {
                            list.add(new UpdateResult(update_cnt, 0));
                        } else {
                            break;
                        }
                    }
                    
                    resultsAvailable = statement.getMoreResults();
                }
                
                return list;
            } catch (SQLException e) {
                // System.out.println("sql: " + sql);
                overlookResult(resultsAvailable, statement);
                e.printStackTrace();
                throw e;
            } finally {
                statement.close();
            }
        }
        
        @SuppressWarnings("OptionalContainsCollection")
        @Override
        protected Optional<List<ExecuteResult>> getSaneResult(final SQLStatement sqlStatement, final SQLException ex) {
            return Optional.empty();
        }
        
        private QueryResult createQueryResult(final ResultSet resultSet, final ConnectionMode connectionMode, final DatabaseType storageType) throws SQLException {
            return ConnectionMode.MEMORY_STRICTLY == connectionMode ? new JDBCStreamQueryResult(resultSet) : new JDBCMemoryQueryResult(resultSet, storageType);
        }
        
        private void overlookResult(boolean resultsAvailable, final Statement statement) throws SQLException {
            try {
                while (true) {
                    if (resultsAvailable) {
                        ResultSet rs = statement.getResultSet();
                    } else {
                        int update_cnt = statement.getUpdateCount();
                        // TODO:
                        if (update_cnt != -1) {
                        } else {
                            break;
                        }
                    }
                    
                    resultsAvailable = statement.getMoreResults();
                }
            } catch (final SQLException ex) {
                ex.printStackTrace();
                throw ex;
            }
        }
    }
    
    private static class BatchedInsertJDBCExecutorCallback extends JDBCExecutorCallback<int[]> {
        
        BatchedInsertJDBCExecutorCallback(final Map<String, DatabaseType> storageTypes, final SQLStatement sqlStatement, final boolean isExceptionThrown) {
            super(TypedSPILoader.getService(DatabaseType.class, "MySQL"), storageTypes, sqlStatement, isExceptionThrown);
        }
        
        protected int[] executeSQL(final String sql, final Statement statement, final ConnectionMode connectionMode, final DatabaseType storageType) throws SQLException {
            try {
                return statement.executeBatch();
            } finally {
                statement.close();
            }
        }
        
        @SuppressWarnings("OptionalContainsCollection")
        @Override
        protected Optional<int[]> getSaneResult(final SQLStatement sqlStatement, final SQLException ex) {
            return Optional.empty();
        }
    }
}
