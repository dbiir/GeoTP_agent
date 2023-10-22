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

package org.dbiir.harp.frontend.mysql.command.query.binary.prepare;

import lombok.RequiredArgsConstructor;
import org.dbiir.harp.db.protocol.constant.MySQLBinaryColumnType;
import org.dbiir.harp.db.protocol.constant.MySQLConstants;
import org.dbiir.harp.db.protocol.packet.command.admin.MySQLComSetOptionPacket;
import org.dbiir.harp.db.protocol.packet.command.query.MySQLColumnDefinition41Packet;
import org.dbiir.harp.db.protocol.packet.command.query.MySQLColumnDefinitionFlag;
import org.dbiir.harp.db.protocol.packet.command.query.binary.prepare.MySQLComStmtPrepareOKPacket;
import org.dbiir.harp.db.protocol.packet.command.query.binary.prepare.MySQLComStmtPreparePacket;
import org.dbiir.harp.db.protocol.packet.generic.MySQLEofPacket;
import org.dbiir.harp.db.protocol.packet.DatabasePacket;
import org.dbiir.harp.frontend.mysql.command.query.binary.MySQLServerPreparedStatement;
import org.dbiir.harp.utils.binder.SQLStatementContextFactory;
import org.dbiir.harp.utils.binder.segment.select.projection.Projection;
import org.dbiir.harp.utils.binder.segment.select.projection.impl.ColumnProjection;
import org.dbiir.harp.utils.binder.statement.SQLStatementContext;
import org.dbiir.harp.utils.binder.statement.dml.SelectStatementContext;
import org.dbiir.harp.utils.common.database.type.DatabaseType;
import org.dbiir.harp.utils.common.database.type.DatabaseTypeEngine;
import org.dbiir.harp.utils.common.metadata.database.AgentDatabase;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentColumn;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentSchema;
import org.dbiir.harp.mode.metadata.MetaDataContexts;
import org.dbiir.harp.parser.rule.SQLParserRule;
import org.dbiir.harp.backend.context.ProxyContext;
import org.dbiir.harp.backend.session.ConnectionSession;
import org.dbiir.harp.frontend.command.executor.CommandExecutor;
import org.dbiir.harp.frontend.mysql.command.ServerStatusFlagCalculator;
import org.dbiir.harp.frontend.mysql.command.query.binary.MySQLStatementIDGenerator;
import org.dbiir.harp.utils.common.segment.generic.ParameterMarkerSegment;
import org.dbiir.harp.utils.common.spi.type.typed.TypedSPILoader;
import org.dbiir.harp.utils.common.statement.AbstractSQLStatement;
import org.dbiir.harp.utils.common.statement.SQLStatement;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * COM_STMT_PREPARE command executor for MySQL.
 */
@RequiredArgsConstructor
public final class MySQLComStmtPrepareExecutor implements CommandExecutor {
    
    private final MySQLComStmtPreparePacket packet;
    
    private final ConnectionSession connectionSession;
    
    @Override
    public Collection<DatabasePacket<?>> execute() {
        failedIfContainsMultiStatements();
        MetaDataContexts metaDataContexts = ProxyContext.getInstance().getContextManager().getMetaDataContexts();
        SQLParserRule sqlParserRule = metaDataContexts.getMetaData().getGlobalRuleMetaData().getSingleRule(SQLParserRule.class);
        SQLStatement sqlStatement = sqlParserRule.getSQLParserEngine(TypedSPILoader.getService(DatabaseType.class, "MySQL").getType()).parse(packet.getSql());
        if (!MySQLComStmtPrepareChecker.isStatementAllowed(sqlStatement)) {
            throw new UnsupportedOperationException();
        }
        SQLStatementContext<?> sqlStatementContext = SQLStatementContextFactory.newInstance(ProxyContext.getInstance().getContextManager().getMetaDataContexts().getMetaData(),
                sqlStatement, connectionSession.getDefaultDatabaseName());
        int statementId = MySQLStatementIDGenerator.getInstance().nextStatementId(connectionSession.getConnectionId());
        MySQLServerPreparedStatement serverPreparedStatement = new MySQLServerPreparedStatement(packet.getSql(), sqlStatementContext, new CopyOnWriteArrayList<>());
        connectionSession.getServerPreparedStatementRegistry().addPreparedStatement(statementId, serverPreparedStatement);
        return createPackets(sqlStatementContext, statementId, serverPreparedStatement);
    }
    
    private void failedIfContainsMultiStatements() {
        // TODO Multi statements should be identified by SQL Parser instead of checking if sql contains ";".
        if (connectionSession.getAttributeMap().hasAttr(MySQLConstants.MYSQL_OPTION_MULTI_STATEMENTS)
                && MySQLComSetOptionPacket.MYSQL_OPTION_MULTI_STATEMENTS_ON == connectionSession.getAttributeMap().attr(MySQLConstants.MYSQL_OPTION_MULTI_STATEMENTS).get()
                && packet.getSql().contains(";")) {
            throw new UnsupportedOperationException();
        }
    }
    
    private Collection<DatabasePacket<?>> createPackets(final SQLStatementContext<?> sqlStatementContext, final int statementId, final MySQLServerPreparedStatement serverPreparedStatement) {
        Collection<DatabasePacket<?>> result = new LinkedList<>();
        List<Projection> projections = getProjections(sqlStatementContext);
        int parameterCount = sqlStatementContext.getSqlStatement().getParameterCount();
        result.add(new MySQLComStmtPrepareOKPacket(statementId, projections.size(), parameterCount, 0));
        int characterSet = connectionSession.getAttributeMap().attr(MySQLConstants.MYSQL_CHARACTER_SET_ATTRIBUTE_KEY).get().getId();
        int statusFlags = ServerStatusFlagCalculator.calculateFor(connectionSession);
        if (parameterCount > 0) {
            result.addAll(createParameterColumnDefinition41Packets(sqlStatementContext, characterSet, serverPreparedStatement));
            result.add(new MySQLEofPacket(statusFlags));
        }
        if (!projections.isEmpty()) {
            result.addAll(createProjectionColumnDefinition41Packets((SelectStatementContext) sqlStatementContext, characterSet));
            result.add(new MySQLEofPacket(statusFlags));
        }
        return result;
    }
    
    private List<Projection> getProjections(final SQLStatementContext<?> sqlStatementContext) {
        return sqlStatementContext instanceof SelectStatementContext ? ((SelectStatementContext) sqlStatementContext).getProjectionsContext().getExpandProjections() : Collections.emptyList();
    }
    
    private Collection<DatabasePacket<?>> createParameterColumnDefinition41Packets(final SQLStatementContext<?> sqlStatementContext, final int characterSet,
                                                                                   final MySQLServerPreparedStatement serverPreparedStatement) {
        Map<ParameterMarkerSegment, AgentColumn> columnsOfParameterMarkers =
                MySQLComStmtPrepareParameterMarkerExtractor.findColumnsOfParameterMarkers(sqlStatementContext.getSqlStatement(), getSchema(sqlStatementContext));
        Collection<ParameterMarkerSegment> parameterMarkerSegments = ((AbstractSQLStatement) sqlStatementContext.getSqlStatement()).getParameterMarkerSegments();
        Collection<DatabasePacket<?>> result = new ArrayList<>(parameterMarkerSegments.size());
        for (ParameterMarkerSegment each : parameterMarkerSegments) {
            AgentColumn column = columnsOfParameterMarkers.get(each);
            if (null != column) {
                int columnDefinitionFlag = calculateColumnDefinitionFlag(column);
                result.add(createMySQLColumnDefinition41Packet(characterSet, columnDefinitionFlag, MySQLBinaryColumnType.valueOfJDBCType(column.getDataType())));
                serverPreparedStatement.getParameterColumnDefinitionFlags().add(columnDefinitionFlag);
            } else {
                result.add(createMySQLColumnDefinition41Packet(characterSet, 0, MySQLBinaryColumnType.MYSQL_TYPE_VAR_STRING));
                serverPreparedStatement.getParameterColumnDefinitionFlags().add(0);
            }
        }
        return result;
    }
    
    private Collection<DatabasePacket<?>> createProjectionColumnDefinition41Packets(final SelectStatementContext selectStatementContext, final int characterSet) {
        Collection<Projection> projections = selectStatementContext.getProjectionsContext().getExpandProjections();
        AgentSchema schema = getSchema(selectStatementContext);
        Map<String, String> columnToTableMap = selectStatementContext.getTablesContext()
                .findTableNamesByColumnProjection(projections.stream().filter(each -> each instanceof ColumnProjection).map(each -> (ColumnProjection) each).collect(Collectors.toList()), schema);
        Collection<DatabasePacket<?>> result = new ArrayList<>(projections.size());
        for (Projection each : projections) {
            // TODO Calculate column definition flag for other projection types
            if (each instanceof ColumnProjection) {
                result.add(Optional.ofNullable(columnToTableMap.get(each.getExpression())).map(schema::getTable).map(table -> table.getColumns().get(((ColumnProjection) each).getName()))
                        .map(column -> createMySQLColumnDefinition41Packet(characterSet, calculateColumnDefinitionFlag(column), MySQLBinaryColumnType.valueOfJDBCType(column.getDataType())))
                        .orElseGet(() -> createMySQLColumnDefinition41Packet(characterSet, 0, MySQLBinaryColumnType.MYSQL_TYPE_VAR_STRING)));
            } else {
                result.add(createMySQLColumnDefinition41Packet(characterSet, 0, MySQLBinaryColumnType.MYSQL_TYPE_VAR_STRING));
            }
        }
        return result;
    }
    
    private AgentSchema getSchema(final SQLStatementContext<?> sqlStatementContext) {
        String databaseName = sqlStatementContext.getTablesContext().getDatabaseName().orElseGet(connectionSession::getDefaultDatabaseName);
        AgentDatabase database = ProxyContext.getInstance().getContextManager().getMetaDataContexts().getMetaData().getDatabase(databaseName);
        return sqlStatementContext.getTablesContext().getSchemaName().map(database::getSchema)
                .orElseGet(() -> database.getSchema(DatabaseTypeEngine.getDefaultSchemaName(sqlStatementContext.getDatabaseType(), database.getName())));
    }
    
    private int calculateColumnDefinitionFlag(final AgentColumn column) {
        int result = 0;
        result |= column.isPrimaryKey() ? MySQLColumnDefinitionFlag.PRIMARY_KEY.getValue() : 0;
        result |= column.isUnsigned() ? MySQLColumnDefinitionFlag.UNSIGNED.getValue() : 0;
        return result;
    }
    
    private MySQLColumnDefinition41Packet createMySQLColumnDefinition41Packet(final int characterSet, final int columnDefinitionFlag, final MySQLBinaryColumnType columnType) {
        return new MySQLColumnDefinition41Packet(characterSet, columnDefinitionFlag, "", "", "", "", "", 0, columnType, 0, false);
    }
}
