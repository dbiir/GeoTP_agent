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

package org.dbiir.harp.frontend.mysql.command.query.text.fieldlist;

import lombok.RequiredArgsConstructor;
import org.dbiir.harp.db.protocol.constant.MySQLBinaryColumnType;
import org.dbiir.harp.db.protocol.constant.MySQLConstants;
import org.dbiir.harp.db.protocol.packet.command.query.MySQLColumnDefinition41Packet;
import org.dbiir.harp.db.protocol.packet.command.query.text.fieldlist.MySQLComFieldListPacket;
import org.dbiir.harp.db.protocol.packet.generic.MySQLEofPacket;
import org.dbiir.harp.db.protocol.packet.DatabasePacket;
import org.dbiir.harp.mode.metadata.MetaDataContexts;
import org.dbiir.harp.parser.rule.SQLParserRule;
import org.dbiir.harp.utils.binder.QueryContext;
import org.dbiir.harp.utils.binder.SQLStatementContextFactory;
import org.dbiir.harp.utils.binder.statement.SQLStatementContext;
import org.dbiir.harp.backend.connector.BackendConnection;
import org.dbiir.harp.backend.connector.DatabaseConnector;
import org.dbiir.harp.backend.connector.DatabaseConnectorFactory;
import org.dbiir.harp.backend.context.ProxyContext;
import org.dbiir.harp.backend.session.ConnectionSession;
import org.dbiir.harp.frontend.command.executor.CommandExecutor;
import org.dbiir.harp.utils.common.database.type.DatabaseType;
import org.dbiir.harp.utils.common.spi.type.typed.TypedSPILoader;
import org.dbiir.harp.utils.common.statement.SQLStatement;
import org.dbiir.harp.frontend.mysql.command.ServerStatusFlagCalculator;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;

/**
 * COM_FIELD_LIST packet executor for MySQL.
 */
@RequiredArgsConstructor
public final class MySQLComFieldListPacketExecutor implements CommandExecutor {
    
    private static final String SQL = "SHOW COLUMNS FROM %s";
    
    private final MySQLComFieldListPacket packet;
    
    private final ConnectionSession connectionSession;
    
    private DatabaseConnector databaseConnector;
    
    @Override
    public Collection<DatabasePacket<?>> execute() throws SQLException {
        String databaseName = connectionSession.getDefaultDatabaseName();
        String sql = String.format(SQL, packet.getTable());
        MetaDataContexts metaDataContexts = ProxyContext.getInstance().getContextManager().getMetaDataContexts();
        SQLParserRule sqlParserRule = metaDataContexts.getMetaData().getGlobalRuleMetaData().getSingleRule(SQLParserRule.class);
        SQLStatement sqlStatement = sqlParserRule.getSQLParserEngine(TypedSPILoader.getService(DatabaseType.class, "MySQL").getType()).parse(sql);
        SQLStatementContext<?> sqlStatementContext = SQLStatementContextFactory.newInstance(metaDataContexts.getMetaData(), sqlStatement, databaseName);
        BackendConnection backendConnection = connectionSession.getBackendConnection();
        QueryContext queryContext = new QueryContext(sqlStatementContext, sql, Collections.emptyList());
        databaseConnector = DatabaseConnectorFactory.getInstance().newInstance(queryContext, backendConnection, false);
        databaseConnector.execute();
        return createColumnDefinition41Packets(databaseName);
    }
    
    private Collection<DatabasePacket<?>> createColumnDefinition41Packets(final String databaseName) throws SQLException {
        Collection<DatabasePacket<?>> result = new LinkedList<>();
        int characterSet = connectionSession.getAttributeMap().attr(MySQLConstants.MYSQL_CHARACTER_SET_ATTRIBUTE_KEY).get().getId();
        while (databaseConnector.next()) {
            String columnName = databaseConnector.getRowData().getCells().iterator().next().getData().toString();
            result.add(new MySQLColumnDefinition41Packet(
                    characterSet, databaseName, packet.getTable(), packet.getTable(), columnName, columnName, 100, MySQLBinaryColumnType.MYSQL_TYPE_VARCHAR, 0, true));
        }
        result.add(new MySQLEofPacket(ServerStatusFlagCalculator.calculateFor(connectionSession)));
        return result;
    }
    
    @Override
    public void close() throws SQLException {
        databaseConnector.close();
    }
}
