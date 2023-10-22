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

package org.dbiir.harp.frontend.mysql.command;

import io.netty.channel.ChannelHandlerContext;
import org.dbiir.harp.db.protocol.packet.command.MySQLCommandPacket;
import org.dbiir.harp.db.protocol.packet.command.MySQLCommandPacketType;
import org.dbiir.harp.db.protocol.packet.command.MySQLCommandPacketTypeLoader;
import org.dbiir.harp.db.protocol.packet.generic.MySQLEofPacket;
import org.dbiir.harp.db.protocol.payload.MySQLPacketPayload;
import org.dbiir.harp.db.protocol.packet.CommandPacket;
import org.dbiir.harp.db.protocol.packet.CommandPacketType;
import org.dbiir.harp.db.protocol.packet.DatabasePacket;
import org.dbiir.harp.db.protocol.payload.PacketPayload;
import org.dbiir.harp.frontend.mysql.err.MySQLErrPacketFactory;
import org.dbiir.harp.mode.metadata.MetaDataContexts;
import org.dbiir.harp.parser.rule.SQLParserRule;
import org.dbiir.harp.backend.connector.BackendConnection;
import org.dbiir.harp.backend.context.ProxyContext;
import org.dbiir.harp.backend.session.ConnectionSession;
import org.dbiir.harp.frontend.command.CommandExecuteEngine;
import org.dbiir.harp.frontend.command.executor.CommandExecutor;
import org.dbiir.harp.frontend.command.executor.QueryCommandExecutor;
import org.dbiir.harp.frontend.command.executor.ResponseType;
import org.dbiir.harp.utils.common.config.props.ConfigurationPropertyKey;

import java.sql.SQLException;
import java.util.Optional;

/**
 * Command execute engine for MySQL.
 */
public final class MySQLCommandExecuteEngine implements CommandExecuteEngine {
    
    @Override
    public MySQLCommandPacketType getCommandPacketType(final PacketPayload payload) {
        return MySQLCommandPacketTypeLoader.getCommandPacketType((MySQLPacketPayload) payload);
    }
    
    @Override
    public MySQLCommandPacket getCommandPacket(final PacketPayload payload, final CommandPacketType type, final ConnectionSession connectionSession) {
        MetaDataContexts metaDataContexts = ProxyContext.getInstance().getContextManager().getMetaDataContexts();
        SQLParserRule sqlParserRule = metaDataContexts.getMetaData().getGlobalRuleMetaData().getSingleRule(SQLParserRule.class);
        return MySQLCommandPacketFactory.newInstance((MySQLCommandPacketType) type, (MySQLPacketPayload) payload, connectionSession, sqlParserRule.isSqlCommentParseEnabled());
    }
    
    @Override
    public CommandExecutor getCommandExecutor(final CommandPacketType type, final CommandPacket packet, final ConnectionSession connectionSession) throws SQLException {
        return MySQLCommandExecutorFactory.newInstance((MySQLCommandPacketType) type, packet, connectionSession);
    }
    
    @Override
    public DatabasePacket<?> getErrorPacket(final Exception cause) {
        return MySQLErrPacketFactory.newInstance(cause);
    }
    
    @Override
    public Optional<DatabasePacket<?>> getOtherPacket(final ConnectionSession connectionSession) {
        return Optional.empty();
    }
    
    @Override
    public void writeQueryData(final ChannelHandlerContext context,
                               final BackendConnection backendConnection, final QueryCommandExecutor queryCommandExecutor, final int headerPackagesCount) throws SQLException {
        if (ResponseType.QUERY != queryCommandExecutor.getResponseType() || !context.channel().isActive()) {
            return;
        }
        int count = 0;
        int flushThreshold = ProxyContext.getInstance().getContextManager().getMetaDataContexts().getMetaData().getProps().<Integer>getValue(ConfigurationPropertyKey.PROXY_FRONTEND_FLUSH_THRESHOLD);
        while (queryCommandExecutor.next()) {
            count++;
            while (!context.channel().isWritable() && context.channel().isActive()) {
                context.flush();
                backendConnection.getResourceLock().doAwait();
            }
            DatabasePacket<?> dataValue = queryCommandExecutor.getQueryRowPacket();
            context.write(dataValue);
            if (flushThreshold == count) {
                context.flush();
                count = 0;
            }
        }
        context.write(new MySQLEofPacket(ServerStatusFlagCalculator.calculateFor(backendConnection.getConnectionSession())));
    }
}
