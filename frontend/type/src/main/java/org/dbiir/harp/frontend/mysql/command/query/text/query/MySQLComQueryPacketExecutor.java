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

import lombok.Getter;
import org.dbiir.harp.backend.context.ProxyContext;
import org.dbiir.harp.backend.handler.ProxyBackendHandler;
import org.dbiir.harp.backend.handler.ProxyBackendHandlerFactory;
import org.dbiir.harp.backend.response.header.ResponseHeader;
import org.dbiir.harp.backend.response.header.query.QueryResponseHeader;
import org.dbiir.harp.backend.response.header.update.UpdateResponseHeader;
import org.dbiir.harp.backend.session.ConnectionSession;
import org.dbiir.harp.db.protocol.constant.MySQLConstants;
import org.dbiir.harp.db.protocol.packet.DatabasePacket;
import org.dbiir.harp.db.protocol.packet.MySQLPacket;
import org.dbiir.harp.db.protocol.packet.command.admin.MySQLComSetOptionPacket;
import org.dbiir.harp.db.protocol.packet.command.query.text.MySQLTextResultSetRowPacket;
import org.dbiir.harp.db.protocol.packet.command.query.text.query.MySQLComQueryPacket;
import org.dbiir.harp.frontend.async.AgentAsyncPrepare;
import org.dbiir.harp.frontend.command.executor.QueryCommandExecutor;
import org.dbiir.harp.frontend.command.executor.ResponseType;
import org.dbiir.harp.frontend.mysql.command.ServerStatusFlagCalculator;
import org.dbiir.harp.frontend.mysql.command.query.builder.ResponsePacketBuilder;
import org.dbiir.harp.mode.metadata.MetaDataContexts;
import org.dbiir.harp.parser.rule.SQLParserRule;
import org.dbiir.harp.utils.common.database.type.DatabaseType;
import org.dbiir.harp.utils.common.spi.type.typed.TypedSPILoader;
import org.dbiir.harp.utils.common.statement.SQLStatement;
import org.dbiir.harp.utils.common.statement.dal.EmptyStatement;
import org.dbiir.harp.utils.common.statement.dml.DeleteStatement;
import org.dbiir.harp.utils.common.statement.dml.InsertStatement;
import org.dbiir.harp.utils.common.statement.dml.UpdateStatement;
import org.dbiir.harp.utils.common.util.SQLUtils;
import org.dbiir.harp.utils.transcation.AgentAsyncXAManager;

import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * COM_QUERY command packet executor for MySQL.
 */
public final class MySQLComQueryPacketExecutor implements QueryCommandExecutor {
    
    private final ConnectionSession connectionSession;
    
    private final ProxyBackendHandler proxyBackendHandler;
    
    private final int characterSet;

    private static final String COMMENT_PREFIX = "/*";

    private static final String COMMENT_SUFFIX = "*/";
    
    @Getter
    private volatile ResponseType responseType;
    
    public MySQLComQueryPacketExecutor(final MySQLComQueryPacket packet, final ConnectionSession connectionSession) throws SQLException {
//        System.out.println("Thread " + Thread.currentThread().getId() + " " + Thread.currentThread().getName() + "connectionSession: " + connectionSession);
        System.out.println("execute: " + packet.getSql());
        this.connectionSession = connectionSession;
        DatabaseType databaseType = TypedSPILoader.getService(DatabaseType.class, "MySQL");
        SQLStatement sqlStatement = parseSql1(packet.getSql(), databaseType);
        
        if (sqlStatement instanceof InsertStatement) {
            proxyBackendHandler = ProxyBackendHandlerFactory.newInstance(databaseType, packet.getSql(), sqlStatement, connectionSession, packet.getHintValueContext());
        } else {
            List<SQLStatement> sqlStatements = parseSql(packet.getSql(), databaseType);
            if (sqlStatements.size() <= 1) {
                proxyBackendHandler = ProxyBackendHandlerFactory.newInstance(databaseType, packet.getSql(), sqlStatements.get(0), connectionSession, packet.getHintValueContext());
            } else {
                proxyBackendHandler = new MySQLMultiStatementsHandler(connectionSession, sqlStatements, packet.getSql(), false);
            }
        }

        this.connectionSession.setLastOnePhase(this.isLastOnePhaseQuery(packet.getSql()));
        this.connectionSession.setLast(this.isLastQuery(packet.getSql()));

        characterSet = connectionSession.getAttributeMap().attr(MySQLConstants.MYSQL_CHARACTER_SET_ATTRIBUTE_KEY).get().getId();
    }

    private SQLStatement parseSql1(final String sql, final DatabaseType databaseType) {
        if (SQLUtils.trimComment(sql).isEmpty()) {
            return new EmptyStatement();
        }
        MetaDataContexts metaDataContexts = ProxyContext.getInstance().getContextManager().getMetaDataContexts();
        SQLParserRule sqlParserRule = metaDataContexts.getMetaData().getGlobalRuleMetaData().getSingleRule(SQLParserRule.class);
        return sqlParserRule.getSQLParserEngine(databaseType.getType()).parse(sql);
    }
    
    private List<SQLStatement> parseSql(final String sql, final DatabaseType databaseType) {
        List<SQLStatement> result = new LinkedList<>();
        if (SQLUtils.trimComment(sql).isEmpty()) {
            result.add(new EmptyStatement());
            return result;
        }
        List<String> singleSqls = SQLUtils.splitMultiSQL(sql);
        if (singleSqls.size() < 1) {
            result.add(new EmptyStatement());
        } else {
            MetaDataContexts metaDataContexts = ProxyContext.getInstance().getContextManager().getMetaDataContexts();
            SQLParserRule sqlParserRule = metaDataContexts.getMetaData().getGlobalRuleMetaData().getSingleRule(SQLParserRule.class);
            for (String each : singleSqls) {
                result.add(sqlParserRule.getSQLParserEngine(databaseType.getType()).parse(each));
            }
        }
        return result;
    }
    
    private boolean areMultiStatements(final ConnectionSession connectionSession, final SQLStatement sqlStatement, final String sql) {
        // TODO Multi statements should be identified by SQL Parser instead of checking if sql contains ";".
        return connectionSession.getAttributeMap().hasAttr(MySQLConstants.MYSQL_OPTION_MULTI_STATEMENTS)
                && MySQLComSetOptionPacket.MYSQL_OPTION_MULTI_STATEMENTS_ON == connectionSession.getAttributeMap().attr(MySQLConstants.MYSQL_OPTION_MULTI_STATEMENTS).get()
                && (sqlStatement instanceof UpdateStatement || sqlStatement instanceof DeleteStatement) && sql.contains(";");
    }
    
    @Override
    public Collection<DatabasePacket<?>> execute() throws SQLException {
        // TODO: multi executor response header
        Collection<DatabasePacket<?>> result = new LinkedList<>();
        List<ResponseHeader> responseHeader;
        try {
             long startTime = System.currentTimeMillis();
            responseHeader = proxyBackendHandler.execute();
             System.out.println("executeTime: " + (System.currentTimeMillis() - startTime) + " ms");
        } catch (SQLException ex) {
            connectionSession.setCurrentTransactionOk(false);
            throw ex;
        }
        
        for (ResponseHeader each : responseHeader) {
            if (each instanceof QueryResponseHeader) {
                result.addAll(processQuery((QueryResponseHeader) each));
                break;
            } else if (each instanceof UpdateResponseHeader) {
                responseType = ResponseType.UPDATE;
                result.addAll(processUpdate((UpdateResponseHeader) each));
                break;
            }
        }
        return result;
    }

    private boolean isLastQuery(String sql) {
        return getComment(sql).equals("last query");
    }

    private boolean isLastOnePhaseQuery(String sql) {
        return getComment(sql).equals("last one phase query");
    }

    private String getComment(final String sql) {
        String result = "";
        if (sql.startsWith(COMMENT_PREFIX)) {
            result = sql.substring(2, sql.indexOf(COMMENT_SUFFIX));
        }

        return result;
    }
    
    private Collection<DatabasePacket<?>> processQuery(final QueryResponseHeader queryResponseHeader) {
        responseType = ResponseType.QUERY;
        return ResponsePacketBuilder.buildQueryResponsePackets(queryResponseHeader, characterSet, ServerStatusFlagCalculator.calculateFor(connectionSession));
    }
    
    private Collection<DatabasePacket<?>> processUpdate(final UpdateResponseHeader updateResponseHeader) {
        return ResponsePacketBuilder.buildUpdateResponsePackets(updateResponseHeader, ServerStatusFlagCalculator.calculateFor(connectionSession));
    }
    
    @Override
    public boolean next() throws SQLException {
        return proxyBackendHandler.next();
    }
    
    @Override
    public MySQLPacket getQueryRowPacket() throws SQLException {
        return new MySQLTextResultSetRowPacket(proxyBackendHandler.getRowData().getData());
    }
    
    @Override
    public void close() throws SQLException {
        proxyBackendHandler.close();
    }
}
