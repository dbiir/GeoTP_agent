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

package org.dbiir.harp.backend.handler.transaction;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.dbiir.harp.backend.connector.DatabaseConnector;
import org.dbiir.harp.backend.connector.DatabaseConnectorFactory;
import org.dbiir.harp.backend.handler.ProxyBackendHandler;
import org.dbiir.harp.backend.response.data.QueryResponseRow;
import org.dbiir.harp.backend.response.header.ResponseHeader;
import org.dbiir.harp.backend.session.ConnectionSession;
import org.dbiir.harp.utils.binder.QueryContext;
import org.dbiir.harp.utils.binder.statement.SQLStatementContext;
import org.dbiir.harp.utils.common.statement.SQLStatement;
import org.dbiir.harp.utils.common.statement.tcl.TCLStatement;
import org.dbiir.harp.utils.common.statement.tcl.XAStatement;
import org.dbiir.harp.utils.transcation.AgentAsyncXAManager;
import org.dbiir.harp.utils.transcation.CustomXID;
import org.dbiir.harp.utils.transcation.XATransactionState;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collections;
import java.util.List;

/**
 * XA transaction handler.
 * TODO Currently XA transaction started with `XA START` doesn't support for database with multiple datasource, a flag should be added for this both in init progress and add datasource from DistSQL.
 */
@RequiredArgsConstructor
@Slf4j
public final class TransactionXAHandler implements ProxyBackendHandler {
    
    private final XAStatement tclStatement;
    
    private final ConnectionSession connectionSession;
    
    private final DatabaseConnector backendHandler;
    
    public TransactionXAHandler(final SQLStatementContext<? extends TCLStatement> sqlStatementContext, final String sql, final ConnectionSession connectionSession) {
        this.tclStatement = (XAStatement) sqlStatementContext.getSqlStatement();
        this.connectionSession = connectionSession;
        QueryContext queryContext = new QueryContext(sqlStatementContext, sql, Collections.emptyList());
        backendHandler = DatabaseConnectorFactory.getInstance().newInstance(queryContext, connectionSession.getBackendConnection(), false);
    }
    
    @Override
    public boolean next() throws SQLException {
        return this.tclStatement.getOp().equals("RECOVER") && this.backendHandler.next();
    }
    
    @Override
    public QueryResponseRow getRowData() throws SQLException {
        return this.tclStatement.getOp().equals("RECOVER") ? this.backendHandler.getRowData() : new QueryResponseRow(Collections.emptyList());
    }
    
    @Override
    public List<ResponseHeader> execute() throws SQLException {
        CustomXID customXID = new CustomXID(tclStatement.getXid());
        switch (tclStatement.getOp()) {
            case "START":
            case "BEGIN":
                /*
                 * we have to let session occupy the thread when doing xa transaction. according to https://dev.mysql.com/doc/refman/5.7/en/xa-states.html XA and local transactions are mutually
                 * exclusive
                 */
                List<ResponseHeader> header = backendHandler.execute();
                if (AgentAsyncXAManager.getInstance().getXAStates().containsKey(customXID))
                    log.warn("xid should be unique");

                AgentAsyncXAManager.getInstance().getXAStates().put(customXID, XATransactionState.ACTIVE);
                connectionSession.getConnectionContext().getTransactionContext().setInTransaction(true);
                connectionSession.setXID(customXID);
                return header;
            case "END":
            case "PREPARE":
            case "RECOVER":
                return backendHandler.execute();
            case "COMMIT":
            case "ROLLBACK":
                checkTransactionStateWhileCommitOrAbort();
                try {
                    return backendHandler.execute();
                } finally {
                    connectionSession.getConnectionContext().clearTransactionConnectionContext();
                    if (AgentAsyncXAManager.getInstance().getXAStates().containsKey(customXID)) {
                        AgentAsyncXAManager.getInstance().getXAStates().remove(customXID);
                    } else {
                        log.error("xa transaction state is not recorded!");
                    }
                    connectionSession.setCurrentTransactionOk(true);
                }
            default:
                throw new SQLFeatureNotSupportedException(String.format("unrecognized XA statement `%s`", tclStatement.getOp()));
        }
    }

    private void checkTransactionStateWhileCommitOrAbort() {
        CustomXID customXID = new CustomXID(tclStatement.getXid());
        if (AgentAsyncXAManager.getInstance().getXAStates().containsKey(customXID)) {
            XATransactionState state = AgentAsyncXAManager.getInstance().getXAStates().get(customXID);
            if (state != XATransactionState.IDLE && state != XATransactionState.PREPARED &&
                    state != XATransactionState.FAILED && state != XATransactionState.ROLLBACK_ONLY) {
                log.error("xa transaction can not commit or rollback");
            }
        }
    }
}
