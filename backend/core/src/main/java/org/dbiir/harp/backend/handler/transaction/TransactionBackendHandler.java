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

import org.dbiir.harp.backend.connector.TransactionManager;
import org.dbiir.harp.backend.connector.jdbc.transaction.BackendTransactionManager;
import org.dbiir.harp.backend.handler.ProxyBackendHandler;
import org.dbiir.harp.backend.response.header.ResponseHeader;
import org.dbiir.harp.backend.response.header.update.UpdateResponseHeader;
import org.dbiir.harp.backend.session.ConnectionSession;
import org.dbiir.harp.kernel.core.core.TransactionOperationType;
import org.dbiir.harp.utils.common.database.type.SchemaSupportedDatabaseType;
import org.dbiir.harp.utils.common.database.type.dialect.MySQLDatabaseType;
import org.dbiir.harp.utils.common.statement.SQLStatement;
import org.dbiir.harp.utils.common.statement.mysql.tcl.MySQLSetAutoCommitStatement;
import org.dbiir.harp.utils.common.statement.tcl.*;
import org.dbiir.harp.utils.exceptions.external.transaction.InTransactionException;


import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.LinkedList;
import java.util.List;

/**
 * Do transaction operation.
 */
public final class TransactionBackendHandler implements ProxyBackendHandler {
    
    private final TCLStatement tclStatement;
    
    private final TransactionOperationType operationType;
    
    private final TransactionManager backendTransactionManager;
    
    private final ConnectionSession connectionSession;
    
    public TransactionBackendHandler(final TCLStatement tclStatement, final TransactionOperationType operationType, final ConnectionSession connectionSession) {
        this.tclStatement = tclStatement;
        this.operationType = operationType;
        this.connectionSession = connectionSession;
        backendTransactionManager = new BackendTransactionManager(connectionSession.getBackendConnection());
    }
    
    @Override
    public List<ResponseHeader> execute() throws SQLException {
        List<ResponseHeader> result = new LinkedList<ResponseHeader>();
        switch (operationType) {
            case BEGIN:
                handleBegin();
                break;
            case SAVEPOINT:
                handleSavepoint();
                break;
            case ROLLBACK_TO_SAVEPOINT:
                handleRollbackToSavepoint();
                break;
            case RELEASE_SAVEPOINT:
                handleReleaseSavepoint();
                break;
            case COMMIT:
                SQLStatement sqlStatement = getSQLStatementByCommit();
                backendTransactionManager.commit();
                result.add(new UpdateResponseHeader(sqlStatement));
                return result;
            case ROLLBACK:
                backendTransactionManager.rollback();
                break;
            case SET_AUTOCOMMIT:
                handleSetAutoCommit();
                break;
            default:
                throw new SQLFeatureNotSupportedException(operationType.name());
        }
        result.add(new UpdateResponseHeader(tclStatement));
        return result;
    }
    
    private void handleBegin() throws SQLException {
        if (connectionSession.getTransactionStatus().isInTransaction()) {
            if (connectionSession.getProtocolType() instanceof MySQLDatabaseType) {
                backendTransactionManager.commit();
            } else if (isSchemaSupportedDatabaseType()) {
                throw new InTransactionException();
            }
        }
        backendTransactionManager.begin();
    }
    
    private void handleSavepoint() throws SQLException {
        backendTransactionManager.setSavepoint(((SavepointStatement) tclStatement).getSavepointName());
    }
    
    private void handleRollbackToSavepoint() throws SQLException {
        backendTransactionManager.rollbackTo(((RollbackStatement) tclStatement).getSavepointName().get());
    }
    
    private void handleReleaseSavepoint() throws SQLException {
        backendTransactionManager.releaseSavepoint(((ReleaseSavepointStatement) tclStatement).getSavepointName());
    }
    
    private boolean isSchemaSupportedDatabaseType() {
        return connectionSession.getProtocolType() instanceof SchemaSupportedDatabaseType;
    }
    
    private SQLStatement getSQLStatementByCommit() {
        return tclStatement;
    }
    
    private void handleSetAutoCommit() throws SQLException {
        if (tclStatement instanceof MySQLSetAutoCommitStatement) {
            handleMySQLSetAutoCommit();
        }
        connectionSession.setAutoCommit(((SetAutoCommitStatement) tclStatement).isAutoCommit());
    }
    
    private void handleMySQLSetAutoCommit() throws SQLException {
        MySQLSetAutoCommitStatement statement = (MySQLSetAutoCommitStatement) tclStatement;
        if (statement.isAutoCommit() && connectionSession.getTransactionStatus().isInTransaction()) {
            backendTransactionManager.commit();
        }
    }
}
