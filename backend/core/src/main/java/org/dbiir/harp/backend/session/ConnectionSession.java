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

package org.dbiir.harp.backend.session;

import io.netty.util.AttributeMap;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.dbiir.harp.utils.binder.QueryContext;
import org.dbiir.harp.utils.common.metadata.user.Grantee;
import org.dbiir.harp.utils.context.ConnectionContext;
import org.dbiir.harp.utils.common.database.type.DatabaseType;
import org.dbiir.harp.executor.sql.prepare.driver.CacheableExecutorConnectionManager;
import org.dbiir.harp.executor.sql.prepare.driver.ExecutorStatementManager;
import org.dbiir.harp.backend.connector.BackendConnection;
import org.dbiir.harp.backend.connector.jdbc.statement.JDBCBackendStatement;
import org.dbiir.harp.backend.session.transaction.TransactionStatus;
import org.dbiir.harp.utils.common.enums.TransactionIsolationLevel;
import org.dbiir.harp.kernel.transaction.api.TransactionType;
import org.dbiir.harp.utils.transcation.CustomXID;

/**
 * Connection session.
 */
@Getter
@Setter
public final class ConnectionSession {
    
    private final DatabaseType protocolType;
    
    @Setter(AccessLevel.NONE)
    private volatile String databaseName;
    
    private volatile int connectionId;

    private volatile Grantee grantee;
    
    private final TransactionStatus transactionStatus;
    
    private final AttributeMap attributeMap;
    
    private volatile boolean autoCommit = true;
    
    private volatile boolean readOnly;
    
    private TransactionIsolationLevel defaultIsolationLevel;
    
    private TransactionIsolationLevel isolationLevel;
    
    private final BackendConnection backendConnection;
    
    private final ExecutorStatementManager statementManager;
    
    private final ServerPreparedStatementRegistry serverPreparedStatementRegistry = new ServerPreparedStatementRegistry();
    
    private final ConnectionContext connectionContext;
    
    private final RequiredSessionVariableRecorder requiredSessionVariableRecorder = new RequiredSessionVariableRecorder();
    
    private volatile String executionId;
    
    private QueryContext queryContext;

    private CustomXID XID;

    private boolean currentTransactionOk;

    private boolean isLast = false;

    private boolean isLastOnePhase = false;

    public ConnectionSession(final DatabaseType protocolType, final TransactionType initialTransactionType, final AttributeMap attributeMap) {
        this.protocolType = protocolType;
        transactionStatus = new TransactionStatus(initialTransactionType);
        this.attributeMap = attributeMap;
        backendConnection = new BackendConnection(this);
        statementManager = new JDBCBackendStatement();
        connectionContext = new ConnectionContext(((CacheableExecutorConnectionManager<?>) backendConnection)::getDataSourceNamesOfCachedConnections);
        currentTransactionOk = true;
    }
    
    /**
     * Change database of current channel.
     *
     * @param databaseName database name
     */
    public void setCurrentDatabase(final String databaseName) {
        if (null == databaseName || !databaseName.equals(this.databaseName)) {
            this.databaseName = databaseName;
        }
    }
    
    /**
     * Get database name.
     *
     * @return database name
     */
    public String getDatabaseName() {
        return "harp";
//        return null == queryContext ? databaseName : queryContext.getDatabaseNameFromSQLStatement().orElse(databaseName);
    }
    
    /**
     * Get default database name.
     *
     * @return default database name
     */
    public String getDefaultDatabaseName() {
        return databaseName;
    }
    
    /**
     * Clear query context.
     */
    public void clearQueryContext() {
        queryContext = null;
    }
}
