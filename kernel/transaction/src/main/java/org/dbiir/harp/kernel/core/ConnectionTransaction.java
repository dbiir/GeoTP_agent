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

package org.dbiir.harp.kernel.core;

import lombok.Getter;
import lombok.Setter;
import org.dbiir.harp.kernel.core.core.TransactionTypeHolder;
import org.dbiir.harp.kernel.core.rule.TransactionRule;
import org.dbiir.harp.kernel.transaction.api.TransactionType;
import org.dbiir.harp.kernel.transaction.spi.AgentTransactionManager;
import org.dbiir.harp.utils.context.transaction.TransactionConnectionContext;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

/**
 * Connection transaction.
 */
public final class ConnectionTransaction {
    
    @Getter
    private final TransactionType transactionType;
    
    private final String databaseName;
    
    @Setter
    @Getter
    private volatile boolean rollbackOnly;
    
    private final AgentTransactionManager transactionManager;
    
    public ConnectionTransaction(final String databaseName, final TransactionRule rule) {
        this(databaseName, TransactionType.XA, rule);
    }
    
    public ConnectionTransaction(final String databaseName, final TransactionType transactionType, final TransactionRule rule) {
        this.databaseName = databaseName;
        this.transactionType = transactionType;
        transactionManager = rule.getResource().getTransactionManager(transactionType);
        TransactionTypeHolder.set(transactionType);
    }
    
    /**
     * Whether in transaction.
     * 
     * @param transactionConnectionContext transaction connection context
     * @return in transaction or not
     */
    public boolean isInTransaction(final TransactionConnectionContext transactionConnectionContext) {
        return transactionConnectionContext.isInTransaction() && null != transactionManager && transactionManager.isInTransaction();
    }
    
    /**
     * Whether in transaction.
     *
     * @return in transaction or not
     */
    public boolean isInTransaction() {
        return null != transactionManager && transactionManager.isInTransaction();
    }
    
    /**
     * Judge is local transaction or not.
     * 
     * @return is local transaction or not
     */
    public boolean isLocalTransaction() {
        return TransactionType.LOCAL == transactionType;
    }
    
    /**
     * Whether hold transaction.
     *
     * @param autoCommit is auto commit
     * @return hold transaction or not
     */
    public boolean isHoldTransaction(final boolean autoCommit) {
        return TransactionType.LOCAL == transactionType && !autoCommit || TransactionType.XA == transactionType && isInTransaction();
    }
    
    /**
     * Get connection in transaction.
     * 
     * @param dataSourceName data source name
     * @param transactionConnectionContext transaction connection context
     * @return connection in transaction
     * @throws SQLException SQL exception
     */
    public Optional<Connection> getConnection(final String dataSourceName, final TransactionConnectionContext transactionConnectionContext) throws SQLException {
        return isInTransaction(transactionConnectionContext) ? Optional.of(transactionManager.getConnection(this.databaseName, dataSourceName)) : Optional.empty();
    }
    
    /**
     * Begin transaction.
     */
    public void begin() {
        transactionManager.begin();
    }
    
    /**
     * Commit transaction.
     */
    public void commit() {
        transactionManager.commit(rollbackOnly);
    }
    
    /**
     * Rollback transaction.
     */
    public void rollback() {
        transactionManager.rollback();
    }
    
    /**
     * Get distributed transaction operation type.
     * 
     * @param autoCommit is auto commit
     * @return distributed transaction operation type
     */
    public DistributedTransactionOperationType getDistributedTransactionOperationType(final boolean autoCommit) {
        if (!autoCommit && !transactionManager.isInTransaction()) {
            return DistributedTransactionOperationType.BEGIN;
        }
        if (autoCommit && transactionManager.isInTransaction()) {
            return DistributedTransactionOperationType.COMMIT;
        }
        return DistributedTransactionOperationType.IGNORE;
    }
    
    public enum DistributedTransactionOperationType {
        BEGIN, COMMIT, IGNORE
    }
}
