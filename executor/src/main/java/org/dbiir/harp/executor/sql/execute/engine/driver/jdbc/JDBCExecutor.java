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

package org.dbiir.harp.executor.sql.execute.engine.driver.jdbc;

import lombok.RequiredArgsConstructor;
import org.dbiir.harp.utils.context.ConnectionContext;
import org.dbiir.harp.executor.kernel.ExecutorEngine;
import org.dbiir.harp.executor.kernel.model.ExecutionGroupContext;
import org.dbiir.harp.executor.sql.execute.engine.SQLExecutorExceptionHandler;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 * JDBC executor.
 */
@RequiredArgsConstructor
public final class JDBCExecutor {
    
    private final ExecutorEngine executorEngine;
    
    // TODO add transaction type to ConnectionContext
    private final ConnectionContext connectionContext;

    /**
     * Execute.
     *
     * @param executionGroupContext execution group context
     * @param callback JDBC execute callback
     * @param <T> class type of return value
     * @return execute result
     * @throws SQLException SQL exception
     */
    public <T> List<T> execute(final ExecutionGroupContext<JDBCExecutionUnit> executionGroupContext, final JDBCExecutorCallback<T> callback) throws SQLException {
        return execute(executionGroupContext, null, callback);
    }
    
    /**
     * Execute.
     *
     * @param executionGroupContext execution group context
     * @param firstCallback first JDBC execute callback
     * @param callback JDBC execute callback
     * @param <T> class type of return value
     * @return execute result
     * @throws SQLException SQL exception
     */
    public <T> List<T> execute(final ExecutionGroupContext<JDBCExecutionUnit> executionGroupContext,
                               final JDBCExecutorCallback<T> firstCallback, final JDBCExecutorCallback<T> callback) throws SQLException {
        try {
            // ZQY: isInTransaction 用于判断该语句是否在一个事务中，事务中的语句只能串行执行，否则可以并行执行 ？？？
            return executorEngine.execute(executionGroupContext, firstCallback, callback, connectionContext.getTransactionContext().isInTransaction());
        } catch (final SQLException ex) {
            SQLExecutorExceptionHandler.handleException(ex);
            return Collections.emptyList();
        }
    }
}
