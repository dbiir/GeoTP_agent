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

package org.dbiir.harp.merger.result.impl.memory;

import org.dbiir.harp.executor.sql.execute.result.query.QueryResult;
import org.dbiir.harp.merger.result.MergedResult;
import org.dbiir.harp.utils.binder.statement.SQLStatementContext;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentSchema;

import java.io.InputStream;
import java.io.Reader;
import java.sql.*;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

/**
 * Memory merged result.
 */
public abstract class MemoryMergedResult implements MergedResult {
    
    private final Iterator<MemoryQueryResultRow> memoryResultSetRows;
    
    private MemoryQueryResultRow currentResultSetRow;
    
    private boolean wasNull;
    
    protected MemoryMergedResult(final AgentSchema schema, final SQLStatementContext<?> sqlStatementContext, final List<QueryResult> queryResults) throws SQLException {
        assert (false);
        List<MemoryQueryResultRow> memoryQueryResultRows = init(schema, sqlStatementContext, queryResults);
        memoryResultSetRows = memoryQueryResultRows.iterator();
        if (!memoryQueryResultRows.isEmpty()) {
            currentResultSetRow = memoryQueryResultRows.get(0);
        }
    }
    
    protected abstract List<MemoryQueryResultRow> init(AgentSchema schema, SQLStatementContext<?> sqlStatementContext, List<QueryResult> queryResults) throws SQLException;
    
    @Override
    public final boolean next() {
        if (memoryResultSetRows.hasNext()) {
            currentResultSetRow = memoryResultSetRows.next();
            return true;
        }
        return false;
    }
    
    @Override
    public final Object getValue(final int columnIndex, final Class<?> type) throws SQLException {
        Object result = currentResultSetRow.getCell(columnIndex);
        wasNull = null == result;
        return result;
    }
    
    @Override
    public final Object getCalendarValue(final int columnIndex, final Class<?> type, final Calendar calendar) {
        // TODO implement with calendar
        Object result = currentResultSetRow.getCell(columnIndex);
        wasNull = null == result;
        return result;
    }
    
    @Override
    public final InputStream getInputStream(final int columnIndex, final String type) throws SQLException {
        throw new SQLFeatureNotSupportedException(String.format("Get input stream from `%s`", type));
    }
    
    @Override
    public final boolean wasNull() {
        return wasNull;
    }
}
