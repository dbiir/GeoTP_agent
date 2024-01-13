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

package org.dbiir.harp.utils.binder;

import lombok.AccessLevel;
import lombok.Getter;
import org.dbiir.harp.utils.binder.statement.SQLStatementContext;
import org.dbiir.harp.utils.binder.type.TableAvailable;
import org.dbiir.harp.utils.common.hint.HintValueContext;

import java.util.List;
import java.util.Optional;

/**
 * Query context.
 */
@Getter
public final class QueryContext {
    
    private final SQLStatementContext<?> sqlStatementContext;
    
    private String sql;
    
    private final List<Object> parameters;
    
    @Getter(AccessLevel.NONE)
    private final String databaseName;
    
    private final HintValueContext hintValueContext;
    
    private final boolean useCache;
    
    public QueryContext(final SQLStatementContext<?> sqlStatementContext, final String sql, final List<Object> params) {
        this(sqlStatementContext, sql, params, new HintValueContext());
    }
    
    public QueryContext(final SQLStatementContext<?> sqlStatementContext, final String sql, final List<Object> params, final HintValueContext hintValueContext) {
        this(sqlStatementContext, sql, params, hintValueContext, false);
    }
    
    public QueryContext(final SQLStatementContext<?> sqlStatementContext, final String sql, final List<Object> params, final HintValueContext hintValueContext, final boolean useCache) {
        this.sqlStatementContext = sqlStatementContext;
        this.sql = sql;
        parameters = params;
        databaseName = sqlStatementContext instanceof TableAvailable ? ((TableAvailable) sqlStatementContext).getTablesContext().getDatabaseName().orElse(null) : null;
        this.hintValueContext = hintValueContext;
        this.useCache = useCache;
    }

    public void setSQL(String dst_sql) {
        sql = dst_sql;
    }
    
    /**
     * Get database name from SQL statement.
     * 
     * @return got database name
     */
    public Optional<String> getDatabaseNameFromSQLStatement() {
        return Optional.ofNullable(databaseName);
    }
}
