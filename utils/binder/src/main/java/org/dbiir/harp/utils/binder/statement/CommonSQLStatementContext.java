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

package org.dbiir.harp.utils.binder.statement;

import lombok.Getter;
import org.dbiir.harp.utils.binder.segment.table.TablesContext;
import org.dbiir.harp.utils.common.database.type.DatabaseType;
import org.dbiir.harp.utils.common.hint.SQLHintExtractor;
import org.dbiir.harp.utils.common.spi.type.typed.TypedSPILoader;
import org.dbiir.harp.utils.common.statement.SQLStatement;
import org.dbiir.harp.utils.common.statement.mysql.MySQLStatement;
import org.dbiir.harp.utils.exceptions.external.sql.type.generic.UnsupportedSQLOperationException;

import java.util.Collections;
import java.util.Optional;

/**
 * Common SQL statement context.
 * 
 * @param <T> type of SQL statement
 */
@Getter
public class CommonSQLStatementContext<T extends SQLStatement> implements SQLStatementContext<T> {
    
    private final T sqlStatement;
    
    private final TablesContext tablesContext;
    
    private final DatabaseType databaseType;
    
    private final SQLHintExtractor sqlHintExtractor;
    
    public CommonSQLStatementContext(final T sqlStatement) {
        this.sqlStatement = sqlStatement;
        databaseType = getDatabaseType(sqlStatement);
        tablesContext = new TablesContext(Collections.emptyList(), databaseType);
        sqlHintExtractor = new SQLHintExtractor(sqlStatement);
    }
    
    private DatabaseType getDatabaseType(final SQLStatement sqlStatement) {
        if (sqlStatement instanceof MySQLStatement) {
            return TypedSPILoader.getService(DatabaseType.class, "MySQL");
        }
        throw new UnsupportedSQLOperationException(sqlStatement.getClass().getName());
    }
    
    /**
     * Find hint data source name.
     *
     * @return dataSource name
     */
    public Optional<String> findHintDataSourceName() {
        return sqlHintExtractor.findHintDataSourceName();
    }
    
    /**
     * Judge whether is hint routed to write data source or not.
     *
     * @return whether is hint routed to write data source or not
     */
    public boolean isHintWriteRouteOnly() {
        return sqlHintExtractor.isHintWriteRouteOnly();
    }
    
    /**
     * Judge whether hint skip sql rewrite or not.
     *
     * @return whether hint skip sql rewrite or not
     */
    public boolean isHintSkipSQLRewrite() {
        return sqlHintExtractor.isHintSkipSQLRewrite();
    }
}
