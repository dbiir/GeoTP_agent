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

package org.dbiir.harp.executor.sql.context;

import com.mysql.cj.jdbc.MysqlDataSource;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.dbiir.harp.utils.binder.segment.table.TablesContext;
import org.dbiir.harp.utils.binder.statement.SQLStatementContext;
import org.dbiir.harp.utils.common.database.type.DatabaseType;
import org.dbiir.harp.utils.common.database.type.dialect.MySQLDatabaseType;
import org.dbiir.harp.utils.common.database.type.dialect.PostgreSQLDatabaseType;
import org.dbiir.harp.utils.common.metadata.database.AgentDatabase;
import org.dbiir.harp.utils.common.statement.dal.DALStatement;
import org.dbiir.harp.utils.common.statement.dal.ShowStatement;
import org.dbiir.harp.utils.common.statement.dml.SelectStatement;
import org.dbiir.harp.utils.common.statement.mysql.dal.MySQLShowColumnsStatement;
import org.dbiir.harp.utils.common.statement.mysql.dal.MySQLShowVariablesStatement;
import org.dbiir.harp.utils.router.context.RouteMapper;

import javax.sql.DataSource;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Execution context builder.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ExecutionContextBuilder {
    
//    /**
//     * Build execution contexts.
//     *
//     * @param database database
//     * @param sqlRewriteResult SQL rewrite result
//     * @param sqlStatementContext SQL statement context
//     * @return execution contexts
//     */
//    public static Collection<ExecutionUnit> build(final AgentDatabase database, final SQLRewriteResult sqlRewriteResult, final SQLStatementContext<?> sqlStatementContext) {
//        return sqlRewriteResult instanceof GenericSQLRewriteResult
//                ? build(database, (GenericSQLRewriteResult) sqlRewriteResult, sqlStatementContext)
//                : build((RouteSQLRewriteResult) sqlRewriteResult);
//    }

    public static Collection<ExecutionUnit> build(final AgentDatabase database, String sql, List<Object> parameter,
                                                   final SQLStatementContext<?> sqlStatementContext) {
        Collection<String> instanceDataSourceNames = database.getResourceMetaData().getAllInstanceDataSourceNames();
        if (instanceDataSourceNames.isEmpty()) {
            return Collections.emptyList();
        }
        // TODO: set router
        String dataSource = "";
        if (sqlStatementContext.getSqlStatement() instanceof SelectStatement && sqlStatementContext.getTablesContext().getTableNames().isEmpty()
                || sqlStatementContext.getSqlStatement() instanceof DALStatement) {
            for (Map.Entry<String, DatabaseType> each: database.getResourceMetaData().getStorageTypes().entrySet()) {
                if (each.getValue() instanceof MySQLDatabaseType) {
                    dataSource = each.getKey();
                    break;
                }
            }
        } else {
            for (Map.Entry<String, DatabaseType> each: database.getResourceMetaData().getStorageTypes().entrySet()) {
                if (each.getValue() instanceof PostgreSQLDatabaseType) {
                    dataSource = each.getKey();
                    break;
                }
            }
        }
        return Collections.singletonList(new ExecutionUnit(dataSource.length() == 0 ? instanceDataSourceNames.iterator().next() : dataSource,
                new SQLUnit(sql, parameter, getGenericTableRouteMappers(sqlStatementContext))));
    }
//
//    private static Collection<ExecutionUnit> build(final RouteSQLRewriteResult sqlRewriteResult) {
//        Collection<ExecutionUnit> result = new LinkedHashSet<>(sqlRewriteResult.getSqlRewriteUnits().size(), 1f);
//        for (Entry<RouteUnit, SQLRewriteUnit> entry : sqlRewriteResult.getSqlRewriteUnits().entrySet()) {
//            result.add(new ExecutionUnit(entry.getKey().getDataSourceMapper().getActualName(),
//                    new SQLUnit(entry.getValue().getSql(), entry.getValue().getParameters(), getRouteTableRouteMappers(entry.getKey().getTableMappers()))));
//        }
//        return result;
//    }

    private static List<RouteMapper> getRouteTableRouteMappers(final Collection<RouteMapper> tableMappers) {
        if (null == tableMappers) {
            return Collections.emptyList();
        }
        List<RouteMapper> result = new ArrayList<>(tableMappers.size());
        for (RouteMapper each : tableMappers) {
            result.add(new RouteMapper(each.getLogicName(), each.getActualName()));
        }
        return result;
    }
    
    private static List<RouteMapper> getGenericTableRouteMappers(final SQLStatementContext<?> sqlStatementContext) {
        TablesContext tablesContext = null;
        if (null != sqlStatementContext) {
            tablesContext = sqlStatementContext.getTablesContext();
        }
        return null == tablesContext ? Collections.emptyList() : tablesContext.getTableNames().stream().map(each -> new RouteMapper(each, each)).collect(Collectors.toList());
    }
}
