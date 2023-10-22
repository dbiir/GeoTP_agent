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

package org.dbiir.harp.backend.response.header.query;

import lombok.RequiredArgsConstructor;
import org.dbiir.harp.executor.sql.execute.result.query.QueryResultMetaData;
import org.dbiir.harp.utils.binder.segment.select.projection.DerivedColumn;
import org.dbiir.harp.utils.binder.segment.select.projection.Projection;
import org.dbiir.harp.utils.binder.segment.select.projection.ProjectionsContext;
import org.dbiir.harp.utils.binder.segment.select.projection.impl.ColumnProjection;
import org.dbiir.harp.utils.common.database.type.DatabaseType;
import org.dbiir.harp.utils.common.metadata.database.AgentDatabase;
import org.dbiir.harp.utils.common.spi.type.typed.TypedSPILoader;

import java.sql.SQLException;

/**
 * Query header builder engine.
 */
@RequiredArgsConstructor
public final class QueryHeaderBuilderEngine {
    
    private final
    DatabaseType databaseType;
    
    /**
     * Build query header builder.
     *
     * @param queryResultMetaData query result meta data
     * @param database database
     * @param columnIndex column index
     * @return query header
     * @throws SQLException SQL exception
     */
    public QueryHeader build(final QueryResultMetaData queryResultMetaData, final AgentDatabase database, final int columnIndex) throws SQLException {
        String columnName = queryResultMetaData.getColumnName(columnIndex);
        String columnLabel = queryResultMetaData.getColumnLabel(columnIndex);
        return TypedSPILoader.getService(QueryHeaderBuilder.class, databaseType.getType()).build(queryResultMetaData, database, columnName, columnLabel, columnIndex);
    }
    
    /**
     * Build query header builder.
     *
     * @param projectionsContext projections context
     * @param queryResultMetaData query result meta data
     * @param database database
     * @param columnIndex column index
     * @return query header
     * @throws SQLException SQL exception
     */
    public QueryHeader build(final ProjectionsContext projectionsContext,
                             final QueryResultMetaData queryResultMetaData, final AgentDatabase database, final int columnIndex) throws SQLException {
        String columnName = getColumnName(projectionsContext, queryResultMetaData, columnIndex);
        String columnLabel = getColumnLabel(projectionsContext, queryResultMetaData, columnIndex);
        return TypedSPILoader.getService(QueryHeaderBuilder.class, databaseType.getType()).build(queryResultMetaData, database, columnName, columnLabel, columnIndex);
    }
    
    private String getColumnLabel(final ProjectionsContext projectionsContext, final QueryResultMetaData queryResultMetaData, final int columnIndex) throws SQLException {
        Projection projection = projectionsContext.getExpandProjections().get(columnIndex - 1);
        return DerivedColumn.isDerivedColumnName(projection.getColumnLabel()) ? projection.getExpression() : queryResultMetaData.getColumnLabel(columnIndex);
    }
    
    private String getColumnName(final ProjectionsContext projectionsContext, final QueryResultMetaData queryResultMetaData, final int columnIndex) throws SQLException {
        Projection projection = projectionsContext.getExpandProjections().get(columnIndex - 1);
        return projection instanceof ColumnProjection ? ((ColumnProjection) projection).getName() : queryResultMetaData.getColumnName(columnIndex);
    }
}
