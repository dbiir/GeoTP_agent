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

package org.dbiir.harp.utils.binder.statement.dml;

import lombok.Getter;
import org.dbiir.harp.utils.binder.segment.table.TablesContext;
import org.dbiir.harp.utils.binder.statement.CommonSQLStatementContext;
import org.dbiir.harp.utils.binder.type.TableAvailable;
import org.dbiir.harp.utils.binder.type.WhereAvailable;
import org.dbiir.harp.utils.common.extractor.TableExtractor;
import org.dbiir.harp.utils.common.segment.dml.column.ColumnSegment;
import org.dbiir.harp.utils.common.segment.dml.expr.BinaryOperationExpression;
import org.dbiir.harp.utils.common.segment.dml.expr.simple.LiteralExpressionSegment;
import org.dbiir.harp.utils.common.segment.dml.predicate.WhereSegment;
import org.dbiir.harp.utils.common.segment.generic.table.SimpleTableSegment;
import org.dbiir.harp.utils.common.statement.dml.UpdateStatement;
import org.dbiir.harp.utils.common.util.ColumnExtractor;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Update SQL statement context.
 */
@Getter
public final class UpdateStatementContext extends CommonSQLStatementContext<UpdateStatement> implements TableAvailable, WhereAvailable {
    
    private final TablesContext tablesContext;
    
    private final Collection<WhereSegment> whereSegments = new LinkedList<>();
    
    private final Collection<ColumnSegment> columnSegments = new LinkedList<>();
    
    public UpdateStatementContext(final UpdateStatement sqlStatement) {
        super(sqlStatement);
        tablesContext = new TablesContext(getAllSimpleTableSegments(), getDatabaseType());
        getSqlStatement().getWhere().ifPresent(whereSegments::add);
        ColumnExtractor.extractColumnSegments(columnSegments, whereSegments);
    }
    
    private Collection<SimpleTableSegment> getAllSimpleTableSegments() {
        TableExtractor tableExtractor = new TableExtractor();
        tableExtractor.extractTablesFromUpdate(getSqlStatement());
        return tableExtractor.getRewriteTables();
    }
    
    @Override
    public Collection<SimpleTableSegment> getAllTables() {
        return tablesContext.getTables();
    }
    
    @Override
    public Collection<WhereSegment> getWhereSegments() {
        return whereSegments;
    }
    
    @Override
    public Collection<ColumnSegment> getColumnSegments() {
        return columnSegments;
    }
    
    public List<Integer> getKey() {
        List<Integer> result = new LinkedList<>();
        for (WhereSegment whereSegment : whereSegments) {
            result.add(whereSegment.getKey());
        }
        return result;
    }
    
    public List<String> getTableName() {
        List<String> result = new LinkedList<>();
        for (SimpleTableSegment simpleSQLStatement : tablesContext.getTables()) {
            result.add(simpleSQLStatement.getTableName().getIdentifier().getValue());
        }
        return result;
    }
}
