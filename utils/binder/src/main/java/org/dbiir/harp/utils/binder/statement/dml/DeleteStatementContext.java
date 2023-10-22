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
import org.dbiir.harp.utils.common.segment.dml.predicate.WhereSegment;
import org.dbiir.harp.utils.common.segment.generic.table.SimpleTableSegment;
import org.dbiir.harp.utils.common.statement.dml.DeleteStatement;
import org.dbiir.harp.utils.common.util.ColumnExtractor;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Delete statement context.
 */
@Getter
public final class DeleteStatementContext extends CommonSQLStatementContext<DeleteStatement> implements TableAvailable, WhereAvailable {
    
    private final TablesContext tablesContext;
    
    private final Collection<WhereSegment> whereSegments = new LinkedList<>();
    
    private final Collection<ColumnSegment> columnSegments = new LinkedList<>();
    
    public DeleteStatementContext(final DeleteStatement sqlStatement) {
        super(sqlStatement);
        tablesContext = new TablesContext(getAllSimpleTableSegments(), getDatabaseType());
        getSqlStatement().getWhere().ifPresent(whereSegments::add);
        ColumnExtractor.extractColumnSegments(columnSegments, whereSegments);
    }
    
    private Collection<SimpleTableSegment> getAllSimpleTableSegments() {
        TableExtractor tableExtractor = new TableExtractor();
        tableExtractor.extractTablesFromDelete(getSqlStatement());
        return filterAliasDeleteTable(tableExtractor.getRewriteTables());
    }
    
    private Collection<SimpleTableSegment> filterAliasDeleteTable(final Collection<SimpleTableSegment> tableSegments) {
        Map<String, SimpleTableSegment> aliasTableSegmentMap = new HashMap<>(tableSegments.size(), 1f);
        for (SimpleTableSegment each : tableSegments) {
            each.getAlias().ifPresent(optional -> aliasTableSegmentMap.putIfAbsent(optional, each));
        }
        Collection<SimpleTableSegment> result = new LinkedList<>();
        for (SimpleTableSegment each : tableSegments) {
            SimpleTableSegment aliasDeleteTable = aliasTableSegmentMap.get(each.getTableName().getIdentifier().getValue());
            if (null == aliasDeleteTable || aliasDeleteTable.equals(each)) {
                result.add(each);
            }
        }
        return result;
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
}
