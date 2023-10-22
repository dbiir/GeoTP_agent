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

package org.dbiir.harp.backend.handler.admin.executor;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.dbiir.harp.backend.session.ConnectionSession;
import org.dbiir.harp.executor.sql.execute.result.query.QueryResult;
import org.dbiir.harp.executor.sql.execute.result.query.QueryResultMetaData;
import org.dbiir.harp.executor.sql.execute.result.query.impl.raw.metadata.RawQueryResultColumnMetaData;
import org.dbiir.harp.executor.sql.execute.result.query.impl.raw.metadata.RawQueryResultMetaData;
import org.dbiir.harp.executor.sql.execute.result.query.impl.raw.type.RawMemoryQueryResult;
import org.dbiir.harp.executor.sql.execute.result.query.type.memory.row.MemoryQueryResultDataRow;
import org.dbiir.harp.merger.result.MergedResult;
import org.dbiir.harp.merger.result.impl.transparent.TransparentMergedResult;
import org.dbiir.harp.utils.binder.segment.select.projection.engine.ProjectionEngine;
import org.dbiir.harp.utils.common.segment.dml.item.ShorthandProjectionSegment;
import org.dbiir.harp.utils.common.segment.generic.table.TableSegment;
import org.dbiir.harp.utils.common.statement.dml.SelectStatement;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * No resource show executor.
 */
@Getter
@RequiredArgsConstructor
public final class NoResourceShowExecutor implements DatabaseAdminQueryExecutor {
    
    private MergedResult mergedResult;
    
    private final SelectStatement sqlStatement;
    
    private Collection<Object> expressions = Collections.emptyList();
    
    @Override
    public void execute(final ConnectionSession connectionSession) {
        TableSegment tableSegment = sqlStatement.getFrom();
        expressions = sqlStatement.getProjections().getProjections().stream().filter(each -> !(each instanceof ShorthandProjectionSegment))
                .map(each -> new ProjectionEngine(null, Collections.emptyMap(), null).createProjection(tableSegment, each))
                .filter(Optional::isPresent).map(each -> each.get().getAlias().isPresent() ? each.get().getAlias().get() : each.get().getExpression()).collect(Collectors.toList());
        mergedResult = new TransparentMergedResult(getQueryResult());
    }
    
    private QueryResult getQueryResult() {
        List<MemoryQueryResultDataRow> rows = new LinkedList<>();
        if (expressions.isEmpty()) {
            rows.add(new MemoryQueryResultDataRow(Collections.singletonList("")));
            return new RawMemoryQueryResult(getQueryResultMetaData(), rows);
        }
        List<Object> row = new ArrayList<>(expressions);
        Collections.fill(row, "");
        rows.add(new MemoryQueryResultDataRow(row));
        return new RawMemoryQueryResult(getQueryResultMetaData(), rows);
    }
    
    @Override
    public QueryResultMetaData getQueryResultMetaData() {
        if (expressions.isEmpty()) {
            RawQueryResultColumnMetaData defaultColumnMetaData = new RawQueryResultColumnMetaData("", "", "", Types.VARCHAR, "VARCHAR", 100, 0);
            return new RawQueryResultMetaData(Collections.singletonList(defaultColumnMetaData));
        }
        List<RawQueryResultColumnMetaData> columns = expressions.stream().map(each -> new RawQueryResultColumnMetaData("", each.toString(), each.toString(), Types.VARCHAR, "VARCHAR", 100, 0))
                .collect(Collectors.toList());
        return new RawQueryResultMetaData(columns);
    }
}
