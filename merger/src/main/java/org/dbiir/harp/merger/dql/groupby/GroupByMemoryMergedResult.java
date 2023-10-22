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

package org.dbiir.harp.merger.dql.groupby;

import com.google.common.collect.Maps;
import org.dbiir.harp.executor.sql.execute.result.query.QueryResult;
import org.dbiir.harp.merger.dql.groupby.aggregation.AggregationUnit;
import org.dbiir.harp.merger.dql.groupby.aggregation.AggregationUnitFactory;
import org.dbiir.harp.merger.result.impl.memory.MemoryMergedResult;
import org.dbiir.harp.merger.result.impl.memory.MemoryQueryResultRow;
import org.dbiir.harp.utils.binder.segment.select.projection.Projection;
import org.dbiir.harp.utils.binder.segment.select.projection.impl.AggregationDistinctProjection;
import org.dbiir.harp.utils.binder.segment.select.projection.impl.AggregationProjection;
import org.dbiir.harp.utils.binder.statement.SQLStatementContext;
import org.dbiir.harp.utils.binder.statement.dml.SelectStatementContext;
import org.dbiir.harp.utils.common.enums.AggregationType;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentColumn;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentSchema;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentTable;
import org.dbiir.harp.utils.common.segment.generic.table.SimpleTableSegment;

import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;

/**
 * Memory merged result for group by.
 */
public final class GroupByMemoryMergedResult extends MemoryMergedResult {
    
    public GroupByMemoryMergedResult(final List<QueryResult> queryResults, final SelectStatementContext selectStatementContext, final AgentSchema schema) throws SQLException {
        super(schema, selectStatementContext, queryResults);
    }
    
    @Override
    protected List<MemoryQueryResultRow> init(final AgentSchema schema,
                                              final SQLStatementContext<?> sqlStatementContext, final List<QueryResult> queryResults) throws SQLException {
        SelectStatementContext selectStatementContext = (SelectStatementContext) sqlStatementContext;
        Map<GroupByValue, MemoryQueryResultRow> dataMap = new HashMap<>(1024);
        Map<GroupByValue, Map<AggregationProjection, AggregationUnit>> aggregationMap = new HashMap<>(1024);
        for (QueryResult each : queryResults) {
            while (each.next()) {
                GroupByValue groupByValue = new GroupByValue(each, selectStatementContext.getGroupByContext().getItems());
                initForFirstGroupByValue(selectStatementContext, each, groupByValue, dataMap, aggregationMap);
                aggregate(selectStatementContext, each, groupByValue, aggregationMap);
            }
        }
        setAggregationValueToMemoryRow(selectStatementContext, dataMap, aggregationMap);
        List<Boolean> valueCaseSensitive = queryResults.isEmpty() ? Collections.emptyList() : getValueCaseSensitive(queryResults.iterator().next(), selectStatementContext, schema);
        return getMemoryResultSetRows(selectStatementContext, dataMap, valueCaseSensitive);
    }
    
    private void initForFirstGroupByValue(final SelectStatementContext selectStatementContext, final QueryResult queryResult,
                                          final GroupByValue groupByValue, final Map<GroupByValue, MemoryQueryResultRow> dataMap,
                                          final Map<GroupByValue, Map<AggregationProjection, AggregationUnit>> aggregationMap) throws SQLException {
        if (!dataMap.containsKey(groupByValue)) {
            dataMap.put(groupByValue, new MemoryQueryResultRow(queryResult));
        }
        if (!aggregationMap.containsKey(groupByValue)) {
            Map<AggregationProjection, AggregationUnit> map = Maps
                    .toMap(selectStatementContext.getProjectionsContext()
                            .getAggregationProjections(), input -> AggregationUnitFactory.create(input.getType(), input instanceof AggregationDistinctProjection));
            aggregationMap.put(groupByValue, map);
        }
    }
    
    private void aggregate(final SelectStatementContext selectStatementContext, final QueryResult queryResult,
                           final GroupByValue groupByValue, final Map<GroupByValue, Map<AggregationProjection, AggregationUnit>> aggregationMap) throws SQLException {
        for (AggregationProjection each : selectStatementContext.getProjectionsContext().getAggregationProjections()) {
            List<Comparable<?>> values = new ArrayList<>(2);
            if (each.getDerivedAggregationProjections().isEmpty()) {
                values.add(getAggregationValue(queryResult, each));
            } else {
                for (AggregationProjection derived : each.getDerivedAggregationProjections()) {
                    values.add(getAggregationValue(queryResult, derived));
                }
            }
            aggregationMap.get(groupByValue).get(each).merge(values);
        }
    }
    
    private Comparable<?> getAggregationValue(final QueryResult queryResult, final AggregationProjection aggregationProjection) throws SQLException {
        Object result = queryResult.getValue(aggregationProjection.getIndex(), Object.class);
        return (Comparable<?>) result;
    }
    
    private void setAggregationValueToMemoryRow(final SelectStatementContext selectStatementContext,
                                                final Map<GroupByValue, MemoryQueryResultRow> dataMap, final Map<GroupByValue, Map<AggregationProjection, AggregationUnit>> aggregationMap) {
        for (Entry<GroupByValue, MemoryQueryResultRow> entry : dataMap.entrySet()) {
            for (AggregationProjection each : selectStatementContext.getProjectionsContext().getAggregationProjections()) {
                entry.getValue().setCell(each.getIndex(), aggregationMap.get(entry.getKey()).get(each).getResult());
            }
        }
    }
    
    private List<Boolean> getValueCaseSensitive(final QueryResult queryResult, final SelectStatementContext selectStatementContext, final AgentSchema schema) throws SQLException {
        List<Boolean> result = new ArrayList<>();
        result.add(false);
        for (int columnIndex = 1; columnIndex <= queryResult.getMetaData().getColumnCount(); columnIndex++) {
            result.add(getValueCaseSensitiveFromTables(queryResult, selectStatementContext, schema, columnIndex));
        }
        return result;
    }
    
    private boolean getValueCaseSensitiveFromTables(final QueryResult queryResult,
                                                    final SelectStatementContext selectStatementContext, final AgentSchema schema, final int columnIndex) throws SQLException {
        for (SimpleTableSegment each : selectStatementContext.getAllTables()) {
            String tableName = each.getTableName().getIdentifier().getValue();
            AgentTable table = schema.getTable(tableName);
            Map<String, AgentColumn> columns = table.getColumns();
            String columnName = queryResult.getMetaData().getColumnName(columnIndex);
            if (columns.containsKey(columnName)) {
                return columns.get(columnName).isCaseSensitive();
            }
        }
        return false;
    }
    
    private List<MemoryQueryResultRow> getMemoryResultSetRows(final SelectStatementContext selectStatementContext,
                                                              final Map<GroupByValue, MemoryQueryResultRow> dataMap, final List<Boolean> valueCaseSensitive) {
        if (dataMap.isEmpty()) {
            Object[] data = generateReturnData(selectStatementContext);
            return Arrays.stream(data).anyMatch(Objects::nonNull) ? Collections.singletonList(new MemoryQueryResultRow(data)) : Collections.emptyList();
        }
        List<MemoryQueryResultRow> result = new ArrayList<>(dataMap.values());
        result.sort(new GroupByRowComparator(selectStatementContext, valueCaseSensitive));
        return result;
    }
    
    private Object[] generateReturnData(final SelectStatementContext selectStatementContext) {
        List<Projection> projections = new LinkedList<>(selectStatementContext.getProjectionsContext().getExpandProjections());
        Object[] result = new Object[projections.size()];
        for (int i = 0; i < projections.size(); i++) {
            if (projections.get(i) instanceof AggregationProjection && AggregationType.COUNT == ((AggregationProjection) projections.get(i)).getType()) {
                result[i] = 0;
            }
        }
        return result;
    }
}
