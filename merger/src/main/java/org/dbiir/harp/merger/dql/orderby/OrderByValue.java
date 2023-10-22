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

package org.dbiir.harp.merger.dql.orderby;

import lombok.Getter;
import org.dbiir.harp.executor.sql.execute.result.query.QueryResult;
import org.dbiir.harp.utils.binder.segment.select.orderby.OrderByItem;
import org.dbiir.harp.utils.binder.statement.dml.SelectStatementContext;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentColumn;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentSchema;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentTable;
import org.dbiir.harp.utils.common.segment.dml.order.item.ColumnOrderByItemSegment;
import org.dbiir.harp.utils.common.segment.dml.order.item.IndexOrderByItemSegment;
import org.dbiir.harp.utils.common.segment.dml.order.item.OrderByItemSegment;
import org.dbiir.harp.utils.common.segment.generic.table.SimpleTableSegment;

import java.sql.SQLException;
import java.util.*;

/**
 * Order by value.
 */
public final class OrderByValue implements Comparable<OrderByValue> {
    
    @Getter
    private final QueryResult queryResult;
    
    private final Collection<OrderByItem> orderByItems;
    
    private final List<Boolean> orderValuesCaseSensitive;
    
    private final SelectStatementContext selectStatementContext;
    
    private List<Comparable<?>> orderValues;
    
    public OrderByValue(final QueryResult queryResult, final Collection<OrderByItem> orderByItems,
                        final SelectStatementContext selectStatementContext, final AgentSchema schema) throws SQLException {
        this.queryResult = queryResult;
        this.orderByItems = orderByItems;
        this.selectStatementContext = selectStatementContext;
        orderValuesCaseSensitive = getOrderValuesCaseSensitive(schema);
    }
    
    private List<Boolean> getOrderValuesCaseSensitive(final AgentSchema schema) throws SQLException {
        List<Boolean> result = new ArrayList<>(orderByItems.size());
        for (OrderByItem eachOrderByItem : orderByItems) {
            result.add(getOrderValuesCaseSensitiveFromTables(schema, eachOrderByItem));
        }
        return result;
    }
    
    private boolean getOrderValuesCaseSensitiveFromTables(final AgentSchema schema, final OrderByItem eachOrderByItem) throws SQLException {
        for (SimpleTableSegment each : selectStatementContext.getAllTables()) {
            String tableName = each.getTableName().getIdentifier().getValue();
            AgentTable table = schema.getTable(tableName);
            Map<String, AgentColumn> columns = table.getColumns();
            OrderByItemSegment orderByItemSegment = eachOrderByItem.getSegment();
            if (orderByItemSegment instanceof ColumnOrderByItemSegment) {
                String columnName = ((ColumnOrderByItemSegment) orderByItemSegment).getColumn().getIdentifier().getValue();
                if (columns.containsKey(columnName)) {
                    return columns.get(columnName).isCaseSensitive();
                }
            } else if (orderByItemSegment instanceof IndexOrderByItemSegment) {
                int columnIndex = ((IndexOrderByItemSegment) orderByItemSegment).getColumnIndex();
                String columnName = queryResult.getMetaData().getColumnName(columnIndex);
                if (columns.containsKey(columnName)) {
                    return columns.get(columnName).isCaseSensitive();
                }
            } else {
                return false;
            }
        }
        return false;
    }
    
    /**
     * Iterate next data.
     *
     * @return has next data
     * @throws SQLException SQL exception
     */
    public boolean next() throws SQLException {
        boolean result = queryResult.next();
        orderValues = result ? getOrderValues() : Collections.emptyList();
        return result;
    }
    
    private List<Comparable<?>> getOrderValues() throws SQLException {
        List<Comparable<?>> result = new ArrayList<>(orderByItems.size());
        for (OrderByItem each : orderByItems) {
            Object value = queryResult.getValue(each.getIndex(), Object.class);
            result.add((Comparable<?>) value);
        }
        return result;
    }
    
    @Override
    public int compareTo(final OrderByValue orderByValue) {
        int i = 0;
        for (OrderByItem each : orderByItems) {
            int result = CompareUtils.compareTo(orderValues.get(i), orderByValue.orderValues.get(i), each.getSegment().getOrderDirection(),
                    each.getSegment().getNullsOrderType(selectStatementContext.getDatabaseType().getType()), orderValuesCaseSensitive.get(i));
            if (0 != result) {
                return result;
            }
            i++;
        }
        return 0;
    }
}
