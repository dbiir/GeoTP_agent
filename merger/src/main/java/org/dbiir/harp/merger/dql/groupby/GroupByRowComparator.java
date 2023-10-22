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

import lombok.RequiredArgsConstructor;
import org.dbiir.harp.merger.dql.orderby.CompareUtils;
import org.dbiir.harp.merger.result.impl.memory.MemoryQueryResultRow;
import org.dbiir.harp.utils.binder.segment.select.orderby.OrderByItem;
import org.dbiir.harp.utils.binder.statement.dml.SelectStatementContext;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;

/**
 * Group by row comparator.
 */
@RequiredArgsConstructor
public final class GroupByRowComparator implements Comparator<MemoryQueryResultRow> {
    
    private final SelectStatementContext selectStatementContext;
    
    private final List<Boolean> valueCaseSensitive;
    
    @Override
    public int compare(final MemoryQueryResultRow o1, final MemoryQueryResultRow o2) {
        if (!selectStatementContext.getOrderByContext().getItems().isEmpty()) {
            return compare(o1, o2, selectStatementContext.getOrderByContext().getItems());
        }
        return compare(o1, o2, selectStatementContext.getGroupByContext().getItems());
    }
    
    @SuppressWarnings("rawtypes")
    private int compare(final MemoryQueryResultRow o1, final MemoryQueryResultRow o2, final Collection<OrderByItem> orderByItems) {
        for (OrderByItem each : orderByItems) {
            Object orderValue1 = o1.getCell(each.getIndex());
            Object orderValue2 = o2.getCell(each.getIndex());
            int result = CompareUtils.compareTo((Comparable) orderValue1, (Comparable) orderValue2, each.getSegment().getOrderDirection(),
                    each.getSegment().getNullsOrderType(selectStatementContext.getDatabaseType().getType()), valueCaseSensitive.get(each.getIndex()));
            if (0 != result) {
                return result;
            }
        }
        return 0;
    }
}
