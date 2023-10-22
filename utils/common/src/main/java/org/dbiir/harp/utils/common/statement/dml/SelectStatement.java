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

package org.dbiir.harp.utils.common.statement.dml;

import lombok.Getter;
import lombok.Setter;
import org.dbiir.harp.utils.common.segment.dml.combine.CombineSegment;
import org.dbiir.harp.utils.common.segment.dml.item.ProjectionsSegment;
import org.dbiir.harp.utils.common.segment.dml.order.GroupBySegment;
import org.dbiir.harp.utils.common.segment.dml.order.OrderBySegment;
import org.dbiir.harp.utils.common.segment.dml.predicate.HavingSegment;
import org.dbiir.harp.utils.common.segment.dml.predicate.WhereSegment;
import org.dbiir.harp.utils.common.segment.generic.table.TableSegment;
import org.dbiir.harp.utils.common.statement.AbstractSQLStatement;

import java.util.Optional;

/**
 * Select statement.
 */
@Getter
@Setter
public abstract class SelectStatement extends AbstractSQLStatement implements DMLStatement {
    
    private ProjectionsSegment projections;
    
    private TableSegment from;
    
    private WhereSegment where;
    
    private GroupBySegment groupBy;
    
    private HavingSegment having;
    
    private OrderBySegment orderBy;
    
    private CombineSegment combine;
    
    /**
     * Get where.
     *
     * @return where segment
     */
    public Optional<WhereSegment> getWhere() {
        return Optional.ofNullable(where);
    }
    
    /**
     * Get group by segment.
     *
     * @return group by segment
     */
    public Optional<GroupBySegment> getGroupBy() {
        return Optional.ofNullable(groupBy);
    }
    
    /**
     * Get having segment.
     *
     * @return having segment
     */
    public Optional<HavingSegment> getHaving() {
        return Optional.ofNullable(having);
    }
    
    /**
     * Get order by segment.
     *
     * @return order by segment
     */
    public Optional<OrderBySegment> getOrderBy() {
        return Optional.ofNullable(orderBy);
    }
    
    /**
     * Get combine segment.
     *
     * @return combine segment
     */
    public Optional<CombineSegment> getCombine() {
        return Optional.ofNullable(combine);
    }
}
