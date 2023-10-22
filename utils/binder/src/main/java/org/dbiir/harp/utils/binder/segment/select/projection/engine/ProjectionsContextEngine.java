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

package org.dbiir.harp.utils.binder.segment.select.projection.engine;

import org.dbiir.harp.utils.binder.segment.select.groupby.GroupByContext;
import org.dbiir.harp.utils.binder.segment.select.orderby.OrderByContext;
import org.dbiir.harp.utils.binder.segment.select.orderby.OrderByItem;
import org.dbiir.harp.utils.binder.segment.select.projection.DerivedColumn;
import org.dbiir.harp.utils.binder.segment.select.projection.Projection;
import org.dbiir.harp.utils.binder.segment.select.projection.ProjectionsContext;
import org.dbiir.harp.utils.binder.segment.select.projection.impl.ColumnProjection;
import org.dbiir.harp.utils.binder.segment.select.projection.impl.DerivedProjection;
import org.dbiir.harp.utils.binder.segment.select.projection.impl.ShorthandProjection;
import org.dbiir.harp.utils.common.database.type.DatabaseType;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentSchema;
import org.dbiir.harp.utils.common.segment.dml.column.ColumnSegment;
import org.dbiir.harp.utils.common.segment.dml.item.ProjectionSegment;
import org.dbiir.harp.utils.common.segment.dml.item.ProjectionsSegment;
import org.dbiir.harp.utils.common.segment.dml.order.item.ColumnOrderByItemSegment;
import org.dbiir.harp.utils.common.segment.dml.order.item.IndexOrderByItemSegment;
import org.dbiir.harp.utils.common.segment.dml.order.item.OrderByItemSegment;
import org.dbiir.harp.utils.common.segment.dml.order.item.TextOrderByItemSegment;
import org.dbiir.harp.utils.common.segment.generic.table.TableSegment;
import org.dbiir.harp.utils.common.util.SQLUtils;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

/**
 * Projections context engine.
 */
public final class ProjectionsContextEngine {
    
    private final ProjectionEngine projectionEngine;
    
    public ProjectionsContextEngine(final String databaseName, final Map<String, AgentSchema> schemas, final DatabaseType databaseType) {
        projectionEngine = new ProjectionEngine(databaseName, schemas, databaseType);
    }
    
    /**
     * Create projections context.
     *
     * @param table table segment
     * @param projectionsSegment projection segments
     * @param groupByContext group by context
     * @param orderByContext order by context
     * @return projections context
     */
    public ProjectionsContext createProjectionsContext(final TableSegment table, final ProjectionsSegment projectionsSegment,
                                                       final GroupByContext groupByContext, final OrderByContext orderByContext) {
        Collection<Projection> projections = getProjections(table, projectionsSegment);
        ProjectionsContext result = new ProjectionsContext(projectionsSegment.getStartIndex(), projectionsSegment.getStopIndex(), projectionsSegment.isDistinctRow(), projections);
        result.getProjections().addAll(getDerivedGroupByColumns(groupByContext, projections));
        result.getProjections().addAll(getDerivedOrderByColumns(orderByContext, projections));
        return result;
    }
    
    private Collection<Projection> getProjections(final TableSegment table, final ProjectionsSegment projectionsSegment) {
        Collection<Projection> result = new LinkedList<>();
        for (ProjectionSegment each : projectionsSegment.getProjections()) {
            projectionEngine.createProjection(table, each).ifPresent(result::add);
        }
        return result;
    }
    
    private Collection<Projection> getDerivedGroupByColumns(final GroupByContext groupByContext, final Collection<Projection> projections) {
        return getDerivedOrderColumns(groupByContext.getItems(), DerivedColumn.GROUP_BY_ALIAS, projections);
    }
    
    private Collection<Projection> getDerivedOrderByColumns(final OrderByContext orderByContext, final Collection<Projection> projections) {
        return getDerivedOrderColumns(orderByContext.getItems(), DerivedColumn.ORDER_BY_ALIAS, projections);
    }
    
    private Collection<Projection> getDerivedOrderColumns(final Collection<OrderByItem> orderItems, final DerivedColumn derivedColumn, final Collection<Projection> projections) {
        Collection<Projection> result = new LinkedList<>();
        int derivedColumnOffset = 0;
        for (OrderByItem each : orderItems) {
            if (!containsProjection(each.getSegment(), projections)) {
                result.add(new DerivedProjection(((TextOrderByItemSegment) each.getSegment()).getText(), derivedColumn.getDerivedColumnAlias(derivedColumnOffset++), each.getSegment()));
            }
        }
        return result;
    }
    
    private boolean containsProjection(final OrderByItemSegment orderByItem, final Collection<Projection> projections) {
        if (orderByItem instanceof IndexOrderByItemSegment) {
            return true;
        }
        for (Projection each : projections) {
            if (orderByItem instanceof ColumnOrderByItemSegment && isSameColumn(each, ((ColumnOrderByItemSegment) orderByItem).getColumn())) {
                return true;
            }
            String text = ((TextOrderByItemSegment) orderByItem).getText();
            if (isSameAlias(each, text) || isSameQualifiedName(each, text)) {
                return true;
            }
        }
        return false;
    }
    
    private boolean isSameColumn(final Projection projection, final ColumnSegment columnSegment) {
        Collection<ColumnProjection> columns = getColumnProjections(projection);
        if (columns.isEmpty()) {
            return false;
        }
        boolean columnSegmentPresent = columnSegment.getOwner().isPresent();
        for (ColumnProjection each : columns) {
            if (columnSegmentPresent ? isSameQualifiedName(each, columnSegment.getQualifiedName()) : isSameName(each, columnSegment.getQualifiedName())) {
                return true;
            }
        }
        return false;
    }
    
    private Collection<ColumnProjection> getColumnProjections(final Projection projection) {
        Collection<ColumnProjection> result = new LinkedList<>();
        if (projection instanceof ColumnProjection) {
            result.add((ColumnProjection) projection);
        }
        if (projection instanceof ShorthandProjection) {
            result.addAll(((ShorthandProjection) projection).getColumnProjections());
        }
        return result;
    }
    
    private boolean isSameName(final ColumnProjection projection, final String text) {
        return SQLUtils.getExactlyValue(text).equalsIgnoreCase(projection.getName());
    }
    
    private boolean isSameAlias(final Projection projection, final String text) {
        return projection.getAlias().isPresent() && SQLUtils.getExactlyValue(text).equalsIgnoreCase(SQLUtils.getExactlyValue(projection.getAlias().get()));
    }
    
    private boolean isSameQualifiedName(final Projection projection, final String text) {
        return SQLUtils.getExactlyValue(text).equalsIgnoreCase(SQLUtils.getExactlyValue(projection.getExpression()));
    }
}
