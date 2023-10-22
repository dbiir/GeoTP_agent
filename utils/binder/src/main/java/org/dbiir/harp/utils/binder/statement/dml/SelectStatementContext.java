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

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;
import org.dbiir.harp.utils.binder.segment.select.groupby.GroupByContext;
import org.dbiir.harp.utils.binder.segment.select.groupby.engine.GroupByContextEngine;
import org.dbiir.harp.utils.binder.segment.select.orderby.OrderByContext;
import org.dbiir.harp.utils.binder.segment.select.orderby.OrderByItem;
import org.dbiir.harp.utils.binder.segment.select.orderby.engine.OrderByContextEngine;
import org.dbiir.harp.utils.binder.segment.select.pagination.PaginationContext;
import org.dbiir.harp.utils.binder.segment.select.pagination.engine.PaginationContextEngine;
import org.dbiir.harp.utils.binder.segment.select.projection.Projection;
import org.dbiir.harp.utils.binder.segment.select.projection.ProjectionsContext;
import org.dbiir.harp.utils.binder.segment.select.projection.engine.ProjectionsContextEngine;
import org.dbiir.harp.utils.binder.segment.select.projection.impl.AggregationDistinctProjection;
import org.dbiir.harp.utils.binder.segment.select.projection.impl.AggregationProjection;
import org.dbiir.harp.utils.binder.segment.select.projection.impl.ColumnProjection;
import org.dbiir.harp.utils.binder.segment.select.projection.impl.ParameterMarkerProjection;
import org.dbiir.harp.utils.binder.segment.table.TablesContext;
import org.dbiir.harp.utils.binder.statement.CommonSQLStatementContext;
import org.dbiir.harp.utils.binder.type.TableAvailable;
import org.dbiir.harp.utils.binder.type.WhereAvailable;
import org.dbiir.harp.utils.common.enums.ParameterMarkerType;
import org.dbiir.harp.utils.common.extractor.TableExtractor;
import org.dbiir.harp.utils.common.metadata.AgentMetaData;
import org.dbiir.harp.utils.common.metadata.database.AgentDatabase;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentSchema;
import org.dbiir.harp.utils.common.segment.dml.column.ColumnSegment;
import org.dbiir.harp.utils.common.segment.dml.expr.ExpressionSegment;
import org.dbiir.harp.utils.common.segment.dml.expr.simple.ParameterMarkerExpressionSegment;
import org.dbiir.harp.utils.common.segment.dml.order.item.*;
import org.dbiir.harp.utils.common.segment.dml.predicate.WhereSegment;
import org.dbiir.harp.utils.common.segment.generic.table.JoinTableSegment;
import org.dbiir.harp.utils.common.segment.generic.table.SimpleTableSegment;
import org.dbiir.harp.utils.common.segment.generic.table.TableSegment;
import org.dbiir.harp.utils.common.statement.dml.SelectStatement;
import org.dbiir.harp.utils.common.util.ExpressionExtractUtils;
import org.dbiir.harp.utils.common.util.SQLUtils;
import org.dbiir.harp.utils.common.util.WhereExtractUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Select SQL statement context.
 */
@Getter
@Setter
public final class SelectStatementContext extends CommonSQLStatementContext<SelectStatement> implements TableAvailable, WhereAvailable {
    
    private final TablesContext tablesContext;
    
    private final ProjectionsContext projectionsContext;
    
    private final GroupByContext groupByContext;
    
    private final OrderByContext orderByContext;
    
    private final Collection<WhereSegment> whereSegments = new LinkedList<>();
    
    private final Collection<ColumnSegment> columnSegments = new LinkedList<>();
    
    private boolean needAggregateRewrite;
    
    private PaginationContext paginationContext;
    
    public SelectStatementContext(final AgentMetaData metaData, final List<Object> params, final SelectStatement sqlStatement, final String defaultDatabaseName) {
        super(sqlStatement);
        extractWhereSegments(whereSegments, sqlStatement);
        tablesContext = new TablesContext(getAllTableSegments(), getDatabaseType());
        String databaseName = tablesContext.getDatabaseName().orElse(defaultDatabaseName);
        groupByContext = new GroupByContextEngine().createGroupByContext(sqlStatement);
        orderByContext = new OrderByContextEngine().createOrderBy(sqlStatement, groupByContext);
        projectionsContext = new ProjectionsContextEngine(databaseName, getSchemas(metaData, databaseName), getDatabaseType())
                .createProjectionsContext(getSqlStatement().getFrom(), getSqlStatement().getProjections(), groupByContext, orderByContext);
        paginationContext = new PaginationContextEngine().createPaginationContext(sqlStatement, projectionsContext, params, whereSegments);
    }
    
//    private Map<Integer, SelectStatementContext> createSubqueryContexts(final AgentMetaData metaData, final List<Object> params, final String defaultDatabaseName) {
//        Collection<SubquerySegment> subquerySegments = SubqueryExtractUtils.getSubquerySegments(getSqlStatement());
//        Map<Integer, SelectStatementContext> result = new HashMap<>(subquerySegments.size(), 1);
//        for (SubquerySegment each : subquerySegments) {
//            SelectStatementContext subqueryContext = new SelectStatementContext(metaData, params, each.getSelect(), defaultDatabaseName);
//            subqueryContext.setSubqueryType(each.getSubqueryType());
//            result.put(each.getStartIndex(), subqueryContext);
//        }
//        return result;
//    }
    
    private Map<String, AgentSchema> getSchemas(final AgentMetaData metaData, final String databaseName) {
        if (null == databaseName) {
            return Collections.emptyMap();
        }
        AgentDatabase database = metaData.getDatabase(databaseName);
        return database.getSchemas();
    }
    
    /**
     * Judge whether contains join query or not.
     *
     * @return whether contains join query or not
     */
    public boolean isContainsJoinQuery() {
        return getSqlStatement().getFrom() instanceof JoinTableSegment;
    }
    
    /**
     * Judge whether contains subquery or not.
     *
     * @return whether contains subquery or not
     */
    public boolean isContainsSubquery() {
        return false;
    }
    
    /**
     * Judge whether contains having or not.
     *
     * @return whether contains having or not
     */
    public boolean isContainsHaving() {
        return getSqlStatement().getHaving().isPresent();
    }
    
    /**
     * Judge whether contains combine or not.
     *
     * @return whether contains combine or not
     */
    public boolean isContainsCombine() {
        return getSqlStatement().getCombine().isPresent();
    }
    
    /**
     * Judge whether contains dollar parameter marker or not.
     * 
     * @return whether contains dollar parameter marker or not
     */
    public boolean isContainsDollarParameterMarker() {
        for (Projection each : projectionsContext.getProjections()) {
            if (each instanceof ParameterMarkerProjection && ParameterMarkerType.DOLLAR == ((ParameterMarkerProjection) each).getParameterMarkerType()) {
                return true;
            }
        }
        for (ParameterMarkerExpressionSegment each : getParameterMarkerExpressions()) {
            if (ParameterMarkerType.DOLLAR == each.getParameterMarkerType()) {
                return true;
            }
        }
        return false;
    }
    
    private Collection<ParameterMarkerExpressionSegment> getParameterMarkerExpressions() {
        Collection<ExpressionSegment> expressions = new LinkedList<>();
        for (WhereSegment each : whereSegments) {
            expressions.add(each.getExpr());
        }
        return ExpressionExtractUtils.getParameterMarkerExpressions(expressions);
    }
    
    /**
     * Judge whether contains partial distinct aggregation.
     * 
     * @return whether contains partial distinct aggregation
     */
    public boolean isContainsPartialDistinctAggregation() {
        Collection<Projection> aggregationProjections = projectionsContext.getProjections().stream().filter(each -> each instanceof AggregationProjection).collect(Collectors.toList());
        Collection<AggregationDistinctProjection> aggregationDistinctProjections = projectionsContext.getAggregationDistinctProjections();
        return aggregationProjections.size() > 1 && !aggregationDistinctProjections.isEmpty() && aggregationProjections.size() != aggregationDistinctProjections.size();
    }
    
    /**
     * Set indexes.
     *
     * @param columnLabelIndexMap map for column label and index
     */
    public void setIndexes(final Map<String, Integer> columnLabelIndexMap) {
        setIndexForAggregationProjection(columnLabelIndexMap);
        setIndexForOrderItem(columnLabelIndexMap, orderByContext.getItems());
        setIndexForOrderItem(columnLabelIndexMap, groupByContext.getItems());
    }
    
    private void setIndexForAggregationProjection(final Map<String, Integer> columnLabelIndexMap) {
        for (AggregationProjection each : projectionsContext.getAggregationProjections()) {
            String columnLabel = SQLUtils.getExactlyValue(each.getColumnLabel());
            Preconditions.checkState(columnLabelIndexMap.containsKey(columnLabel), "Can't find index: %s, please add alias for aggregate selections", each);
            each.setIndex(columnLabelIndexMap.get(columnLabel));
            for (AggregationProjection derived : each.getDerivedAggregationProjections()) {
                String derivedColumnLabel = SQLUtils.getExactlyValue(derived.getColumnLabel());
                Preconditions.checkState(columnLabelIndexMap.containsKey(derivedColumnLabel), "Can't find index: %s", derived);
                derived.setIndex(columnLabelIndexMap.get(derivedColumnLabel));
            }
        }
    }
    
    private void setIndexForOrderItem(final Map<String, Integer> columnLabelIndexMap, final Collection<OrderByItem> orderByItems) {
        for (OrderByItem each : orderByItems) {
            if (each.getSegment() instanceof IndexOrderByItemSegment) {
                each.setIndex(((IndexOrderByItemSegment) each.getSegment()).getColumnIndex());
                continue;
            }
            if (each.getSegment() instanceof ColumnOrderByItemSegment && ((ColumnOrderByItemSegment) each.getSegment()).getColumn().getOwner().isPresent()) {
                Optional<Integer> itemIndex = projectionsContext.findProjectionIndex(((ColumnOrderByItemSegment) each.getSegment()).getText());
                if (itemIndex.isPresent()) {
                    each.setIndex(itemIndex.get());
                    continue;
                }
            }
            String columnLabel = getAlias(each.getSegment()).orElseGet(() -> getOrderItemText((TextOrderByItemSegment) each.getSegment()));
            Preconditions.checkState(columnLabelIndexMap.containsKey(columnLabel), "Can't find index: %s", each);
            if (columnLabelIndexMap.containsKey(columnLabel)) {
                each.setIndex(columnLabelIndexMap.get(columnLabel));
            }
        }
    }
    
    private Optional<String> getAlias(final OrderByItemSegment orderByItem) {
        if (projectionsContext.isUnqualifiedShorthandProjection()) {
            return Optional.empty();
        }
        String rawName = SQLUtils.getExactlyValue(((TextOrderByItemSegment) orderByItem).getText());
        for (Projection each : projectionsContext.getProjections()) {
            if (SQLUtils.getExactlyExpression(rawName).equalsIgnoreCase(SQLUtils.getExactlyExpression(SQLUtils.getExactlyValue(each.getExpression())))) {
                return each.getAlias();
            }
            if (rawName.equalsIgnoreCase(each.getAlias().orElse(null))) {
                return Optional.of(rawName);
            }
            if (isSameColumnName(each, rawName)) {
                return each.getAlias();
            }
        }
        return Optional.empty();
    }
    
    private boolean isSameColumnName(final Projection projection, final String name) {
        return projection instanceof ColumnProjection && name.equalsIgnoreCase(((ColumnProjection) projection).getName());
    }
    
    private String getOrderItemText(final TextOrderByItemSegment orderByItemSegment) {
        if (orderByItemSegment instanceof ColumnOrderByItemSegment) {
            return SQLUtils.getExactlyValue(((ColumnOrderByItemSegment) orderByItemSegment).getColumn().getIdentifier().getValue());
        }
        return SQLUtils.getExactlyValue(((ExpressionOrderByItemSegment) orderByItemSegment).getExpression());
    }
    
    /**
     * Judge group by and order by sequence is same or not.
     *
     * @return group by and order by sequence is same or not
     */
    public boolean isSameGroupByAndOrderByItems() {
        return !groupByContext.getItems().isEmpty() && groupByContext.getItems().equals(orderByContext.getItems());
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
    
    private void extractWhereSegments(final Collection<WhereSegment> whereSegments, final SelectStatement selectStatement) {
        selectStatement.getWhere().ifPresent(whereSegments::add);
//        whereSegments.addAll(WhereExtractUtils.getSubqueryWhereSegments(selectStatement));
        whereSegments.addAll(WhereExtractUtils.getJoinWhereSegments(selectStatement));
    }
    
    private Collection<TableSegment> getAllTableSegments() {
        TableExtractor tableExtractor = new TableExtractor();
        tableExtractor.extractTablesFromSelect(getSqlStatement());
        Collection<TableSegment> result = new LinkedList<>(tableExtractor.getRewriteTables());
        return result;
    }

    public void setUpParameters(final List<Object> params) {
        paginationContext = new PaginationContextEngine().createPaginationContext(getSqlStatement(), projectionsContext, params, whereSegments);
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
