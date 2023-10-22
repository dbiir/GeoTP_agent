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

package org.dbiir.harp.utils.common.extractor;

import lombok.Getter;
import org.dbiir.harp.utils.common.handler.dml.InsertStatementHandler;
import org.dbiir.harp.utils.common.handler.dml.SelectStatementHandler;
import org.dbiir.harp.utils.common.segment.dml.assignment.AssignmentSegment;
import org.dbiir.harp.utils.common.segment.dml.column.ColumnSegment;
import org.dbiir.harp.utils.common.segment.dml.combine.CombineSegment;
import org.dbiir.harp.utils.common.segment.dml.expr.*;
import org.dbiir.harp.utils.common.segment.dml.expr.subquery.SubqueryExpressionSegment;
import org.dbiir.harp.utils.common.segment.dml.item.*;
import org.dbiir.harp.utils.common.segment.dml.order.item.ColumnOrderByItemSegment;
import org.dbiir.harp.utils.common.segment.dml.order.item.OrderByItemSegment;
import org.dbiir.harp.utils.common.segment.dml.predicate.LockSegment;
import org.dbiir.harp.utils.common.segment.generic.OwnerAvailable;
import org.dbiir.harp.utils.common.segment.generic.OwnerSegment;
import org.dbiir.harp.utils.common.segment.generic.table.*;
import org.dbiir.harp.utils.common.statement.SQLStatement;

import org.dbiir.harp.utils.common.statement.dml.DeleteStatement;
import org.dbiir.harp.utils.common.statement.dml.InsertStatement;
import org.dbiir.harp.utils.common.statement.dml.SelectStatement;
import org.dbiir.harp.utils.common.statement.dml.UpdateStatement;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Optional;

@Getter
public final class TableExtractor {
    
    private final Collection<SimpleTableSegment> rewriteTables = new LinkedList<>();
    
    private final Collection<TableSegment> tableContext = new LinkedList<>();
    
    /**
     * Extract table that should be rewritten from select statement.
     *
     * @param selectStatement select statement
     */
    public void extractTablesFromSelect(final SelectStatement selectStatement) {
        if (selectStatement.getCombine().isPresent()) {
            CombineSegment combineSegment = selectStatement.getCombine().get();
            extractTablesFromSelect(combineSegment.getLeft());
            extractTablesFromSelect(combineSegment.getRight());
        }
        if (null != selectStatement.getFrom() && !selectStatement.getCombine().isPresent()) {
            extractTablesFromTableSegment(selectStatement.getFrom());
        }
        if (selectStatement.getWhere().isPresent()) {
            extractTablesFromExpression(selectStatement.getWhere().get().getExpr());
        }
        if (null != selectStatement.getProjections()) {
            extractTablesFromProjections(selectStatement.getProjections());
        }
        if (selectStatement.getGroupBy().isPresent()) {
            extractTablesFromOrderByItems(selectStatement.getGroupBy().get().getGroupByItems());
        }
        if (selectStatement.getOrderBy().isPresent()) {
            extractTablesFromOrderByItems(selectStatement.getOrderBy().get().getOrderByItems());
        }
        Optional<LockSegment> lockSegment = SelectStatementHandler.getLockSegment(selectStatement);
        lockSegment.ifPresent(this::extractTablesFromLock);
    }
    
    private void extractTablesFromTableSegment(final TableSegment tableSegment) {
        if (tableSegment instanceof SimpleTableSegment) {
            tableContext.add(tableSegment);
            rewriteTables.add((SimpleTableSegment) tableSegment);
        }
        if (tableSegment instanceof SubqueryTableSegment) {
            tableContext.add(tableSegment);
            TableExtractor tableExtractor = new TableExtractor();
            tableExtractor.extractTablesFromSelect(((SubqueryTableSegment) tableSegment).getSubquery().getSelect());
            rewriteTables.addAll(tableExtractor.rewriteTables);
        }
        if (tableSegment instanceof JoinTableSegment) {
            extractTablesFromJoinTableSegment((JoinTableSegment) tableSegment);
        }
        if (tableSegment instanceof DeleteMultiTableSegment) {
            DeleteMultiTableSegment deleteMultiTableSegment = (DeleteMultiTableSegment) tableSegment;
            rewriteTables.addAll(deleteMultiTableSegment.getActualDeleteTables());
            extractTablesFromTableSegment(deleteMultiTableSegment.getRelationTable());
        }
    }
    
    private void extractTablesFromJoinTableSegment(final JoinTableSegment tableSegment) {
        extractTablesFromTableSegment(tableSegment.getLeft());
        extractTablesFromTableSegment(tableSegment.getRight());
        extractTablesFromExpression(tableSegment.getCondition());
    }
    
    private void extractTablesFromExpression(final ExpressionSegment expressionSegment) {
        if (expressionSegment instanceof ColumnSegment) {
            if (((ColumnSegment) expressionSegment).getOwner().isPresent() && needRewrite(((ColumnSegment) expressionSegment).getOwner().get())) {
                OwnerSegment ownerSegment = ((ColumnSegment) expressionSegment).getOwner().get();
                rewriteTables.add(new SimpleTableSegment(new TableNameSegment(ownerSegment.getStartIndex(), ownerSegment.getStopIndex(), ownerSegment.getIdentifier())));
            }
        }
        if (expressionSegment instanceof ListExpression) {
            for (ExpressionSegment each : ((ListExpression) expressionSegment).getItems()) {
                extractTablesFromExpression(each);
            }
        }
        if (expressionSegment instanceof ExistsSubqueryExpression) {
            extractTablesFromSelect(((ExistsSubqueryExpression) expressionSegment).getSubquery().getSelect());
        }
        if (expressionSegment instanceof BetweenExpression) {
            extractTablesFromExpression(((BetweenExpression) expressionSegment).getLeft());
            extractTablesFromExpression(((BetweenExpression) expressionSegment).getBetweenExpr());
            extractTablesFromExpression(((BetweenExpression) expressionSegment).getAndExpr());
        }
        if (expressionSegment instanceof InExpression) {
            extractTablesFromExpression(((InExpression) expressionSegment).getLeft());
            extractTablesFromExpression(((InExpression) expressionSegment).getRight());
        }
        if (expressionSegment instanceof SubqueryExpressionSegment) {
            extractTablesFromSelect(((SubqueryExpressionSegment) expressionSegment).getSubquery().getSelect());
        }
        if (expressionSegment instanceof BinaryOperationExpression) {
            extractTablesFromExpression(((BinaryOperationExpression) expressionSegment).getLeft());
            extractTablesFromExpression(((BinaryOperationExpression) expressionSegment).getRight());
        }
    }
    
    private void extractTablesFromProjections(final ProjectionsSegment projections) {
        for (ProjectionSegment each : projections.getProjections()) {
            if (each instanceof SubqueryProjectionSegment) {
                extractTablesFromSelect(((SubqueryProjectionSegment) each).getSubquery().getSelect());
            } else if (each instanceof OwnerAvailable) {
                if (((OwnerAvailable) each).getOwner().isPresent() && needRewrite(((OwnerAvailable) each).getOwner().get())) {
                    OwnerSegment ownerSegment = ((OwnerAvailable) each).getOwner().get();
                    rewriteTables.add(createSimpleTableSegment(ownerSegment));
                }
            } else if (each instanceof ColumnProjectionSegment) {
                if (((ColumnProjectionSegment) each).getColumn().getOwner().isPresent() && needRewrite(((ColumnProjectionSegment) each).getColumn().getOwner().get())) {
                    OwnerSegment ownerSegment = ((ColumnProjectionSegment) each).getColumn().getOwner().get();
                    rewriteTables.add(createSimpleTableSegment(ownerSegment));
                }
            } else if (each instanceof AggregationProjectionSegment) {
                ((AggregationProjectionSegment) each).getParameters().forEach(this::extractTablesFromExpression);
            }
        }
    }
    
    private SimpleTableSegment createSimpleTableSegment(final OwnerSegment ownerSegment) {
        SimpleTableSegment result = new SimpleTableSegment(new TableNameSegment(ownerSegment.getStartIndex(), ownerSegment.getStopIndex(), ownerSegment.getIdentifier()));
        ownerSegment.getOwner().ifPresent(result::setOwner);
        return result;
    }
    
    private void extractTablesFromOrderByItems(final Collection<OrderByItemSegment> orderByItems) {
        for (OrderByItemSegment each : orderByItems) {
            if (each instanceof ColumnOrderByItemSegment) {
                Optional<OwnerSegment> owner = ((ColumnOrderByItemSegment) each).getColumn().getOwner();
                if (owner.isPresent() && needRewrite(owner.get())) {
                    rewriteTables.add(new SimpleTableSegment(new TableNameSegment(owner.get().getStartIndex(), owner.get().getStopIndex(), owner.get().getIdentifier())));
                }
            }
        }
    }
    
    private void extractTablesFromLock(final LockSegment lockSegment) {
        rewriteTables.addAll(lockSegment.getTables());
    }
    
    /**
     * Extract table that should be rewritten from delete statement.
     *
     * @param deleteStatement delete statement
     */
    public void extractTablesFromDelete(final DeleteStatement deleteStatement) {
        extractTablesFromTableSegment(deleteStatement.getTable());
        if (deleteStatement.getWhere().isPresent()) {
            extractTablesFromExpression(deleteStatement.getWhere().get().getExpr());
        }
    }
    
    /**
     * Extract table that should be rewritten from insert statement.
     *
     * @param insertStatement insert statement
     */
    public void extractTablesFromInsert(final InsertStatement insertStatement) {
        if (null != insertStatement.getTable()) {
            extractTablesFromTableSegment(insertStatement.getTable());
        }
        if (!insertStatement.getColumns().isEmpty()) {
            for (ColumnSegment each : insertStatement.getColumns()) {
                extractTablesFromExpression(each);
            }
        }
        InsertStatementHandler.getOnDuplicateKeyColumnsSegment(insertStatement).ifPresent(optional -> extractTablesFromAssignmentItems(optional.getColumns()));
        if (insertStatement.getInsertSelect().isPresent()) {
            extractTablesFromSelect(insertStatement.getInsertSelect().get().getSelect());
        }
    }
    
    private void extractTablesFromAssignmentItems(final Collection<AssignmentSegment> assignmentItems) {
        assignmentItems.forEach(each -> extractTablesFromColumnSegments(each.getColumns()));
    }
    
    private void extractTablesFromColumnSegments(final Collection<ColumnSegment> columnSegments) {
        columnSegments.forEach(each -> {
            if (each.getOwner().isPresent() && needRewrite(each.getOwner().get())) {
                OwnerSegment ownerSegment = each.getOwner().get();
                rewriteTables.add(new SimpleTableSegment(new TableNameSegment(ownerSegment.getStartIndex(), ownerSegment.getStopIndex(), ownerSegment.getIdentifier())));
            }
        });
    }
    
    /**
     * Extract table that should be rewritten from update statement.
     *
     * @param updateStatement update statement.
     */
    public void extractTablesFromUpdate(final UpdateStatement updateStatement) {
        extractTablesFromTableSegment(updateStatement.getTable());
        updateStatement.getSetAssignment().getAssignments().forEach(each -> extractTablesFromExpression(each.getColumns().get(0)));
        if (updateStatement.getWhere().isPresent()) {
            extractTablesFromExpression(updateStatement.getWhere().get().getExpr());
        }
    }
    
    /**
     * Check if the table needs to be overwritten.
     *
     * @param owner owner
     * @return boolean
     */
    public boolean needRewrite(final OwnerSegment owner) {
        for (TableSegment each : tableContext) {
            if (owner.getIdentifier().getValue().equalsIgnoreCase(each.getAlias().orElse(null))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Extract table that should be rewritten from SQL statement.
     *
     * @param sqlStatement SQL statement
     */
    public void extractTablesFromSQLStatement(final SQLStatement sqlStatement) {
        if (sqlStatement instanceof SelectStatement) {
            extractTablesFromSelect((SelectStatement) sqlStatement);
        } else if (sqlStatement instanceof InsertStatement) {
            extractTablesFromInsert((InsertStatement) sqlStatement);
        } else if (sqlStatement instanceof UpdateStatement) {
            extractTablesFromUpdate((UpdateStatement) sqlStatement);
        } else if (sqlStatement instanceof DeleteStatement) {
            extractTablesFromDelete((DeleteStatement) sqlStatement);
        }
    }
}
