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

package org.dbiir.harp.utils.common.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.dbiir.harp.utils.common.enums.LogicalOperator;
import org.dbiir.harp.utils.common.segment.dml.expr.*;
import org.dbiir.harp.utils.common.segment.dml.expr.simple.ParameterMarkerExpressionSegment;
import org.dbiir.harp.utils.common.segment.dml.predicate.AndPredicate;

import java.util.*;

/**
 * Expression extract utility class.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ExpressionExtractUtils {
    
    /**
     * Get and predicate collection.
     * 
     * @param expression expression segment
     * @return and predicate collection
     */
    public static Collection<AndPredicate> getAndPredicates(final ExpressionSegment expression) {
        Collection<AndPredicate> result = new LinkedList<>();
        extractAndPredicates(result, expression);
        return result;
    }
    
    private static void extractAndPredicates(final Collection<AndPredicate> result, final ExpressionSegment expression) {
        if (!(expression instanceof BinaryOperationExpression)) {
            result.add(createAndPredicate(expression));
            return;
        }
        BinaryOperationExpression binaryExpression = (BinaryOperationExpression) expression;
        Optional<LogicalOperator> logicalOperator = LogicalOperator.valueFrom(binaryExpression.getOperator());
        if (logicalOperator.isPresent() && LogicalOperator.OR == logicalOperator.get()) {
            extractAndPredicates(result, binaryExpression.getLeft());
            extractAndPredicates(result, binaryExpression.getRight());
        } else if (logicalOperator.isPresent() && LogicalOperator.AND == logicalOperator.get()) {
            Collection<AndPredicate> predicates = getAndPredicates(binaryExpression.getRight());
            for (AndPredicate each : getAndPredicates(binaryExpression.getLeft())) {
                extractCombinedAndPredicates(result, each, predicates);
            }
        } else {
            result.add(createAndPredicate(expression));
        }
    }
    
    private static void extractCombinedAndPredicates(final Collection<AndPredicate> result, final AndPredicate current, final Collection<AndPredicate> predicates) {
        for (AndPredicate each : predicates) {
            AndPredicate predicate = new AndPredicate();
            predicate.getPredicates().addAll(current.getPredicates());
            predicate.getPredicates().addAll(each.getPredicates());
            result.add(predicate);
        }
    }
    
    private static AndPredicate createAndPredicate(final ExpressionSegment expression) {
        AndPredicate result = new AndPredicate();
        result.getPredicates().add(expression);
        return result;
    }
    
    /**
     * Get parameter marker expression collection.
     * 
     * @param expressions expression collection
     * @return parameter marker expression collection
     */
    public static List<ParameterMarkerExpressionSegment> getParameterMarkerExpressions(final Collection<ExpressionSegment> expressions) {
        List<ParameterMarkerExpressionSegment> result = new ArrayList<>();
        extractParameterMarkerExpressions(result, expressions);
        return result;
    }
    
    private static void extractParameterMarkerExpressions(final List<ParameterMarkerExpressionSegment> result, final Collection<ExpressionSegment> expressions) {
        for (ExpressionSegment each : expressions) {
            if (each instanceof ParameterMarkerExpressionSegment) {
                result.add((ParameterMarkerExpressionSegment) each);
            }
            // TODO support more expression type if necessary
            if (each instanceof BinaryOperationExpression) {
                extractParameterMarkerExpressions(result, Collections.singletonList(((BinaryOperationExpression) each).getLeft()));
                extractParameterMarkerExpressions(result, Collections.singletonList(((BinaryOperationExpression) each).getRight()));
            }
            if (each instanceof FunctionSegment) {
                extractParameterMarkerExpressions(result, ((FunctionSegment) each).getParameters());
            }
            if (each instanceof TypeCastExpression) {
                extractParameterMarkerExpressions(result, Collections.singletonList(((TypeCastExpression) each).getExpression()));
            }
            if (each instanceof InExpression) {
                extractParameterMarkerExpressions(result, ((InExpression) each).getExpressionList());
            }
        }
    }
}
