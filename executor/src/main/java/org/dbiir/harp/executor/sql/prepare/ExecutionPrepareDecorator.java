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

package org.dbiir.harp.executor.sql.prepare;

import org.dbiir.harp.executor.kernel.model.ExecutionGroup;
import org.dbiir.harp.utils.router.context.RouteContext;
import org.dbiir.harp.utils.common.rule.AgentRule;
import org.dbiir.harp.utils.common.spi.annotation.SingletonSPI;
import org.dbiir.harp.utils.common.spi.type.ordered.OrderedSPI;

import java.util.Collection;

/**
 * Execution prepare decorator.
 * 
 * @param <T> type of input value
 * @param <R> type of Agent rule
 */
@SingletonSPI
public interface ExecutionPrepareDecorator<T, R extends AgentRule> extends OrderedSPI<R> {
    
    /**
     * Decorate execution groups.
     * 
     * @param routeContext route context
     * @param rule Agent rule
     * @param executionGroups execution groups to be decorated
     * @return decorated execution groups
     */
    Collection<ExecutionGroup<T>> decorate(RouteContext routeContext, R rule, Collection<ExecutionGroup<T>> executionGroups);
}
