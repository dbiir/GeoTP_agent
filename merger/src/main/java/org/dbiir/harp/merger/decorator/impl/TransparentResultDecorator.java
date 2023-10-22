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

package org.dbiir.harp.merger.decorator.impl;

import org.dbiir.harp.executor.sql.execute.result.query.QueryResult;
import org.dbiir.harp.merger.decorator.ResultDecorator;
import org.dbiir.harp.merger.result.MergedResult;
import org.dbiir.harp.merger.result.impl.transparent.TransparentMergedResult;
import org.dbiir.harp.utils.binder.statement.SQLStatementContext;
import org.dbiir.harp.utils.common.rule.AgentRule;

/**
 * Transparent result decorator.
 */
public final class TransparentResultDecorator implements ResultDecorator<AgentRule> {
    
    @Override
    public MergedResult decorate(final QueryResult queryResult, final SQLStatementContext<?> sqlStatementContext, final AgentRule rule) {
        return new TransparentMergedResult(queryResult);
    }
    
    @Override
    public MergedResult decorate(final MergedResult mergedResult, final SQLStatementContext<?> sqlStatementContext, final AgentRule rule) {
        return mergedResult;
    }
}
