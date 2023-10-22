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

package org.dbiir.harp.executor.kernel.context;


import org.dbiir.harp.executor.sql.context.ExecutionContext;
import org.dbiir.harp.executor.sql.log.SQLLogger;
import org.dbiir.harp.utils.binder.QueryContext;
import org.dbiir.harp.utils.common.config.props.ConfigurationProperties;
import org.dbiir.harp.utils.common.config.props.ConfigurationPropertyKey;
import org.dbiir.harp.utils.common.metadata.database.AgentDatabase;
import org.dbiir.harp.utils.common.metadata.database.rule.AgentRuleMetaData;
import org.dbiir.harp.utils.context.ConnectionContext;
import org.dbiir.harp.utils.router.context.RouteContext;
import org.dbiir.harp.utils.router.engine.SQLRouteEngine;
import org.dbiir.harp.executor.sql.context.ExecutionContextBuilder;

import java.util.List;

/**
 * Kernel processor.
 */
public final class KernelProcessor {
    
    /**
     * Generate execution context.
     *
     * @param queryContext query context
     * @param database database
     * @param globalRuleMetaData global rule meta data
     * @param props configuration properties
     * @param connectionContext connection context
     * @return execution context
     */
    public ExecutionContext generateExecutionContext(final QueryContext queryContext, final AgentDatabase database, final AgentRuleMetaData globalRuleMetaData,
                                                     final ConfigurationProperties props, final ConnectionContext connectionContext) {
        RouteContext routeContext = route(queryContext, database, globalRuleMetaData, props, connectionContext);
        String sql = queryContext.getSql();
        List<Object> parameters = queryContext.getParameters();
        ExecutionContext result = createExecutionContext(queryContext, database, routeContext, sql, parameters);
        logSQL(queryContext, props, result);
        return result;
    }
    
    private RouteContext route(final QueryContext queryContext, final AgentDatabase database,
                               final AgentRuleMetaData globalRuleMetaData, final ConfigurationProperties props, final ConnectionContext connectionContext) {
        return new SQLRouteEngine(database.getRuleMetaData().getRules(), props).route(connectionContext, queryContext, globalRuleMetaData, database);
    }

    private ExecutionContext createExecutionContext(final QueryContext queryContext, final AgentDatabase database, final RouteContext routeContext, String sql, List<Object> parameters) {
        return new ExecutionContext(queryContext, ExecutionContextBuilder.build(database, sql, parameters, queryContext.getSqlStatementContext()), routeContext);
    }
    
    private void logSQL(final QueryContext queryContext, final ConfigurationProperties props, final ExecutionContext executionContext) {
        if (props.<Boolean>getValue(ConfigurationPropertyKey.SQL_SHOW)) {
            SQLLogger.logSQL(queryContext, props.<Boolean>getValue(ConfigurationPropertyKey.SQL_SIMPLE), executionContext);
        }
    }
}
