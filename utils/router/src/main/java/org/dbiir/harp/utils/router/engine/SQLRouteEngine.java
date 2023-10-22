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

package org.dbiir.harp.utils.router.engine;

import lombok.RequiredArgsConstructor;
import org.dbiir.harp.utils.binder.QueryContext;
import org.dbiir.harp.utils.common.config.props.ConfigurationProperties;
import org.dbiir.harp.utils.router.context.RouteContext;
import org.dbiir.harp.utils.router.engine.impl.AllSQLRouteExecutor;
import org.dbiir.harp.utils.router.engine.impl.PartialSQLRouteExecutor;
import org.dbiir.harp.utils.common.rule.AgentRule;
import org.dbiir.harp.utils.common.statement.SQLStatement;
import org.dbiir.harp.utils.common.statement.mysql.dal.MySQLShowTableStatusStatement;
import org.dbiir.harp.utils.common.statement.mysql.dal.MySQLShowTablesStatement;
import org.dbiir.harp.utils.context.ConnectionContext;
import org.dbiir.harp.utils.common.metadata.database.AgentDatabase;
import org.dbiir.harp.utils.common.metadata.database.rule.AgentRuleMetaData;


import java.util.Collection;

/**
 * SQL route engine.
 */
@RequiredArgsConstructor
public final class SQLRouteEngine {
    
    private final Collection<AgentRule> rules;
    
    private final ConfigurationProperties props;
    
    /**
     * Route SQL.
     *
     * @param connectionContext connection context
     * @param queryContext query context
     * @param globalRuleMetaData global rule meta data
     * @param database database
     * @return route context
     */
    public RouteContext route(final ConnectionContext connectionContext, final QueryContext queryContext, final AgentRuleMetaData globalRuleMetaData, final AgentDatabase database) {
        SQLRouteExecutor executor = isNeedAllSchemas(queryContext.getSqlStatementContext().getSqlStatement()) ? new AllSQLRouteExecutor() : new PartialSQLRouteExecutor(rules, props);
        return executor.route(connectionContext, queryContext, globalRuleMetaData, database);
    }
    
    // TODO use dynamic config to judge unconfigured schema
    private boolean isNeedAllSchemas(final SQLStatement sqlStatement) {
        return sqlStatement instanceof MySQLShowTablesStatement || sqlStatement instanceof MySQLShowTableStatusStatement;
    }
}
