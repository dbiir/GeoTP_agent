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

package org.dbiir.harp.utils.router.engine.impl;


import org.dbiir.harp.utils.binder.QueryContext;
import org.dbiir.harp.utils.binder.statement.CommonSQLStatementContext;
import org.dbiir.harp.utils.binder.statement.SQLStatementContext;
import org.dbiir.harp.utils.common.config.props.ConfigurationProperties;
import org.dbiir.harp.utils.common.hint.HintManager;
import org.dbiir.harp.utils.common.hint.SQLHintDataSourceNotExistsException;
import org.dbiir.harp.utils.common.metadata.database.AgentDatabase;
import org.dbiir.harp.utils.common.metadata.database.rule.AgentRuleMetaData;
import org.dbiir.harp.utils.router.SQLRouter;
import org.dbiir.harp.utils.router.context.RouteContext;
import org.dbiir.harp.utils.router.context.RouteMapper;
import org.dbiir.harp.utils.router.context.RouteUnit;
import org.dbiir.harp.utils.router.engine.SQLRouteExecutor;
import org.dbiir.harp.utils.common.rule.AgentRule;
import org.dbiir.harp.utils.common.spi.type.ordered.OrderedSPILoader;
import org.dbiir.harp.utils.context.ConnectionContext;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

/**
 * Partial SQL route executor.
 */
public final class PartialSQLRouteExecutor implements SQLRouteExecutor {
    
    private final ConfigurationProperties props;
    
    @SuppressWarnings("rawtypes")
    private final Map<AgentRule, SQLRouter> routers;
    
    public PartialSQLRouteExecutor(final Collection<AgentRule> rules, final ConfigurationProperties props) {
        this.props = props;
        routers = OrderedSPILoader.getServices(SQLRouter.class, rules);
    }
    
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public RouteContext route(final ConnectionContext connectionContext, final QueryContext queryContext, final AgentRuleMetaData globalRuleMetaData, final AgentDatabase database) {
        RouteContext result = new RouteContext();
        Optional<String> dataSourceName = findDataSourceByHint(queryContext.getSqlStatementContext(), database.getResourceMetaData().getDataSources());
        if (dataSourceName.isPresent()) {
            result.getRouteUnits().add(new RouteUnit(new RouteMapper(dataSourceName.get(), dataSourceName.get()), Collections.emptyList()));
            return result;
        }
        for (Entry<AgentRule, SQLRouter> entry : routers.entrySet()) {
            if (result.getRouteUnits().isEmpty()) {
                result = entry.getValue().createRouteContext(queryContext, globalRuleMetaData, database, entry.getKey(), props, connectionContext);
            } else {
                entry.getValue().decorateRouteContext(result, queryContext, database, entry.getKey(), props, connectionContext);
            }
        }
        if (result.getRouteUnits().isEmpty() && 1 == database.getResourceMetaData().getDataSources().size()) {
            String singleDataSourceName = database.getResourceMetaData().getDataSources().keySet().iterator().next();
            result.getRouteUnits().add(new RouteUnit(new RouteMapper(singleDataSourceName, singleDataSourceName), Collections.emptyList()));
        }
        return result;
    }
    
    private Optional<String> findDataSourceByHint(final SQLStatementContext<?> sqlStatementContext, final Map<String, DataSource> dataSources) {
        Optional<String> result;
        if (HintManager.isInstantiated() && HintManager.getDataSourceName().isPresent()) {
            result = HintManager.getDataSourceName();
        } else {
            result = ((CommonSQLStatementContext<?>) sqlStatementContext).findHintDataSourceName();
        }
        if (result.isPresent() && !dataSources.containsKey(result.get())) {
            throw new SQLHintDataSourceNotExistsException(result.get());
        }
        return result;
    }
}
