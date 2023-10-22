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

package org.dbiir.harp.utils.router;


import org.dbiir.harp.utils.binder.QueryContext;
import org.dbiir.harp.utils.common.config.props.ConfigurationProperties;
import org.dbiir.harp.utils.common.metadata.database.AgentDatabase;
import org.dbiir.harp.utils.common.metadata.database.rule.AgentRuleMetaData;
import org.dbiir.harp.utils.common.rule.AgentRule;
import org.dbiir.harp.utils.common.spi.annotation.SingletonSPI;
import org.dbiir.harp.utils.common.spi.type.ordered.OrderedSPI;
import org.dbiir.harp.utils.context.ConnectionContext;
import org.dbiir.harp.utils.router.context.RouteContext;

/**
 * SQL Router.
 * 
 * @param <T> type of rule
 */
@SingletonSPI
public interface SQLRouter<T extends AgentRule> extends OrderedSPI<T> {
    
    /**
     * Create route context.
     *
     * @param queryContext query context
     * @param globalRuleMetaData global rule meta data
     * @param database database
     * @param rule rule
     * @param props configuration properties
     * @param connectionContext connection context
     * @return route context
     */
    RouteContext createRouteContext(QueryContext queryContext,
                                    AgentRuleMetaData globalRuleMetaData, AgentDatabase database, T rule, ConfigurationProperties props, ConnectionContext connectionContext);
    
    /**
     * Decorate route context.
     * 
     * @param routeContext route context
     * @param queryContext query context
     * @param database database
     * @param rule rule
     * @param props configuration properties
     * @param connectionContext connection context
     */
    void decorateRouteContext(RouteContext routeContext, QueryContext queryContext, AgentDatabase database, T rule, ConfigurationProperties props, ConnectionContext connectionContext);
}
