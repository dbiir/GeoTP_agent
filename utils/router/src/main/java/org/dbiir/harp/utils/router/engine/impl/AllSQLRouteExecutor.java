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
import org.dbiir.harp.utils.common.metadata.database.AgentDatabase;
import org.dbiir.harp.utils.common.metadata.database.rule.AgentRuleMetaData;
import org.dbiir.harp.utils.router.context.RouteContext;
import org.dbiir.harp.utils.router.context.RouteMapper;
import org.dbiir.harp.utils.router.context.RouteUnit;
import org.dbiir.harp.utils.router.engine.SQLRouteExecutor;
import org.dbiir.harp.utils.context.ConnectionContext;

import java.util.Collections;

/**
 * All SQL route executor.
 */
public final class AllSQLRouteExecutor implements SQLRouteExecutor {
    
    @Override
    public RouteContext route(final ConnectionContext connectionContext, final QueryContext queryContext, final AgentRuleMetaData globalRuleMetaData, final AgentDatabase database) {
        RouteContext result = new RouteContext();
        for (String each : database.getResourceMetaData().getDataSources().keySet()) {
            result.getRouteUnits().add(new RouteUnit(new RouteMapper(each, each), Collections.emptyList()));
        }
        return result;
    }
}
