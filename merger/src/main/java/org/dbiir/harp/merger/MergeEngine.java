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

package org.dbiir.harp.merger;


import org.dbiir.harp.executor.sql.execute.result.query.QueryResult;
import org.dbiir.harp.merger.decorator.ResultDecorator;
import org.dbiir.harp.merger.decorator.ResultDecoratorEngine;
import org.dbiir.harp.merger.merger.ResultMerger;
import org.dbiir.harp.merger.merger.ResultMergerEngine;
import org.dbiir.harp.merger.result.MergedResult;
import org.dbiir.harp.merger.result.impl.local.LocalDataMergedResult;
import org.dbiir.harp.merger.result.impl.transparent.TransparentMergedResult;
import org.dbiir.harp.utils.binder.statement.SQLStatementContext;
import org.dbiir.harp.utils.common.config.props.ConfigurationProperties;
import org.dbiir.harp.utils.common.metadata.database.AgentDatabase;
import org.dbiir.harp.utils.common.rule.AgentRule;
import org.dbiir.harp.utils.common.spi.type.ordered.OrderedSPILoader;
import org.dbiir.harp.utils.context.ConnectionContext;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

/**
 * Merge engine.
 */
public final class MergeEngine {
    
    private final AgentDatabase database;
    
    private final ConfigurationProperties props;
    
    @SuppressWarnings("rawtypes")
    private final Map<AgentRule, ResultProcessEngine> engines;
    
    private final ConnectionContext connectionContext;
    
    public MergeEngine(final AgentDatabase database, final ConfigurationProperties props, final ConnectionContext connectionContext) {
        this.database = database;
        this.props = props;
        engines = OrderedSPILoader.getServices(ResultProcessEngine.class, database.getRuleMetaData().getRules());
        this.connectionContext = connectionContext;
    }
    
    /**
     * Merge.
     *
     * @param queryResults query results
     * @param sqlStatementContext SQL statement context
     * @return merged result
     * @throws SQLException SQL exception
     */
    public MergedResult merge(final List<QueryResult> queryResults, final SQLStatementContext<?> sqlStatementContext) throws SQLException {
        Optional<MergedResult> mergedResult = executeMerge(queryResults, sqlStatementContext);
        Optional<MergedResult> result = mergedResult.isPresent() ? Optional.of(decorate(mergedResult.get(), sqlStatementContext)) : decorate(queryResults.get(0), sqlStatementContext);
        return result.orElseGet(() -> new TransparentMergedResult(queryResults.get(0)));
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Optional<MergedResult> executeMerge(final List<QueryResult> queryResults, final SQLStatementContext<?> sqlStatementContext) throws SQLException {
        for (Entry<AgentRule, ResultProcessEngine> entry : engines.entrySet()) {
            if (entry.getValue() instanceof ResultMergerEngine) {
                ResultMerger resultMerger = ((ResultMergerEngine) entry.getValue()).newInstance(database.getName(), entry.getKey(), database.getProtocolType(), props, sqlStatementContext);
                return Optional.of(resultMerger.merge(queryResults, sqlStatementContext, database, connectionContext));
            }
        }
        return Optional.empty();
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    private MergedResult decorate(final MergedResult mergedResult, final SQLStatementContext<?> sqlStatementContext) throws SQLException {
        MergedResult result = null;
        for (Entry<AgentRule, ResultProcessEngine> entry : engines.entrySet()) {
            if (entry.getValue() instanceof ResultDecoratorEngine) {
                ResultDecorator resultDecorator = ((ResultDecoratorEngine) entry.getValue()).newInstance(database, entry.getKey(), props, sqlStatementContext);
                result = null == result ? resultDecorator.decorate(mergedResult, sqlStatementContext, entry.getKey()) : resultDecorator.decorate(result, sqlStatementContext, entry.getKey());
            }
        }
        return null == result ? mergedResult : result;
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Optional<MergedResult> decorate(final QueryResult queryResult, final SQLStatementContext<?> sqlStatementContext) throws SQLException {
        MergedResult result = null;
        for (Entry<AgentRule, ResultProcessEngine> entry : engines.entrySet()) {
            if (entry.getValue() instanceof ResultDecoratorEngine) {
                ResultDecorator resultDecorator = ((ResultDecoratorEngine) entry.getValue()).newInstance(database, entry.getKey(), props, sqlStatementContext);
                result = null == result ? resultDecorator.decorate(queryResult, sqlStatementContext, entry.getKey()) : resultDecorator.decorate(result, sqlStatementContext, entry.getKey());
            }
        }
        return Optional.ofNullable(result);
    }
}
