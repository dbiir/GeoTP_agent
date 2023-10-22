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


import org.dbiir.harp.merger.dql.ShardingDQLResultMerger;
import org.dbiir.harp.merger.merger.ResultMerger;
import org.dbiir.harp.merger.merger.ResultMergerEngine;
import org.dbiir.harp.merger.merger.impl.TransparentResultMerger;
import org.dbiir.harp.utils.binder.statement.SQLStatementContext;
import org.dbiir.harp.utils.binder.statement.dml.SelectStatementContext;
import org.dbiir.harp.utils.common.config.props.ConfigurationProperties;
import org.dbiir.harp.utils.common.database.type.DatabaseType;
import org.dbiir.harp.utils.common.rule.AgentRule;

/**
 * Result merger engine for sharding.
 */
public final class ShardingResultMergerEngine implements ResultMergerEngine<AgentRule> {

    @Override
    public ResultMerger newInstance(final String databaseName, AgentRule rule, final DatabaseType protocolType, final ConfigurationProperties props,
                                    final SQLStatementContext<?> sqlStatementContext) {
        if (sqlStatementContext instanceof SelectStatementContext) {
            return new ShardingDQLResultMerger(protocolType);
        }
//        if (sqlStatementContext.getSqlStatement() instanceof DDLStatement) {
//            return new ShardingDDLResultMerger();
//        }
//        if (sqlStatementContext.getSqlStatement() instanceof DALStatement) {
//            return new ShardingDALResultMerger(databaseName, shardingRule);
//        }
        return new TransparentResultMerger();
    }

    @Override
    public int getOrder() {
        return 0;
    }

    @Override
    public Class getTypeClass() {
        assert (false);
        return null;
    }

//    @Override
//    public Class<ShardingRule> getTypeClass() {
//        return ShardingRule.class;
//    }
}
