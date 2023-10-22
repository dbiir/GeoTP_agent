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

package org.dbiir.harp.merger.merger;


import org.dbiir.harp.merger.ResultProcessEngine;
import org.dbiir.harp.utils.binder.statement.SQLStatementContext;
import org.dbiir.harp.utils.common.config.props.ConfigurationProperties;
import org.dbiir.harp.utils.common.database.type.DatabaseType;
import org.dbiir.harp.utils.common.rule.AgentRule;

/**
 * Result merger engine.
 */
public interface ResultMergerEngine<T extends AgentRule> extends ResultProcessEngine<T> {
    
    /**
     * Create new instance of result merger engine.
     *
     * @param databaseName database name
     * @param protocolType protocol type
     * @param props Agent properties
     * @param sqlStatementContext SQL statement context
     * @return created instance
     */
    ResultMerger newInstance(String databaseName, T rule, DatabaseType protocolType, ConfigurationProperties props, SQLStatementContext<?> sqlStatementContext);
}
