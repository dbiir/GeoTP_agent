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

package org.dbiir.harp.parser.rule;

import lombok.Getter;
import org.dbiir.harp.parser.AgentSQLParserEngine;
import org.dbiir.harp.parser.SQLParserEngine;
import org.dbiir.harp.parser.SimpleSQLParserEngine;
import org.dbiir.harp.parser.api.SQLParserRuleConfiguration;
import org.dbiir.harp.utils.common.rule.identifier.scope.GlobalRule;

/**
 * SQL parser rule.
 */
@Getter
public final class SQLParserRule implements GlobalRule {
    
    private final SQLParserRuleConfiguration configuration;
    
    private final boolean sqlCommentParseEnabled;
    
    private final String engineType;
    
    public SQLParserRule(final SQLParserRuleConfiguration ruleConfig) {
        configuration = ruleConfig;
        sqlCommentParseEnabled = ruleConfig.isSqlCommentParseEnabled();
        engineType = "Standard";
    }
    
    /**
     * Get SQL parser engine.
     * 
     * @param databaseType database type
     * @return SQL parser engine
     */
    public SQLParserEngine getSQLParserEngine(final String databaseType) {
        return "Standard".equals(engineType)
                ? new AgentSQLParserEngine(databaseType, sqlCommentParseEnabled)
                : new SimpleSQLParserEngine();
    }
    
    @Override
    public String getType() {
        return SQLParserRule.class.getSimpleName();
    }
}
