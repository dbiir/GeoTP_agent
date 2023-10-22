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

package org.dbiir.harp.kernel.core.rule.builder;


import org.dbiir.harp.kernel.core.constant.TransactionOrder;
import org.dbiir.harp.kernel.core.rule.TransactionRule;
import org.dbiir.harp.kernel.transaction.config.TransactionRuleConfiguration;
import org.dbiir.harp.utils.common.config.props.ConfigurationProperties;
import org.dbiir.harp.utils.common.metadata.database.AgentDatabase;
import org.dbiir.harp.utils.common.rule.builder.global.GlobalRuleBuilder;

import java.util.Map;

/**
 * Transaction rule builder.
 */
public final class TransactionRuleBuilder implements GlobalRuleBuilder<TransactionRuleConfiguration> {
    
    @Override
    public TransactionRule build(final TransactionRuleConfiguration ruleConfig, final Map<String, AgentDatabase> databases, final ConfigurationProperties props) {
        return new TransactionRule(ruleConfig, databases);
    }
    
    @Override
    public int getOrder() {
        return TransactionOrder.ORDER;
    }
    
    @Override
    public Class<TransactionRuleConfiguration> getTypeClass() {
        return TransactionRuleConfiguration.class;
    }
}
