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

package org.dbiir.harp.kernel.core.rule;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.dbiir.harp.kernel.core.AgentTransactionManagerEngine;
import org.dbiir.harp.kernel.transaction.api.TransactionType;
import org.dbiir.harp.kernel.transaction.config.TransactionRuleConfiguration;
import org.dbiir.harp.utils.common.database.type.DatabaseType;
import org.dbiir.harp.utils.common.metadata.database.AgentDatabase;
import org.dbiir.harp.utils.common.rule.identifier.scope.GlobalRule;
import org.dbiir.harp.utils.common.rule.identifier.type.ResourceHeldRule;

import javax.sql.DataSource;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Transaction rule.
 */
@Getter
@Slf4j
public final class TransactionRule implements GlobalRule, ResourceHeldRule<AgentTransactionManagerEngine> {

    private final TransactionRuleConfiguration configuration; 

    private final TransactionType defaultType;

    private final String providerType;

    private final Properties props;

    private final Map<String, AgentDatabase> databases;
    
    private volatile AgentTransactionManagerEngine resource;
    
    public TransactionRule(final TransactionRuleConfiguration ruleConfig, final Map<String, AgentDatabase> databases) {
        configuration = ruleConfig;
        defaultType = TransactionType.valueOf(ruleConfig.getDefaultType().toUpperCase());
        providerType = ruleConfig.getProviderType();
        props = ruleConfig.getProps();
        this.databases = new ConcurrentHashMap<>(databases);
        resource = createTransactionManagerEngine(this.databases);
    }
    
    private synchronized AgentTransactionManagerEngine createTransactionManagerEngine(final Map<String, AgentDatabase> databases) {
        if (databases.isEmpty()) {
            return new AgentTransactionManagerEngine();
        }
        Map<String, DataSource> dataSourceMap = new LinkedHashMap<>(databases.size(), 1);
        Map<String, DatabaseType> databaseTypes = new LinkedHashMap<>(databases.size(), 1);
        for (Entry<String, AgentDatabase> entry : databases.entrySet()) {
            AgentDatabase database = entry.getValue();
            database.getResourceMetaData().getDataSources().forEach((key, value) -> dataSourceMap.put(database.getName() + "." + key, value));
            database.getResourceMetaData().getStorageTypes().forEach((key, value) -> databaseTypes.put(database.getName() + "." + key, value));
        }
        if (dataSourceMap.isEmpty()) {
            return new AgentTransactionManagerEngine();
        }
        AgentTransactionManagerEngine result = new AgentTransactionManagerEngine();
        // TODO:
        result.init(databaseTypes, dataSourceMap, "");
        return result;
    }
    
    public synchronized void addResource(final AgentDatabase database) {
        // TODO process null when for information_schema
        if (null == database) {
            return;
        }
        databases.put(database.getName(), database);
        rebuildEngine();
    }
    
    public synchronized void closeStaleResource(final String databaseName) {
        if (!databases.containsKey(databaseName.toLowerCase())) {
            return;
        }
        databases.remove(databaseName);
        rebuildEngine();
    }
    
    public synchronized void closeStaleResource() {
        databases.clear();
        closeEngine();
    }
    
    private void rebuildEngine() {
        AgentTransactionManagerEngine previousEngine = resource;
        if (null != previousEngine) {
            closeEngine(previousEngine);
        }
        resource = createTransactionManagerEngine(databases);
    }
    
    private void closeEngine() {
        AgentTransactionManagerEngine engine = resource;
        if (null != engine) {
            closeEngine(engine);
            resource = new AgentTransactionManagerEngine();
        }
    }
    
    private void closeEngine(final AgentTransactionManagerEngine engine) {
        try {
            engine.close();
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            log.error("Close transaction engine failed", ex);
        }
    }

    public String getType() {
        return TransactionRule.class.getSimpleName();
    }
}
