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

package org.dbiir.harp.kernel.core;

import lombok.extern.slf4j.Slf4j;

import org.dbiir.harp.kernel.transaction.api.TransactionType;
import org.dbiir.harp.kernel.transaction.spi.AgentTransactionManager;
import org.dbiir.harp.utils.common.database.type.DatabaseType;
import org.dbiir.harp.utils.common.spi.HarpServiceLoader;

import javax.sql.DataSource;
import java.util.EnumMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Agent transaction manager engine.
 */
@Slf4j
public final class AgentTransactionManagerEngine {
    
    private final Map<TransactionType, AgentTransactionManager> transactionManagers = new EnumMap<>(TransactionType.class);
    
    public AgentTransactionManagerEngine() {
        loadTransactionManager();
    }
    
    private void loadTransactionManager() {
        for (AgentTransactionManager each : HarpServiceLoader.getServiceInstances(AgentTransactionManager.class)) {
            if (transactionManagers.containsKey(each.getTransactionType())) {
                log.warn("Find more than one {} transaction manager implementation class, use `{}` now",
                        each.getTransactionType(), transactionManagers.get(each.getTransactionType()).getClass().getName());
                continue;
            }
            transactionManagers.put(each.getTransactionType(), each);
        }
    }
    
    /**
     * Initialize transaction managers.
     *
     * @param databaseTypes database types
     * @param dataSourceMap data source map
     * @param providerType transaction manager provider type
     */
    public void init(final Map<String, DatabaseType> databaseTypes, final Map<String, DataSource> dataSourceMap, final String providerType) {
        transactionManagers.forEach((key, value) -> value.init(databaseTypes, dataSourceMap, providerType));
    }
    
    /**
     * Get transaction manager.
     *
     * @param transactionType transaction type
     * @return transaction manager
     */
    public AgentTransactionManager getTransactionManager(final TransactionType transactionType) {
        AgentTransactionManager result = transactionManagers.get(transactionType);
        return result;
    }
    
    /**
     * Close transaction managers.
     * 
     * @throws Exception exception
     */
    public void close() throws Exception {
        for (Entry<TransactionType, AgentTransactionManager> entry : transactionManagers.entrySet()) {
            entry.getValue().close();
        }
    }
}
