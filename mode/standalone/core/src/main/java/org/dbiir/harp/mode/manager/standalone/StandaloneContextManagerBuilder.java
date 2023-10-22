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

package org.dbiir.harp.mode.manager.standalone;


import org.dbiir.harp.kernel.metadata.persist.MetaDataPersistService;
import org.dbiir.harp.mode.lock.GlobalLockContext;
import org.dbiir.harp.mode.manager.ContextManager;
import org.dbiir.harp.mode.manager.ContextManagerBuilder;
import org.dbiir.harp.mode.manager.ContextManagerBuilderParameter;
import org.dbiir.harp.mode.manager.standalone.generator.StandaloneWorkerIdGenerator;
import org.dbiir.harp.mode.metadata.MetaDataContexts;
import org.dbiir.harp.mode.metadata.MetaDataContextsFactory;
import org.dbiir.harp.mode.standalone.StandalonePersistRepository;
import org.dbiir.harp.mode.manager.standalone.subscriber.ProcessStandaloneSubscriber;
import org.dbiir.harp.mode.standalone.repository.provider.JDBCRepository;
import org.dbiir.harp.utils.common.config.mode.PersistRepositoryConfiguration;
import org.dbiir.harp.utils.common.instance.ComputeNodeInstance;
import org.dbiir.harp.utils.common.instance.InstanceContext;
import org.dbiir.harp.utils.common.spi.type.typed.TypedSPILoader;
import org.dbiir.harp.utils.context.eventbus.EventBusContext;

import java.sql.SQLException;
import java.util.Properties;

/**
 * Standalone context manager builder.
 */
public final class StandaloneContextManagerBuilder implements ContextManagerBuilder {
    
    @Override
    public ContextManager build(final ContextManagerBuilderParameter param) throws SQLException {
        PersistRepositoryConfiguration repositoryConfig = param.getModeConfiguration().getRepository();
        StandalonePersistRepository repository = new JDBCRepository();
        repository.init(new Properties());
//        StandalonePersistRepository repository = TypedSPILoader.getService(
//                StandalonePersistRepository.class, null == repositoryConfig ? null : repositoryConfig.getType(), null == repositoryConfig ? new Properties() : repositoryConfig.getProps());

        MetaDataPersistService persistService = new MetaDataPersistService(repository);
        InstanceContext instanceContext = buildInstanceContext(param);
        new ProcessStandaloneSubscriber(instanceContext.getEventBusContext());
        MetaDataContexts metaDataContexts = MetaDataContextsFactory.create(persistService, param, instanceContext);
        ContextManager result = new ContextManager(metaDataContexts, instanceContext);
        setContextManagerAware(result);
        return result;
    }
    
    private InstanceContext buildInstanceContext(final ContextManagerBuilderParameter param) {
        return new InstanceContext(new ComputeNodeInstance(param.getInstanceMetaData()),
                new StandaloneWorkerIdGenerator(), param.getModeConfiguration(), new StandaloneModeContextManager(), new GlobalLockContext(null), new EventBusContext());
    }
    
    private void setContextManagerAware(final ContextManager contextManager) {
        ((StandaloneModeContextManager) contextManager.getInstanceContext().getModeContextManager()).setContextManagerAware(contextManager);
    }
    
    @Override
    public String getType() {
        return "Standalone";
    }
    
    @Override
    public boolean isDefault() {
        return true;
    }
}
