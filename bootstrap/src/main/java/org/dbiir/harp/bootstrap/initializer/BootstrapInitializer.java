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

package org.dbiir.harp.bootstrap.initializer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.dbiir.harp.backend.config.ProxyConfiguration;
import org.dbiir.harp.backend.config.YamlProxyConfiguration;
import org.dbiir.harp.backend.config.yaml.swapper.YamlProxyConfigurationSwapper;
import org.dbiir.harp.backend.context.ProxyContext;
import org.dbiir.harp.mode.manager.ContextManager;
import org.dbiir.harp.mode.manager.ContextManagerBuilder;
import org.dbiir.harp.mode.manager.ContextManagerBuilderParameter;
import org.dbiir.harp.mode.manager.listener.ContextManagerLifecycleListener;
import org.dbiir.harp.mode.manager.standalone.StandaloneContextManagerBuilder;
import org.dbiir.harp.utils.common.config.mode.ModeConfiguration;
import org.dbiir.harp.utils.common.config.props.ConfigurationPropertyKey;
import org.dbiir.harp.utils.common.instance.metadata.InstanceMetaData;
import org.dbiir.harp.utils.common.instance.metadata.InstanceMetaDataBuilder;
import org.dbiir.harp.utils.common.spi.HarpServiceLoader;
import org.dbiir.harp.utils.common.spi.type.typed.TypedSPILoader;
import org.dbiir.harp.utils.common.yaml.config.swapper.mode.YamlModeConfigurationSwapper;

import java.sql.SQLException;

/**
 * Bootstrap initializer.
 */
@RequiredArgsConstructor
@Slf4j
public final class BootstrapInitializer {
    
    /**
     * Initialize.
     *
     * @param yamlConfig YAML proxy configuration
     * @param port proxy port
     * @param force force start
     * @throws SQLException SQL exception
     */
    public void init(final YamlProxyConfiguration yamlConfig, final int port, final boolean force) throws SQLException {
        ModeConfiguration modeConfig = null == yamlConfig.getServerConfiguration().getMode() ? null : new YamlModeConfigurationSwapper().swapToObject(yamlConfig.getServerConfiguration().getMode());
        ProxyConfiguration proxyConfig = new YamlProxyConfigurationSwapper().swap(yamlConfig);
        ContextManager contextManager = createContextManager(proxyConfig, modeConfig, port, force);
        ProxyContext.init(contextManager);
        contextManagerInitializedCallback(contextManager);
    }
    
    private ContextManager createContextManager(final ProxyConfiguration proxyConfig, final ModeConfiguration modeConfig, final int port, final boolean force) throws SQLException {
        ContextManagerBuilderParameter param = new ContextManagerBuilderParameter(modeConfig, proxyConfig.getDatabaseConfigurations(),
                proxyConfig.getGlobalConfiguration().getRules(), proxyConfig.getGlobalConfiguration().getProperties(), proxyConfig.getGlobalConfiguration().getLabels(),
                createInstanceMetaData(proxyConfig, port), force);
//        return TypedSPILoader.getService(ContextManagerBuilder.class, null == modeConfig ? null : modeConfig.getType()).build(param);
        StandaloneContextManagerBuilder builder = new StandaloneContextManagerBuilder();
        return builder.build(param);
    }
    
    private InstanceMetaData createInstanceMetaData(final ProxyConfiguration proxyConfig, final int port) {
        String instanceType = proxyConfig.getGlobalConfiguration().getProperties().getProperty(
                ConfigurationPropertyKey.PROXY_INSTANCE_TYPE.getKey(), ConfigurationPropertyKey.PROXY_INSTANCE_TYPE.getDefaultValue());
        return TypedSPILoader.getService(InstanceMetaDataBuilder.class, instanceType).build(port);
    }
    
    private void contextManagerInitializedCallback(final ContextManager contextManager) {
        for (ContextManagerLifecycleListener each : HarpServiceLoader.getServiceInstances(ContextManagerLifecycleListener.class)) {
            try {
                each.onInitialized(null, contextManager);
                // CHECKSTYLE:OFF
            } catch (final Exception ex) {
                // CHECKSTYLE:ON
                log.error("contextManager onInitialized callback for '{}' failed", each.getClass().getName(), ex);
            }
        }
    }
}
