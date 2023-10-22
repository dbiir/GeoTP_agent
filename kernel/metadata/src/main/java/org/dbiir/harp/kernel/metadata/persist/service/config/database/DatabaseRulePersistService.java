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

package org.dbiir.harp.kernel.metadata.persist.service.config.database;

import com.google.common.base.Strings;
import lombok.RequiredArgsConstructor;
import org.dbiir.harp.kernel.metadata.persist.node.DatabaseMetaDataNode;
import org.dbiir.harp.mode.spi.PersistRepository;
import org.dbiir.harp.utils.common.config.rule.RuleConfiguration;
import org.dbiir.harp.utils.common.rule.AgentRule;
import org.dbiir.harp.utils.common.yaml.YamlEngine;
import org.dbiir.harp.utils.common.yaml.config.pojo.rule.YamlRuleConfiguration;
import org.dbiir.harp.utils.common.yaml.config.swapper.rule.YamlRuleConfigurationSwapperEngine;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

/**
 * Database rule persist service.
 */
@RequiredArgsConstructor
public final class DatabaseRulePersistService implements DatabaseRuleBasedPersistService<Collection<RuleConfiguration>> {
    
    private static final String DEFAULT_VERSION = "0";
    
    private final PersistRepository repository;
    
    @Override
    public void persist(final String databaseName, final Map<String, DataSource> dataSources,
                        final Collection<AgentRule> rules, final Collection<RuleConfiguration> configs) {
        if (Strings.isNullOrEmpty(getDatabaseActiveVersion(databaseName))) {
            repository.persist(DatabaseMetaDataNode.getActiveVersionPath(databaseName), DEFAULT_VERSION);
        }
        repository.persist(DatabaseMetaDataNode.getRulePath(databaseName, getDatabaseActiveVersion(databaseName)),
                YamlEngine.marshal(createYamlRuleConfigurations(dataSources, rules, configs)));
    }
    
    @Override
    public void persist(final String databaseName, final Collection<RuleConfiguration> configs) {
        if (Strings.isNullOrEmpty(getDatabaseActiveVersion(databaseName))) {
            repository.persist(DatabaseMetaDataNode.getActiveVersionPath(databaseName), DEFAULT_VERSION);
        }
        repository.persist(DatabaseMetaDataNode.getRulePath(databaseName, getDatabaseActiveVersion(databaseName)),
                YamlEngine.marshal(createYamlRuleConfigurations(configs)));
    }
    
    @Override
    public void persist(final String databaseName, final String version, final Map<String, DataSource> dataSources,
                        final Collection<AgentRule> rules, final Collection<RuleConfiguration> configs) {
        repository.persist(DatabaseMetaDataNode.getRulePath(databaseName, version), YamlEngine.marshal(createYamlRuleConfigurations(dataSources, rules, configs)));
    }
    
    private Collection<YamlRuleConfiguration> createYamlRuleConfigurations(final Collection<RuleConfiguration> ruleConfigs) {
        return new YamlRuleConfigurationSwapperEngine().swapToYamlRuleConfigurations(ruleConfigs);
    }
    
    // TODO Load single table refer to #22887
    private Collection<YamlRuleConfiguration> createYamlRuleConfigurations(final Map<String, DataSource> dataSources, final Collection<AgentRule> rules,
                                                                           final Collection<RuleConfiguration> ruleConfigs) {
        return new YamlRuleConfigurationSwapperEngine().swapToYamlRuleConfigurations(ruleConfigs);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public Collection<RuleConfiguration> load(final String databaseName) {
        return isExisted(databaseName)
                ? new YamlRuleConfigurationSwapperEngine().swapToRuleConfigurations(YamlEngine.unmarshal(repository.getDirectly(DatabaseMetaDataNode.getRulePath(databaseName,
                        getDatabaseActiveVersion(databaseName))), Collection.class, true))
                : new LinkedList<>();
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public Collection<RuleConfiguration> load(final String databaseName, final String version) {
        String yamlContent = repository.getDirectly(DatabaseMetaDataNode.getRulePath(databaseName, version));
        return Strings.isNullOrEmpty(yamlContent) ? new LinkedList<>()
                : new YamlRuleConfigurationSwapperEngine().swapToRuleConfigurations(YamlEngine.unmarshal(repository.getDirectly(DatabaseMetaDataNode
                        .getRulePath(databaseName, getDatabaseActiveVersion(databaseName))), Collection.class, true));
    }
    
    @Override
    public boolean isExisted(final String databaseName) {
        return !Strings.isNullOrEmpty(getDatabaseActiveVersion(databaseName))
                && !Strings.isNullOrEmpty(repository.getDirectly(DatabaseMetaDataNode.getRulePath(databaseName, getDatabaseActiveVersion(databaseName))));
    }
    
    private String getDatabaseActiveVersion(final String databaseName) {
        return repository.getDirectly(DatabaseMetaDataNode.getActiveVersionPath(databaseName));
    }
}
