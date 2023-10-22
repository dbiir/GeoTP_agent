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

package org.dbiir.harp.utils.common.yaml.config.pojo;

import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;
import org.dbiir.harp.utils.common.yaml.YamlConfiguration;
import org.dbiir.harp.utils.common.yaml.config.pojo.mode.YamlModeConfiguration;
import org.dbiir.harp.utils.common.yaml.config.pojo.rule.YamlRuleConfiguration;

import java.util.*;

/**
 * YAML root configuration.
 */
@Getter
@Setter
public final class YamlRootConfiguration implements YamlConfiguration {
    
    private String databaseName;
    
    /**
     * Schema name.
     * 
     * @deprecated Should use databaseName, schemaName will remove in next version.
     */
    @Deprecated
    private String schemaName;
    
    private Map<String, Map<String, Object>> dataSources = new HashMap<>();
    
    private Collection<YamlRuleConfiguration> rules = new LinkedList<>();
    
    private YamlModeConfiguration mode;
    
    private Properties props = new Properties();
    
    /**
     * Get database name.
     * 
     * @return database name
     */
    public String getDatabaseName() {
        return Strings.isNullOrEmpty(databaseName) ? schemaName : databaseName;
    }
}
