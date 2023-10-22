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

package org.dbiir.harp.kernel.authority.yaml.swapper;


import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.dbiir.harp.kernel.authority.yaml.config.YamlUserConfiguration;
import org.dbiir.harp.utils.common.metadata.user.AgentUser;
import org.dbiir.harp.utils.common.metadata.user.Grantee;
import org.dbiir.harp.utils.common.yaml.swapper.YamlConfigurationSwapper;

import java.util.Objects;

/**
 * YAML user swapper.
 */
public final class YamlUserSwapper implements YamlConfigurationSwapper<YamlUserConfiguration, AgentUser> {
    
    @Override
    public YamlUserConfiguration swapToYamlConfiguration(final AgentUser data) {
        if (Objects.isNull(data)) {
            return null;
        }
        YamlUserConfiguration result = new YamlUserConfiguration();
        result.setUser(data.getGrantee().toString());
        result.setPassword(data.getPassword());
        result.setAuthenticationMethodName(data.getAuthenticationMethodName());
        return result;
    }
    
    @Override
    public AgentUser swapToObject(final YamlUserConfiguration yamlConfig) {
        if (Objects.isNull(yamlConfig)) {
            return null;
        }
        Grantee grantee = convertYamlUserToGrantee(yamlConfig.getUser());
        return new AgentUser(grantee.getUsername(), yamlConfig.getPassword(), grantee.getHostname(), yamlConfig.getAuthenticationMethodName());
    }
    
    private Grantee convertYamlUserToGrantee(final String yamlUser) {
        if (!yamlUser.contains("@")) {
            return new Grantee(yamlUser, "");
        }
        String username = yamlUser.substring(0, yamlUser.indexOf("@"));
        String hostname = yamlUser.substring(yamlUser.indexOf("@") + 1);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(username), "user configuration `%s` is invalid, the legal format is `username@hostname`", yamlUser);
        return new Grantee(username, hostname);
    }
}
