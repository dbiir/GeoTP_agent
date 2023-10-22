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

package org.dbiir.harp.kernel.authority.provider.database.builder;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.dbiir.harp.kernel.authority.model.AgentPrivileges;
import org.dbiir.harp.kernel.authority.provider.database.DatabasePermittedPrivilegesProvider;
import org.dbiir.harp.kernel.authority.provider.database.model.privilege.DatabasePermittedPrivileges;
import org.dbiir.harp.utils.common.metadata.user.AgentUser;
import org.dbiir.harp.utils.common.metadata.user.Grantee;

import java.util.*;
import java.util.Map.Entry;

/**
 * Database privilege builder.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DatabasePrivilegeBuilder {
    
    /**
     * Build privileges.
     *
     * @param users users
     * @param props props
     * @return privileges
     */
    public static Map<AgentUser, AgentPrivileges> build(final Collection<AgentUser> users, final Properties props) {
        String mappingProp = props.getProperty(DatabasePermittedPrivilegesProvider.PROP_USER_DATABASE_MAPPINGS, "");
        checkDatabases(mappingProp);
        return buildPrivileges(users, mappingProp);
    }
    
    /**
     * Check databases.
     *
     * @param mappingProp user database mapping props
     */
    private static void checkDatabases(final String mappingProp) {
        Preconditions.checkArgument(!"".equals(mappingProp), "user-database-mappings configuration `%s` can not be null", mappingProp);
        Arrays.stream(mappingProp.split(",")).forEach(each -> Preconditions.checkArgument(0 < each.indexOf("@") && 0 < each.indexOf("="),
                "user-database-mappings configuration `%s` is invalid, the configuration format should be like `username@hostname=database`", each));
    }
    
    private static Map<AgentUser, AgentPrivileges> buildPrivileges(final Collection<AgentUser> users, final String mappingProp) {
        Map<AgentUser, Collection<String>> userDatabaseMappings = convertDatabases(mappingProp);
        Map<AgentUser, AgentPrivileges> result = new HashMap<>(users.size(), 1);
        users.forEach(each -> result.put(each, new DatabasePermittedPrivileges(new HashSet<>(getUserDatabases(each, userDatabaseMappings)))));
        return result;
    }
    
    /**
     * Convert databases.
     *
     * @param mappingProp user database mapping props
     * @return user database mapping map
     */
    private static Map<AgentUser, Collection<String>> convertDatabases(final String mappingProp) {
        String[] mappings = mappingProp.split(",");
        Map<AgentUser, Collection<String>> result = new HashMap<>(mappings.length, 1);
        Arrays.asList(mappings).forEach(each -> {
            String[] userDatabasePair = each.trim().split("=");
            String yamlUser = userDatabasePair[0];
            String username = yamlUser.substring(0, yamlUser.indexOf("@"));
            String hostname = yamlUser.substring(yamlUser.indexOf("@") + 1);
            AgentUser shardingSphereUser = new AgentUser(username, "", hostname);
            Collection<String> databases = result.getOrDefault(shardingSphereUser, new HashSet<>());
            databases.add(userDatabasePair[1]);
            result.putIfAbsent(shardingSphereUser, databases);
        });
        return result;
    }
    
    private static Collection<String> getUserDatabases(final AgentUser shardingSphereUser, final Map<AgentUser, Collection<String>> userDatabaseMappings) {
        Set<String> result = new HashSet<>();
        for (Entry<AgentUser, Collection<String>> entry : userDatabaseMappings.entrySet()) {
            boolean isAnyOtherHost = checkAnyOtherHost(entry.getKey().getGrantee(), shardingSphereUser);
            if (isAnyOtherHost || shardingSphereUser == entry.getKey() || shardingSphereUser.equals(entry.getKey())) {
                result.addAll(entry.getValue());
            }
        }
        return result;
    }
    
    private static boolean checkAnyOtherHost(final Grantee grantee, final AgentUser shardingSphereUser) {
        return ("%".equals(grantee.getHostname())
                || grantee.getHostname().equals(shardingSphereUser.getGrantee().getHostname())) && grantee.getUsername().equals(shardingSphereUser.getGrantee().getUsername());
    }
}
