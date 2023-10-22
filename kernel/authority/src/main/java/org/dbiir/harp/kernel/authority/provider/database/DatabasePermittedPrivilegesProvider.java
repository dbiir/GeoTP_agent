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

package org.dbiir.harp.kernel.authority.provider.database;


import org.dbiir.harp.kernel.authority.model.AuthorityRegistry;
import org.dbiir.harp.kernel.authority.provider.database.builder.DatabasePrivilegeBuilder;
import org.dbiir.harp.kernel.authority.registry.UserPrivilegeMapAuthorityRegistry;
import org.dbiir.harp.kernel.authority.spi.AuthorityProvider;
import org.dbiir.harp.utils.common.metadata.database.AgentDatabase;
import org.dbiir.harp.utils.common.metadata.user.AgentUser;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Database permitted privileges provider.
 */
public final class DatabasePermittedPrivilegesProvider implements AuthorityProvider {
    
    public static final String PROP_USER_DATABASE_MAPPINGS = "user-database-mappings";
    
    private Properties props;
    
    @Override
    public void init(final Properties props) {
        this.props = props;
    }
    
    @Override
    public AuthorityRegistry buildAuthorityRegistry(final Map<String, AgentDatabase> databases, final Collection<AgentUser> users) {
        return new UserPrivilegeMapAuthorityRegistry(DatabasePrivilegeBuilder.build(users, props));
    }
    
    @Override
    public String getType() {
        return "DATABASE_PERMITTED";
    }
    
    @Override
    public Collection<String> getTypeAliases() {
        return Collections.singleton("SCHEMA_PRIVILEGES_PERMITTED");
    }
}
