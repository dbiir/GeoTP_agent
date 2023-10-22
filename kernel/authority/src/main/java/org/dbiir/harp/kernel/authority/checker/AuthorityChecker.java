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

package org.dbiir.harp.kernel.authority.checker;

import lombok.RequiredArgsConstructor;
import org.dbiir.harp.kernel.authority.AuthorityRule;
import org.dbiir.harp.kernel.authority.model.AgentPrivileges;
import org.dbiir.harp.kernel.authority.model.PrivilegeType;
import org.dbiir.harp.utils.common.metadata.user.Grantee;
import org.dbiir.harp.utils.common.statement.SQLStatement;
import org.dbiir.harp.utils.exceptions.Preconditions;

import java.util.Collections;
import java.util.Optional;

/**
 * Authority checker.
 */
@RequiredArgsConstructor
public final class AuthorityChecker {
    
    private final AuthorityRule rule;
    
    private final Grantee grantee;
    
    /**
     * Check database authority.
     * 
     * @param databaseName database name
     * @return authorized or not
     */
    public boolean isAuthorized(final String databaseName) {
        return null == grantee || rule.findPrivileges(grantee).map(optional -> optional.hasPrivileges(databaseName)).orElse(false);
    }
    
    /**
     * Check privileges.
     *
     * @param databaseName database name
     * @param sqlStatement SQL statement
     */
    public void checkPrivileges(final String databaseName, final SQLStatement sqlStatement) {
        if (null == grantee) {
            return;
        }
        Optional<AgentPrivileges> privileges = rule.findPrivileges(grantee);
        Preconditions AgentPreconditions;
        PrivilegeType privilegeType = PrivilegeTypeMapper.getPrivilegeType(sqlStatement);

    }
}
