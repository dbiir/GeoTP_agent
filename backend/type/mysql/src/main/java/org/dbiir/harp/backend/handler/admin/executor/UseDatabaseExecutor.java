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

package org.dbiir.harp.backend.handler.admin.executor;

import lombok.RequiredArgsConstructor;
import org.dbiir.harp.backend.context.ProxyContext;
import org.dbiir.harp.backend.session.ConnectionSession;
import org.dbiir.harp.kernel.authority.AuthorityRule;
import org.dbiir.harp.kernel.authority.checker.AuthorityChecker;
import org.dbiir.harp.utils.common.statement.dal.UseStatement;
import org.dbiir.harp.utils.common.util.SQLUtils;
import org.dbiir.harp.utils.exceptions.external.UnknownDatabaseException;

/**
 * Use database executor.
 */
@RequiredArgsConstructor
public final class UseDatabaseExecutor implements DatabaseAdminExecutor {
    
    private final UseStatement useStatement;
    
    @Override
    public void execute(final ConnectionSession connectionSession) {
        String databaseName = SQLUtils.getExactlyValue(useStatement.getSchema());
        AuthorityRule authorityRule = ProxyContext.getInstance().getContextManager().getMetaDataContexts().getMetaData().getGlobalRuleMetaData().getSingleRule(AuthorityRule.class);
        connectionSession.setCurrentDatabase(databaseName);
    }
}
